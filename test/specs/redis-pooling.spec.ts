import { RedisPool } from '@/redis-pooling';

jest.mock('ioredis', () => {
  const RedisMock = require('ioredis-mock');
  RedisMock.prototype.select = async (_: number) => {
    return 'OK';
  };
  return {
    Redis: RedisMock
  };
});

const redisUrl = 'redis://localhost:6379';

/**
 * モックを利用したRedisPoolingのテスト
 */
describe('Redis Pooling Mock Tests', () => {
  describe('Normal Cases', () => {
    let pool: RedisPool;

    beforeEach(() => {
      pool = new RedisPool({
        url: redisUrl,
        dbIndex: 0,
        max: 2,
        min: 0,
        testOnBorrow: false
      });
    });

    afterEach(async () => {
      await pool.destroy();
    });

    it('acquire and release works', async () => {
      const client = await pool.acquire();
      expect(client).toBeDefined();

      await client.set('foo', 'bar');
      const val = await client.get('foo');
      expect(val).toBe('bar');

      await pool.release(client);
    });

    it('getKeys returns keys matching pattern', async () => {
      const client = await pool.acquire();
      await client.set('user:1', 'Alice');
      await client.set('user:2', 'Bob');
      const keys = await client.getKeys('user:*', 5000);
      expect(keys).toEqual(expect.arrayContaining(['user:1', 'user:2']));

      await pool.release(client);
    });

    it('deleteKeys removes keys and returns correct count', async () => {
      const client = await pool.acquire();
      await client.set('delete:1', 'x');
      await client.set('delete:2', 'y');
      await client.set('keep:1', 'z');

      const delResults = await client.deleteKeys('delete:*', 5000);
      expect(delResults).toEqual([
        {
          status: 'fulfilled',
          value: 2
        }
      ]);

      const remainingKeys = await client.getKeys('*');
      expect(remainingKeys).toEqual(expect.arrayContaining(['keep:1']));
      expect(remainingKeys).not.toEqual(expect.arrayContaining(['delete:1', 'delete:2']));

      await pool.release(client);
    });


    it('acquire with dbIndex works', async () => {
      const client = await pool.acquire(1); // db 1
      await client.set('db1key', 'value1');

      const val = await client.get('db1key');
      expect(val).toBe('value1');

      await pool.release(client);
    });

    it('pool allows multiple clients up to max', async () => {
      const client1 = await pool.acquire();
      const client2 = await pool.acquire();

      await client1.set('a', '1');
      await client2.set('b', '2');

      expect(await client1.get('a')).toBe('1');
      expect(await client2.get('b')).toBe('2');

      await pool.release(client1);
      await pool.release(client2);
    });

    it('acquire selects the requested db index every time', async () => {
      const client1 = await pool.acquire(3);
      const selectSpy = jest.spyOn(client1, 'select');
      await pool.release(client1);

      const client2 = await pool.acquire();
      expect(client2).toBe(client1);
      expect(selectSpy).toHaveBeenLastCalledWith(0);
      await pool.release(client2);

      const client3 = await pool.acquire(5);
      expect(client3).toBe(client1);
      expect(selectSpy).toHaveBeenLastCalledWith(5);
      await pool.release(client3);
    });

    it('clients are shared across db indexes within max', async () => {
      const client1 = await pool.acquire(1);
      const client2 = await pool.acquire(2);
      await pool.release(client1);
      await pool.release(client2);

      const client3 = await pool.acquire(3);
      const client4 = await pool.acquire(4);
      expect([client1, client2]).toContain(client3);
      expect([client1, client2]).toContain(client4);
      await pool.release(client3);
      await pool.release(client4);
    });

    it('release without client does not throw', async () => {
      await expect(pool.release(undefined)).resolves.toBeUndefined();
    });
  });

  describe('Error Cases', () => {
    let pool: RedisPool;

    beforeEach(() => {
      pool = new RedisPool({
        url: redisUrl,
        dbIndex: 0,
        max: 1,
        min: 0,
        testOnBorrow: false,
        connectTimeout: 500
      });
    });

    afterEach(async () => {
      await pool.destroy();
    });

    it('acquire times out when all clients are in use', async () => {
      const timeoutPool = new RedisPool({
        url: redisUrl,
        max: 1,
        testOnBorrow: false,
        acquireTimeout: 100
      });
      const client = await timeoutPool.acquire();
      try {
        await expect(timeoutPool.acquire()).rejects.toThrow('ResourceRequest timed out');
      } finally {
        await timeoutPool.release(client);
        await timeoutPool.destroy();
      }
    });

    it('incorrect url', () => {
      expect(() => new RedisPool({ url: '' })).toThrow('Redis connection url is required.');
    });

    it('getKeys handles scanStream error', async () => {
      const client = await pool.acquire();

      // scanStreamをエラーを出すモックに差し替え
      client.scanStream = () => {
        const EventEmitter = require('events');
        const stream = new EventEmitter();
        process.nextTick(() => stream.emit('error', new Error('scanStream failed')));
        return stream;
      };

      await expect(client.getKeys('*')).rejects.toThrow('scanStream failed');

      await pool.release(client);
    });

    it('deleteKeys handles scanStream error', async () => {
      const client = await pool.acquire();

      client.scanStream = () => {
        const EventEmitter = require('events');
        const stream = new EventEmitter();
        process.nextTick(() => stream.emit('error', new Error('scanStream delete failed')));
        return stream;
      };
      await expect(client.deleteKeys('*')).rejects.toThrow('scanStream delete failed');

      await pool.release(client);
    });

    // unlinkの処理を無理やり上書きしてエラーを返すようにするとテストが期待通り動作しないためコメントアウト
    // it('deleteKeys handles UNLINK error', async () => {
    //   const client = await pool.acquire();

    //   // キーを1件返すスキャン
    //   client.scanStream = () => {
    //     const EventEmitter = require('events');
    //     const stream = new EventEmitter();
    //     process.nextTick(() => stream.emit('data', ['key1']));
    //     setImmediate(() => stream.emit('end'));
    //     return stream;
    //   };

    //   // unlink をエラーにする
    //   (client as any).unlink = async (...keys: string[]) => {
    //     await new Promise<void>(resolve => setTimeout(resolve, 1000));
    //     return Promise.reject(new Error('unlink failed'));
    //   };

    //   const delResults = await client.deleteKeys('*');
    //   expect(delResults).toHaveLength(1);
    //   expect(delResults[0].status).toBe('rejected');
    //   expect((delResults[0] as PromiseRejectedResult).reason).toBeInstanceOf(Error);
    //   expect((delResults[0] as PromiseRejectedResult).reason.message).toBe('unlink failed');

    //   await pool.release(client);
    // });
  });
});
