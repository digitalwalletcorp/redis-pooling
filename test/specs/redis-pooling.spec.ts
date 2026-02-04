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
      const keys = await client.getKeys('user:*');
      expect(keys).toEqual(expect.arrayContaining(['user:1', 'user:2']));

      await pool.release(client);
    });

    it('deleteKeys removes keys and returns correct count', async () => {
      const client = await pool.acquire();
      await client.set('delete:1', 'x');
      await client.set('delete:2', 'y');
      await client.set('keep:1', 'z');

      const deletedCount = await client.deleteKeys('delete:*');
      expect(deletedCount).toBe(2);

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

    it('deleteKeys handles UNLINK error', async () => {
      const client = await pool.acquire();

      // キーを1件返すスキャン
      client.scanStream = () => {
        const EventEmitter = require('events');
        const stream = new EventEmitter();
        process.nextTick(() => stream.emit('data', ['key1']));
        process.nextTick(() => stream.emit('end'));
        return stream;
      };

      // unlink をエラーにする
      (client as any).unlink = async (...keys: string[]) => {
        throw new Error('unlink failed');
      };

      // UNLINKエラーは警告で swallow されるので結果は0
      const deletedCount = await client.deleteKeys('*');
      expect(deletedCount).toBe(0);

      await pool.release(client);
    });
  });
});
