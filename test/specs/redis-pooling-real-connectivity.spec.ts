import { RedisPool } from '@/redis-pooling';

// 実接続テストなのでタイムアウトを延伸
jest.setTimeout(60 * 1000);

const prefix = 'redis-pooling-test';

/**
 * Redisへの実接続テスト
 * GitHub-CI上ではRedisに接続できないのでコミット時は`skip`を付与
 */
describe.skip('Redis Pooling Real Connectivity Tests', () => {

  describe('Real connectivity', () => {
    let pool: RedisPool;

    beforeEach(() => {
      // 環境変数に実際に接続可能なURLを定義するか、テストコードを書き換えてテストを実施する
      // URLの形式は`redis://:{password}@{host}:{port}`
      const url = process.env.REDIS_URL as string;
      pool = new RedisPool({
        url: url,
        dbIndex: 0,
        max: 1,
        min: 0,
        enableTls: true
      });
    });

    afterEach(async () => {
      await pool.destroy();
    });

    it('一般的な操作', async () => {
      const client = await pool.acquire();
      try {
        // 最初に`tedis-pooling-test:`で始まるキーを全てクリア
        await client.deleteKeys(`${prefix}:*`);

        // 1つめのキーを登録
        await client.set(`${prefix}:key1`, '123');
        const v1 = await client.get(`${prefix}:key1`);
        expect(v1).toBe('123');

        // 2つめのキーを登録
        await client.set(`${prefix}:key2`, '456');
        const v2 = await client.get(`${prefix}:key2`);
        expect(v2).toBe('456');

        // キー情報を取得
        const accKeys1 = await client.getKeys(`${prefix}:*`);
        expect(accKeys1.length).toBe(2);

        // キーを削除
        const delResults = await client.deleteKeys(`${prefix}:*`);
        expect(delResults).toEqual([
          {
            status: 'fulfilled',
            value: 2
          }
        ]);
        const succeeded = delResults.filter(a => a.status === 'fulfilled');
        console.log(succeeded.map(a => a.value));
        expect(succeeded.length).toBe(1);
        const delCount = succeeded.reduce((acc, cur) => acc + (cur as PromiseFulfilledResult<number>).value, 0);
        expect(delCount).toBe(2);
        const failed = delResults.filter(a => a.status === 'rejected');
        expect(failed.length).toBe(0);

        const accKeys2 = await client.getKeys(`${prefix}:*`);
        expect(accKeys2.length).toBe(0);
      } finally {
        await pool.release(client);
      }
    });

    it('途中でDBインデックスを選択', async () => {
      // DBインデックス1のRedisクライアントを取得
      const client1 = await pool.acquire(1);
      try {
        // 最初に`tedis-pooling-test:`で始まるキーを全てクリア
        await client1.deleteKeys(`${prefix}:*`);

        // キーを登録
        await client1.set(`${prefix}:key1`, '123');
        const v1 = await client1.get(`${prefix}:key1`);
        expect(v1).toBe('123');

        // キー情報を取得
        const accKeys1 = await client1.getKeys(`${prefix}:*`);
        expect(accKeys1.length).toBe(1);

        // DBインデックスを変更してリリース
        await client1.select(2);
      } finally {
        await pool.release(client1);
      }

      const client2 = await pool.acquire(1);
      try {
        // さきほど登録したデータが参照できること→選択されているDBインデックスが期待通りであることの確認
        const v1 = await client2.get(`${prefix}:key1`);
        expect(v1).toBe('123');
      } finally {
        await pool.release(client2);
      }
    });

    it('途中でプールを破棄', async () => {
      // DBインデックス1のRedisクライアントを取得
      const client1 = await pool.acquire(1);
      try {
        // 最初に`tedis-pooling-test:`で始まるキーを全てクリア
        await client1.deleteKeys(`${prefix}:*`);

        // キーを登録
        await client1.set(`${prefix}:key1`, '123');
      } finally {
        await pool.release(client1);
      }

      // プールを破棄
      await pool.destroy();

      // プールから再度クライアント取得
      // 一度破棄したプールを再利用するのは好ましくないが、
      // 単にマップが空になって新しくプールが生成されるだけなので技術的には使える状態にある
      const client2 = await pool.acquire(1);
      try {
        // さきほど登録したデータが参照できること→プールが正常に動作することの確認
        const v1 = await client2.get(`${prefix}:key1`);
        expect(v1).toBe('123');
      } finally {
        await pool.release(client2);
      }
    });

    it('destroyの間違った使い方によるデッドロックエラー', async () => {
      const client = await pool.acquire(1);
      try {
        // クライアントリリース前にプールを破棄
        await expect(pool.destroy(100)).rejects.toThrow('Timeout while draining Redis pool for DB index 1');
      } finally {
        await pool.release(client);
      }
    });

    it('pool作成時のDBインデックスがデフォルトで設定される', async () => {
      // テスト用乱数生成
      const randomValue = Math.random().toString(36).slice(2, 12);
      console.log(randomValue);

      // 初期インデックス2で別のプールを作成
      const url = process.env.REDIS_URL as string;
      const redisPool = new RedisPool({
        url: url,
        dbIndex: 2,
        max: 1,
        min: 0,
        enableTls: true
      });
      try {
        // DBインデックス指定なしでacquire呼び出し
        const client1 = await redisPool.acquire();
        try {
          // テストデータクリア
        await client1.deleteKeys(`${prefix}:*`);

          // キーを登録
          await client1.set(`${prefix}:key1`, randomValue);
          const v1 = await client1.get(`${prefix}:key1`);
          expect(v1).toBe(randomValue);
        } finally {
          await redisPool.release(client1);
        }
      } finally {
        await redisPool.destroy();
      }

      // 初期で作成しているプールからDBインデックス2でクライアント取得
      const client2 = await pool.acquire(2);
      try {
        // 最初の処理がDBインデックス2にデータを登録していることを確認
        const v2 = await client2.get(`${prefix}:key1`);
        expect(v2).toBe(randomValue);
      } finally {
        await pool.release(client2);
      }
    });

    it('パスワード不正(接続不可)', async () => {
      const url = process.env.REDIS_URL as string;
      const urlObject = new URL(url);
      urlObject.password = 'invalid-password';
      const redisPool = new RedisPool({
        url: urlObject.toString(),
        max: 1,
        min: 0,
        enableTls: true
      });
      try {
        await expect(redisPool.acquire()).rejects.toThrow('WRONGPASS');
      } finally {
        await redisPool.destroy();
      }
    });

    it('ホスト不正(接続不可)', async () => {
      const url = process.env.REDIS_URL as string;
      const urlObject = new URL(url);
      urlObject.hostname = 'invalid-host';
      const redisPool = new RedisPool({
        url: urlObject.toString(),
        max: 1,
        min: 0,
        enableTls: true
      });
      try {
        await expect(redisPool.acquire()).rejects.toThrow('ENOTFOUND');
      } finally {
        await redisPool.destroy();
      }
    });

    it('TLS不正(接続不可)', async () => {
      // SSLが要求されているRedisに非SSLで接続した時のエラー
      // SSLハンドシェイクの間待ち時間が発生するため、即座にエラーにはならずデフォルトでは10秒ほどかかる
      const url = process.env.REDIS_URL as string;
      const redisPool = new RedisPool({
        url: url,
        max: 1,
        min: 0,
        enableTls: false
      });
      try {
        await expect(redisPool.acquire()).rejects.toThrow(/ECONNRESET|handshake|SSL/);
      } finally {
        await redisPool.destroy();
      }
    });
  });
});
