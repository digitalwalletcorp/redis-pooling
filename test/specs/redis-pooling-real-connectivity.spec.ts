import { createRedisPool, ManagedRedisClient } from '@/redis-pooling';

// 実接続テストなのでタイムアウトを延伸
jest.setTimeout(60 * 1000);

const prefix = 'redis-pooling-test';

/**
 * Redisへの実接続テスト
 * GitHub-CI上ではRedisに接続できないのでコミット時は`skip`を付与
 */
describe.skip('Redis Pooling Real Connectivity Tests', () => {

  describe('Real connectivity', () => {
    let pool: ReturnType<typeof createRedisPool>;

    beforeEach(() => {
      // 環境変数に実際に接続可能なURLを定義するか、テストコードを書き換えてテストを実施する
      // URLの形式は`redis://:{password}@{host}:{port}`
      const url = process.env.REDIS_URL as string;
      console.log(url);
      pool = createRedisPool({
        url: url,
        db: 0,
        max: 1,
        min: 0,
        enableTls: true,
        testOnBorrow: true
      });
    });

    afterEach(async () => {
      await pool.destroy();
    });

    it('一般的な操作', async () => {
      const client = await pool.acquire();
      try {
        // 最初に`tedis-pooling-test:`で始まるキーを全てクリア
        await clearTestKeys(client);

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
        const delCount = await client.deleteKeys(`${prefix}:*`);
        expect(delCount).toBe(2);

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
        await clearTestKeys(client1);

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
        await clearTestKeys(client1);

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
  });
});

/**
 * 現在選択されているDBインデックスからprefixで始まるデータを削除する
 *
 * @param {ManagedRedisClient} client
 */
const clearTestKeys = async (client: ManagedRedisClient): Promise<void> => {
  const keys = await client.getKeys(`${prefix}:*`);
  for (const key of keys) {
    await client.del(key);
  }
};
