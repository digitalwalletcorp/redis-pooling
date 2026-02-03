import { Redis } from 'ioredis';
import * as genericPool from 'generic-pool';

export interface RedisConfig {
  url: string;
  db?: number;
  connectTimeout?: number;
  max?: number;
  min?: number;
  testOnBorrow?: boolean;
  enableTls?: boolean;
}

export interface ManagedRedisPool {
  acquire(dbIndex?: number): Promise<Redis>;
  release(client?: Redis): Promise<void>;
  destroy(): Promise<void>;
}

export interface ManagedRedisClient extends Redis {
  getKeys(pattern: string): Promise<string[]>;
  deleteKeys(pattern: string): Promise<number>;
}

const logHeader = '[RedisPooling]';
const DEFAULT_CONNECT_TIMEOUT = 5000;
const DEFAULT_MAX_POOLING_SIZE = 10;
const DEFAULT_MIN_POOLING_SIZE = 0;
const REDIS_PING_TIMEOUT_MS = 3000; // 3秒

/**
 * Redisコネクションプーリングを生成する
 *
 * @param {RedisConfig} config
 * @returns {ManagedRedisPool}
 */
export const createRedisPool = (config: RedisConfig): ManagedRedisPool => {

  const url = config.url;
  const db = config.db ?? 0;
  const connectTimeout = config.connectTimeout ?? DEFAULT_CONNECT_TIMEOUT;
  const max = config.max ?? DEFAULT_MAX_POOLING_SIZE;
  const min = config.min ?? DEFAULT_MIN_POOLING_SIZE;
  const testOnBorrow = config.testOnBorrow ?? true;

  if (!url) {
    throw new Error(`${logHeader} Redis connection url is required.`);
  }

  const factory: genericPool.Factory<ManagedRedisClient> = {
    create: async () => {
      let redisClient;
      try {
        redisClient = new Redis(url, {
          db: db,
          connectTimeout: connectTimeout,
          keepAlive: 1,
          enableOfflineQueue: true,
          tls: config.enableTls
            ? { rejectUnauthorized: false }
            : undefined,
          retryStrategy: (times) => {
            const delay = Math.min(times * 50, 1000);
            console.log(logHeader, `index '${db}' retry strategy has been called ${times} times. delay ${delay}ms`);
            return delay;
          },
          reconnectOnError: (error) => {
            process.emitWarning(`${logHeader} index '${db}' detected error (on reconnect). ${error.message}`);
            return true;
          }
        }) as ManagedRedisClient;
      } catch (error) {
        console.error(error);
        throw error;
      }

      redisClient.on('error', error => {
        process.emitWarning(`${logHeader} detected error (on error). ${error.message}`);
      });

      // カスタムメソッド START

      /**
       * 指定されたパターンに一致するキーを Redis から全て取得する
       *
       * @param {string} pattern
       * @returns {Promise<string[]>}
       */
      redisClient.getKeys = async function(pattern: string): Promise<string[]> {
        const allKeys: string[] = [];
        const stream = this.scanStream({
          match: pattern,
          count: 1000 // ioredisが1度にスキャンする件数の目安。1000件を超えるデータがあっても全件返却される
        });

        return new Promise((resolve, reject) => {
          stream.on('data', (keys: string[]) => {
            if (keys.length) {
              allKeys.push(...keys);
            }
          });
          stream.on('end', () => resolve(allKeys));
          stream.on('error', (err: Error) => {
            console.error(logHeader, `Error during getKeys scan for pattern '${pattern}'.`, err);
            reject(err);
          });
        });
      };
      /**
       * 指定されたパターンに一致するキーを Redis から全て削除する (UNLINKを使用)
       *
       * @param {string} pattern
       * @returns {Promise<number>}
       */
      redisClient.deleteKeys = async function(pattern: string): Promise<number> {
        let deletedCount = 0;
        const stream = this.scanStream({
          match: pattern,
          count: 1000 // 1度にスキャンする件数
        });

        return new Promise((resolve, reject) => {
          const tasks: Promise<void>[] = [];
          stream.on('data', async (keys: string[]) => {
            if (keys.length) {
              tasks.push((async () => {
                try {
                  // UNLINK を使用し、現在のインスタンス (this) で実行
                  const unlinkResult = await this.unlink(...keys);
                  deletedCount += unlinkResult;
                  console.log(logHeader, `Deleted ${unlinkResult} keys in a batch for pattern '${pattern}'. Total: ${deletedCount}`);
                } catch (error: any) {
                  process.emitWarning(`${logHeader} Error during UNLINK for pattern '${pattern}'. ${error.message}`);
                }
              })());
            }
          });
          stream.on('end', async () => {
            await Promise.all(tasks);
            resolve(deletedCount);
          });
          stream.on('error', (err: Error) => {
            console.error(logHeader, `Error during deleteKeys scan for pattern '${pattern}'.`, err);
            reject(err);
          });
        });
      };
      // カスタムメソッド END

      return redisClient;
    },
    destroy: async (client: Redis) => {
      try {
        await client.quit();
        console.log(logHeader, 'client quited');
      } catch (error) {
        client.disconnect();
        console.log(logHeader, 'client disconnected');
      }
    },
    validate: async (client: Redis) => {
      try {
        console.log(logHeader, 'ping has been called');
        const timeout = new Promise<void>((_, reject) => {
          setTimeout(() => reject(new Error(`Redis PING timeout after ${REDIS_PING_TIMEOUT_MS}ms`)), REDIS_PING_TIMEOUT_MS);
        });
        await Promise.race([
          client.ping(),
          timeout
        ]);
        console.log(logHeader, 'ping returned connected');
        return true;
      } catch (error) {
        console.log(logHeader, 'ping returned closed');
        return false;
      }
    }
  };

  const pool = genericPool.createPool(factory, {
    max,
    min,
    testOnBorrow
  });

  return {
    acquire: async (dbIndex?: number): Promise<ManagedRedisClient> => {
      const redisClient = await pool.acquire();
      try {
        if (dbIndex != null) {
          await redisClient.select(dbIndex);
        }
        return redisClient as ManagedRedisClient;
      } catch (error) {
        pool.release(redisClient);
        throw error;
      }
    },
    release: async (redisClient?: ManagedRedisClient): Promise<void> => {
      if (redisClient) {
        await pool.release(redisClient);
      }
    },
    destroy: async (): Promise<void> => {
      console.log(logHeader, 'Destroying default Redis pool.');
      await pool.drain();
      await pool.clear();
      console.log(logHeader, 'Default Redis pool destroyed.');
    }
  }
};
