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
  acquire(dbIndex?: number): Promise<ManagedRedisClient>;
  release(client?: Redis): Promise<void>;
  destroy(timeoutMs?: number): Promise<void>;
}

export interface ManagedRedisClient extends Redis {
  getKeys(pattern: string): Promise<string[]>;
  deleteKeys(pattern: string): Promise<number>;
  _originalDbIndex?: number; // 内部状態管理用変数
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

  // dbIndexごとのプール管理
  const poolMap = new Map<number, genericPool.Pool<ManagedRedisClient>>();

  const createSingleDbPool = (dbIndex: number): genericPool.Pool<ManagedRedisClient> => {
    const factory: genericPool.Factory<ManagedRedisClient> = {
      create: async (): Promise<ManagedRedisClient> => {
        let client;
        try {
          client = new Redis(url, {
            db: dbIndex,
            connectTimeout: connectTimeout,
            keepAlive: 1,
            enableOfflineQueue: true,
            tls: config.enableTls ? { rejectUnauthorized: false } : undefined,
            retryStrategy: (times) => {
              const delay = Math.min(times * 50, 1000);
              if (process.env.NODE_ENV !== 'test') {
                console.debug(logHeader, `retry strategy called ${times} times. delaying ${delay}ms`);
              }
              return delay;
            },
            reconnectOnError: (error) => {
              process.emitWarning(`${logHeader} detected error (on reconnect). ${error.message}`);
              return true;
            }
          }) as ManagedRedisClient;
        } catch (error) {
          console.error(error);
          throw error;
        }

        client._originalDbIndex = dbIndex;
        client.on('error', error => {
          process.emitWarning(`${logHeader} detected error (on error). ${error.message}`);
        });

        // カスタムメソッド START

        /**
         * 指定されたパターンに一致するキーを Redis から全て取得する
         *
         * @param {string} pattern
         * @returns {Promise<string[]>}
         */
        client.getKeys = async function(pattern: string): Promise<string[]> {
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
        client.deleteKeys = async function(pattern: string): Promise<number> {
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
                    console.debug(logHeader, `Deleted ${unlinkResult} keys in a batch for pattern '${pattern}'. Total: ${deletedCount}`);
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

        // 接続が不安定な場合は以下のロジックが生きるかもしれないので残しておく
        // 動作確認した限りでは以下のロジックはなくても問題なく動作する
        // await new Promise<void>((resolve, reject) => {
        //   const onReady = () => {
        //     cleanup();
        //     resolve();
        //   };
        //   const onError = (error: Error) => {
        //     cleanup();
        //     reject(error);
        //   };
        //   const cleanup = () => {
        //     client.off('ready', onReady);
        //     client.off('error', onError);
        //   };
        //   client.once('ready', onReady);
        //   client.once('error', onError);
        // });

        return client;
      },
      destroy: async (client: Redis) => {
        try {
          await client.quit();
          console.debug(logHeader, 'client quit');
        } catch (error) {
          client.disconnect();
          console.debug(logHeader, 'client disconnected');
        }
      },
      validate: async (client: Redis) => {
        try {
          console.debug(logHeader, 'start validate');
          const timeout = new Promise<void>((_, reject) => {
            setTimeout(() => reject(new Error(`Redis PING timeout after ${REDIS_PING_TIMEOUT_MS}ms`)), REDIS_PING_TIMEOUT_MS);
          });
          await Promise.race([
            client.ping(),
            timeout
          ]);
          console.debug(logHeader, 'ping succeeded');
          return client.status === 'ready';
        } catch (error) {
          console.debug(logHeader, 'ping failed');
          return false;
        }
      }
    };

    return genericPool.createPool(factory, {
      max,
      min,
      testOnBorrow
    });
  };

  const getPool = (dbIndex: number) => {
    let pool = poolMap.get(dbIndex);
    if (!pool) {
      pool = createSingleDbPool(dbIndex);
      poolMap.set(dbIndex, pool);
    }
    return pool;
  };

  return {
    acquire: async (dbIndex?: number): Promise<ManagedRedisClient> => {
      const index = dbIndex ?? 0;
      const pool = getPool(index);
      const client = await pool.acquire();
      console.debug(logHeader, `Redis client ${index} has been acquired.`);
      return client;
    },
    release: async (client?: ManagedRedisClient): Promise<void> => {
      if (client) {
        const dbIndex = client._originalDbIndex ?? db;
        let needDestroy = false;
        switch (true) {
          case client.status === 'end':
          case client.status === 'close':
            // Redisクライアントの状態が再利用できない場合はプールから破棄
            needDestroy = true;
            break;
          case client.status === 'ready':
            try {
              // 元のDBインデックスに戻す
              await client.select(dbIndex);
            } catch (error) {
              // selectに失敗する→Redisクライアントが不正な状態にあると判断できるのでプールから破棄
              needDestroy = true;
            }
            break;
          default:
        }
        const pool = poolMap.get(dbIndex);
        if (pool) {
          if (needDestroy) {
            // Redisクライアントの破棄
            await pool.destroy(client);
            console.debug(logHeader, `Redis client ${dbIndex} destroyed due to invalid status.`);
          } else {
            // Redisクライアントの返却
            await pool.release(client);
            console.debug(logHeader, `Redis client ${dbIndex} released.`);
          }
        }
      }
    },
    destroy: async (timeoutMs = 5000): Promise<void> => {
      for (const [dbIndex, pool] of poolMap.entries()) {
        console.debug(logHeader, `Destroying Redis pool for DB index ${dbIndex}...`);
        await Promise.race([
          (async () => {
            await pool.drain();
            await pool.clear();
          })(),
          new Promise<void>((_, reject) => {
            setTimeout(() => reject(new Error(`${logHeader} Timeout while draining Redis pool for DB index ${dbIndex}`)), timeoutMs);
          })
        ]);
        console.debug(logHeader, `Redis pool for DB index ${dbIndex} destroyed.`);
        poolMap.delete(dbIndex);
      }
    }
  }
};
