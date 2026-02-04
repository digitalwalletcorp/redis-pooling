import { Redis } from 'ioredis';
import * as genericPool from 'generic-pool';

export interface RedisConfig {
  url: string;
  dbIndex?: number;
  connectTimeout?: number;
  max?: number;
  min?: number;
  testOnBorrow?: boolean;
  enableTls?: boolean;
}

export interface RedisClient extends Redis {
  getKeys(pattern: string): Promise<string[]>;
  deleteKeys(pattern: string): Promise<PromiseSettledResult<number>[]>;
  _originalDbIndex?: number; // 内部状態管理用変数
}

const logHeader = '[RedisPooling]';
const DEFAULT_CONNECT_TIMEOUT = 5000;
const DEFAULT_MAX_POOLING_SIZE = 10;
const DEFAULT_MIN_POOLING_SIZE = 0;
const REDIS_PING_TIMEOUT_MS = 3000; // 3秒

export class RedisPool {

  private readonly url: string;
  private readonly db: number;
  private readonly connectTimeout: number;
  private readonly max: number;
  private readonly min: number;
  private readonly testOnBorrow: boolean;
  private readonly tls?: { rejectUnauthorized: false };

  private readonly poolMap = new Map<number, genericPool.Pool<RedisClient>>();
  private initialized = false;
  private readonly debug: boolean;

  constructor(config: RedisConfig, options?: {
    debug?: boolean;
  }) {
    if (!config.url) {
      throw new Error(`${logHeader} Redis connection url is required.`);
    }

    this.url = config.url;
    this.db = config.dbIndex ?? 0;
    this.connectTimeout = config.connectTimeout ?? DEFAULT_CONNECT_TIMEOUT;
    this.max = config.max ?? DEFAULT_MAX_POOLING_SIZE;
    this.min = config.min ?? DEFAULT_MIN_POOLING_SIZE;
    this.testOnBorrow = config.testOnBorrow ?? true;
    this.tls = config.enableTls ? { rejectUnauthorized: false } : undefined;

    this.debug = options?.debug ?? false;
  }

  public async acquire(dbIndex?: number): Promise<RedisClient> {
    if (!this.initialized) {
      // 初回acquire呼び出し時のみ接続チェックを行う
      // ホスト不正やパスワード不正による接続不可等を検知する
      // ※ generic-poolのfactoryの方に入ってしまうとエラーを呼び出し元に伝播させることが難しいため、プールとは別の接続でチェックする
      await this.checkConnectivity(dbIndex ?? this.db);
      this.initialized = true;
    }

    const index = dbIndex ?? this.db;
    const pool = this.getPool(index);
    try {
      const client = await pool.acquire();

      this.debugLog(logHeader, `Redis client ${index} has been acquired.`);
      return client;
    } catch (error) {
      throw error;
    }
  }

  public async release(client?: RedisClient): Promise<void> {
    if (client) {
      const dbIndex = client._originalDbIndex ?? this.db;
      const pool = this.poolMap.get(dbIndex);
      if (!pool) {
        return;
      }
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
      if (needDestroy) {
        // Redisクライアントの破棄
        await pool.destroy(client);
        this.debugLog(logHeader, `Redis client ${dbIndex} destroyed due to invalid status.`);
      } else {
        // Redisクライアントの返却
        await pool.release(client);
        this.debugLog(logHeader, `Redis client ${dbIndex} released.`);
      }
    }
  }

  public async destroy(timeoutMs = 5000): Promise<void> {
    for (const [dbIndex, pool] of this.poolMap.entries()) {
      this.debugLog(logHeader, `Destroying Redis pool for DB index ${dbIndex}...`);
      await Promise.race([
        (async () => {
          await pool.drain();
          await pool.clear();
        })(),
        new Promise<void>((_, reject) => {
          setTimeout(() => reject(new Error(`${logHeader} Timeout while draining Redis pool for DB index ${dbIndex}`)), timeoutMs);
        })
      ]);
      this.debugLog(logHeader, `Redis pool for DB index ${dbIndex} destroyed.`);
      this.poolMap.delete(dbIndex);
    }
  }

  /**
   * 初めてacquireが呼ばれた時に、指定された接続情報で接続できるかチェックする
   *
   * @param {number} dbIndex
   */
  private async checkConnectivity(dbIndex: number): Promise<void> {
    const client = new Redis(this.url, {
      db: dbIndex,
      retryStrategy: () => null,     // 再接続無効
      reconnectOnError: () => false, // 再接続無効
      connectTimeout: this.connectTimeout,
      tls: this.tls,
    });

    return new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        client.quit().catch(() => client.disconnect());
      };

      client.once('ready', () => {
        cleanup();
        resolve();
      });

      client.once('error', (error) => {
        cleanup();
        reject(error);
      });
    });
  }

  private async ping(client: Redis): Promise<boolean> {
    try {
      this.debugLog(logHeader, 'start validate');
      const timeout = new Promise<void>((_, reject) => {
        setTimeout(() => reject(new Error(`Redis PING timeout after ${REDIS_PING_TIMEOUT_MS}ms`)), REDIS_PING_TIMEOUT_MS);
      });
      await Promise.race([
        client.ping(),
        timeout
      ]);
      this.debugLog(logHeader, 'ping succeeded');
      return client.status === 'ready';
    } catch (error: any) {
      this.debugLog(logHeader, 'ping failed');
      return false;
    }
  }

  public debugLog(...args: any[]): void {
    if (this.debug) {
      console.debug(...args);
    }
  }

  private getPool(dbIndex: number): genericPool.Pool<RedisClient> {
    let pool = this.poolMap.get(dbIndex);
    if (!pool) {
      pool = this.createSingleDbPool(dbIndex);
      this.poolMap.set(dbIndex, pool);
    }
    return pool;
  }

  private createSingleDbPool(dbIndex: number): genericPool.Pool<RedisClient> {
    const factory: genericPool.Factory<RedisClient> = {
      create: async (): Promise<RedisClient> => {
        const client = new Redis(this.url, {
          db: dbIndex,
          connectTimeout: this.connectTimeout,
          keepAlive: 1,
          enableOfflineQueue: true,
          tls: this.tls,
          retryStrategy: (times) => {
            const delay = Math.min(times * 50, 1000);
            if (process.env.NODE_ENV !== 'test') {
              this.debugLog(logHeader, `retry strategy called ${times} times. delaying ${delay}ms`);
            }
            return delay;
          },
          reconnectOnError: (error) => {
            process.emitWarning(`${logHeader} detected error (on reconnectOnError). ${error.message}`);
            return true;
          }
        }) as RedisClient;

        client._originalDbIndex = dbIndex;
        client.on('error', error => {
          process.emitWarning(`${logHeader} detected error (on error). ${error.message}`);
        });

        // カスタムメソッド内の処理でthis.debugLogなどが参照できなくなるため、poolのインスタンスを変数キャプチャする
        const poolInstance = this;

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
              reject(err);
            });
          });
        };
        /**
         * 指定されたパターンに一致するキーを Redis から全て削除する (UNLINKを使用)
         * 返却値の配列サイズはscanStreamが'data'を受信した回数で、この受信したデータで削除された件数がvalueに設定されている。
         *
         * [
         *   { status: 'fulfilled', value: 100 }, // 1バッチ目で100件削除
         *   { status: 'fulfilled', value: 80 } // 2バッチ目で80件削除
         * ]
         *
         *  成功した件数は以下のようにして取得可能
         *
         * const delResults = await redisClient.deleteKeys('pattern');
         * const delCount = delResults.filter(a => a.status === 'fulfilled')
         *   .reduce((acc, cur) => acc + (cur as PromiseFulfilledResult<number>).value, 0);
         *
         * @param {string} pattern
         * @returns {Promise<PromiseSettledResult<number>>}
         */
        client.deleteKeys = async function(pattern: string): Promise<PromiseSettledResult<number>[]> {
          const stream = this.scanStream({
            match: pattern,
            count: 1000 // 1度にスキャンする件数
          });

          return new Promise((resolve, reject) => {
            const tasks: Promise<number>[] = [];
            stream.on('data', async (keys: string[]) => {
              if (keys.length) {
                tasks.push((async () => {
                  // UNLINK を使用し、現在のインスタンス (this) で実行
                  const unlinkResult = await this.unlink(...keys);
                  return unlinkResult;
                })());
              }
            });
            stream.on('end', async () => {
              const results = await Promise.allSettled(tasks);
              resolve(results);
            });
            stream.on('error', (err: Error) => {
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
          this.debugLog(logHeader, 'client quit');
        } catch (error) {
          client.disconnect();
          this.debugLog(logHeader, 'client disconnected');
        }
      },
      validate: async (client: Redis) => {
        return await this.ping(client);
      }
    };

    return genericPool.createPool(factory, {
      max: this.max,
      min: this.min,
      testOnBorrow: this.testOnBorrow
    });
  }
}
