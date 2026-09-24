import { Redis } from 'ioredis';
import * as genericPool from 'generic-pool';

export interface RedisConfig {
  url: string;
  dbIndex?: number;
  connectTimeout?: number;
  acquireTimeout?: number;
  max?: number;
  min?: number;
  testOnBorrow?: boolean;
  enableTls?: boolean;
}

export interface RedisPoolOptions {
  debug?: boolean;
}

export interface RedisClient extends Redis {
  getKeys(pattern: string, count?: number): Promise<string[]>;
  deleteKeys(pattern: string, count?: number): Promise<PromiseSettledResult<number>[]>;
}

const logHeader = '[RedisPooling]';
const DEFAULT_CONNECT_TIMEOUT = 5000;
const DEFAULT_ACQUIRE_TIMEOUT = 10000;
const DEFAULT_MAX_POOLING_SIZE = 10;
const DEFAULT_MIN_POOLING_SIZE = 0;
const REDIS_PING_TIMEOUT_MS = 3000; // 3秒

export class RedisPool {

  private readonly url: string;
  private readonly db: number;
  private readonly connectTimeout: number;
  private readonly acquireTimeout: number;
  private readonly max: number;
  private readonly min: number;
  private readonly testOnBorrow: boolean;
  private readonly tls?: { rejectUnauthorized: false };

  private pool?: genericPool.Pool<RedisClient>;
  private initialized = false;
  private readonly debug: boolean;

  constructor(config: RedisConfig, options?: RedisPoolOptions) {
    if (!config.url) {
      throw new Error(`${logHeader} Redis connection url is required.`);
    }

    this.url = config.url;
    this.db = config.dbIndex ?? 0;
    this.connectTimeout = config.connectTimeout ?? DEFAULT_CONNECT_TIMEOUT;
    this.acquireTimeout = config.acquireTimeout ?? DEFAULT_ACQUIRE_TIMEOUT;
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
    const pool = this.getPool();
    const client = await pool.acquire();
    try {
      // プールの接続は、前の利用者が選択したDBを保持したまま返却される。そのため貸し出すたびに対象のDBを選択する
      await client.select(index);
    } catch (error) {
      await pool.destroy(client);
      throw error;
    }
    this.debugLog(logHeader, `Redis client ${index} has been acquired.`);
    return client;
  }

  public async release(client?: RedisClient): Promise<void> {
    if (!client || !this.pool) {
      return;
    }
    if (client.status === 'end' || client.status === 'close') {
      // 再利用できない状態のRedisクライアントはプールから破棄する
      await this.pool.destroy(client);
      this.debugLog(logHeader, 'Redis client destroyed due to invalid status.');
    } else {
      await this.pool.release(client);
      this.debugLog(logHeader, 'Redis client released.');
    }
  }

  public async destroy(timeoutMs = 5000): Promise<void> {
    const pool = this.pool;
    if (!pool) {
      return;
    }
    this.debugLog(logHeader, 'Destroying Redis pool...');
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        (async () => {
          await pool.drain();
          await pool.clear();
        })(),
        new Promise<void>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`${logHeader} Timeout while draining Redis pool`)), timeoutMs);
        })
      ]);
    } finally {
      clearTimeout(timer);
    }
    this.debugLog(logHeader, 'Redis pool destroyed.');
    this.pool = undefined;
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

    try {
      await this.waitForReady(client);
    } finally {
      client.quit().catch(() => client.disconnect());
    }
  }

  /**
   * Redisクライアントの接続が完了するまで待つ。接続に失敗した場合は、そのエラーで reject する
   *
   * @param {Redis} client
   */
  private async waitForReady(client: Redis): Promise<void> {
    if (client.status === 'ready') {
      return;
    }
    return new Promise<void>((resolve, reject) => {
      const onReady = () => {
        cleanup();
        resolve();
      };
      const onError = (error: Error) => {
        cleanup();
        reject(error);
      };
      const cleanup = () => {
        client.off('ready', onReady);
        client.off('error', onError);
      };
      client.once('ready', onReady);
      client.once('error', onError);
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

  private debugLog(...args: any[]): void {
    if (this.debug) {
      console.debug(...args);
    }
  }

  private getPool(): genericPool.Pool<RedisClient> {
    if (!this.pool) {
      this.pool = this.createPool();
    }
    return this.pool;
  }

  private createPool(): genericPool.Pool<RedisClient> {
    const factory: genericPool.Factory<RedisClient> = {
      create: async (): Promise<RedisClient> => {
        const client = new Redis(this.url, {
          db: this.db,
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
            // フェイルオーバー後にレプリカへ接続したままになっている場合だけ、再接続で解消できる
            return error.message.startsWith('READONLY');
          }
        }) as RedisClient;
        client.on('error', error => {
          process.emitWarning(`${logHeader} detected error (on error). ${error.message}`);
        });

        // カスタムメソッド START

        /**
         * 指定されたパターンに一致するキーを Redis から全て取得する
         *
         * @param {string} pattern
         * @param {number} [count] 1度にスキャンする件数 デフォルト:1000
         * @returns {Promise<string[]>}
         */
        client.getKeys = async function(pattern: string, count?: number): Promise<string[]> {
          const allKeys: string[] = [];
          const stream = this.scanStream({
            match: pattern,
            count: count ?? 1000
          });

          return new Promise((resolve, reject) => {
            stream.on('data', (keys: string[]) => {
              if (keys.length) {
                allKeys.push(...keys);
              }
            });
            stream.once('end', () => resolve(allKeys));
            stream.once('error', (err: Error) => {
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
         * @param {number} [count] 1度にスキャンする件数 デフォルト:1000
         * @returns {Promise<PromiseSettledResult<number>>}
         */
        client.deleteKeys = async function(pattern: string, count?: number): Promise<PromiseSettledResult<number>[]> {
          const stream = this.scanStream({
            match: pattern,
            count: count ?? 1000
          });
          const results: PromiseSettledResult<number>[] = [];

          return new Promise((resolve, reject) => {
            stream.on('data', async (keys: string[]) => {
              if (keys.length) {
                stream.pause();
                try {
                  // UNLINK を使用し、現在のインスタンス (this) で実行
                  const unlinkResult = await this.unlink(...keys);
                  results.push({
                    status: 'fulfilled',
                    value: unlinkResult
                  });
                } catch (err) {
                  results.push({
                    status: 'rejected',
                    reason: err
                  });
                } finally {
                  stream.resume();
                }
              }
            });
            stream.once('end', async () => {
              resolve(results);
            });
            stream.once('error', (err: Error) => {
              reject(err);
            });
          });
        };
        // カスタムメソッド END

        try {
          await this.waitForReady(client);
        } catch (error) {
          // 接続できなかったクライアントはプールに入れない。ioredisの再接続も止める
          client.disconnect();
          throw error;
        }

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
      testOnBorrow: this.testOnBorrow,
      acquireTimeoutMillis: this.acquireTimeout
    });
  }
}
