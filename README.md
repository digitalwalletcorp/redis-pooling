# Redis Pooling

[![NPM Version](https://img.shields.io/npm/v/%40digitalwalletcorp%2Fredis-pooling)](https://www.npmjs.com/package/@digitalwalletcorp/redis-pooling) [![License](https://img.shields.io/npm/l/%40digitalwalletcorp%2Fredis-pooling)](https://opensource.org/licenses/MIT) [![Build Status](https://img.shields.io/github/actions/workflow/status/digitalwalletcorp/redis-pooling/ci.yml?branch=main)](https://github.com/digitalwalletcorp/redis-pooling/actions) [![Test Coverage](https://img.shields.io/codecov/c/github/digitalwalletcorp/redis-pooling.svg)](https://codecov.io/gh/digitalwalletcorp/redis-pooling)

A lightweight TypeScript/JavaScript library for managing Redis connections with pooling support.
Designed for both server-side Node.js applications and cron-style background jobs, it wraps ioredis clients in a pool with safe acquire/release methods, connection validation, and optional custom utility functions like getKeys and deleteKeys.

#### ✨ Features

* **Connection Pooling**: Efficiently manage multiple Redis connections with `min` and `max` pool size configuration.
* **Safe Acquire/Release**: Guaranteed handling of Redis client lifecycle with optional database selection (`SELECT dbIndex`).
* **Custom Utilities**: Built-in helper methods like `getKeys(pattern)` and `deleteKeys(pattern)` for convenient Redis key operations.
* **Connection Validation**: PING-based validation before borrowing from the pool.
* **TLS Support**: Optional TLS configuration for secure Redis connections.

#### 📦 Instllation

```bash
npm install @digitalwalletcorp/redis-pooling
# or
yarn add @digitalwalletcorp/redis-pooling
```

#### 📖 Usage

##### Example 1: Pure NodeJS Style Instantiation

This pattern is ideal for short-lived Node.js scripts, such as cron jobs, where a pool is created, used, and destroyed within a single run.

```typescript
import { createRedisPool, ManagedRedisClient } from '@digitalwalletcorp/redis-pooling';

const redisPool = createRedisPool({
  url: 'redis://localhost:6379',
  db: 0,
  max: 10,
  min: 2,
  connectTimeout: 5000,
  enableTls: false
});

async function main() {
  const client: ManagedRedisClient = await redisPool.acquire();
  try {
    await client.set('key', 'value');
    const value = await client.get('key');
    console.log('Redis value:', value);
  } finally {
    await redisPool.release(client);
  }
}

main().catch(console.error);
```

##### Example 2: Singleton Pool Instantiation using Web-App

For long-running web applications, a singleton Redis pool ensures efficient reuse of connections across requests.


`@/server/singleton/redis-pooling`
```typescript
import { createRedisPool, ManagedRedisClient } from '@digitalwalletcorp/redis-pooling';

export const redisPool = createRedisPool({
  url: 'redis://localhost:6379',
  db: 0,
  max: 20,
  min: 5,
  connectTimeout: 5000,
  enableTls: false
});
```

`@/server/api/some-rest-api`
```typescript
import { redisPool } from '@/server/singleton/redis-pooling';

async function handleRequest() {
  const client: ManagedRedisClient = await redisPool.acquire();
  try {
    const value = await client.get('session:1234');
    console.log('Session value:', value);
  } finally {
    await redisPool.release(client);
  }
}
```

##### Example 3: Using Custom Methods (`getKeys`, `deleteKeys`)

`ManagedRedisClient` extends the standard `ioredis` client with convenient helper methods.
`getKeys` and `deleteKeys` use `SCAN` and `UNLINK` internally to avoid blocking Redis with large datasets (unlike `KEYS`).
All other standard Redis commands remain available.

```typescript
import { redisPool } from '@/server/singleton/redis-pooling';

async function manageCache() {
  const client = await redisPool.acquire();

  try {
    // Get all keys matching a pattern using SCAN (avoids blocking Redis like KEYS does)
    const keys = await client.getKeys('user:*');
    console.log('Matching keys:', keys);

    // Delete all matching keys using UNLINK
    const deleted = await client.deleteKeys('cache:*');
    console.log(`Deleted ${deleted} keys`);
  } finally {
    await redisPool.release(client);
  }
}
```

#### 📚 API Reference

##### `createRedisPool(config: RedisConfig): ManagedRedisPool`

Creates a managed Redis pool.

| Property         | Type    | Default  | Description                            |
| ---------------- | ------- | -------- | -------------------------------------- |
| `url`            | string  | required | Redis connection URL.                  |
| `db`             | number  | 0        | Default Redis database index.          |
| `connectTimeout` | number  | 5000     | Connection timeout in milliseconds.    |
| `max`            | number  | 10       | Maximum number of clients in the pool. |
| `min`            | number  | 0        | Minimum number of clients in the pool. |
| `enableTls`      | boolean | false    | Enable TLS for Redis connection.       |

##### `ManagedRedisPool` Methods

| Method                                | Signature                     | Description                                                                            |
| ------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------- |
| `acquire(dbIndex?: number)`           | `Promise<ManagedRedisClient>` | Acquire a Redis client from the pool. Optionally switch to a different database index. |
| `release(client: ManagedRedisClient)` | `Promise<void>`               | Release the Redis client back to the pool.                                             |
| `destroy()`                           | `Promise<void>`               | Drain and clear all connections in the pool.                                           |

##### `ManagedRedisClient` Methods

Extends the standard `ioredis` `Redis` client with additional helpers:

| Method                        | Signature           | Description                                                                                     |
| ----------------------------- | ------------------- | ----------------------------------------------------------------------------------------------- |
| `getKeys(pattern: string)`    | `Promise<string[]>` | Scan and return all keys matching a pattern.                                                    |
| `deleteKeys(pattern: string)` | `Promise<number>`   | Scan and delete all keys matching a pattern using `UNLINK`. Returns the number of deleted keys. |

---
#### 💡 Notes

* Always release clients back to the pool using `release(client)` to avoid connection leaks.
* Use `acquire()` and `release()` inside `try/finally` blocks for safe resource management.

#### 📜 License

This project is licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
