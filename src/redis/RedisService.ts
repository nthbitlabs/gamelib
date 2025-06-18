import Redis, { Redis as RedisType, RedisOptions } from 'ioredis';
import { createPool, Pool } from 'generic-pool';

interface ConnectionStatus {
    connect?(): void;
    ready?(): void;
    error?(err: any): void;
    close?(): void;
    reconnecting?(): void;
}

interface Logger {
    info(message: string, ...meta: any[]): void;
    warn(message: string, ...meta: any[]): void;
    error(message: string, ...meta: any[]): void;
}

const defaultLogger: Logger = {
    info: console.log,
    warn: console.warn,
    error: console.error,
};

export class RedisService {
    private static instance: RedisService;
    private static initLock: Promise<void> | null = null;

    private readonly logger: Logger;
    private readonly callback: ConnectionStatus;
    private readonly options: RedisOptions;

    private pool: Pool<RedisType> | null = null;
    private initPromise: Promise<void> | null = null;

    /**
     * @constructor
     * @param options Redis client options
     * @param connstatus Optional connection lifecycle callbacks
     * @param logger Optional logger for debug/info/error messages
     * @author Vikas Sood
     */
    private constructor(
        options: RedisOptions,
        connstatus: ConnectionStatus,
        logger?: Logger
    ) {
        options.retryStrategy = (times) => {
            if (times > 10) return null;
            return Math.min(times * 100, 2000);
        };
        this.options = options;
        this.callback = connstatus;
        this.logger = logger ?? defaultLogger;
    }

    /**
     * Get or create a singleton instance of RedisService
     * @param options Redis connection options (required on first call)
     * @param callback Connection lifecycle callbacks (required on first call)
     * @param logger Optional logger instance
     * @returns RedisService instance (singleton)
     * @author Vikas Sood
     */
    static async init(
        options?: RedisOptions,
        callback?: ConnectionStatus,
        logger?: Logger
    ): Promise<void> {
        if (RedisService.instance) return;

        if (!options || !callback) {
            throw new Error('Redis service must be initialized with options on first use');
        }

        if (!RedisService.initLock) {
            RedisService.initLock = (async () => {
                RedisService.instance = new RedisService(options, callback, logger);
            })();
        }

        await RedisService.initLock;
    }

    static getInstance(): RedisService {
        if (!RedisService.instance) {
            throw new Error('RedisService has not been initialized. Call init() first.');
        }
        return RedisService.instance;
    }

    /**
     * Lazily initializes the Redis connection pool
     * @private
     * @returns void
     * @author Vikas Sood
     */
    private async initPool(): Promise<void> {
        if (this.pool) return;

        if (!this.initPromise) {
            this.initPromise = (async () => {
                this.logger.info('Initializing Redis connection pool');
                this.pool = createPool<RedisType>(
                    {
                        create: async () => {
                            const client = new Redis(this.options);
                            this.setupListeners(client);
                            this.logger.info('Redis client created');
                            return client;
                        },
                        destroy: async (client) => {
                            await client.quit();
                            this.logger.info('Redis client destroyed');
                        },
                        validate: async (client) => {
                            try {
                                await client.ping();
                                return true;
                            } catch {
                                return false;
                            }
                        },
                    },
                    {
                        min: 2,
                        max: 10,
                        testOnBorrow: true,
                    }
                );
            })();
        }

        await this.initPromise;
    }

    /**
     * Attaches event listeners to a Redis client instance
     * @param client Redis client instance
     * @private
     * @returns void
     * @author Vikas Sood
     */
    private setupListeners(client: RedisType) {
        if (this.callback.connect) {
            client.on('connect', () => {
                this.callback.connect?.();
            });
        }
        if (this.callback.ready) {
            client.on('ready', () => {
                this.callback.ready?.();
            });
        }
        if (this.callback.error) {
            client.on('error', (err) => {
                this.logger.error('Redis error', err);
                this.callback.error?.(err);
            });
        }
        if (this.callback.close) {
            client.on('close', () => {
                this.logger.info('Redis connection closed');
                this.callback.close?.();
            });
        }
        if (this.callback.reconnecting) {
            client.on('reconnecting', () => {
                this.logger.warn('Redis reconnecting...');
                this.callback.reconnecting?.();
            });
        }
    }

    /**
     * Acquires a client from the pool and runs the provided async action
     * @param action Function that uses a Redis client
     * @returns Result of the async action execution with Redis client instance provided as argument to the function you supply here.
     * @author Vikas Sood
     */
    private async withClient<T>(action: (client: RedisType) => Promise<T>): Promise<T> {
        await this.initPool();
        const client = await this.pool!.acquire();
        try {
            return await action(client);
        } catch (err) {
            this.logger.error('Redis operation failed', err);
            throw err;
        } finally {
            this.pool!.release(client);
        }
    }

    /**
     * Sets a key-value pair in Redis (JSON serialized)
     * @param key Redis key
     * @param value Serializable value
     * @param ttlInSeconds Optional TTL in seconds
     * @returns void. If TTL is provided, Redis key expires after specified seconds. Otherwise, it persists.
     * @author Vikas Sood
     */
    async set<T>(key: string, value: T, ttlInSeconds?: number): Promise<void> {
        await this.withClient(async (client) => {
            const json = JSON.stringify(value);
            if (ttlInSeconds) {
                await client.set(key, json, 'EX', ttlInSeconds);
            } else {
                await client.set(key, json);
            }
        });
    }

    /**
     * Gets and parses a key's value from Redis
     * @param key Redis key
     * @returns Parsed value or null if not found or parsing fails (in which case error is logged too)
     * @author Vikas Sood
     */
    async get<T>(key: string): Promise<T | null> {
        return this.withClient(async (client) => {
            const val = await client.get(key);
            if (!val) return null;
            try {
                return JSON.parse(val);
            } catch (err) {
                this.logger.error(`Failed to parse Redis value for key "${key}"`, err);
                return null;
            }
        });
    }

    /**
     * Updates an existing key in Redis
     * @param key Redis key
     * @param value New value
     * @param ttlInSeconds Optional TTL to reset
     * @returns void
     * @throws Error if key does not exist in Redis at time of update attempt
     * @author Vikas Sood
     */
    async update<T>(key: string, value: T, ttlInSeconds?: number): Promise<void> {
        await this.withClient(async (client) => {
            const exists = await client.exists(key);
            if (!exists) throw new Error(`Key "${key}" does not exist`);
            await this.set(key, value, ttlInSeconds);
        });
    }

    /**
     * Removes a key from Redis
     * @param key Redis key
     * @returns void. Key is permanently deleted from Redis if it exists, no-op otherwise.
     * @author Vikas Sood
     */
    async remove(key: string): Promise<void> {
        await this.withClient(async (client) => {
            await client.del(key);
        });
    }

    /**
     * Scans Redis keys matching a pattern.
     * @param pattern Glob pattern (e.g., "user:*")
     * @param count Approximate number of keys to return per scan iteration
     * @returns All matching keys
     */
    async scanKeys(pattern: string, count: number = 100): Promise<string[]> {
        const keys: string[] = [];

        await this.withClient(async (client) => {
            let cursor = '0';
            do {
                const [nextCursor, batch] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', count);
                cursor = nextCursor;
                keys.push(...batch);
            } while (cursor !== '0');
        });

        return keys;
    }

    /**
     * Scans Redis keys with pattern and fetches raw string values.
     * @param pattern Glob pattern (e.g., "session:*")
     * @param count Approximate number of keys to fetch per scan
     * @returns Map of key-value pairs (values are raw strings or null if missing)
     */
    async scanAndGet(pattern: string, count: number = 100): Promise<Record<string, string | null>> {
        const result: Record<string, string | null> = {};

        await this.withClient(async (client) => {
            let cursor = '0';
            do {
                const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', count);
                cursor = nextCursor;

                if (keys.length > 0) {
                    const values = await client.mget(...keys);
                    keys.forEach((key, i) => {
                        result[key] = values[i] ?? null;
                    });
                }
            } while (cursor !== '0');
        });

        return result;
    }

    /**
     * Performs a single SCAN iteration to support paginated scanning.
     * @param cursor The SCAN cursor to resume from (use "0" for first call)
     * @param pattern Key pattern (e.g., "log:*")
     * @param count Approximate number of keys to return
     * @returns Object with next cursor and keys
     */
    async scanKeysCursor(
        cursor: string,
        pattern: string,
        count: number = 100
    ): Promise<{ cursor: string; keys: string[] }> {
        return this.withClient(async (client) => {
            const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', count);
            return { cursor: nextCursor, keys };
        });
    }

    /**
     * Performs a single SCAN iteration and fetches parsed JSON values.
     * @param cursor The SCAN cursor to resume from (use "0" for first call)
     * @param pattern Key pattern
     * @param count Approximate number of keys to return
     * @returns Object with next cursor and parsed key-value map
     */
    async scanAndGetJSONCursor<T = any>(
        cursor: string,
        pattern: string,
        count: number = 100
    ): Promise<{ cursor: string; result: Record<string, T | null> }> {
        return this.withClient(async (client) => {
            const result: Record<string, T | null> = {};
            const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', count);

            if (keys.length > 0) {
                const pipeline = client.pipeline();
                keys.forEach((key) => pipeline.get(key));
                const responses = await pipeline.exec();

                // Ensure responses is non-null and length matches
                if (responses && responses.length === keys.length) {
                    keys.forEach((key, idx) => {
                        const response = responses[idx];
                        const raw = Array.isArray(response) ? response[1] : null;

                        if (typeof raw === 'string') {
                            try {
                                result[key] = JSON.parse(raw);
                            } catch {
                                result[key] = null;
                            }
                        } else {
                            result[key] = null;
                        }
                    });
                }
            }

            return { cursor: nextCursor, result };
        });
    }

    /**
     * Checks if a key exists in Redis
     * @param key Redis key
     * @returns true if key exists, false otherwise. Redis command used: EXISTS.
     * @author Vikas Sood
     */
    async has(key: string): Promise<boolean> {
        return this.withClient(async (client) => {
            const exists = await client.exists(key);
            return Boolean(exists);
        });
    }

    /**
     * Provides direct access to a Redis client for custom operations
     * @param fn Function using the raw Redis client
     * @returns Result of the custom operation supplied by the caller as a function
     * @author Vikas Sood
     */
    async useRawClient<T>(fn: (client: RedisType) => Promise<T>): Promise<T> {
        return this.withClient(fn);
    }

    /**
     * Gracefully shuts down the Redis connection pool
     * @returns void. All pooled clients are drained and destroyed. Instance can be reinitialized afterward.
     * @author Vikas Sood
     */
    async shutdown(): Promise<void> {
        if (this.pool) {
            await this.pool.drain();
            await this.pool.clear();
            this.pool = null;
            this.initPromise = null;
            this.logger.info('Redis shutdown complete');
        }
    }
}
