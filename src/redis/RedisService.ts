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

    static async getInstance(
        options?: RedisOptions,
        callback?: ConnectionStatus,
        logger?: Logger
    ): Promise<RedisService> {
        if (RedisService.instance) return RedisService.instance;

        if (!options || !callback) {
            throw new Error('Redis service must be initialized with options on first use');
        }

        if (!RedisService.initLock) {
            RedisService.initLock = (async () => {
                RedisService.instance = new RedisService(options, callback, logger);
            })();
        }

        await RedisService.initLock;
        return RedisService.instance!;
    }

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

    private setupListeners(client: RedisType) {
        if (this.callback.connect) {
            client.on('connect', (...args) => {
                this.logger.info('Redis connected');
                this.callback.connect?.();
            });
        }
        if (this.callback.ready) {
            client.on('ready', (...args) => {
                this.logger.info('Redis ready');
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
            client.on('close', (...args) => {
                this.logger.info('Redis connection closed');
                this.callback.close?.();
            });
        }
        if (this.callback.reconnecting) {
            client.on('reconnecting', (...args) => {
                this.logger.warn('Redis reconnecting...');
                this.callback.reconnecting?.();
            });
        }
    }

    private async withClient<T>(action: (client: RedisType) => Promise<T>): Promise<T> {
        await this.initPool();
        const client = await this.pool!.acquire();
        this.logger.info('Redis client acquired');
        try {
            return await action(client);
        } catch (err) {
            this.logger.error('Redis operation failed', err);
            throw err;
        } finally {
            this.pool!.release(client);
            this.logger.info('Redis client released');
        }
    }

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

    async update<T>(key: string, value: T, ttlInSeconds?: number): Promise<void> {
        await this.withClient(async (client) => {
            const exists = await client.exists(key);
            if (!exists) throw new Error(`Key "${key}" does not exist`);
            await this.set(key, value, ttlInSeconds);
        });
    }

    async remove(key: string): Promise<void> {
        await this.withClient(async (client) => {
            await client.del(key);
        });
    }

    async has(key: string): Promise<boolean> {
        return this.withClient(async (client) => {
            const exists = await client.exists(key);
            return Boolean(exists);
        });
    }

    async useRawClient<T>(fn: (client: RedisType) => Promise<T>): Promise<T> {
        return this.withClient(fn);
    }

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
