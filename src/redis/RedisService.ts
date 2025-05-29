import Redis, { Redis as RedisType, RedisOptions } from 'ioredis';
import { createPool, Pool } from 'generic-pool';

export interface ExtendedredisOptions extends RedisOptions {
    connect?(): void;
    ready?(): void;
    error?(): void;
    close?(): void;
    reconnecting?(): void;
};

export class RedisService {
    private static instance: RedisService;
    private pool: Pool<RedisType> | null = null;
    private initPromise: Promise<void> | null = null;

    private constructor(private options: ExtendedredisOptions) {
        options.retryStrategy = (times) => {
            if (times > 10) return null;
            return Math.min(times * 100, 2000);
        };
    }

    static getInstance(options?: RedisOptions): RedisService {
        if (!RedisService.instance) {
            if (!options) throw new Error('Redis service must be initialized with options on first use');
            RedisService.instance = new RedisService(options);
        }
        return RedisService.instance;
    }

    private async initPool(): Promise<void> {
        if (this.pool) return;

        if (!this.initPromise) {
            this.initPromise = (async () => {
                this.pool = createPool<RedisType>(
                    {
                        create: async () => {
                            const client = new Redis(this.options);
                            this.setupListeners(client);
                            return client;
                        },
                        destroy: async (client) => {
                            await client.quit();
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
        client.on('connect', () => this.options.connect);
        client.on('ready', () => this.options.ready);
        client.on('error', (err) => this.options.error);
        client.on('close', () => this.options.close);
        client.on('reconnecting', () => this.options.reconnecting);
    }

    private async withClient<T>(action: (client: RedisType) => Promise<T>): Promise<T> {
        await this.initPool();
        const client = await this.pool!.acquire();
        try {
            return await action(client);
        } finally {
            this.pool!.release(client);
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
            return val ? JSON.parse(val) : null;
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

    async shutdown(): Promise<void> {
        if (this.pool) {
            await this.pool.drain();
            await this.pool.clear();
            this.pool = null;
            this.initPromise = null;
            console.log(`Redis shutdown complete`);
        }
    }
}
