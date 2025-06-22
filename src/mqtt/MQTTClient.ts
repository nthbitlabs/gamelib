import mqtt, { MqttClient, IClientOptions, IConnackPacket, Packet } from 'mqtt';

type EventHandler = (topic: string, message: Buffer, packet: Packet) => void;

interface MqttClientWrapperOptions extends IClientOptions {
    reconnectInterval?: number;
    maxReconnectInterval?: number;
    connectionTimeout?: number;
}

interface ConnectionStatus {
    connecting?(): void;
    connected?(): void;
    disconnected?(): void;
    timedout?(): void;
    closed?(): void;
    error?(): void;
}

export class MqttClientWrapper {
    private client: MqttClient | null = null;
    private url: string;
    private options: MqttClientWrapperOptions;
    private reconnectInterval: number;
    private maxReconnectInterval: number;
    private currentReconnectInterval: number;
    private connectionTimeout: number;
    private eventHandlers: Map<string, EventHandler> = new Map();
    private callback: ConnectionStatus;
    private reconnectTimeoutId: NodeJS.Timeout | null = null;
    private connectionTimeoutId: NodeJS.Timeout | null = null;
    private isExplicitlyDisconnected: boolean = false;

    constructor(url: string,
        options: MqttClientWrapperOptions = {},
        connstatus: ConnectionStatus) {
        this.url = url;
        this.options = options;
        this.reconnectInterval = options.reconnectInterval ?? 1000;
        this.maxReconnectInterval = options.maxReconnectInterval ?? 30000;
        this.currentReconnectInterval = this.reconnectInterval;
        this.connectionTimeout = options.connectionTimeout ?? 5000;
        this.callback = connstatus;
    }

    public connect(): void {
        if (this.client) {
            this.client.end(true);
        }

        this.isExplicitlyDisconnected = false;

        //inform the caller connection in progress
        if (this.callback.connecting &&
            typeof this.callback.connecting === 'function')
            this.callback.connecting();

        this.client = mqtt.connect(this.url, this.options);

        this.connectionTimeoutId = setTimeout(() => {
            if (this.callback.timedout &&
                typeof this.callback.timedout === 'function')
                this.callback.timedout();
            this.client?.end(true);
        }, this.connectionTimeout);

        this.client.once('connect', (packet: IConnackPacket) => {
            clearTimeout(this.connectionTimeoutId!);
            this.currentReconnectInterval = this.reconnectInterval;
            if (this.callback.connected &&
                typeof this.callback.connected === 'function')
                this.callback.connected();
        });

        this.client.on('message', (topic: string, message: Buffer, packet: Packet) => {
            try {
                let matched = false;
                for (const [pattern, handler] of this.eventHandlers.entries()) {
                    if (this.matchTopic(pattern, topic)) {
                        handler(topic, message, packet);
                        matched = true;
                    }
                }

                if (!matched) {
                    console.warn(`No handler matched for topic "${topic}"`);
                }
            } catch (err: any) {
                console.error('Error in message handler:', err);
            }
            /*
            try {
                const handler = this.eventHandlers.get(topic);
                if (handler) {
                    handler(topic, message, packet);
                } else {
                    throw new Error(`No handler registered for topic "${topic}"`);
                }
            }
            catch (err: any) {
                console.error(err.message);
            }
            */
        });

        this.client.on('close', () => {
            if (this.callback.closed &&
                typeof this.callback.closed === 'function')
                this.callback.closed();
            if (!this.isExplicitlyDisconnected) {
                this.reconnectWithBackoff();
            }
        });

        this.client.on('error', (error: Error) => {
            if (this.callback.error &&
                typeof this.callback.error === 'function')
                this.callback.error();
            console.error('MQTT error:', error.message);
            clearTimeout(this.connectionTimeoutId!);
        });
    }

    private reconnectWithBackoff() {
        this.reconnectTimeoutId = setTimeout(() => {
            //inform the caller connection in progress
            if (this.callback.connecting &&
                typeof this.callback.connecting === 'function')
                this.callback.connecting();

            this.connect();
            this.currentReconnectInterval = Math.min(
                this.currentReconnectInterval * 2,
                this.maxReconnectInterval
            );
        }, this.currentReconnectInterval);
    }

    public subscribe(topic: string): void {
        if (!this.client) throw new Error('MQTT client not connected');
        this.client.subscribe(topic, (err) => {
            if (err) {
                console.error(`Failed to subscribe to topic "${topic}"`, err);
            }
        });
    }

    public publish(topic: string, message: string | Buffer): void {
        if (!this.client) throw new Error('MQTT client not connected');
        this.client.publish(topic, message, (err) => {
            if (err) {
                console.error(`Failed to publish to topic "${topic}"`, err);
            }
        });
    }

    public unsubscribe(topic: string): void {
        if (!this.client) throw new Error('MQTT client not connected');
        this.client.unsubscribe(topic, (err) => {
            if (err) {
                console.error(`Failed to unsubscribe from topic "${topic}"`, err);
            }
        });
    }

    public registerHandler(topic: string, handler: EventHandler): void {
        this.eventHandlers.set(topic, handler);
        this.subscribe(topic);
    }

    public unregisterHandler(topic: string): void {
        this.eventHandlers.delete(topic);
        this.unsubscribe(topic);
    }

    public disconnect(): void {
        this.isExplicitlyDisconnected = true;

        if (this.reconnectTimeoutId) {
            clearTimeout(this.reconnectTimeoutId);
            this.reconnectTimeoutId = null;
        }

        if (this.connectionTimeoutId) {
            clearTimeout(this.connectionTimeoutId);
            this.connectionTimeoutId = null;
        }

        this.eventHandlers.clear();

        if (this.client) {
            this.client.end(true, () => {
                //inform the caller connection in progress
                if (this.callback.disconnected &&
                    typeof this.callback.disconnected === 'function')
                    this.callback.disconnected();

                this.client = null;
            });
        }
    }

    public isConnected(): boolean {
        return this.client?.connected ?? false;
    }

    private matchTopic(pattern: string, topic: string): boolean {
        const patternSegments = pattern.split('/');
        const topicSegments = topic.split('/');

        for (let i = 0; i < patternSegments.length; i++) {
            const p = patternSegments[i];
            const t = topicSegments[i];

            if (p === '#') {
                // '#' must be last in the pattern
                return i === patternSegments.length - 1;
            }

            if (p === '+') {
                if (!t) return false;
                continue;
            }

            if (p !== t) {
                return false;
            }
        }

        return patternSegments.length === topicSegments.length;
    }
}
