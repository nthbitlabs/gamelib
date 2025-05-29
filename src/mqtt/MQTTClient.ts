import mqtt, { MqttClient, IClientOptions, IConnackPacket, Packet } from 'mqtt';
import { Logger } from '../logger';

type EventHandler = (topic: string, message: Buffer, packet: Packet) => void;

interface MqttClientWrapperOptions extends IClientOptions {
    reconnectInterval?: number;
    maxReconnectInterval?: number;
    connectionTimeout?: number;
}

interface ConnectionStatus {
    connected(): void;
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

    private readonly logger: Logger = Logger.getInstance();

    constructor(url: string,
        options: MqttClientWrapperOptions = {},
        connected: ConnectionStatus) {
        this.url = url;
        this.options = options;
        this.reconnectInterval = options.reconnectInterval ?? 1000;
        this.maxReconnectInterval = options.maxReconnectInterval ?? 30000;
        this.currentReconnectInterval = this.reconnectInterval;
        this.connectionTimeout = options.connectionTimeout ?? 5000;
        this.callback = connected;
    }

    public connect(): void {
        if (this.client) {
            this.client.end(true);
        }

        this.isExplicitlyDisconnected = false;
        this.client = mqtt.connect(this.url, this.options);

        this.connectionTimeoutId = setTimeout(() => {
            this.logger.warn('MQTT connection timed out');
            this.client?.end(true);
        }, this.connectionTimeout);

        this.client.once('connect', (packet: IConnackPacket) => {
            clearTimeout(this.connectionTimeoutId!);
            this.logger.info('MQTT connected');
            this.currentReconnectInterval = this.reconnectInterval;
            this.callback.connected();
        });

        this.client.on('message', (topic: string, message: Buffer, packet: Packet) => {
            const handler = this.eventHandlers.get(topic);
            if (handler) {
                handler(topic, message, packet);
            } else {
                this.logger.warn(`No handler registered for topic "${topic}"`);
            }
        });

        this.client.on('close', () => {
            this.logger.warn('MQTT connection closed.');
            if (!this.isExplicitlyDisconnected) {
                this.logger.info('Attempting to reconnect...');
                this.reconnectWithBackoff();
            }
        });

        this.client.on('error', (error: Error) => {
            this.logger.error('MQTT error:', error.message);
            clearTimeout(this.connectionTimeoutId!);
        });
    }

    private reconnectWithBackoff() {
        this.reconnectTimeoutId = setTimeout(() => {
            this.logger.info(`Reconnecting to MQTT broker... (${this.currentReconnectInterval}ms)`);
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
                this.logger.error(`Failed to subscribe to topic "${topic}"`, err);
            } else {
                this.logger.info(`Subscribed to topic "${topic}"`);
            }
        });
    }

    public publish(topic: string, message: string | Buffer): void {
        if (!this.client) throw new Error('MQTT client not connected');
        this.client.publish(topic, message, (err) => {
            if (err) {
                this.logger.error(`Failed to publish to topic "${topic}"`, err);
            }
        });
    }

    public unsubscribe(topic: string): void {
        if (!this.client) throw new Error('MQTT client not connected');
        this.client.unsubscribe(topic, (err) => {
            if (err) {
                this.logger.error(`Failed to unsubscribe from topic "${topic}"`, err);
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
        this.logger.warn('Disconnecting MQTT client...');
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
                this.logger.warn('MQTT client disconnected');
                this.client = null;
            });
        }
    }

    public isConnected(): boolean {
        return this.client?.connected ?? false;
    }
}
