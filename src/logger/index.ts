import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import SlackHook from 'winston-slack-webhook-transport';

interface LoggerOptions {
    level?: string;
    console?: boolean;
    file?: {
        filename: string;
        dirname?: string;
        maxSize?: string;
        maxFiles?: string;
    };
    slack?: {
        webhookUrl: string;
        level?: string;
    };
}

export class Logger {
    private static instance: Logger;
    private logger: winston.Logger;

    private constructor(options: LoggerOptions) {
        const transports: winston.transport[] = [];

        // Console transport
        if (options.console) {
            transports.push(
                new winston.transports.Console({
                    format: winston.format.combine(
                        winston.format.colorize(),
                        winston.format.timestamp(),
                        winston.format.printf(({ level, message, timestamp }) => {
                            return `[${timestamp}] ${level}: ${message}`;
                        })
                    ),
                })
            );
        }

        // File transport
        if (options.file) {
            transports.push(
                new DailyRotateFile({
                    dirname: options.file.dirname || 'logs',
                    filename: options.file.filename,
                    datePattern: 'YYYY-MM-DD',
                    zippedArchive: true,
                    maxSize: options.file.maxSize || '10m',
                    maxFiles: options.file.maxFiles || '14d',
                    format: winston.format.combine(
                        winston.format.timestamp(),
                        winston.format.json()
                    ),
                })
            );
        }

        // Slack transport
        if (options.slack?.webhookUrl) {
            transports.push(
                new SlackHook({
                    webhookUrl: options.slack.webhookUrl,
                    level: options.slack.level || 'error',
                    formatter: (info) => {
                        return {
                            text: `*${info.level.toUpperCase()}* - ${info.message}`,
                        };
                    },
                })
            );
        }

        this.logger = winston.createLogger({
            level: options.level || 'info',
            levels: winston.config.syslog.levels,
            format: winston.format.combine(
                winston.format.errors({ stack: true }),
                winston.format.splat(),
                winston.format.timestamp()
            ),
            transports,
        });
    }

    // Singleton accessor
    public static getInstance(options?: LoggerOptions): Logger {
        if (!Logger.instance) {
            if (!options) {
                throw new Error('Logger must be initialized with options on first use');
            }
            Logger.instance = new Logger(options);
        }
        return Logger.instance;
    }

    public info(message: string, ...meta: any[]) {
        this.logger.info(message, ...meta);
    }

    public warn(message: string, ...meta: any[]) {
        this.logger.warning(message, ...meta);
    }

    public error(message: string, ...meta: any[]) {
        this.logger.error(message, ...meta);
    }

    public debug(message: string, ...meta: any[]) {
        this.logger.debug(message, ...meta);
    }

    public log(level: string, message: string, ...meta: any[]) {
        this.logger.log(level, message, ...meta);
    }

    public getWinstonInstance(): winston.Logger {
        return this.logger;
    }
}
