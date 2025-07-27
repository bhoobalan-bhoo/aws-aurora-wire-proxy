const winston = require('winston');
const path = require('path');
const fs = require('fs');

// Constants
const LOGS_DIR = path.join(process.cwd(), 'logs');
const MAX_FILE_SIZE = 5 * 1024 * 1024; // 5MB
const MAX_FILES = 5;

/**
 * Ensure the logs directory exists
 */
if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR, { recursive: true });
}

/**
 * Format for file logs: JSON with timestamp and stack trace
 */
const fileLogFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

/**
 * Format for console logs: colored, human-readable
 */
const consoleLogFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: 'HH:mm:ss' }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    let log = `${timestamp} [${level}]: ${message}`;
    if (Object.keys(meta).length > 0) {
      log += '\n' + JSON.stringify(meta, null, 2);
    }
    return log;
  })
);

/**
 * Create and configure the Winston logger instance
 */
const createLoggerInstance = () => {
  const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: fileLogFormat,
    defaultMeta: { service: 'rds-data-api-proxy' },
    transports: [
      new winston.transports.File({
        filename: path.join(LOGS_DIR, 'error.log'),
        level: 'error',
        maxsize: MAX_FILE_SIZE,
        maxFiles: MAX_FILES
      }),
      new winston.transports.File({
        filename: path.join(LOGS_DIR, 'combined.log'),
        maxsize: MAX_FILE_SIZE,
        maxFiles: MAX_FILES
      })
    ]
  });

  // Add console logger for non-production environments
  if (process.env.NODE_ENV !== 'production') {
    logger.add(new winston.transports.Console({
      format: consoleLogFormat
    }));
  }

  return logger;
};

// Create root logger instance
const logger = createLoggerInstance();

/**
 * Create a namespaced logger for a specific component
 * @param {string} component
 * @returns {winston.Logger}
 */
const createChildLogger = (component) => logger.child({ component });

module.exports = {
  logger,
  createChildLogger,
  protocolLogger: createChildLogger('protocol'),
  dataApiLogger: createChildLogger('data-api'),
  translationLogger: createChildLogger('translation'),
  serverLogger: createChildLogger('server'),
};
