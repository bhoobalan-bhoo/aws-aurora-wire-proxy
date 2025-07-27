#!/usr/bin/env node

require('dotenv').config();

const http = require('http');
const ProxyServer = require('./server/proxy-server');
const { validateConfig } = require('./config/aws');
const { logger } = require('./config/logger');

// Constants
const REQUIRED_ENV_VARS = ['RDS_CLUSTER_ARN', 'RDS_SECRET_ARN', 'RDS_DATABASE_NAME'];
const DEFAULT_PROXY_HOST = 'localhost';
const DEFAULT_PROXY_PORT = 5432;
const DEFAULT_HEALTH_CHECK_PORT = 3000;
const STATS_LOG_INTERVAL_MS = 5 * 60 * 1000;

/**
 * Validates required environment variables and AWS config.
 */
async function validateEnvironment() {
  try {
    validateConfig();

    const missing = REQUIRED_ENV_VARS.filter(key => !process.env[key]);
    if (missing.length > 0) {
      throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
    }

    logger.info('✅ Environment validation passed');
  } catch (error) {
    logger.error('❌ Environment validation failed', {
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

/**
 * Starts the optional HTTP health check server.
 */
function startHealthCheckServer(proxyServer) {
  const port = parseInt(process.env.HEALTH_CHECK_PORT, 10) || DEFAULT_HEALTH_CHECK_PORT;

  const server = http.createServer((req, res) => {
    if (req.url === '/health') {
      const isHealthy = proxyServer.isHealthy();
      const status = proxyServer.getStatus();

      res.writeHead(isHealthy ? 200 : 500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: isHealthy ? 'healthy' : 'unhealthy',
        timestamp: new Date().toISOString(),
        ...status
      }, null, 2));
    } else if (req.url === '/metrics') {
      const stats = proxyServer.getStats();

      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end(`
# HELP proxy_connections_total Total number of connections
# TYPE proxy_connections_total counter
proxy_connections_total ${stats.totalConnections}

# HELP proxy_connections_active Current active connections
# TYPE proxy_connections_active gauge
proxy_connections_active ${stats.activeConnections}

# HELP proxy_errors_total Total number of errors
# TYPE proxy_errors_total counter
proxy_errors_total ${stats.errors}

# HELP proxy_uptime_seconds Server uptime in seconds
# TYPE proxy_uptime_seconds gauge
proxy_uptime_seconds ${Math.round(stats.uptime / 1000)}
      `.trim());
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not Found');
    }
  });

  server.listen(port, () => {
    logger.info('✅ Health check server started', {
      port,
      endpoints: ['/health', '/metrics']
    });
  });
}

/**
 * Initializes and starts the proxy server.
 */
async function main() {
  try {
    logger.info('🚀 Starting RDS Data API Proxy Server', {
      nodeVersion: process.version,
      platform: process.platform,
      pid: process.pid
    });

    await validateEnvironment();

    const proxyServer = new ProxyServer({
      host: process.env.PROXY_HOST || DEFAULT_PROXY_HOST,
      port: parseInt(process.env.PROXY_PORT, 10) || DEFAULT_PROXY_PORT
    });

    const serverInfo = await proxyServer.start();

    logger.info('🟢 Proxy server is running', {
      host: serverInfo.host,
      port: serverInfo.port,
      pid: serverInfo.pid
    });

    logger.info('📋 Connection Info', {
      connectionString: `postgresql://postgres:password@${serverInfo.host}:${serverInfo.port}/${process.env.RDS_DATABASE_NAME}`,
      username: 'any (ignored)',
      password: 'any (ignored)',
    });

    // Log server stats every 5 minutes
    setInterval(() => {
      const stats = proxyServer.getStats();
      logger.info('📊 Server Statistics', {
        uptimeMins: Math.round(stats.uptime / 1000 / 60),
        totalConnections: stats.totalConnections,
        activeConnections: stats.activeConnections,
        errors: stats.errors,
        memory: process.memoryUsage()
      });
    }, STATS_LOG_INTERVAL_MS);

    // Graceful shutdown handlers
    const shutdown = async (reason) => {
      logger.info(`⚠️  Received ${reason}, shutting down gracefully...`);
      try {
        await proxyServer.stop();
        logger.info('✅ Shutdown complete');
        process.exit(0);
      } catch (err) {
        logger.error('❌ Error during shutdown', { error: err.message });
        process.exit(1);
      }
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('uncaughtException', (err) => {
      logger.error('💥 Uncaught Exception', { error: err.message, stack: err.stack });
      shutdown('uncaughtException');
    });
    process.on('unhandledRejection', (reason) => {
      logger.error('💥 Unhandled Rejection', {
        reason: reason instanceof Error ? reason.message : reason,
        stack: reason instanceof Error ? reason.stack : undefined
      });
      shutdown('unhandledRejection');
    });

    // Start health server if enabled
    if (process.env.ENABLE_HEALTH_CHECK === 'true') {
      startHealthCheckServer(proxyServer);
    }

  } catch (error) {
    logger.error('❌ Failed to start application', {
      error: error.message,
      stack: error.stack
    });
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { main };
