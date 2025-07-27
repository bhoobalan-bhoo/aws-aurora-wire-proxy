const net = require('net');
const ConnectionHandler = require('./connection-handler');
const { serverLogger } = require('../config/logger');

class ProxyServer {
  /**
   * @param {Object} options
   * @param {string} [options.host]
   * @param {number} [options.port]
   */
  constructor(options = {}) {
    this.host = options.host || process.env.PROXY_HOST || '127.0.0.1';
    this.port = options.port || parseInt(process.env.PROXY_PORT, 10) || 5432;
    this.server = null;
    this.connections = new Map();
    this.logger = serverLogger;

    this.isRunning = false;

    this.stats = {
      startTime: null,
      totalConnections: 0,
      activeConnections: 0,
      errors: 0
    };
  }

  /**
   * Start the proxy server.
   * @returns {Promise<Object>}
   */
  async start() {
    return new Promise((resolve, reject) => {
      try {
        this.server = net.createServer(this.handleNewConnection.bind(this));
        this.server.maxConnections = 100;

        this.setupServerListeners();

        this.server.listen(this.port, this.host, () => {
          this.isRunning = true;
          this.stats.startTime = new Date();

          this.logger.info('Proxy server started', {
            host: this.host,
            port: this.port,
            pid: process.pid,
            maxConnections: this.server.maxConnections
          });

          resolve({
            host: this.host,
            port: this.port,
            pid: process.pid
          });
        });
      } catch (error) {
        this.logger.error('Error starting server', {
          error: error.message,
          host: this.host,
          port: this.port
        });
        reject(error);
      }
    });
  }

  /**
   * Stop the proxy server gracefully.
   * @returns {Promise<void>}
   */
  async stop() {
    return new Promise((resolve) => {
      if (!this.server || !this.isRunning) return resolve();

      this.logger.info('Stopping proxy server...');

      this.closeAllConnections('server_shutdown');

      this.server.close(() => {
        this.isRunning = false;
        this.stats.startTime = null;
        this.logger.info('Proxy server stopped');
        resolve();
      });
    });
  }

  setupServerListeners() {
    this.server.on('error', (error) => this.handleServerError(error));
    this.server.on('close', () => this.logger.info('TCP server closed'));
    this.server.on('listening', () => {
      const address = this.server.address();
      this.logger.info('Server is listening', { address });
    });

    process.once('SIGINT', () => {
      this.logger.warn('Received SIGINT - shutting down');
      this.stop();
    });

    process.once('SIGTERM', () => {
      this.logger.warn('Received SIGTERM - shutting down');
      this.stop();
    });
  }

  /**
   * Handle a new incoming connection.
   * @param {net.Socket} socket
   */
  handleNewConnection(socket) {
    try {
      const connectionHandler = new ConnectionHandler(socket);
      const connectionId = connectionHandler.connectionId;

      this.connections.set(connectionId, connectionHandler);
      this.stats.totalConnections++;
      this.stats.activeConnections++;

      this.logger.info('New connection established', {
        connectionId,
        remoteAddress: socket.remoteAddress,
        remotePort: socket.remotePort,
        activeConnections: this.stats.activeConnections
      });

      connectionHandler.on('close', (id) => this.handleConnectionClose(id));
      connectionHandler.on('error', ({ connectionId: id, error }) =>
        this.handleConnectionError(id, error)
      );
    } catch (error) {
      this.stats.errors++;

      this.logger.error('Failed to handle new connection', {
        error: error.message,
        remoteAddress: socket.remoteAddress,
        remotePort: socket.remotePort
      });

      try {
        socket.destroy();
      } catch (destroyError) {
        this.logger.error('Failed to destroy socket', {
          error: destroyError.message
        });
      }
    }
  }

  /**
   * Handle connection closure.
   * @param {string} connectionId
   */
  handleConnectionClose(connectionId) {
    const connection = this.connections.get(connectionId);
    if (connection) {
      this.connections.delete(connectionId);
      this.stats.activeConnections--;

      this.logger.info('Connection closed', {
        connectionId,
        activeConnections: this.stats.activeConnections,
        connectionInfo: connection.getInfo()
      });
    }
  }

  /**
   * Handle connection errors.
   * @param {string} connectionId
   * @param {Error} error
   */
  handleConnectionError(connectionId, error) {
    this.stats.errors++;
    this.logger.error('Connection error', {
      connectionId,
      error: error.message
    });
  }

  /**
   * Handle server-level errors.
   * @param {Error & { code?: string }} error
   */
  handleServerError(error) {
    this.logger.error('Server error', {
      error: error.message,
      code: error.code,
      port: this.port,
      host: this.host
    });

    this.stats.errors++;

    if (['EADDRINUSE', 'EACCES'].includes(error.code)) {
      this.logger.fatal('Critical error - terminating', {
        reason: error.code
      });
      process.exit(1);
    }
  }

  /**
   * Close all active connections.
   * @param {string} reason
   */
  closeAllConnections(reason = 'admin_shutdown') {
    this.logger.info('Closing all active connections', {
      activeConnections: this.stats.activeConnections,
      reason
    });

    for (const [id, connection] of this.connections.entries()) {
      try {
        connection.forceClose(reason);
      } catch (error) {
        this.logger.error('Error closing connection', {
          connectionId: id,
          error: error.message
        });
      }
    }

    this.connections.clear();
    this.stats.activeConnections = 0;
  }

  /**
   * Get overall server statistics.
   */
  getStats() {
    const now = new Date();
    const uptime = this.stats.startTime
      ? now.getTime() - this.stats.startTime.getTime()
      : 0;

    return {
      isRunning: this.isRunning,
      host: this.host,
      port: this.port,
      startTime: this.stats.startTime,
      uptime,
      totalConnections: this.stats.totalConnections,
      activeConnections: this.stats.activeConnections,
      errors: this.stats.errors,
      maxConnections: this.server?.maxConnections || null
    };
  }

  /**
   * Get detailed server + connection status.
   */
  getStatus() {
    const stats = this.getStats();
    const connections = [];

    for (const [id, conn] of this.connections.entries()) {
      try {
        connections.push(conn.getInfo());
      } catch (error) {
        this.logger.error('Failed to retrieve connection info', {
          connectionId: id,
          error: error.message
        });
      }
    }

    return {
      server: stats,
      connections,
      health: {
        healthy: this.isHealthy(),
        memoryUsage: process.memoryUsage()
      }
    };
  }

  /**
   * Basic health check.
   */
  isHealthy() {
    return (
      this.isRunning &&
      this.server?.listening &&
      this.stats.errors < 100
    );
  }

  /**
   * Get a specific connection handler.
   * @param {string} connectionId
   * @returns {ConnectionHandler | undefined}
   */
  getConnection(connectionId) {
    return this.connections.get(connectionId);
  }

  /**
   * Get all active connections.
   * @returns {ConnectionHandler[]}
   */
  getConnections() {
    return Array.from(this.connections.values());
  }

  /**
   * Close a specific connection by ID.
   * @param {string} connectionId
   * @param {string} [reason]
   * @returns {boolean}
   */
  closeConnection(connectionId, reason = 'manual') {
    const conn = this.getConnection(connectionId);
    if (conn) {
      conn.forceClose(reason);
      return true;
    }
    return false;
  }
}

module.exports = ProxyServer;
