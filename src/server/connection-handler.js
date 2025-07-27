'use strict';

const { EventEmitter } = require('events');
const PostgreSQLProtocolHandler = require('../protocol/postgres-protocol');
const { serverLogger } = require('../config/logger');

class ConnectionHandler extends EventEmitter {
  constructor(socket) {
    super();

    if (!socket || typeof socket.on !== 'function') {
      throw new TypeError('A valid socket object is required with an "on" method.');
    }

    this.socket = socket;
    this.connectionId = this.#generateConnectionId();
    this.logger = serverLogger.child({ connectionId: this.connectionId });

    this.remoteAddress = socket.remoteAddress;
    this.remotePort = socket.remotePort;
    this.connectTime = new Date();

    this.protocolHandler = new PostgreSQLProtocolHandler(socket);
    this.#setupSocketListeners();

    this.logger.info('New connection established', {
      remoteAddress: this.remoteAddress,
      remotePort: this.remotePort
    });
  }

  // Private method to generate unique connection ID
  #generateConnectionId() {
    return Math.random().toString(36).substring(2, 15);
  }

  // Private method to bind socket event listeners
  #setupSocketListeners() {
    this.socket.setNoDelay(true);
    this.socket.setKeepAlive(true, 60000);
    this.socket.setTimeout(300000);

    this.socket.on('data', this.#handleData.bind(this));
    this.socket.on('close', this.#handleClose.bind(this));
    this.socket.on('error', this.#handleError.bind(this));
    this.socket.on('timeout', this.#handleTimeout.bind(this));
  }

  // Handle incoming data
  async #handleData(data) {
    this.logger.debug('Received data', {
      size: data.length,
      firstBytes: data.subarray(0, Math.min(16, data.length)).toString('hex')
    });

    try {
      await this.protocolHandler.handleData(data);
    } catch (error) {
      this.logger.error('Error handling data', {
        error: error.message,
        stack: error.stack,
        dataSize: data.length,
        dataHex: data.toString('hex')
      });

      try {
        await this.protocolHandler.sendError(error);
      } catch (sendError) {
        this.logger.error('Failed to send error to client', {
          error: sendError.message
        });
      }

      this.closeConnection('protocol-error');
    }
  }

  // Handle connection close
  #handleClose(hadError) {
    const duration = Date.now() - this.connectTime.getTime();

    this.logger.info('Connection closed', {
      hadError,
      duration,
      remoteAddress: this.remoteAddress,
      remotePort: this.remotePort
    });

    this.protocolHandler?.handleClose();
    this.emit('close', this.connectionId);
  }

  // Handle socket error
  #handleError(error) {
    this.logger.error('Socket error occurred', {
      message: error.message,
      code: error.code,
      remoteAddress: this.remoteAddress,
      remotePort: this.remotePort
    });

    this.protocolHandler?.handleError(error);
    this.emit('error', { connectionId: this.connectionId, error });
  }

  // Handle connection timeout
  #handleTimeout() {
    this.logger.warn('Connection timed out', {
      remoteAddress: this.remoteAddress,
      remotePort: this.remotePort,
      duration: Date.now() - this.connectTime.getTime()
    });

    this.closeConnection('timeout');
  }

  // Close the connection safely
  closeConnection(reason = 'unknown') {
    this.logger.info('Closing connection', {
      reason,
      remoteAddress: this.remoteAddress,
      remotePort: this.remotePort
    });

    if (!this.socket.destroyed) {
      try {
        this.socket.destroy();
      } catch (error) {
        this.logger.error('Error during socket destruction', {
          error: error.message
        });
      }
    }
  }

  // Public: Get stats of current connection
  getStats() {
    const duration = Date.now() - this.connectTime.getTime();
    return {
      connectionId: this.connectionId,
      remoteAddress: this.remoteAddress,
      remotePort: this.remotePort,
      connectTime: this.connectTime,
      duration,
      state: this.protocolHandler?.getState() ?? 'unknown',
      socketReadyState: this.socket.readyState,
      socketDestroyed: this.socket.destroyed
    };
  }

  // Public: Check connection health
  isHealthy() {
    const state = this.protocolHandler?.getState();
    return (
      !this.socket.destroyed &&
      this.socket.readyState === 'open' &&
      state?.state !== 'terminated'
    );
  }

  // Public: Get detailed info for monitoring
  getInfo() {
    const stats = this.getStats();
    const state = this.protocolHandler?.getState() || {};

    return {
      ...stats,
      healthy: this.isHealthy(),
      protocolState: state.state,
      authenticated: state.authenticated,
      user: state.user,
      database: state.database,
      inTransaction: state.sessionState?.inTransaction ?? false,
      transactionId: state.sessionState?.transactionId
    };
  }

  // Public: Force close connection
  forceClose(reason = 'forced') {
    this.logger.warn('Force closing connection', {
      reason,
      connectionId: this.connectionId
    });

    this.closeConnection(reason);
  }
}

module.exports = ConnectionHandler;
