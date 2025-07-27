const MessageParser = require('./message-parser');
const MessageBuilder = require('./message-builder');
const QueryTranslator = require('../translation/query-translator');
const { protocolLogger } = require('../config/logger');
const { TRANSACTION_STATUS } = require('../utils/constants');

// Constants
const STATES = {
  STARTUP: 'startup',
  AUTHENTICATION: 'authentication',
  READY: 'ready',
  TERMINATED: 'terminated'
};

class PostgreSQLProtocolHandler {
  constructor(socket) {
    this.socket = socket;
    this.parser = new MessageParser();
    this.builder = new MessageBuilder();
    this.translator = new QueryTranslator();
    this.state = STATES.STARTUP;
    this.buffer = Buffer.alloc(0);
    this.authenticated = false;
    this.startupParameters = {};

    this.logger = protocolLogger.child({ connectionId: this.generateConnectionId() });
    this.logger.info('PostgreSQL protocol handler initialized');
  }

  generateConnectionId() {
    return Math.random().toString(36).substring(2, 15);
  }

  /**
   * Handle raw incoming data from the client
   * @param {Buffer} data
   */
  async handleData(data) {
    try {
      this.logger.debug('Received data', {
        size: data.length,
        state: this.state,
        hex: data.toString('hex')
      });

      this.buffer = Buffer.concat([this.buffer, data]);

      const parseResult = this.parser.parseBuffer(this.buffer);
      this.buffer = parseResult.remaining;

      for (const message of parseResult.messages) {
        if (message.isSSL) {
          this.logger.info('Rejecting SSL request');
          await this.sendMessage(Buffer.from('N'));
          continue;
        }
        await this.processMessage(message);
      }
    } catch (err) {
      this.logger.error('Error handling data', {
        error: err.message,
        stack: err.stack
      });
      await this.sendError(err);
    }
  }

  /**
   * Process individual parsed message
   * @param {Object} message
   */
  async processMessage(message) {
    try {
      switch (this.state) {
        case STATES.STARTUP:
          return await this.handleStartup(message);
        case STATES.AUTHENTICATION:
          return await this.handleAuthentication(message);
        case STATES.READY:
          return await this.handleQuery(message);
        case STATES.TERMINATED:
          this.logger.debug('Ignoring message after termination');
          return;
        default:
          throw new Error(`Invalid state: ${this.state}`);
      }
    } catch (err) {
      this.logger.error('Failed to process message', {
        type: message.type,
        error: err.message
      });
      await this.sendError(err);
    }
  }

  /**
   * Handle startup message
   * @param {Object} message
   */
  async handleStartup(message) {
    if (message.type !== 'startup') throw new Error('Expected startup message');

    this.startupParameters = message.parameters;

    this.logger.info('Startup received', {
      user: message.parameters.user,
      database: message.parameters.database
    });

    await this.sendAuthenticationRequest();
    this.state = STATES.AUTHENTICATION;
  }

  async handleAuthentication(message) {
    if (message.type !== 'password') throw new Error('Expected password message');

    this.logger.info('Client authenticated', { user: this.startupParameters.user });
    await this.sendAuthenticationSuccess();
    this.authenticated = true;
    this.state = STATES.READY;
  }

  async handleQuery(message) {
    switch (message.type) {
      case 'query':
        return await this.handleSimpleQuery(message);
      case 'parse':
        return await this.handleParse(message);
      case 'bind':
        return await this.handleBind(message);
      case 'execute':
        return await this.handleExecute(message);
      case 'describe':
        return await this.handleDescribe(message);
      case 'close':
        return await this.handleClose(message);
      case 'sync':
        return await this.handleSync();
      case 'terminate':
        return await this.handleTerminate();
      default:
        this.logger.warn('Unhandled message type', { type: message.type });
    }
  }

  async handleSimpleQuery(message) {
    const sql = message.sql?.substring(0, 100);
    const start = Date.now();

    try {
      this.logger.info('Running query', { sql });

      const result = await this.translator.translateAndExecute(message.sql);
      await this.sendMessage(this.builder.buildQueryResponse(result));
      this.logger.info('Query success', {
        duration: Date.now() - start,
        recordCount: result.records?.length || 0
      });

    } catch (err) {
      this.logger.error('Query failed', {
        sql,
        duration: Date.now() - start,
        error: err.message
      });
      await this.sendError(err);
      await this.sendMessage(this.builder.buildReadyForQuery());
    }
  }

  async handleParse(message) {
    this.logger.debug('Parsing SQL', {
      name: message.statementName,
      sql: message.sql?.substring(0, 100)
    });
    await this.sendMessage(this.builder.buildParseComplete());
  }

  async handleBind(message) {
    this.logger.debug('Binding parameters', {
      portal: message.portalName,
      statement: message.statementName
    });
    await this.sendMessage(this.builder.buildBindComplete());
  }

  async handleExecute(message) {
    this.logger.debug('Executing portal', {
      portal: message.portalName,
      maxRows: message.maxRows
    });
    const result = { records: [], columnMetadata: [] };
    await this.sendMessage(this.builder.buildQueryResponse(result));
  }

  async handleDescribe(message) {
    this.logger.debug('Describing object', {
      type: message.objectType,
      name: message.objectName
    });
    await this.sendMessage(this.builder.buildRowDescription([]));
  }

  async handleClose(message) {
    this.logger.debug('Closing object', {
      type: message.objectType,
      name: message.objectName
    });
    const CLOSE_COMPLETE = Buffer.from([0x33, 0x00, 0x00, 0x00, 0x04]);
    await this.sendMessage(CLOSE_COMPLETE);
  }

  async handleSync() {
    const inTx = this.translator.getSessionState().inTransaction;
    const status = inTx ? TRANSACTION_STATUS.TRANSACTION : TRANSACTION_STATUS.IDLE;
    await this.sendMessage(this.builder.buildReadyForQuery(status));
  }

  async handleTerminate() {
    this.logger.info('Client terminated connection');
    try {
      await this.translator.cleanup();
    } catch (err) {
      this.logger.error('Cleanup error', { error: err.message });
    }
    this.state = STATES.TERMINATED;
    this.socket.end();
  }

  async sendAuthenticationRequest() {
    await this.sendMessage(this.builder.buildAuthenticationCleartextPassword());
  }

  async sendAuthenticationSuccess() {
    await this.sendMessage(this.builder.buildAuthenticationOk());
    await this.sendMessage(this.builder.buildBackendKeyData());
    await this.sendMessage(this.builder.buildStartupParameters());
    await this.sendMessage(this.builder.buildReadyForQuery());
  }

  async sendError(error) {
    try {
      const errorMessage = this.builder.buildErrorResponse({
        severity: error.severity || 'ERROR',
        code: error.code || 'XX000',
        message: error.message,
        detail: error.detail,
        hint: error.hint
      });
      await this.sendMessage(errorMessage);
    } catch (sendError) {
      this.logger.error('Failed to send error', {
        original: error.message,
        sendError: sendError.message
      });
    }
  }

  async sendMessage(message) {
    if (this.socket.destroyed) {
      throw new Error('Socket is destroyed');
    }
    return new Promise((resolve, reject) => {
      this.socket.write(message, (err) => {
        if (err) {
          this.logger.error('Send error', { error: err.message });
          return reject(err);
        }
        this.logger.debug('Message sent', {
          size: message.length,
          type: message[0] ? String.fromCharCode(message[0]) : 'startup'
        });
        resolve();
      });
    });
  }

  async handleClose() {
    this.logger.info('Connection closed');
    try {
      await this.translator.cleanup();
    } catch (err) {
      this.logger.error('Cleanup failed', { error: err.message });
    }
    this.state = STATES.TERMINATED;
  }

  handleError(error) {
    this.logger.error('Connection error', { error: error.message });
    if (!this.socket.destroyed) this.socket.destroy();
  }

  getState() {
    return {
      state: this.state,
      authenticated: this.authenticated,
      user: this.startupParameters.user,
      database: this.startupParameters.database,
      sessionState: this.translator.getSessionState()
    };
  }
}

module.exports = PostgreSQLProtocolHandler;
