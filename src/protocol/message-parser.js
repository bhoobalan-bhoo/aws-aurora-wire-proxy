const { BufferReader } = require('../utils/buffer-utils');
const { MESSAGE_TYPES } = require('../utils/constants');
const { protocolLogger } = require('../config/logger');

/**
 * Class responsible for parsing PostgreSQL protocol messages.
 */
class MessageParser {
  constructor() {
    this.logger = protocolLogger;
  }

  /**
   * Parses a startup message (no type byte).
   * @param {Buffer} buffer
   * @returns {Object}
   */
  parseStartupMessage(buffer) {
    try {
      const reader = new BufferReader(buffer);
      const length = reader.readInt32();
      const protocolVersion = reader.readInt32();
      const parameters = {};

      while (reader.remaining() > 1) {
        const name = reader.readCString();
        if (!name) break;
        const value = reader.readCString();
        parameters[name] = value;
      }

      this.logger.debug('Parsed startup message', {
        protocolVersion: protocolVersion.toString(16),
        parameterCount: Object.keys(parameters).length,
        user: parameters.user,
        database: parameters.database
      });

      return { type: 'startup', length, protocolVersion, parameters };
    } catch (error) {
      this.logger.error('Error parsing startup message', { error: error.message });
      throw new Error(`Invalid startup message: ${error.message}`);
    }
  }

  /**
   * Parses a PostgreSQL message with a type byte.
   * @param {Buffer} buffer
   * @returns {Object}
   */
  parseMessage(buffer) {
    if (buffer.length < 5) throw new Error('Message too short');

    try {
      const reader = new BufferReader(buffer);
      const messageType = reader.readByte();
      const length = reader.readInt32();

      if (length < 4 || length > buffer.length - 1) throw new Error(`Invalid message length: ${length}`);

      const messageData = reader.readBytes(length - 4);

      const typeMap = {
        [MESSAGE_TYPES.QUERY]: this.parseQuery,
        [MESSAGE_TYPES.PARSE]: this.parseParse,
        [MESSAGE_TYPES.BIND]: this.parseBind,
        [MESSAGE_TYPES.EXECUTE]: this.parseExecute,
        [MESSAGE_TYPES.DESCRIBE]: this.parseDescribe,
        [MESSAGE_TYPES.CLOSE]: this.parseClose,
        [MESSAGE_TYPES.SYNC]: this.parseSync,
        [MESSAGE_TYPES.TERMINATE]: this.parseTerminate,
        [MESSAGE_TYPES.PASSWORD]: this.parsePasswordResponse
      };

      const handler = typeMap[messageType];
      const parsedMessage = handler ? handler.call(this, messageData) : {
        type: 'unknown', messageType, data: messageData
      };

      if (!handler) {
        this.logger.warn('Unknown message type', {
          messageType: messageType.toString(16),
          char: String.fromCharCode(messageType)
        });
      }

      return { ...parsedMessage, messageType, length };
    } catch (error) {
      this.logger.error('Error parsing message', { error: error.message });
      throw new Error(`Message parsing failed: ${error.message}`);
    }
  }

  parseQuery(data) {
    const reader = new BufferReader(data);
    const sql = reader.readCString();
    this.logger.debug('Parsed query message', {
      sql: sql.length > 100 ? sql.substring(0, 100) + '...' : sql
    });
    return { type: 'query', sql };
  }

  parseParse(data) {
    const reader = new BufferReader(data);
    const statementName = reader.readCString();
    const sql = reader.readCString();
    const parameterCount = reader.readInt16();
    const parameterTypes = Array.from({ length: parameterCount }, () => reader.readInt32());

    this.logger.debug('Parsed parse message', {
      statementName, sql: sql.substring(0, 100), parameterCount
    });
    return { type: 'parse', statementName, sql, parameterTypes };
  }

  parseBind(data) {
    const reader = new BufferReader(data);
    const portalName = reader.readCString();
    const statementName = reader.readCString();
    const parameterFormatCount = reader.readInt16();
    const parameterFormats = Array.from({ length: parameterFormatCount }, () => reader.readInt16());
    const parameterCount = reader.readInt16();
    const parameters = Array.from({ length: parameterCount }, () => {
      const length = reader.readInt32();
      return length === -1 ? null : reader.readBytes(length).toString('utf8');
    });
    const resultFormatCount = reader.readInt16();
    const resultFormats = Array.from({ length: resultFormatCount }, () => reader.readInt16());

    this.logger.debug('Parsed bind message', {
      portalName, statementName, parameterCount, resultFormatCount
    });
    return { type: 'bind', portalName, statementName, parameterFormats, parameters, resultFormats };
  }

  parseExecute(data) {
    const reader = new BufferReader(data);
    const portalName = reader.readCString();
    const maxRows = reader.readInt32();
    this.logger.debug('Parsed execute message', { portalName, maxRows });
    return { type: 'execute', portalName, maxRows };
  }

  parseDescribe(data) {
    const reader = new BufferReader(data);
    const objectType = String.fromCharCode(reader.readByte());
    const objectName = reader.readCString();
    this.logger.debug('Parsed describe message', { objectType, objectName });
    return { type: 'describe', objectType, objectName };
  }

  parseClose(data) {
    const reader = new BufferReader(data);
    const objectType = String.fromCharCode(reader.readByte());
    const objectName = reader.readCString();
    this.logger.debug('Parsed close message', { objectType, objectName });
    return { type: 'close', objectType, objectName };
  }

  parseSync() {
    this.logger.debug('Parsed sync message');
    return { type: 'sync' };
  }

  parseTerminate() {
    this.logger.debug('Parsed terminate message');
    return { type: 'terminate' };
  }

  parsePasswordResponse(data) {
    const reader = new BufferReader(data);
    const password = reader.readCString();
    this.logger.debug('Parsed password response message', { passwordLength: password.length });
    return { type: 'password', password };
  }

  isCompleteMessage(buffer) {
    if (buffer.length < 4) return false;

    if (buffer.length >= 8) {
      const potentialLength = buffer.readInt32BE(0);
      const potentialVersion = buffer.readInt32BE(4);

      if (potentialLength === 8 && potentialVersion === 0x04d2162f) return true;
      if (potentialLength <= buffer.length && (potentialVersion >> 16) === 3) return buffer.length >= potentialLength;
    }

    if (buffer.length < 5) return false;

    const length = buffer.readInt32BE(1);
    return buffer.length >= 1 + length;
  }

  extractMessage(buffer) {
    if (!this.isCompleteMessage(buffer)) return { message: null, remaining: buffer };

    if (buffer.length >= 8) {
      const reader = new BufferReader(buffer);
      const length = reader.readInt32();
      const code = reader.readInt32();

      if (length === 8 && code === 0x04d2162f) return {
        message: buffer.subarray(0, length),
        remaining: buffer.subarray(length),
        isStartup: false,
        isSSL: true
      };

      if (length > 8 && (code >> 16) === 3) return {
        message: buffer.subarray(0, length),
        remaining: buffer.subarray(length),
        isStartup: true
      };
    }

    if (buffer.length >= 5) {
      const reader = new BufferReader(buffer);
      reader.skip(1);
      const messageLength = reader.readInt32();
      const totalLength = 1 + messageLength;
      return {
        message: buffer.subarray(0, totalLength),
        remaining: buffer.subarray(totalLength),
        isStartup: false
      };
    }

    return { message: null, remaining: buffer };
  }

  parseBuffer(buffer) {
    const messages = [];
    let remaining = buffer;

    this.logger.debug('Parsing buffer', {
      size: buffer.length,
      hex: buffer.toString('hex')
    });

    while (remaining.length > 0) {
      const result = this.extractMessage(remaining);
      if (!result.message) break;

      try {
        const parsed = result.isSSL
          ? { type: 'ssl_request', isSSL: true, length: 8, code: 0x04d2162f }
          : result.isStartup
          ? this.parseStartupMessage(result.message)
          : this.parseMessage(result.message);

        messages.push(parsed);
        this.logger.debug('Parsed message successfully', parsed);
      } catch (err) {
        this.logger.error('Error parsing message', {
          error: err.message,
          messageHex: result.message.toString('hex'),
          isStartup: result.isStartup,
          isSSL: result.isSSL
        });
      }

      remaining = result.remaining;
    }

    this.logger.debug('Buffer parsing complete', {
      messagesFound: messages.length,
      remainingBytes: remaining.length
    });

    return { messages, remaining };
  }
}

module.exports = MessageParser;
