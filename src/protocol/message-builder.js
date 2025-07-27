/**
 * @file MessageBuilder.js
 * @description Handles PostgreSQL-compatible message construction for Data API Proxy.
 */

const { BufferWriter, formatValue } = require('../utils/buffer-utils');
const {
  MESSAGE_TYPES,
  AUTH_TYPES,
  TRANSACTION_STATUS,
  PG_TYPES,
  ERROR_SEVERITY,
  DEFAULT_PARAMETERS,
} = require('../utils/constants');
const { protocolLogger } = require('../config/logger');

/**
 * MessageBuilder constructs protocol-level messages for client-server communication.
 */
class MessageBuilder {
  constructor() {
    this.logger = protocolLogger;
  }

  buildAuthenticationOk() {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.AUTHENTICATION).writeInt32(8).writeInt32(AUTH_TYPES.OK);
    this.logger.debug('Built AuthenticationOk message');
    return writer.toBuffer();
  }

  buildAuthenticationCleartextPassword() {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.AUTHENTICATION).writeInt32(8).writeInt32(AUTH_TYPES.CLEARTEXT_PASSWORD);
    this.logger.debug('Built AuthenticationCleartextPassword message');
    return writer.toBuffer();
  }

  buildBackendKeyData(processId = 12345, secretKey = 67890) {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.BACKEND_KEY_DATA).writeInt32(12).writeInt32(processId).writeInt32(secretKey);
    this.logger.debug('Built BackendKeyData message', { processId, secretKey });
    return writer.toBuffer();
  }

  buildParameterStatus(name, value) {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.PARAMETER_STATUS);

    const messageLength = 4 + Buffer.byteLength(name) + 1 + Buffer.byteLength(value) + 1;
    writer.writeInt32(messageLength).writeCString(name).writeCString(value);

    this.logger.debug('Built ParameterStatus message', { name, value });
    return writer.toBuffer();
  }

  buildStartupParameters() {
    return Buffer.concat(
      Object.entries(DEFAULT_PARAMETERS).map(([name, value]) => this.buildParameterStatus(name, value))
    );
  }

  buildReadyForQuery(status = TRANSACTION_STATUS.IDLE) {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.READY_FOR_QUERY).writeInt32(5).writeByte(status);
    this.logger.debug('Built ReadyForQuery message', { status: String.fromCharCode(status) });
    return writer.toBuffer();
  }

  buildRowDescription(columnMetadata = []) {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.ROW_DESCRIPTION);

    let messageLength = 4 + 2;
    for (const column of columnMetadata) {
      messageLength += Buffer.byteLength(column.name || 'column') + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }

    writer.writeInt32(messageLength).writeInt16(columnMetadata.length);

    columnMetadata.forEach((column, index) => {
      const columnName = column.name || `column_${index}`;
      const pgType = this.mapDataTypeToPgType(column.typeName || 'text');

      writer.writeCString(columnName)
        .writeInt32(0)
        .writeInt16(index + 1)
        .writeInt32(pgType.oid)
        .writeInt16(pgType.size)
        .writeInt32(-1)
        .writeInt16(0);
    });

    this.logger.debug('Built RowDescription message', { columnCount: columnMetadata.length });
    return writer.toBuffer();
  }

  buildDataRow(record, columnMetadata = []) {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.DATA_ROW);

    const values = columnMetadata.map((column, index) => {
      const columnName = column.name || `column_${index}`;
      const value = record[columnName];
      return value == null ? null : formatValue(value, column.typeName || 'text');
    });

    let messageLength = 4 + 2 + values.reduce((len, val) => len + (val === null ? 4 : 4 + Buffer.byteLength(val)), 0);

    writer.writeInt32(messageLength).writeInt16(values.length);

    values.forEach(value => {
      if (value === null) {
        writer.writeInt32(-1);
      } else {
        const valueBytes = Buffer.from(String(value), 'utf8');
        writer.writeInt32(valueBytes.length).writeBytes(valueBytes);
      }
    });

    return writer.toBuffer();
  }

  buildCommandComplete(commandTag, rowCount = 0) {
    const tag = typeof commandTag === 'string' ? commandTag : `${commandTag} ${rowCount}`;
    const tagBytes = Buffer.from(tag, 'utf8');

    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.COMMAND_COMPLETE).writeInt32(4 + tagBytes.length + 1).writeCString(tag);

    this.logger.debug('Built CommandComplete message', { tag });
    return writer.toBuffer();
  }

  buildErrorResponse(error = {}) {
    const { severity = ERROR_SEVERITY.ERROR, code = 'XX000', message = 'Unknown error', detail = '', hint = '' } = error;

    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.ERROR_RESPONSE);

    let messageLength = 4 + this.#getCStringLength('S', severity) + this.#getCStringLength('C', code) + this.#getCStringLength('M', message);
    if (detail) messageLength += this.#getCStringLength('D', detail);
    if (hint) messageLength += this.#getCStringLength('H', hint);
    messageLength += 1; // terminator

    writer.writeInt32(messageLength);
    writer.writeByte('S'.charCodeAt(0)).writeCString(severity);
    writer.writeByte('C'.charCodeAt(0)).writeCString(code);
    writer.writeByte('M'.charCodeAt(0)).writeCString(message);
    if (detail) writer.writeByte('D'.charCodeAt(0)).writeCString(detail);
    if (hint) writer.writeByte('H'.charCodeAt(0)).writeCString(hint);
    writer.writeByte(0);

    this.logger.debug('Built ErrorResponse message', { severity, code, message });
    return writer.toBuffer();
  }

  buildNoticeResponse({ severity = 'NOTICE', message = '' } = {}) {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.NOTICE_RESPONSE);

    const messageLength = 4 + this.#getCStringLength('S', severity) + this.#getCStringLength('M', message) + 1;
    writer.writeInt32(messageLength);
    writer.writeByte('S'.charCodeAt(0)).writeCString(severity);
    writer.writeByte('M'.charCodeAt(0)).writeCString(message);
    writer.writeByte(0);

    this.logger.debug('Built NoticeResponse message', { severity, message });
    return writer.toBuffer();
  }

  buildEmptyQueryResponse() {
    const writer = new BufferWriter();
    writer.writeByte(MESSAGE_TYPES.EMPTY_QUERY).writeInt32(4);
    this.logger.debug('Built EmptyQueryResponse message');
    return writer.toBuffer();
  }

  buildParseComplete() {
    return new BufferWriter().writeByte(MESSAGE_TYPES.PARSE_COMPLETE).writeInt32(4).toBuffer();
  }

  buildBindComplete() {
    return new BufferWriter().writeByte(MESSAGE_TYPES.BIND_COMPLETE).writeInt32(4).toBuffer();
  }

  buildQueryResponse(result) {
    try {
      const messages = [];
      if (!result || (!result.records && !result.numberOfRecordsUpdated)) {
        return Buffer.concat([this.buildEmptyQueryResponse(), this.buildReadyForQuery()]);
      }

      if (result.records?.length > 0) {
        messages.push(this.buildRowDescription(result.columnMetadata || []));
        result.records.forEach(record => messages.push(this.buildDataRow(record, result.columnMetadata || [])));
        messages.push(this.buildCommandComplete('SELECT', result.records.length));
      } else if (result.numberOfRecordsUpdated !== undefined) {
        messages.push(this.buildCommandComplete(this.inferCommandTag(result), result.numberOfRecordsUpdated));
      } else {
        messages.push(this.buildCommandComplete(result.commandTag || 'OK'));
      }

      messages.push(this.buildReadyForQuery());

      this.logger.debug('Built complete query response', {
        messageCount: messages.length,
        recordCount: result.records?.length || 0,
        updatedCount: result.numberOfRecordsUpdated || 0,
      });

      return Buffer.concat(messages);
    } catch (error) {
      this.logger.error('Error building query response', { error: error.message });
      return Buffer.concat([
        this.buildErrorResponse({ message: `Error building response: ${error.message}` }),
        this.buildReadyForQuery(),
      ]);
    }
  }

  inferCommandTag(result) {
    if (result.numberOfRecordsUpdated > 0) return 'UPDATE';
    return result.commandTag || 'OK';
  }

  mapDataTypeToPgType(typeName = 'text') {
    const map = {
      varchar: [PG_TYPES.VARCHAR, -1],
      text: [PG_TYPES.TEXT, -1],
      char: [PG_TYPES.BPCHAR, -1],
      name: [PG_TYPES.NAME, 64],
      integer: [PG_TYPES.INT4, 4],
      int: [PG_TYPES.INT4, 4],
      int4: [PG_TYPES.INT4, 4],
      bigint: [PG_TYPES.INT8, 8],
      int8: [PG_TYPES.INT8, 8],
      smallint: [PG_TYPES.INT2, 2],
      int2: [PG_TYPES.INT2, 2],
      boolean: [PG_TYPES.BOOL, 1],
      bool: [PG_TYPES.BOOL, 1],
      real: [PG_TYPES.FLOAT4, 4],
      float4: [PG_TYPES.FLOAT4, 4],
      'double precision': [PG_TYPES.FLOAT8, 8],
      float8: [PG_TYPES.FLOAT8, 8],
      numeric: [PG_TYPES.NUMERIC, -1],
      decimal: [PG_TYPES.NUMERIC, -1],
      date: [PG_TYPES.DATE, 4],
      timestamp: [PG_TYPES.TIMESTAMP, 8],
      timestamptz: [PG_TYPES.TIMESTAMPTZ, 8],
      time: [PG_TYPES.TIME, 8],
      timetz: [PG_TYPES.TIMETZ, 12],
      json: [PG_TYPES.JSON, -1],
      jsonb: [PG_TYPES.JSONB, -1],
      uuid: [PG_TYPES.UUID, 16],
      bytea: [PG_TYPES.BYTEA, -1],
      oid: [PG_TYPES.OID, 4],
      _aclitem: [PG_TYPES.ACLITEM, -1],
    };

    const [oid, size] = map[typeName.toLowerCase()] || map.text;
    this.logger.debug('Mapped data type', { original: typeName, mapped: oid });
    return { oid, size };
  }

  /**
   * Calculates byte size of a field including its identifier and null terminator.
   * @private
   */
  #getCStringLength(typeChar, str) {
    return 1 + Buffer.byteLength(str, 'utf8') + 1;
  }
}

module.exports = MessageBuilder;
