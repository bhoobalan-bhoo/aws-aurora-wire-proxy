'use strict';

/**
 * Utilities for reading and writing PostgreSQL wire protocol buffers.
 */

const NULL_TERMINATOR = 0;

class BufferWriter {
  /**
   * @param {number} initialSize
   */
  constructor(initialSize = 1024) {
    this.buffer = Buffer.alloc(initialSize);
    this.position = 0;
  }

  ensureCapacity(additionalBytes) {
    const required = this.position + additionalBytes;
    if (required > this.buffer.length) {
      const newSize = Math.max(required, this.buffer.length * 2);
      const newBuffer = Buffer.alloc(newSize);
      this.buffer.copy(newBuffer);
      this.buffer = newBuffer;
    }
  }

  writeByte(value) {
    this.ensureCapacity(1);
    this.buffer.writeUInt8(value, this.position++);
    return this;
  }

  writeInt16(value) {
    this.ensureCapacity(2);
    this.buffer.writeInt16BE(value, this.position);
    this.position += 2;
    return this;
  }

  writeInt32(value) {
    this.ensureCapacity(4);
    this.buffer.writeInt32BE(value, this.position);
    this.position += 4;
    return this;
  }

  writeCString(str) {
    const stringValue = String(str);
    const bytes = Buffer.from(stringValue, 'utf8');
    this.ensureCapacity(bytes.length + 1);
    bytes.copy(this.buffer, this.position);
    this.position += bytes.length;
    this.buffer.writeUInt8(NULL_TERMINATOR, this.position++);
    return this;
  }

  writeBytes(bytes) {
    this.ensureCapacity(bytes.length);
    bytes.copy(this.buffer, this.position);
    this.position += bytes.length;
    return this;
  }

  writeString(str) {
    const stringValue = String(str);
    const bytes = Buffer.from(stringValue, 'utf8');
    this.writeInt32(bytes.length);
    this.writeBytes(bytes);
    return this;
  }

  toBuffer() {
    return this.buffer.subarray(0, this.position);
  }

  getPosition() {
    return this.position;
  }

  reset() {
    this.position = 0;
    return this;
  }
}

class BufferReader {
  /**
   * @param {Buffer} buffer
   */
  constructor(buffer) {
    this.buffer = buffer;
    this.position = 0;
  }

  canRead(bytes) {
    return this.position + bytes <= this.buffer.length;
  }

  readByte() {
    this.#checkAvailable(1, 'byte');
    return this.buffer.readUInt8(this.position++);
  }

  readInt16() {
    this.#checkAvailable(2, 'int16');
    const val = this.buffer.readInt16BE(this.position);
    this.position += 2;
    return val;
  }

  readInt32() {
    this.#checkAvailable(4, 'int32');
    const val = this.buffer.readInt32BE(this.position);
    this.position += 4;
    return val;
  }

  readCString() {
    const start = this.position;
    let end = start;

    while (end < this.buffer.length && this.buffer[end] !== NULL_TERMINATOR) {
      end++;
    }

    if (end >= this.buffer.length) {
      throw new Error('CString read failed: null terminator not found.');
    }

    const str = this.buffer.subarray(start, end).toString('utf8');
    this.position = end + 1;
    return str;
  }

  readBytes(length) {
    this.#checkAvailable(length, `${length} bytes`);
    const slice = this.buffer.subarray(this.position, this.position + length);
    this.position += length;
    return slice;
  }

  readString() {
    const length = this.readInt32();
    return this.readBytes(length).toString('utf8');
  }

  remaining() {
    return this.buffer.length - this.position;
  }

  getPosition() {
    return this.position;
  }

  setPosition(position) {
    if (position < 0 || position > this.buffer.length) {
      throw new RangeError('Invalid buffer position');
    }
    this.position = position;
    return this;
  }

  skip(bytes) {
    this.#checkAvailable(bytes, `${bytes} bytes to skip`);
    this.position += bytes;
    return this;
  }

  #checkAvailable(bytes, label) {
    if (!this.canRead(bytes)) {
      throw new Error(`Cannot read ${label}: insufficient buffer data`);
    }
  }
}

/**
 * Create a PostgreSQL protocol message buffer.
 * @param {number|null} type - 1-byte message type or null
 * @param {Buffer|string|null} data - Message payload
 * @returns {Buffer}
 */
const createMessage = (type, data) => {
  const writer = new BufferWriter();

  if (type !== null) writer.writeByte(type);

  if (data) {
    const dataBuffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data));
    writer.writeInt32(dataBuffer.length + 4); // Include the length field
    writer.writeBytes(dataBuffer);
  } else {
    writer.writeInt32(4); // Only length field
  }

  return writer.toBuffer();
};

/**
 * Parse the type and length of a PostgreSQL protocol message.
 * @param {Buffer} buffer
 * @returns {{type: number, length: number, totalLength: number}|null}
 */
const parseMessageLength = (buffer) => {
  if (buffer.length < 5) return null;

  const reader = new BufferReader(buffer);
  const type = reader.readByte();
  const length = reader.readInt32();

  return {
    type,
    length,
    totalLength: length + 1 // Include type byte
  };
};

/**
 * Format a JavaScript value as a PostgreSQL wire protocol string.
 * @param {*} value
 * @param {string} pgType
 * @returns {string|null}
 */
const formatValue = (value, pgType) => {
  if (value === null || value === undefined) return null;

  switch (pgType.toLowerCase()) {
    case 'boolean':
    case 'bool':
      return value ? 't' : 'f';
    case 'int2':
    case 'int4':
    case 'int8':
    case 'smallint':
    case 'integer':
    case 'bigint':
    case 'float4':
    case 'float8':
    case 'real':
    case 'double precision':
    case 'numeric':
      return String(value);
    case 'text':
    case 'varchar':
    case 'char':
    case 'name':
      return String(value);
    case 'date':
      return value instanceof Date ? value.toISOString().split('T')[0] : String(value);
    case 'timestamp':
    case 'timestamptz':
      return value instanceof Date ? value.toISOString() : String(value);
    case 'json':
    case 'jsonb':
      return typeof value === 'string' ? value : JSON.stringify(value);
    default:
      return String(value);
  }
};

module.exports = {
  BufferWriter,
  BufferReader,
  createMessage,
  parseMessageLength,
  formatValue
};
