// query-translator.js

const MetadataQueryHandler = require('./metadata-queries');
const DataAPIClient = require('../data-api/client');
const { translationLogger } = require('../config/logger');

/**
 * QueryTranslator handles the translation, classification, and execution of SQL queries.
 */
class QueryTranslator {
  constructor() {
    this.metadataHandler = new MetadataQueryHandler();
    this.dataApiClient = new DataAPIClient();
    this.sessionState = {
      inTransaction: false,
      parameters: new Map(),
      preparedStatements: new Map(),
    };
  }

  /**
   * Main entry point: Translates and executes SQL.
   * @param {string} sql 
   * @param {Array} parameters 
   * @returns {Promise<Object>}
   */
  async translateAndExecute(sql, parameters = []) {
    const startTime = Date.now();

    try {
      const truncatedSql = sql?.substring(0, 200);
      translationLogger.info('Processing query', {
        sql: truncatedSql + (sql.length > 200 ? '...' : ''),
        parameterCount: parameters.length,
      });

      const cleanSql = this.cleanSql(sql);
      if (!cleanSql) return this.createEmptyQueryResponse();

      // Transaction control (BEGIN, COMMIT, ROLLBACK)
      const transactionResult = await this.handleTransactionStatement(cleanSql);
      if (transactionResult) return this.logAndReturn('transaction', startTime, transactionResult);

      // SET statement (e.g., SET timezone = 'UTC')
      const setResult = this.handleSetStatement(cleanSql);
      if (setResult) return this.logAndReturn('set', startTime, setResult);

      // SHOW statement (e.g., SHOW timezone)
      const showResult = this.handleShowStatement(cleanSql);
      if (showResult) return this.logAndReturn('show', startTime, showResult);

      // Default: forward to Data API
      const result = await this.dataApiClient.executeStatement(cleanSql, parameters);
      return this.logAndReturn('data', startTime, result);

    } catch (error) {
      this.handleQueryError(sql, error, startTime);
      throw error;
    }
  }

  /**
   * Normalize and trim SQL input.
   * @param {string} sql 
   * @returns {string}
   */
  cleanSql(sql) {
    if (typeof sql !== 'string') return '';

    const trimmed = sql.trim().replace(/;+$/, '');
    if (!trimmed) return '';

    if (trimmed !== sql.trim()) {
      translationLogger.debug('SQL cleaned', {
        original: sql.substring(0, 100),
        cleaned: trimmed.substring(0, 100),
      });
    }

    return trimmed;
  }

  /**
   * Handles transaction commands like BEGIN, COMMIT, ROLLBACK.
   * @param {string} sql 
   * @returns {Promise<Object|null>}
   */
  async handleTransactionStatement(sql) {
    const normalized = sql.toLowerCase();

    if (/^begin(;)?$|^start transaction(;)?$/.test(normalized)) {
      translationLogger.info('Transaction started');
      await this.dataApiClient.beginTransaction();
      this.sessionState.inTransaction = true;
      return this.createSimpleResponse('BEGIN');
    }

    if (/^commit(;)?$|^commit work(;)?$/.test(normalized)) {
      if (this.dataApiClient.isInTransaction()) await this.dataApiClient.commitTransaction();
      translationLogger.info('Transaction committed');
      this.sessionState.inTransaction = false;
      return this.createSimpleResponse('COMMIT');
    }

    if (/^rollback(;)?$|^rollback work(;)?$/.test(normalized)) {
      if (this.dataApiClient.isInTransaction()) await this.dataApiClient.rollbackTransaction();
      translationLogger.info('Transaction rolled back');
      this.sessionState.inTransaction = false;
      return this.createSimpleResponse('ROLLBACK');
    }

    return null;
  }

  /**
   * Parses and stores SET statement values.
   * @param {string} sql 
   * @returns {Object|null}
   */
  handleSetStatement(sql) {
    const match = sql.match(/^SET\s+(\w+)\s*=\s*(.+)$/i);
    if (!match) return null;

    const [, parameter, value] = match;
    const cleanValue = value.replace(/^['"]|['"]$/g, '');

    this.sessionState.parameters.set(parameter.toLowerCase(), cleanValue);
    translationLogger.debug('Session parameter set', { parameter, value: cleanValue });

    return this.createSimpleResponse('SET');
  }

  /**
   * Returns mock values for SHOW statements.
   * @param {string} sql 
   * @returns {Object|null}
   */
  handleShowStatement(sql) {
    const match = sql.toLowerCase().match(/^show\s+(.+)/);
    if (!match) return null;

    const parameter = match[1].trim();
    let value = this.sessionState.parameters.get(parameter) || 'unknown';

    switch (parameter) {
      case 'server_version':
        value = process.env.MOCK_PG_VERSION || '14.9';
        break;
      case 'server_encoding':
      case 'client_encoding':
        value = 'UTF8';
        break;
      case 'timezone':
      case 'time zone':
        value = 'UTC';
        break;
      case 'datestyle':
        value = 'ISO, MDY';
        break;
    }

    return {
      records: [{ [parameter]: value }],
      columnMetadata: [{ name: parameter, typeName: 'text', nullable: 0 }],
      numberOfRecordsUpdated: 0,
    };
  }

  /**
   * Handles prepared statement creation.
   */
  async handlePreparedStatement(name, sql, parameters = []) {
    this.sessionState.preparedStatements.set(name, {
      sql,
      createdAt: new Date(),
    });

    return this.translateAndExecute(sql, parameters);
  }

  /**
   * Executes a named prepared statement.
   */
  async executePreparedStatement(name, parameters = []) {
    const statement = this.sessionState.preparedStatements.get(name);
    if (!statement) throw new Error(`Prepared statement "${name}" does not exist`);

    return this.translateAndExecute(statement.sql, parameters);
  }

  /**
   * Removes a prepared statement.
   */
  closePreparedStatement(name) {
    const existed = this.sessionState.preparedStatements.delete(name);

    return this.createSimpleResponse('DEALLOCATE');
  }

  /**
   * Logs query result and returns it.
   */
  logAndReturn(type, startTime, result) {
    const duration = Date.now() - startTime;

    translationLogger.info('Query completed', {
      type,
      duration,
      recordCount: result.records?.length || 0,
      columnCount: result.columnMetadata?.length || 0,
      recordsUpdated: result.numberOfRecordsUpdated || 0,
    });

    return result;
  }

  /**
   * Logs a query failure.
   */
  handleQueryError(sql, error, startTime) {
    const duration = Date.now() - startTime;

    translationLogger.error('Query processing failed', {
      error: error.message,
      sql: sql.substring(0, 100),
      duration,
      stack: error.stack,
    });
  }

  /**
   * Utility: Builds a default response structure.
   */
  createSimpleResponse(commandTag) {
    return {
      records: [],
      columnMetadata: [],
      numberOfRecordsUpdated: 0,
      commandTag,
    };
  }

  createEmptyQueryResponse() {
    return this.createSimpleResponse('EMPTY');
  }

  /**
   * Returns current session state.
   */
  getSessionState() {
    return {
      inTransaction: this.sessionState.inTransaction,
      parameters: Object.fromEntries(this.sessionState.parameters),
      isDataApiTransaction: this.dataApiClient.isInTransaction(),
      transactionId: this.dataApiClient.getTransactionId(),
    };
  }

  /**
   * Resets session state and cleans up.
   */
  async cleanup() {
    try {
      translationLogger.info('Cleaning up session state');
      await this.dataApiClient.cleanup();
      this.sessionState.parameters.clear();
      this.sessionState.preparedStatements.clear();
      this.sessionState.inTransaction = false;
    } catch (err) {
      translationLogger.error('Cleanup failed', { error: err.message });
    }
  }
}

module.exports = QueryTranslator;
