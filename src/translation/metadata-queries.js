const { SYSTEM_QUERIES } = require('../utils/constants');
const { translationLogger } = require('../config/logger');

/**
 * Handles PostgreSQL system/metadata queries by mocking or partially executing them.
 * Supports compatibility with tools like pgAdmin.
 */
class MetadataQueryHandler {
  constructor() {
    this.logger = translationLogger;
    this.mockResponses = new Map();
    this.#initMockResponses();
  }

  /**
   * Determines if the given SQL is a known PostgreSQL system/metadata query.
   * @param {string} sql - The raw SQL statement.
   * @returns {string|null} - A query type identifier or null if not system query.
   */
  isSystemQuery(sql) {
    const normalizedSql = sql.trim().toLowerCase();
    this.logger.debug('Checking if system query', { sql: sql.slice(0, 100) });

    const systemPatterns = [
      /pg_catalog\./, /information_schema\./,
      /pg_class/, /pg_namespace/, /pg_attribute/, /pg_type/,
      /pg_index/, /pg_constraint/, /pg_proc/, /pg_stat_activity/,
      /pg_tables/, /pg_database/, /proname/, /attname/, /typname/,
      /relname/, /datname/, /nspname/, /version\(\)/,
      /current_schema/, /current_user/, /current_database/,
      /show\s+/, /pg_settings/
    ];

    for (const pattern of systemPatterns) {
      if (pattern.test(normalizedSql)) {
        this.logger.debug('Detected system query by pattern', {
          pattern: pattern.toString(),
          sql: sql.slice(0, 100)
        });
        return 'GENERIC_SYSTEM';
      }
    }

    for (const [type, pattern] of Object.entries(SYSTEM_QUERIES)) {
      if (pattern.test(normalizedSql)) {
        this.logger.debug('Matched system query type', { queryType: type });
        return type;
      }
    }

    return null;
  }

  /**
   * Handles system/metadata queries by returning mocks or partially querying the real database.
   * @param {string} sql - The SQL query string.
   * @param {string} queryType - Identified query type.
   * @returns {Promise<object>} - A query result object with records and column metadata.
   */
  async handleSystemQuery(sql, queryType) {
    this.logger.info('Handling system query', {
      queryType,
      sql: sql.slice(0, 200)
    });

    const normalizedSql = sql.trim().toLowerCase();

    // Table listing queries (real call to Aurora)
    if (
      normalizedSql.includes('information_schema.tables') ||
      (normalizedSql.includes('table_name') && normalizedSql.includes('information_schema'))
    ) {
      return await this.#handleTableListQuery(sql);
    }

    // Direct system function mocks
    const mocks = {
      VERSION: 'version',
      CURRENT_SCHEMA: 'current_schema',
      CURRENT_USER: 'current_user',
      CURRENT_DATABASE: 'current_database'
    };

    if (mocks[queryType]) return this.mockResponses.get(mocks[queryType]);

    if (normalizedSql.includes('pg_database') || normalizedSql.includes('datname')) {
      return this.#mockDatabaseList();
    }

    // Generic fallback for unknown system queries
    const columnMetadata = this.#extractExpectedColumns(normalizedSql);
    this.logger.info('Returning generic system response', {
      columns: columnMetadata.map(c => c.name)
    });

    return {
      records: [],
      columnMetadata,
      numberOfRecordsUpdated: 0
    };
  }

  /**
   * Extracts likely column names based on known system columns in the SQL.
   * @param {string} sql - Normalized SQL string.
   * @returns {Array<object>} - Array of column metadata.
   */
  #extractExpectedColumns(sql) {
    const commonColumns = {
      proname:     { name: 'proname', typeName: 'name', nullable: 0 },
      attname:     { name: 'attname', typeName: 'name', nullable: 0 },
      typname:     { name: 'typname', typeName: 'name', nullable: 0 },
      relname:     { name: 'relname', typeName: 'name', nullable: 0 },
      nspname:     { name: 'nspname', typeName: 'name', nullable: 0 },
      datname:     { name: 'datname', typeName: 'name', nullable: 0 },
      oid:         { name: 'oid', typeName: 'oid', nullable: 0 },
      relkind:     { name: 'relkind', typeName: 'char', nullable: 0 },
      attnum:      { name: 'attnum', typeName: 'int2', nullable: 0 },
      atttypid:    { name: 'atttypid', typeName: 'oid', nullable: 0 }
    };

    const found = Object.values(commonColumns).filter(col => sql.includes(col.name));
    return found.length > 0 ? found : [{ name: 'result', typeName: 'text', nullable: 1 }];
  }

  /**
   * Simulates a pg_database result row.
   * @returns {object} - Mocked database list result.
   */
  #mockDatabaseList() {
    return {
      records: [{
        did: 12345,
        datname: process.env.RDS_DATABASE_NAME || 'postgres',
        datallowconn: true,
        serverencoding: 'UTF8',
        cancreate: false,
        datistemplate: false
      }],
      columnMetadata: [
        { name: 'did', typeName: 'oid', nullable: 0 },
        { name: 'datname', typeName: 'name', nullable: 0 },
        { name: 'datallowconn', typeName: 'bool', nullable: 0 },
        { name: 'serverencoding', typeName: 'text', nullable: 0 },
        { name: 'cancreate', typeName: 'bool', nullable: 0 },
        { name: 'datistemplate', typeName: 'bool', nullable: 0 }
      ]
    };
  }

  /**
   * Runs the table listing query against Aurora via Data API.
   * @param {string} sql - Original SQL query string.
   * @returns {Promise<object>} - Query result or fallback mock.
   */
  async #handleTableListQuery(sql) {
    this.logger.info('Forwarding table query to Aurora');
    try {
      const DataAPIClient = require('../data-api/client');
      const dataApiClient = new DataAPIClient();
      const result = await dataApiClient.executeStatement(sql);
      this.logger.info('Aurora table query successful', {
        records: result?.records?.length || 0
      });
      return result;
    } catch (err) {
      this.logger.error('Aurora query failed, using fallback', { error: err.message });
      return {
        records: [{ table_name: 'fallback_table' }],
        columnMetadata: [{ name: 'table_name', typeName: 'varchar', nullable: 0 }]
      };
    }
  }

  /**
   * Initializes the static mock responses for common system queries.
   */
  #initMockResponses() {
    const set = (key, records, columnMetadata) =>
      this.mockResponses.set(key, { records, columnMetadata });

    set('version', [{
      version: process.env.MOCK_SERVER_VERSION ||
        'PostgreSQL 14.9 on x86_64-pc-linux-gnu'
    }], [{ name: 'version', typeName: 'text', nullable: 0 }]);

    set('current_schema', [{ current_schema: 'public' }],
      [{ name: 'current_schema', typeName: 'name', nullable: 0 }]);

    set('current_user', [{ current_user: 'postgres' }],
      [{ name: 'current_user', typeName: 'name', nullable: 0 }]);

    set('current_database', [{
      current_database: process.env.RDS_DATABASE_NAME || 'postgres'
    }], [{ name: 'current_database', typeName: 'name', nullable: 0 }]);

    const emptySystemTables = [
      'pg_class', 'pg_namespace', 'pg_attribute', 'pg_type',
      'pg_index', 'pg_constraint', 'pg_proc',
      'pg_stat_activity', 'pg_tables'
    ];

    for (const table of emptySystemTables) {
      set(table, [], []);
    }
  }
}

module.exports = MetadataQueryHandler;
