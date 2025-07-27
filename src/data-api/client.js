const { ExecuteStatementCommand, BeginTransactionCommand, CommitTransactionCommand, RollbackTransactionCommand } = require('@aws-sdk/client-rds-data');
const { createRDSDataClient, clusterConfig } = require('../config/aws');
const { dataApiLogger } = require('../config/logger');

class DataAPIClient {
  /**
   * Initializes the DataAPI client with necessary configurations.
   * This includes setting up the RDS Data API client and initializing
   * transaction and prepared statement caches.
   * @constructor
   * @param {Object} [options] - Optional configuration parameters.
   * @param {string} [options.clusterArn] - The ARN of the RDS cluster.
   * @param {string} [options.database] - The name of the database to connect to.
   * @param {string} [options.secretArn] - The ARN of the secret containing database credentials.
   * @param {number} [options.processId] - Optional process ID for the connection.
   * @param {number} [options.secretKey] - Optional secret key for the connection.
   * @param {string} [options.serverVersion] - PostgreSQL server version.
   * @param {string} [options.serverEncoding] - Server encoding (default: 'UTF8').
   * @param {string} [options.clientEncoding] - Client encoding (default: 'UTF8').
   * @param {string} [options.timezone] - Timezone setting (default: 'UTC').
   * @param {string} [options.transactionId] - Optional transaction ID for managing transactions.
   * @param {Map} [options.preparedStatements] - Map to store prepared statements.
   * @param {string} [options.currentTableName] - Name of the currently active table.
   * @param {Map} [options.tableMetadataCache] - Cache for table metadata to avoid redundant queries.
  */
  constructor() {
    this.client = createRDSDataClient();
    this.transactionId = null;
    this.preparedStatements = new Map(); // Cache for prepared statements
    this.currentTableName = null;
    this.tableMetadataCache = new Map(); // Cache table metadata
    this.connectionInfo = {
      processId: Math.floor(Math.random() * 10000),
      secretKey: Math.floor(Math.random() * 10000),
      serverVersion: '14.9', // Mimic PostgreSQL 14.9
      serverEncoding: 'UTF8',
      clientEncoding: 'UTF8',
      timezone: 'UTC'
    };

    dataApiLogger.info('DataAPI client initialized', {
      clusterArn: clusterConfig.clusterArn,
      database: clusterConfig.database,
      processId: this.connectionInfo.processId
    });
  }

  // Handle PostgreSQL startup message
  async handleStartup(params = {}) {
    dataApiLogger.debug('Handling PostgreSQL startup', { params });

    return {
      readyForQuery: 'I', // Idle
      backendKeyData: {
        processId: this.connectionInfo.processId,
        secretKey: this.connectionInfo.secretKey
      },
      parameterStatus: {
        server_version: this.connectionInfo.serverVersion,
        server_encoding: this.connectionInfo.serverEncoding,
        client_encoding: this.connectionInfo.clientEncoding,
        TimeZone: this.connectionInfo.timezone,
        integer_datetimes: 'on',
        standard_conforming_strings: 'on'
      }
    };
  }

  // Handle prepared statement creation (Parse message)
  async handleParse(statementName, query, paramTypes = []) {
    dataApiLogger.debug('Parsing prepared statement', {
      statementName,
      query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
      paramCount: paramTypes.length
    });

    // Sanitize and enhance the query
    const enhancedQuery = await this.enhanceSQLForPgAdmin(this.sanitizeSQL(query));

    // Store prepared statement
    this.preparedStatements.set(statementName, {
      query: enhancedQuery,
      originalQuery: query,
      paramTypes,
      paramCount: paramTypes.length,
      createdAt: Date.now()
    });

    return {
      parseComplete: true,
      statementName,
      paramCount: paramTypes.length
    };
  }

  // Handle parameter binding (Bind message)
  async handleBind(portalName, statementName, parameters = []) {
    const preparedStmt = this.preparedStatements.get(statementName);

    if (!preparedStmt) {
      throw new Error(`Prepared statement "${statementName}" does not exist`);
    }

    dataApiLogger.debug('Binding parameters to prepared statement', {
      portalName,
      statementName,
      expectedParams: preparedStmt.paramCount,
      providedParams: parameters.length
    });

    // Validate parameter count
    if (parameters.length !== preparedStmt.paramCount) {
      throw new Error(
        `bind message supplies ${parameters.length} parameters, but prepared statement "${statementName}" requires ${preparedStmt.paramCount}`
      );
    }

    // Store portal (bound statement)
    this.preparedStatements.set(portalName, {
      ...preparedStmt,
      boundParameters: parameters,
      isPortal: true
    });

    return {
      bindComplete: true,
      portalName
    };
  }

  // Handle query execution (Execute message)
  async handleExecute(portalName, maxRows = 0) {
    const portal = this.preparedStatements.get(portalName);

    if (!portal || !portal.isPortal) {
      throw new Error(`Portal "${portalName}" does not exist`);
    }

    dataApiLogger.debug('Executing portal', {
      portalName,
      maxRows,
      paramCount: portal.boundParameters?.length || 0
    });

    const result = await this.executeStatement(portal.query, portal.boundParameters || []);

    // Add PostgreSQL-specific response fields
    result.commandTag = this.generateCommandTag(portal.query, result);
    result.portalSuspended = maxRows > 0 && result.records.length > maxRows;

    if (maxRows > 0 && result.records.length > maxRows) {
      result.records = result.records.slice(0, maxRows);
    }

    return result;
  }

  // Generate PostgreSQL command tag
  generateCommandTag(query, result) {
    const queryType = query.trim().split(/\s+/)[0].toUpperCase();

    switch (queryType) {
      case 'SELECT':
        return `SELECT ${result.records.length}`;
      case 'INSERT':
        return `INSERT 0 ${result.numberOfRecordsUpdated || 1}`;
      case 'UPDATE':
        return `UPDATE ${result.numberOfRecordsUpdated || 0}`;
      case 'DELETE':
        return `DELETE ${result.numberOfRecordsUpdated || 0}`;
      case 'CREATE':
        return 'CREATE TABLE';
      case 'DROP':
        return 'DROP TABLE';
      case 'ALTER':
        return 'ALTER TABLE';
      default:
        return queryType;
    }
  }

  // Get comprehensive table metadata
  async getTableMetadata(tableName, schemaName = 'public') {
    const cacheKey = `${schemaName}.${tableName}`;

    if (this.tableMetadataCache.has(cacheKey)) {
      return this.tableMetadataCache.get(cacheKey);
    }

    try {
      const metadataQuery = `
        WITH table_info AS (
          SELECT 
            t.oid as table_oid,
            t.relname as table_name,
            t.relhasoids,
            n.nspname as schema_name
          FROM pg_class t
          JOIN pg_namespace n ON n.oid = t.relnamespace
          WHERE t.relname = $1 AND n.nspname = $2
        ),
        column_info AS (
          SELECT 
            a.attname as column_name,
            a.atttypid as type_oid,
            a.attnum as ordinal_position,
            a.attnotnull as not_null,
            a.atthasdef as has_default,
            pg_get_expr(d.adbin, d.adrelid) as default_value,
            t.typname as data_type,
            t.typlen as type_length,
            a.atttypmod as type_modifier,
            CASE 
              WHEN t.typname = 'varchar' OR t.typname = 'char' THEN a.atttypmod - 4
              WHEN t.typname = 'numeric' THEN (a.atttypmod - 4) >> 16
              ELSE NULL
            END as character_maximum_length,
            CASE 
              WHEN t.typname = 'numeric' THEN (a.atttypmod - 4) & 65535
              ELSE NULL
            END as numeric_scale
          FROM pg_attribute a
          JOIN pg_type t ON t.oid = a.atttypid
          LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
          JOIN table_info ti ON ti.table_oid = a.attrelid
          WHERE a.attnum > 0 AND NOT a.attisdropped
          ORDER BY a.attnum
        ),
        constraint_info AS (
          SELECT 
            kcu.column_name,
            tc.constraint_type,
            tc.constraint_name,
            CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN TRUE ELSE FALSE END as is_primary_key,
            CASE WHEN tc.constraint_type = 'UNIQUE' THEN TRUE ELSE FALSE END as is_unique,
            CASE WHEN tc.constraint_type = 'FOREIGN KEY' THEN TRUE ELSE FALSE END as is_foreign_key
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          WHERE tc.table_name = $1 AND tc.table_schema = $2
        )
        SELECT 
          ti.*,
          ci.column_name,
          ci.type_oid,
          ci.ordinal_position,
          ci.not_null,
          ci.has_default,
          ci.default_value,
          ci.data_type,
          ci.type_length,
          ci.type_modifier,
          ci.character_maximum_length,
          ci.numeric_scale,
          COALESCE(con.is_primary_key, FALSE) as is_primary_key,
          COALESCE(con.is_unique, FALSE) as is_unique,
          COALESCE(con.is_foreign_key, FALSE) as is_foreign_key,
          con.constraint_name
        FROM table_info ti
        JOIN column_info ci ON TRUE
        LEFT JOIN constraint_info con ON con.column_name = ci.column_name
        ORDER BY ci.ordinal_position
      `;

      const result = await this.executeRawQuery(metadataQuery, [tableName, schemaName]);

      if (!result.records || result.records.length === 0) {
        throw new Error(`Table ${schemaName}.${tableName} not found`);
      }

      const metadata = {
        tableOid: result.records[0].table_oid,
        tableName: result.records[0].table_name,
        schemaName: result.records[0].schema_name,
        hasOids: result.records[0].relhasoids,
        columns: result.records.map(row => ({
          name: row.column_name,
          typeOid: row.type_oid,
          ordinalPosition: row.ordinal_position,
          notNull: row.not_null,
          hasDefault: row.has_default,
          defaultValue: row.default_value,
          dataType: row.data_type,
          typeLength: row.type_length,
          typeModifier: row.type_modifier,
          characterMaximumLength: row.character_maximum_length,
          numericScale: row.numeric_scale,
          isPrimaryKey: row.is_primary_key,
          isUnique: row.is_unique,
          isForeignKey: row.is_foreign_key,
          constraintName: row.constraint_name
        })),
        primaryKeyColumns: result.records
          .filter(row => row.is_primary_key)
          .map(row => row.column_name)
      };

      this.tableMetadataCache.set(cacheKey, metadata);
      return metadata;

    } catch (error) {
      dataApiLogger.error('Failed to get table metadata', {
        tableName,
        schemaName,
        error: error.message
      });
      throw error;
    }
  }

  // Enhanced column metadata function for pgAdmin compatibility
  async enhanceColumnMetadata(columnMetadata) {
    if (!this.currentTableName || !columnMetadata.length) {
      return this.addBasicEnhancements(columnMetadata);
    }

    try {
      // Get comprehensive column information for the current table
      const enhancementQuery = `
  WITH table_oids AS (
    SELECT 
      c.oid as table_oid,
      c.relname as table_name,
      c.relhasoids,
      n.nspname as schema_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = $1 AND c.relkind = 'r'
  ),
  column_details AS (
    SELECT 
      a.attname as column_name,
      a.attnum as column_number,
      a.atttypid as type_oid,
      a.attlen as type_length,
      a.atttypmod as type_modifier,
      a.attnotnull as not_null,
      a.atthasdef as has_default,
      a.attidentity as identity_type,
      a.attgenerated as generated_type,
      pg_get_expr(ad.adbin, ad.adrelid) as default_expression,
      t.typname as type_name,
      t.typcategory as type_category,
      t.typelem as element_type_oid,
      t.typlen as base_type_length,
      t.typdelim as type_delimiter,
      CASE 
        WHEN t.typname = 'varchar' OR t.typname = 'char' THEN 
          CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END
        WHEN t.typname = 'bpchar' THEN
          CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END
        ELSE NULL
      END as character_maximum_length,
      CASE 
        WHEN t.typname = 'numeric' THEN 
          CASE WHEN a.atttypmod > 0 THEN ((a.atttypmod - 4) >> 16) & 65535 ELSE NULL END
        ELSE NULL
      END as numeric_precision,
      CASE 
        WHEN t.typname = 'numeric' THEN 
          CASE WHEN a.atttypmod > 0 THEN (a.atttypmod - 4) & 65535 ELSE NULL END
        ELSE NULL
      END as numeric_scale,
      format_type(a.atttypid, a.atttypmod) as formatted_type
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
    JOIN table_oids to_info ON to_info.table_oid = a.attrelid
    WHERE a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
  ),
  -- FIXED: Primary key detection using direct pg_index lookup
  primary_key_columns AS (
    SELECT 
      a.attname as column_name,
      i.indisprimary,
      i.indisunique,
      array_position(i.indkey, a.attnum) as key_position
    FROM pg_index i
    JOIN table_oids to_info ON to_info.table_oid = i.indrelid
    JOIN pg_attribute a ON a.attrelid = i.indrelid 
    WHERE i.indisprimary = true 
    AND a.attnum = ANY(i.indkey)
    AND a.attnum > 0 
    AND NOT a.attisdropped
  ),
  -- Additional constraint information
  other_constraints AS (
    SELECT 
      kcu.column_name,
      tc.constraint_type,
      tc.constraint_name,
      tc.is_deferrable,
      tc.initially_deferred,
      ccu.table_name as foreign_table_name,
      ccu.column_name as foreign_column_name,
      rc.match_option as fk_match_type,
      rc.update_rule as fk_update_rule,
      rc.delete_rule as fk_delete_rule
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu 
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    LEFT JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
      AND tc.table_schema = ccu.table_schema
    LEFT JOIN information_schema.referential_constraints rc
      ON tc.constraint_name = rc.constraint_name
      AND tc.table_schema = rc.constraint_schema
    WHERE tc.table_name = $1
    AND tc.table_schema = COALESCE($2, 'public')  
    AND tc.constraint_type != 'PRIMARY KEY' -- Exclude PK as we handle it separately
  ),
  -- Index information for non-primary indexes
  index_info AS (
    SELECT 
      a.attname as column_name,
      i.indisunique as is_unique_index,
      am.amname as index_method,
      ic.relname as index_name
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_class tc ON tc.oid = i.indrelid
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    JOIN pg_am am ON am.oid = ic.relam
    JOIN table_oids to_info ON to_info.table_name = tc.relname
    WHERE tc.relname = $1
    AND i.indisprimary = false -- Only non-primary indexes
  )
  SELECT 
    to_info.table_oid,
    to_info.table_name,
    to_info.schema_name,
    to_info.relhasoids,
    cd.column_name,
    cd.column_number,
    cd.type_oid,
    cd.type_length,
    cd.type_modifier,
    cd.not_null,
    cd.has_default,
    cd.identity_type,
    cd.generated_type,
    cd.default_expression,
    cd.type_name,
    cd.type_category,
    cd.element_type_oid,
    cd.base_type_length,
    cd.type_delimiter,
    cd.character_maximum_length,
    cd.numeric_precision,
    cd.numeric_scale,
    cd.formatted_type,
    -- FIXED: Direct primary key detection
    COALESCE(pkc.indisprimary, FALSE) as is_primary_key,
    pkc.key_position,
    -- Other constraint information
    COALESCE(ii.is_unique_index, FALSE) as is_unique,
    (oc.constraint_type = 'FOREIGN KEY') as is_foreign_key,
    (oc.constraint_type = 'CHECK') as has_check_constraint,
    (oc.constraint_type = 'UNIQUE') as has_unique_constraint,
    oc.constraint_name,
    oc.is_deferrable,
    oc.initially_deferred,
    oc.foreign_table_name,
    oc.foreign_column_name,
    oc.fk_match_type,
    oc.fk_update_rule,
    oc.fk_delete_rule,
    -- Index information
    ii.index_method,
    ii.index_name
  FROM table_oids to_info
  JOIN column_details cd ON TRUE
  LEFT JOIN primary_key_columns pkc ON pkc.column_name = cd.column_name
  LEFT JOIN other_constraints oc ON oc.column_name = cd.column_name
  LEFT JOIN index_info ii ON ii.column_name = cd.column_name
  ORDER BY cd.column_number
`;

      const schemaName = this.currentTableName.includes('.')
        ? this.currentTableName.split('.')[0]
        : 'public';
      const tableName = this.currentTableName.includes('.')
        ? this.currentTableName.split('.')[1]
        : this.currentTableName;

      const enhancementResult = await this.executeRawQuery(
        enhancementQuery,
        [tableName, schemaName]
      );

      if (!enhancementResult.records || enhancementResult.records.length === 0) {
        dataApiLogger.warn('No column enhancement data found, using basic enhancements', {
          tableName: this.currentTableName
        });
        return this.addBasicEnhancements(columnMetadata);
      }

      // Create a map for quick lookup of enhancement data
      const enhancementMap = new Map();
      enhancementResult.records.forEach(row => {
        enhancementMap.set(row.column_name.toLowerCase(), row);
      });

      const enhancedColumns = columnMetadata.map((originalCol, index) => {
        const columnName = originalCol.name.toLowerCase();
        const enhancement = enhancementMap.get(columnName);

        if (!enhancement) {
          dataApiLogger.debug('No enhancement data for column, using defaults', {
            columnName: originalCol.name,
            tableName: this.currentTableName
          });
          return this.addBasicColumnEnhancement(originalCol, index);
        }

        return {
          // Original RDS Data API properties
          name: originalCol.name,
          typeName: originalCol.typeName || enhancement.type_name,
          arrayBaseColumnType: originalCol.arrayBaseColumnType || 0,
          isAutoIncrement: originalCol.isAutoIncrement || (enhancement.identity_type !== ''),
          isCaseSensitive: originalCol.isCaseSensitive || this.isTypeCaseSensitive(enhancement.type_name),
          isCurrency: originalCol.isCurrency || (enhancement.type_name === 'money'),
          isSigned: originalCol.isSigned || this.isTypeSigned(enhancement.type_name),
          label: originalCol.label || originalCol.name,
          nullable: originalCol.nullable !== undefined ? originalCol.nullable : (enhancement.not_null ? 0 : 1),
          precision: originalCol.precision || enhancement.numeric_precision || enhancement.character_maximum_length || 0,
          scale: originalCol.scale || enhancement.numeric_scale || 0,
          schemaName: originalCol.schemaName || enhancement.schema_name || 'public',
          type: originalCol.type || this.mapPostgreSQLTypeToJDBC(enhancement.type_oid),

          // Enhanced PostgreSQL-specific properties
          tableOid: enhancement.table_oid,
          tableName: enhancement.table_name,
          columnNumber: enhancement.column_number,
          typeOid: enhancement.type_oid,
          typeLength: enhancement.type_length,
          typeModifier: enhancement.type_modifier,
          typeName: enhancement.type_name,
          typeCategory: enhancement.type_category,
          elementTypeOid: enhancement.element_type_oid,
          typeDelimiter: enhancement.type_delimiter,
          formattedType: enhancement.formatted_type,

          // Column properties
          notNull: enhancement.not_null,
          hasDefault: enhancement.has_default,
          defaultExpression: enhancement.default_expression,
          identityType: enhancement.identity_type,
          generatedType: enhancement.generated_type,

          // FIXED: Primary key detection with explicit boolean conversion
          isPrimaryKey: Boolean(enhancement.is_primary_key),
          keyPosition: enhancement.key_position,

          // Other constraints
          isUnique: Boolean(enhancement.is_unique || enhancement.has_unique_constraint),
          isForeignKey: Boolean(enhancement.is_foreign_key),
          hasCheckConstraint: Boolean(enhancement.has_check_constraint),
          constraintName: enhancement.constraint_name,
          isDeferrable: enhancement.is_deferrable,
          initiallyDeferred: enhancement.initially_deferred,

          // Foreign key properties
          foreignTableName: enhancement.foreign_table_name,
          foreignColumnName: enhancement.foreign_column_name,
          fkMatchType: enhancement.fk_match_type,
          fkUpdateRule: enhancement.fk_update_rule,
          fkDeleteRule: enhancement.fk_delete_rule,

          // Index information
          indexMethod: enhancement.index_method,
          indexName: enhancement.index_name,

          // CRITICAL: pgAdmin editing flags - Fixed logic
          isUpdatable: Boolean(enhancement.is_primary_key) || this.hasUniqueIdentifier(enhancement),
          isInsertable: !enhancement.generated_type && enhancement.identity_type !== 'a',
          isSelectable: true,
          isKey: Boolean(enhancement.is_primary_key || enhancement.is_unique),

          // Additional metadata for editing
          canBeNull: !enhancement.not_null,
          hasDefaultValue: enhancement.has_default,
          isEditable: this.isColumnEditable(enhancement) && (Boolean(enhancement.is_primary_key) || this.hasUniqueIdentifier(enhancement)),

          // Display properties
          displaySize: this.calculateDisplaySize(enhancement),
          columnDisplaySize: this.calculateDisplaySize(enhancement)
        };
      });
      dataApiLogger.info('Primary key detection results', {
        tableName: this.currentTableName,
        totalColumns: enhancedColumns.length,
        primaryKeyColumns: enhancedColumns.filter(col => col.isPrimaryKey).map(col => ({
          name: col.name,
          isPrimaryKey: col.isPrimaryKey,
          isEditable: col.isEditable,
          isUpdatable: col.isUpdatable
        })),
        editableColumns: enhancedColumns.filter(col => col.isEditable).length,
        updatableColumns: enhancedColumns.filter(col => col.isUpdatable).length
      });


      dataApiLogger.info('Successfully enhanced column metadata', {
        tableName: this.currentTableName,
        columnCount: enhancedColumns.length,
        primaryKeyColumns: enhancedColumns.filter(col => col.isPrimaryKey).length,
        editableColumns: enhancedColumns.filter(col => col.isEditable).length
      });

      return enhancedColumns;

    } catch (error) {
      dataApiLogger.error('Failed to enhance column metadata', {
        error: error.message,
        tableName: this.currentTableName,
        stack: error.stack
      });

      // Fallback to basic enhancements
      return this.addBasicEnhancements(columnMetadata);
    }
  }

  // Check if table has a unique identifier (primary key, unique constraint, or ctid)
  hasUniqueIdentifier(tableEnhancement) {
    // Check if table has any primary key columns
    const hasPrimaryKey = tableEnhancement.is_primary_key;

    // Check if table has unique constraints that can act as row identifiers
    const hasUniqueConstraint = tableEnhancement.is_unique || tableEnhancement.has_unique_constraint;

    // Check if we have ctid (always available as row identifier)
    const hasCtid = true; // ctid is always available in PostgreSQL

    return hasPrimaryKey || hasUniqueConstraint || hasCtid;
  }

  // Helper function: Add basic enhancements when detailed metadata isn't available
  addBasicEnhancements(columnMetadata) {
    return columnMetadata.map((col, index) => this.addBasicColumnEnhancement(col, index));
  }
  
  // Helper function: Add basic column enhancements
  addBasicColumnEnhancement(originalCol, index) {
    const typeName = originalCol.typeName?.toLowerCase() || 'text';
    const isPrimaryKey = originalCol.name === 'id' ||
      originalCol.name === 'customer_id' ||
      (originalCol.name.endsWith('_id') && index === 0); // First column ending with _id

    return {
      ...originalCol,
      // Basic PostgreSQL compatibility
      tableOid: 0,
      tableName: this.currentTableName || 'unknown',
      columnNumber: index + 1,
      typeOid: this.mapTypeNameToOid(typeName),
      typeName: typeName,

      // FIXED: Better primary key detection for basic enhancement
      isPrimaryKey: isPrimaryKey,
      isUnique: isPrimaryKey,
      isForeignKey: originalCol.name.endsWith('_id') && !isPrimaryKey,

      // CRITICAL: Enable editing when we have a primary key
      isUpdatable: isPrimaryKey || originalCol.name === 'ctid',
      isInsertable: true,
      isSelectable: true,
      isEditable: isPrimaryKey || originalCol.name === 'ctid',
      isKey: isPrimaryKey,

      canBeNull: originalCol.nullable !== 0,
      hasDefaultValue: false,

      // Type-specific properties
      isCaseSensitive: this.isTypeCaseSensitive(typeName),
      isSigned: this.isTypeSigned(typeName),
      displaySize: this.calculateBasicDisplaySize(typeName, originalCol.precision),

      // Safe defaults
      notNull: originalCol.nullable === 0,
      hasDefault: false,
      schemaName: originalCol.schemaName || 'public'
    };
  }

  // Helper function: Determine if column is editable
  isColumnEditable(enhancement) {
    // Cannot edit generated columns
    if (enhancement.generated_type) return false;

    // Cannot edit always identity columns
    if (enhancement.identity_type === 'a') return false;

    // System columns are not editable
    if (['oid', 'ctid', 'xmin', 'xmax', 'cmin', 'cmax'].includes(enhancement.column_name)) {
      return false;
    }

    return true;
  }

  // Helper function: Calculate display size for a column
  calculateDisplaySize(enhancement) {
    const typeName = enhancement.type_name?.toLowerCase();

    switch (typeName) {
      case 'varchar':
      case 'char':
      case 'bpchar':
        return enhancement.character_maximum_length || 255;
      case 'text':
        return 65535;
      case 'int2':
        return 6;
      case 'int4':
        return 11;
      case 'int8':
        return 20;
      case 'numeric':
        return (enhancement.numeric_precision || 10) + 2; // +2 for decimal point and sign
      case 'float4':
        return 15;
      case 'float8':
        return 24;
      case 'bool':
        return 5;
      case 'date':
        return 10;
      case 'timestamp':
      case 'timestamptz':
        return 29;
      case 'time':
      case 'timetz':
        return 15;
      case 'uuid':
        return 36;
      case 'json':
      case 'jsonb':
        return 65535;
      default:
        return enhancement.type_length > 0 ? enhancement.type_length : 255;
    }
  }

  // Helper function: Calculate basic display size
  calculateBasicDisplaySize(typeName, precision) {
    const type = typeName?.toLowerCase();

    if (precision && precision > 0) return precision;

    switch (type) {
      case 'text': return 65535;
      case 'varchar': return 255;
      case 'int4': return 11;
      case 'int8': return 20;
      case 'float8': return 24;
      case 'bool': return 5;
      case 'date': return 10;
      case 'timestamp': return 29;
      default: return 255;
    }
  }

  // Helper function: Check if type is case sensitive
  isTypeCaseSensitive(typeName) {
    const caseInsensitiveTypes = ['citext'];
    return !caseInsensitiveTypes.includes(typeName?.toLowerCase());
  }

  // Helper function: Check if type is signed
  isTypeSigned(typeName) {
    const signedTypes = ['int2', 'int4', 'int8', 'float4', 'float8', 'numeric', 'decimal'];
    return signedTypes.includes(typeName?.toLowerCase());
  }

  // Helper function: Map type name to PostgreSQL OID
  mapTypeNameToOid(typeName) {
    const typeOidMap = {
      'bool': 16, 'bytea': 17, 'char': 18, 'name': 19, 'int8': 20,
      'int2': 21, 'int2vector': 22, 'int4': 23, 'regproc': 24, 'text': 25,
      'oid': 26, 'tid': 27, 'xid': 28, 'cid': 29, 'oidvector': 30,
      'json': 114, 'xml': 142, 'point': 600, 'lseg': 601, 'path': 602,
      'box': 603, 'polygon': 604, 'line': 628, 'float4': 700, 'float8': 701,
      'abstime': 702, 'reltime': 703, 'tinterval': 704, 'unknown': 705,
      'circle': 718, 'money': 790, 'macaddr': 829, 'inet': 869, 'cidr': 650,
      'bpchar': 1042, 'varchar': 1043, 'date': 1082, 'time': 1083,
      'timestamp': 1114, 'timestamptz': 1184, 'interval': 1186, 'timetz': 1266,
      'bit': 1560, 'varbit': 1562, 'numeric': 1700, 'refcursor': 1790,
      'regprocedure': 2202, 'regoper': 2203, 'regoperator': 2204,
      'regclass': 2205, 'regtype': 2206, 'uuid': 2950, 'txid_snapshot': 2970,
      'jsonb': 3802
    };

    return typeOidMap[typeName?.toLowerCase()] || 25; // Default to text
  }

  // Helper function: Map PostgreSQL type OID to JDBC type
  mapPostgreSQLTypeToJDBC(typeOid) {
    const jdbcTypeMap = {
      16: -7,    // BOOLEAN -> BIT
      17: -2,    // BYTEA -> BINARY
      18: 1,     // CHAR -> CHAR
      20: -5,    // INT8 -> BIGINT
      21: 5,     // INT2 -> SMALLINT
      23: 4,     // INT4 -> INTEGER
      25: 12,    // TEXT -> VARCHAR
      700: 7,    // FLOAT4 -> REAL
      701: 8,    // FLOAT8 -> DOUBLE
      1042: 1,   // BPCHAR -> CHAR
      1043: 12,  // VARCHAR -> VARCHAR
      1082: 91,  // DATE -> DATE
      1083: 92,  // TIME -> TIME
      1114: 93,  // TIMESTAMP -> TIMESTAMP
      1184: 93,  // TIMESTAMPTZ -> TIMESTAMP
      1700: 2,   // NUMERIC -> NUMERIC
      2950: 12,  // UUID -> VARCHAR
      114: 12,   // JSON -> VARCHAR
      3802: 12   // JSONB -> VARCHAR
    };

    return jdbcTypeMap[typeOid] || 12; // Default to VARCHAR
  }

  // Helper function: Get minimum value for numeric types
  getMinimumValue(typeName) {
    const type = typeName?.toLowerCase();
    switch (type) {
      case 'int2': return -32768;
      case 'int4': return -2147483648;
      case 'int8': return '-9223372036854775808';
      case 'float4': return -3.4028235e+38;
      case 'float8': return -1.7976931348623157e+308;
      default: return null;
    }
  }

  // Helper function: Get maximum value for numeric types
  getMaximumValue(typeName) {
    const type = typeName?.toLowerCase();
    switch (type) {
      case 'int2': return 32767;
      case 'int4': return 2147483647;
      case 'int8': return '9223372036854775807';
      case 'float4': return 3.4028235e+38;
      case 'float8': return 1.7976931348623157e+308;
      default: return null;
    }
  }
  

  // Enhanced SQL sanitization
  sanitizeSQL(sql) {
    let rewrittenSQL = sql;

    // Handle deprecated columns
    rewrittenSQL = rewrittenSQL.replace(/\brelhasoids\b/gi, 'false AS relhasoids');

    // Convert char types
    rewrittenSQL = rewrittenSQL.replace(/::\s*"?char"?/gi, '::text');

    // Handle pgAdmin-specific queries
    if (rewrittenSQL.includes('pg_get_expr')) {
      // Ensure pg_get_expr calls are properly formatted
      rewrittenSQL = rewrittenSQL.replace(
        /pg_get_expr\(([^,]+),\s*([^)]+)\)/gi,
        'pg_get_expr($1, $2)'
      );
    }

    // Handle array type queries
    rewrittenSQL = rewrittenSQL.replace(/::regtype/gi, '::oid');

    return rewrittenSQL;
  }

  // Enhanced SQL enhancement for pgAdmin compatibility
  async enhanceSQLForPgAdmin(sql) {
    const selectMatch = sql.match(/^\s*SELECT\s+.*?\s+FROM\s+([^\s;,]+)/i);

    if (selectMatch && !sql.toLowerCase().includes('join')) {
      const tableName = selectMatch[1].replace(/["`]/g, '');
      this.currentTableName = tableName;

      try {
        const metadata = await this.getTableMetadata(tableName);

        // Add row identifier if not present
        if (!sql.toLowerCase().includes('oid') &&
          !sql.toLowerCase().includes('ctid') &&
          !metadata.primaryKeyColumns.some(pk => sql.toLowerCase().includes(pk.toLowerCase()))) {

          if (metadata.hasOids) {
            sql = sql.replace(/SELECT\s+/i, 'SELECT oid, ');
          } else if (metadata.primaryKeyColumns.length > 0) {
            const pkColumns = metadata.primaryKeyColumns.join(', ');
            if (!metadata.primaryKeyColumns.some(pk =>
              new RegExp(`\\b${pk}\\b`, 'i').test(sql))) {
              sql = sql.replace(/SELECT\s+/i, `SELECT ${pkColumns}, `);
            }
          } else {
            sql = sql.replace(/SELECT\s+/i, 'SELECT ctid, ');
          }
        }

        // Add ORDER BY for consistent results (pgAdmin expects this)
        if (!sql.toLowerCase().includes('order by') && metadata.primaryKeyColumns.length > 0) {
          const orderBy = metadata.primaryKeyColumns.join(', ');
          sql = sql.replace(/;?\s*$/, ` ORDER BY ${orderBy};`);
        }

      } catch (error) {
        dataApiLogger.warn('Could not enhance SQL for pgAdmin compatibility', {
          error: error.message,
          tableName
        });
      }
    }

    return sql;
  }

  // Enhanced response formatting with full PostgreSQL compatibility
  async formatResponse(response) {
    const result = {
      records: [],
      columnMetadata: await this.enhanceColumnMetadata(response.columnMetadata || []),
      numberOfRecordsUpdated: response.numberOfRecordsUpdated || 0,
      generatedFields: response.generatedFields || [],
      // PostgreSQL protocol fields
      command: this.extractCommandFromSQL(response.sql || ''),
      rowCount: response.records?.length || 0,
      oid: null,
      rows: [],
      // pgAdmin-specific fields
      status: 'PGRES_TUPLES_OK',
      fields: [],
      affectedRows: response.numberOfRecordsUpdated || 0
    };

    if (response.records && response.records.length > 0) {
      result.records = response.records.map((record, rowIndex) => {
        const row = {};

        record.forEach((field, index) => {
          const columnMeta = response.columnMetadata[index];
          const columnName = columnMeta?.name || `column_${index}`;

          // Extract and format value based on PostgreSQL type
          row[columnName] = this.formatFieldValue(field, columnMeta);
        });

        return row;
      });

      result.rows = result.records;
    }

    // Generate field descriptions for pgAdmin
    result.fields = (response.columnMetadata || []).map((col, index) => ({
      name: col.name,
      tableOid: col.tableOid || 0,
      columnAttrNumber: index + 1,
      dataTypeOid: this.mapToPostgreSQLTypeOid(col.typeName),
      dataTypeSize: col.precision || -1,
      dataTypeModifier: col.scale || -1,
      format: 0 // Text format
    }));

    return result;
  }

  // Format field value according to PostgreSQL standards
  formatFieldValue(field, columnMeta) {
    if (field.isNull) {
      return null;
    }

    const typeName = columnMeta?.typeName?.toLowerCase() || '';

    if (field.stringValue !== undefined) {
      // Handle specific string types
      if (typeName.includes('json')) {
        try {
          return JSON.parse(field.stringValue);
        } catch {
          return field.stringValue;
        }
      }
      return field.stringValue;
    } else if (field.longValue !== undefined) {
      return field.longValue;
    } else if (field.doubleValue !== undefined) {
      return field.doubleValue;
    } else if (field.booleanValue !== undefined) {
      return field.booleanValue;
    } else if (field.blobValue !== undefined) {
      return Buffer.from(field.blobValue);
    }

    return null;
  }

  // Map AWS RDS Data API types to PostgreSQL OIDs
  mapToPostgreSQLTypeOid(typeName) {
    const typeMap = {
      'varchar': 1043,
      'text': 25,
      'char': 1042,
      'int4': 23,
      'int8': 20,
      'int2': 21,
      'float4': 700,
      'float8': 701,
      'numeric': 1700,
      'bool': 16,
      'date': 1082,
      'timestamp': 1114,
      'timestamptz': 1184,
      'time': 1083,
      'timetz': 1266,
      'json': 114,
      'jsonb': 3802,
      'uuid': 2950,
      'bytea': 17
    };

    return typeMap[typeName?.toLowerCase()] || 25; // Default to text type
  }

  // Extract command from SQL for PostgreSQL protocol
  extractCommandFromSQL(sql) {
    const command = sql.trim().split(/\s+/)[0]?.toUpperCase() || 'UNKNOWN';
    return command;
  }


  // Enhanced parameter formatting with PostgreSQL type handling
  formatParameters(parameters) {
    if (!parameters || parameters.length === 0) {
      return [];
    }

    return parameters.map((param, index) => {
      const formatted = {
        name: `param${index + 1}`,
        value: {}
      };

      if (param === null || param === undefined) {
        formatted.value.isNull = true;
      } else if (typeof param === 'string') {
        formatted.value.stringValue = param;
      } else if (typeof param === 'number') {
        if (Number.isInteger(param) && param >= -2147483648 && param <= 2147483647) {
          formatted.value.longValue = param;
        } else {
          formatted.value.doubleValue = param;
        }
      } else if (typeof param === 'boolean') {
        formatted.value.booleanValue = param;
      } else if (param instanceof Date) {
        formatted.value.stringValue = param.toISOString();
      } else if (Buffer.isBuffer(param)) {
        formatted.value.blobValue = param;
      } else if (typeof param === 'object') {
        formatted.value.stringValue = JSON.stringify(param);
      } else {
        formatted.value.stringValue = String(param);
      }

      return formatted;
    });
  }

  // Enhanced error formatting with PostgreSQL error codes
  formatError(error) {
    const pgError = new Error(error.message || 'Unknown Data API error');

    // Map AWS errors to PostgreSQL SQLSTATE codes
    const errorMapping = {
      'BadRequestException': { code: '42601', severity: 'ERROR' }, // syntax_error
      'ForbiddenException': { code: '42501', severity: 'ERROR' }, // insufficient_privilege  
      'ServiceUnavailableException': { code: '08006', severity: 'FATAL' }, // connection_failure
      'StatementTimeoutException': { code: '57014', severity: 'ERROR' }, // query_canceled
      'ResourceNotFoundException': { code: '42P01', severity: 'ERROR' }, // undefined_table
      'ValidationException': { code: '22023', severity: 'ERROR' }, // invalid_parameter_value
      'ThrottlingException': { code: '53300', severity: 'ERROR' }, // too_many_connections
      'InternalServerErrorException': { code: 'XX000', severity: 'ERROR' } // internal_error
    };

    const mapping = errorMapping[error.name] || { code: 'XX000', severity: 'ERROR' };

    pgError.code = mapping.code;
    pgError.severity = mapping.severity;
    pgError.detail = error.message;
    pgError.hint = this.generateErrorHint(error);
    pgError.position = null;
    pgError.internalPosition = null;
    pgError.internalQuery = null;
    pgError.where = null;
    pgError.schema = null;
    pgError.table = this.currentTableName;
    pgError.column = null;
    pgError.datatype = null;
    pgError.constraint = null;
    pgError.file = null;
    pgError.line = null;
    pgError.routine = null;
    pgError.originalError = error;

    return pgError;
  }

  // Generate helpful error hints
  generateErrorHint(error) {
    if (error.message?.includes('parameter')) {
      return 'Check that the number of parameters matches the prepared statement.';
    }
    if (error.message?.includes('syntax')) {
      return 'Check the SQL syntax for any PostgreSQL incompatibilities.';
    }
    if (error.message?.includes('permission')) {
      return 'Verify that the RDS Data API has proper permissions.';
    }
    return null;
  }

  // Clear prepared statements cache
  clearPreparedStatements() {
    this.preparedStatements.clear();
    dataApiLogger.debug('Cleared prepared statements cache');
  }

  // Clear table metadata cache
  clearMetadataCache() {
    this.tableMetadataCache.clear();
    dataApiLogger.debug('Cleared table metadata cache');
  }

  // Enhanced cleanup
  async cleanup() {
    try {
      // Rollback any active transaction
      if (this.transactionId) {
        await this.rollbackTransaction();
      }

      // Clear caches
      this.clearPreparedStatements();
      this.clearMetadataCache();

      dataApiLogger.info('DataAPI client cleanup completed');
    } catch (error) {
      dataApiLogger.warn('Error during cleanup', {
        error: error.message
      });
    }
  }

  // Get connection info for PostgreSQL protocol
  getConnectionInfo() {
    return { ...this.connectionInfo };
  }

  // Health check
  async healthCheck() {
    try {
      const result = await this.executeRawQuery('SELECT 1 as health_check');
      return {
        healthy: true,
        timestamp: new Date().toISOString(),
        responseTime: Date.now()
      };
    } catch (error) {
      return {
        healthy: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  // Enhanced raw query execution

  // Your existing methods with enhancements...
  async executeStatement(sql, parameters = []) {
    const startTime = Date.now();

    try {
      sql = this.sanitizeSQL(sql);
      sql = await this.enhanceSQLForPgAdmin(sql);

      dataApiLogger.debug('Executing SQL statement', {
        sql: sql.substring(0, 200) + (sql.length > 200 ? '...' : ''),
        parameterCount: parameters.length,
        hasTransaction: !!this.transactionId
      });


      const command = new ExecuteStatementCommand({
        resourceArn: clusterConfig.clusterArn,
        secretArn: clusterConfig.secretArn,
        database: clusterConfig.database,
        sql,
        parameters: this.formatParameters(parameters),
        includeResultMetadata: true,
        transactionId: this.transactionId
      });

      const response = await this.client.send(command);
      response.sql = sql; // Store for command tag generation

      const duration = Date.now() - startTime;

      dataApiLogger.info('SQL statement executed successfully', {
        duration,
        recordCount: response.records?.length || 0,
        columnCount: response.columnMetadata?.length || 0,
        transactionId: this.transactionId
      });

      return this.formatResponse(response);

    } catch (error) {
      const duration = Date.now() - startTime;
      dataApiLogger.error('SQL statement execution failed', {
        error: error.message,
        sql: sql?.substring(0, 100),
        duration,
        errorCode: error.$metadata?.httpStatusCode
      });
      throw this.formatError(error);
    }
  }

  // [Include your existing transaction methods here - beginTransaction, commitTransaction, rollbackTransaction]
  // [Include your existing executeRawQuery and formatSimpleResponse methods]
  // [Include your existing utility methods]

  // Begin a transaction
  async beginTransaction() {
    try {
      dataApiLogger.debug('Beginning transaction');

      const command = new BeginTransactionCommand({
        resourceArn: clusterConfig.clusterArn,
        secretArn: clusterConfig.secretArn,
        database: clusterConfig.database
      });

      const response = await this.client.send(command);
      this.transactionId = response.transactionId;

      dataApiLogger.info('Transaction started', {
        transactionId: this.transactionId
      });

      return this.transactionId;

    } catch (error) {
      dataApiLogger.error('Failed to begin transaction', {
        error: error.message
      });
      throw this.formatError(error);
    }
  }

  // Commit the current transaction
  async commitTransaction() {
    if (!this.transactionId) {
      throw new Error('No active transaction to commit');
    }

    try {
      dataApiLogger.debug('Committing transaction', {
        transactionId: this.transactionId
      });

      const command = new CommitTransactionCommand({
        resourceArn: clusterConfig.clusterArn,
        secretArn: clusterConfig.secretArn,
        transactionId: this.transactionId
      });

      const response = await this.client.send(command);
      const committedTransactionId = this.transactionId;
      this.transactionId = null;

      dataApiLogger.info('Transaction committed', {
        transactionId: committedTransactionId,
        transactionStatus: response.transactionStatus
      });

      return response;

    } catch (error) {
      dataApiLogger.error('Failed to commit transaction', {
        error: error.message,
        transactionId: this.transactionId
      });
      throw this.formatError(error);
    }
  }

  // Rollback the current transaction
  async rollbackTransaction() {
    if (!this.transactionId) {
      throw new Error('No active transaction to rollback');
    }

    try {
      dataApiLogger.debug('Rolling back transaction', {
        transactionId: this.transactionId
      });

      const command = new RollbackTransactionCommand({
        resourceArn: clusterConfig.clusterArn,
        secretArn: clusterConfig.secretArn,
        transactionId: this.transactionId
      });

      const response = await this.client.send(command);
      const rolledBackTransactionId = this.transactionId;
      this.transactionId = null;

      dataApiLogger.info('Transaction rolled back', {
        transactionId: rolledBackTransactionId,
        transactionStatus: response.transactionStatus
      });

      return response;

    } catch (error) {
      dataApiLogger.error('Failed to rollback transaction', {
        error: error.message,
        transactionId: this.transactionId
      });
      throw this.formatError(error);
    }
  }

  // Format parameters for RDS Data API
  formatParameters(parameters) {
    if (!parameters || parameters.length === 0) {
      return [];
    }

    return parameters.map((param, index) => {
      const formatted = {
        name: `param${index + 1}`,
        value: {}
      };

      if (param === null || param === undefined) {
        formatted.value.isNull = true;
      } else if (typeof param === 'string') {
        formatted.value.stringValue = param;
      } else if (typeof param === 'number') {
        if (Number.isInteger(param)) {
          formatted.value.longValue = param;
        } else {
          formatted.value.doubleValue = param;
        }
      } else if (typeof param === 'boolean') {
        formatted.value.booleanValue = param;
      } else if (param instanceof Date) {
        formatted.value.stringValue = param.toISOString();
      } else if (typeof param === 'object') {
        formatted.value.stringValue = JSON.stringify(param);
      } else {
        formatted.value.stringValue = String(param);
      }

      return formatted;
    });
  }

  // Format AWS errors to our internal error format
  formatError(error) {
    const formattedError = new Error(error.message || 'Unknown Data API error');

    // Map AWS error codes to PostgreSQL-like error codes
    if (error.name === 'BadRequestException') {
      formattedError.code = '42601'; // syntax_error
      formattedError.severity = 'ERROR';
    } else if (error.name === 'ForbiddenException') {
      formattedError.code = '42501'; // insufficient_privilege
      formattedError.severity = 'ERROR';
    } else if (error.name === 'ServiceUnavailableException') {
      formattedError.code = '08006'; // connection_failure
      formattedError.severity = 'FATAL';
    } else if (error.name === 'StatementTimeoutException') {
      formattedError.code = '57014'; // query_canceled
      formattedError.severity = 'ERROR';
    } else {
      formattedError.code = 'XX000'; // internal_error
      formattedError.severity = 'ERROR';
    }

    formattedError.detail = error.message;
    formattedError.originalError = error;

    return formattedError;
  }

  // Check if currently in a transaction
  isInTransaction() {
    return !!this.transactionId;
  }

  // Get current transaction ID
  getTransactionId() {
    return this.transactionId;
  }

  // Close any active transaction (cleanup)
  async cleanup() {
    if (this.transactionId) {
      try {
        await this.rollbackTransaction();
        dataApiLogger.info('Cleaned up active transaction on client disconnect');
      } catch (error) {
        dataApiLogger.warn('Failed to cleanup transaction during disconnect', {
          error: error.message,
          transactionId: this.transactionId
        });
      }
    }
  }
}

module.exports = DataAPIClient;