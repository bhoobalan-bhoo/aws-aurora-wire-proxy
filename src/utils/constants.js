/**
 * PostgreSQL Wire Protocol Constants
 * Covers frontend/backend message types, auth types, error codes, and common defaults
 */

'use strict';

// Message types (first byte of each message)
const MESSAGE_TYPES = Object.freeze({
  // Frontend (client) messages
  STARTUP: 0,
  QUERY: 'Q'.charCodeAt(0),
  PARSE: 'P'.charCodeAt(0),
  BIND: 'B'.charCodeAt(0),
  EXECUTE: 'E'.charCodeAt(0),
  DESCRIBE: 'D'.charCodeAt(0),
  CLOSE: 'C'.charCodeAt(0),
  SYNC: 'S'.charCodeAt(0),
  TERMINATE: 'X'.charCodeAt(0),
  PASSWORD: 'p'.charCodeAt(0),

  // Backend (server) messages
  AUTHENTICATION: 'R'.charCodeAt(0),
  BACKEND_KEY_DATA: 'K'.charCodeAt(0),
  PARAMETER_STATUS: 'S'.charCodeAt(0),
  READY_FOR_QUERY: 'Z'.charCodeAt(0),
  ROW_DESCRIPTION: 'T'.charCodeAt(0),
  DATA_ROW: 'D'.charCodeAt(0),
  COMMAND_COMPLETE: 'C'.charCodeAt(0),
  ERROR_RESPONSE: 'E'.charCodeAt(0),
  NOTICE_RESPONSE: 'N'.charCodeAt(0),
  PARAMETER_DESCRIPTION: 't'.charCodeAt(0),
  NO_DATA: 'n'.charCodeAt(0),
  PARSE_COMPLETE: '1'.charCodeAt(0),
  BIND_COMPLETE: '2'.charCodeAt(0),
  CLOSE_COMPLETE: '3'.charCodeAt(0),
  PORTAL_SUSPENDED: 's'.charCodeAt(0),
  EMPTY_QUERY: 'I'.charCodeAt(0)
});

// Authentication types
const AUTH_TYPES = Object.freeze({
  OK: 0,
  KERBEROS_V5: 2,
  CLEARTEXT_PASSWORD: 3,
  MD5_PASSWORD: 5,
  SCM_CREDENTIAL: 6,
  GSS: 7,
  SSPI: 9,
  SASL: 10,
  SASL_CONTINUE: 11,
  SASL_FINAL: 12
});

// Transaction status codes
const TRANSACTION_STATUS = Object.freeze({
  IDLE: 'I'.charCodeAt(0),
  TRANSACTION: 'T'.charCodeAt(0),
  FAILED_TRANSACTION: 'E'.charCodeAt(0)
});

// PostgreSQL Data Types (OIDs)
const PG_TYPES = Object.freeze({
  BOOL: 16,
  BYTEA: 17,
  CHAR: 18,
  NAME: 19,
  INT8: 20,
  INT2: 21,
  INT2VECTOR: 22,
  INT4: 23,
  REGPROC: 24,
  TEXT: 25,
  OID: 26,
  TID: 27,
  XID: 28,
  CID: 29,
  OIDVECTOR: 30,
  JSON: 114,
  XML: 142,
  PGNODETREE: 194,
  POINT: 600,
  LSEG: 601,
  PATH: 602,
  BOX: 603,
  POLYGON: 604,
  LINE: 628,
  FLOAT4: 700,
  FLOAT8: 701,
  ABSTIME: 702,
  RELTIME: 703,
  TINTERVAL: 704,
  UNKNOWN: 705,
  CIRCLE: 718,
  CASH: 790,
  MACADDR: 829,
  INET: 869,
  CIDR: 650,
  MACADDR8: 774,
  ACLITEM: 1033,
  BPCHAR: 1042,
  VARCHAR: 1043,
  DATE: 1082,
  TIME: 1083,
  TIMESTAMP: 1114,
  TIMESTAMPTZ: 1184,
  INTERVAL: 1186,
  TIMETZ: 1266,
  BIT: 1560,
  VARBIT: 1562,
  NUMERIC: 1700,
  REFCURSOR: 1790,
  REGPROCEDURE: 2202,
  REGOPER: 2203,
  REGOPERATOR: 2204,
  REGCLASS: 2205,
  REGTYPE: 2206,
  REGROLE: 4096,
  REGNAMESPACE: 4089,
  UUID: 2950,
  TXID_SNAPSHOT: 2970,
  FDW_HANDLER: 3115,
  PG_LSN: 3220,
  TSM_HANDLER: 3310,
  JSONB: 3802,
  INT4RANGE: 3904,
  NUMRANGE: 3906,
  TSRANGE: 3908,
  TSTZRANGE: 3910,
  DATERANGE: 3912,
  INT8RANGE: 3926
});

// Error severity levels
const ERROR_SEVERITY = Object.freeze({
  ERROR: 'ERROR',
  FATAL: 'FATAL',
  PANIC: 'PANIC',
  WARNING: 'WARNING',
  NOTICE: 'NOTICE',
  DEBUG: 'DEBUG',
  INFO: 'INFO',
  LOG: 'LOG'
});

// SQL state codes (subset)
const SQL_STATES = Object.freeze({
  SUCCESS: '00000',
  WARNING: '01000',
  NO_DATA: '02000',
  CONNECTION_EXCEPTION: '08000',
  CONNECTION_DOES_NOT_EXIST: '08003',
  CONNECTION_FAILURE: '08006',
  FEATURE_NOT_SUPPORTED: '0A000',
  INVALID_TRANSACTION_STATE: '25000',
  ACTIVE_SQL_TRANSACTION: '25001',
  SYNTAX_ERROR: '42601',
  UNDEFINED_TABLE: '42P01',
  UNDEFINED_COLUMN: '42703',
  AMBIGUOUS_COLUMN: '42702',
  DUPLICATE_COLUMN: '42701',
  INSUFFICIENT_PRIVILEGE: '42501',
  INTERNAL_ERROR: 'XX000',
  DATA_CORRUPTED: 'XX001',
  INDEX_CORRUPTED: 'XX002'
});

// Default parameters sent on startup
const DEFAULT_PARAMETERS = Object.freeze({
  server_version: process.env.MOCK_SERVER_VERSION || 'PostgreSQL 14.9 on x86_64-pc-linux-gnu',
  server_encoding: 'UTF8',
  client_encoding: 'UTF8',
  application_name: 'rds-data-api-proxy',
  is_superuser: 'off',
  session_authorization: 'postgres',
  DateStyle: 'ISO, MDY',
  IntervalStyle: 'postgres',
  TimeZone: 'UTC',
  integer_datetimes: 'on',
  standard_conforming_strings: 'on'
});

// Common system queries (for mock recognition)
const SYSTEM_QUERIES = Object.freeze({
  VERSION: /^SELECT\s+version\(\s*\)/i,
  PG_CLASS: /^SELECT\s+.*FROM\s+pg_class/i,
  PG_NAMESPACE: /^SELECT\s+.*FROM\s+pg_namespace/i,
  PG_ATTRIBUTE: /^SELECT\s+.*FROM\s+pg_attribute/i,
  PG_TYPE: /^SELECT\s+.*FROM\s+pg_type/i,
  PG_INDEX: /^SELECT\s+.*FROM\s+pg_index/i,
  PG_CONSTRAINT: /^SELECT\s+.*FROM\s+pg_constraint/i,
  PG_PROC: /^SELECT\s+.*FROM\s+pg_proc/i,
  PG_STAT_ACTIVITY: /^SELECT\s+.*FROM\s+pg_stat_activity/i,
  INFORMATION_SCHEMA: /^SELECT\s+.*FROM\s+information_schema\./i,
  SHOW_TABLES: /^SELECT\s+.*FROM\s+pg_tables/i,
  CURRENT_SCHEMA: /^SELECT\s+current_schema\(\s*\)/i,
  CURRENT_USER: /^SELECT\s+current_user/i,
  CURRENT_DATABASE: /^SELECT\s+current_database\(\s*\)/i
});

module.exports = {
  MESSAGE_TYPES,
  AUTH_TYPES,
  TRANSACTION_STATUS,
  PG_TYPES,
  ERROR_SEVERITY,
  SQL_STATES,
  DEFAULT_PARAMETERS,
  SYSTEM_QUERIES
};
