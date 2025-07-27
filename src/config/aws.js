const {
  fromEnv,
  fromContainerMetadata,
  fromInstanceMetadata,
  fromNodeProviderChain,
} = require('@aws-sdk/credential-providers');
const { RDSDataClient } = require('@aws-sdk/client-rds-data');
const { dataApiLogger } = require('./logger');

/**
 * Validate that essential environment variables are present.
 * Throws an error if any are missing.
 */
const validateConfig = () => {
  const requiredVars = ['RDS_CLUSTER_ARN', 'RDS_SECRET_ARN', 'RDS_DATABASE_NAME'];
  const missing = requiredVars.filter((key) => !process.env[key]);

  if (missing.length > 0) {
    const error = new Error(`Missing required environment variables: ${missing.join(', ')}`);
    dataApiLogger.error('Configuration validation failed', { missingVars: missing });
    throw error;
  }

  dataApiLogger.info('AWS configuration validated successfully');
};

/**
 * Resolve AWS credentials using a preferred provider chain.
 * Includes environment, ECS metadata, EC2 metadata, and default provider chain.
 * @returns {AwsCredentialIdentityProvider}
 */
const resolveCredentials = () => {
  if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
    dataApiLogger.info('Using environment variables for AWS credentials');
    return fromEnv();
  }

  try {
    dataApiLogger.info('Attempting to resolve credentials using container or instance metadata');
    return fromContainerMetadata();
  } catch (e1) {
    try {
      return fromInstanceMetadata();
    } catch (e2) {
      dataApiLogger.warn('Falling back to default credential provider chain');
      return fromNodeProviderChain();
    }
  }
};

// Common AWS SDK configuration
const awsConfig = {
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: resolveCredentials(),
  maxAttempts: 3,
  retryMode: 'adaptive',
};

/**
 * Create and return a configured RDS Data API client
 * @returns {RDSDataClient}
 */
const createRDSDataClient = () => {
  const client = new RDSDataClient(awsConfig);

  dataApiLogger.info('RDS Data API client created successfully', {
    region: awsConfig.region,
    clusterArn: process.env.RDS_CLUSTER_ARN,
    databaseName: process.env.RDS_DATABASE_NAME,
  });

  return client;
};

// Exported cluster configuration for use in SQL calls
const clusterConfig = {
  clusterArn: process.env.RDS_CLUSTER_ARN,
  secretArn: process.env.RDS_SECRET_ARN,
  database: process.env.RDS_DATABASE_NAME,
};

module.exports = {
  awsConfig,
  createRDSDataClient,
  validateConfig,
  clusterConfig,
};
