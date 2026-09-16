/**
 * Destination object-store credentials loaded from a bootstrap path/URI.
 * Native JSON for one scheme: GCS service account, AWS keys, R2 keys, or
 * Drive service account with subject_to_impersonate.
 */

function firstString(obj, keys) {
  for (const key of keys) {
    const value = obj?.[key];
    if (value != null && String(value).trim() !== '') return String(value);
  }
  return undefined;
}

export function inferCredentialsScheme(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    throw new Error('credentials file must be a JSON object');
  }
  if (obj.subject_to_impersonate) return 'gdrive';
  if (obj.type === 'service_account' || (obj.client_email && obj.private_key)) return 'gs';
  const endpoint = String(obj.endpoint || '');
  if (
    obj.account_id ||
    obj.CLOUDFLARE_R2_ACCOUNT_ID ||
    obj.r2_account_id ||
    /r2\.cloudflarestorage/i.test(endpoint)
  ) {
    return 'r2';
  }
  if (
    obj.accessKeyId ||
    obj.AccessKeyId ||
    obj.aws_access_key_id ||
    obj.AWS_ACCESS_KEY_ID
  ) {
    return 's3';
  }
  throw new Error(
    'Could not infer object-store scheme from credentials file (expected a GCS service account, AWS keys, R2 keys, or a Drive key with subject_to_impersonate)'
  );
}

export function s3ClientConfig(obj) {
  const accessKeyId = firstString(obj, ['accessKeyId', 'AccessKeyId', 'aws_access_key_id', 'AWS_ACCESS_KEY_ID']);
  const secretAccessKey = firstString(obj, [
    'secretAccessKey',
    'SecretAccessKey',
    'aws_secret_access_key',
    'AWS_SECRET_ACCESS_KEY'
  ]);
  const sessionToken = firstString(obj, ['sessionToken', 'SessionToken', 'aws_session_token', 'AWS_SESSION_TOKEN']);
  const region = firstString(obj, ['region', 'AWS_REGION', 'AWS_DEFAULT_REGION']) || 'us-east-1';
  if (!accessKeyId || !secretAccessKey) {
    throw new Error('S3 credentials require accessKeyId and secretAccessKey');
  }
  const credentials = { accessKeyId, secretAccessKey };
  if (sessionToken) credentials.sessionToken = sessionToken;
  return { region, credentials };
}

export function r2ClientConfig(obj) {
  const accountId = firstString(obj, ['account_id', 'CLOUDFLARE_R2_ACCOUNT_ID', 'r2_account_id']);
  const accessKeyId = firstString(obj, [
    'accessKeyId',
    'AccessKeyId',
    'CLOUDFLARE_R2_ACCESS_KEY_ID',
    'aws_access_key_id'
  ]);
  const secretAccessKey = firstString(obj, [
    'secretAccessKey',
    'SecretAccessKey',
    'CLOUDFLARE_R2_SECRET_ACCESS_KEY',
    'aws_secret_access_key'
  ]);
  if (!accountId || !accessKeyId || !secretAccessKey) {
    throw new Error('R2 credentials require account_id, accessKeyId, and secretAccessKey');
  }
  return {
    region: firstString(obj, ['region']) || 'auto',
    endpoint: firstString(obj, ['endpoint']) || `https://${accountId}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId, secretAccessKey },
    forcePathStyle: true
  };
}

export function gcsClientConfig(obj) {
  if (!obj?.client_email || !obj?.private_key) {
    throw new Error('GCS credentials require client_email and private_key');
  }
  const config = { credentials: obj };
  if (obj.project_id) config.projectId = obj.project_id;
  return config;
}

export default {
  inferCredentialsScheme,
  s3ClientConfig,
  r2ClientConfig,
  gcsClientConfig
};
