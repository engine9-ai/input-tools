import assert from 'node:assert/strict';
import nodetest from 'node:test';
import { FileUtilities, writeTempFile } from '../index.js';
import {
  inferCredentialsScheme,
  s3ClientConfig,
  r2ClientConfig,
  gcsClientConfig
} from '../file/credentials.js';
import GCSWorker from '../file/GCS.js';
import S3Worker from '../file/S3.js';

const { describe, it } = nodetest;

const GCS_SA = {
  type: 'service_account',
  project_id: 'proj',
  client_email: 'bot@proj.iam.gserviceaccount.com',
  private_key: '-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n'
};

describe('inferCredentialsScheme', () => {
  it('detects GCS, Drive, S3, and R2 native files', () => {
    assert.equal(inferCredentialsScheme(GCS_SA), 'gs');
    assert.equal(inferCredentialsScheme({ ...GCS_SA, subject_to_impersonate: 'user@example.com' }), 'gdrive');
    assert.equal(inferCredentialsScheme({ accessKeyId: 'AKI', secretAccessKey: 'secret' }), 's3');
    assert.equal(
      inferCredentialsScheme({
        account_id: 'cfacct',
        accessKeyId: 'AKI',
        secretAccessKey: 'secret'
      }),
      'r2'
    );
  });

  it('rejects unknown objects', () => {
    assert.throws(() => inferCredentialsScheme({ token: 'x' }), /Could not infer object-store scheme/);
  });
});

describe('client configs', () => {
  it('builds S3, R2, and GCS client options', () => {
    assert.deepEqual(s3ClientConfig({ aws_access_key_id: 'AKI', aws_secret_access_key: 'sec', region: 'us-west-2' }), {
      region: 'us-west-2',
      credentials: { accessKeyId: 'AKI', secretAccessKey: 'sec' }
    });
    const r2 = r2ClientConfig({ account_id: 'abc', accessKeyId: 'AKI', secretAccessKey: 'sec' });
    assert.equal(r2.endpoint, 'https://abc.r2.cloudflarestorage.com');
    assert.equal(r2.forcePathStyle, true);
    const gcs = gcsClientConfig(GCS_SA);
    assert.equal(gcs.projectId, 'proj');
    assert.equal(gcs.credentials.client_email, GCS_SA.client_email);
  });
});

describe('FileUtilities.ensureCredentials', () => {
  it('loads a local key file and attaches it only to the matching scheme worker', async () => {
    const { filename } = await writeTempFile({
      accountId: 'test',
      content: JSON.stringify(GCS_SA),
      postfix: '.json'
    });
    const futil = new FileUtilities({ accountId: 'test', credentials: filename });
    await futil.ensureCredentials();
    assert.equal(futil._credentialsScheme, 'gs');
    const gcs = new GCSWorker();
    gcs.resolvedCredentials = futil._resolvedCredentials;
    const storage = gcs.getClient();
    assert.ok(storage);
    const s3 = new S3Worker();
    assert.equal(s3.resolvedCredentials, undefined);
  });

  it('accepts an inline credentials object', async () => {
    const futil = new FileUtilities({
      accountId: 'test',
      credentials: { accessKeyId: 'AKI', secretAccessKey: 'sec' }
    });
    await futil.ensureCredentials();
    assert.equal(futil._credentialsScheme, 's3');
  });
});
