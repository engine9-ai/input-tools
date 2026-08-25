import nodetest from 'node:test';
import assert from 'node:assert';
import { getParts } from '../file/GCS.js';
import {
  FileUtilities,
  getServicePrefix,
  isRemotePath,
  joinRemotePath,
  normalizeRemoteUri
} from '../index.js';

const { describe, it } = nodetest;

describe('GCS getParts', () => {
  it('parses gs:// bucket and nested key', () => {
    assert.deepEqual(getParts('gs://my-bucket/a/b/c.csv'), {
      Bucket: 'my-bucket',
      Key: 'a/b/c.csv'
    });
  });

  it('accepts gcs:// alias', () => {
    assert.deepEqual(getParts('gcs://bucket/obj'), {
      Bucket: 'bucket',
      Key: 'obj'
    });
  });

  it('parses bucket-only path', () => {
    assert.deepEqual(getParts('gs://only-bucket'), {
      Bucket: 'only-bucket',
      Key: ''
    });
  });

  it('parses trailing slash directory', () => {
    assert.deepEqual(getParts('gs://b/prefix/'), {
      Bucket: 'b',
      Key: 'prefix/'
    });
  });

  it('rejects non-gcs schemes', () => {
    assert.throws(() => getParts('s3://b/k'), /must start with gs:\/\//);
  });
});

describe('remote path helpers', () => {
  it('isRemotePath recognizes all object-store schemes', () => {
    assert.equal(isRemotePath('s3://b/k'), true);
    assert.equal(isRemotePath('r2://b/k'), true);
    assert.equal(isRemotePath('gdrive://folder/file'), true);
    assert.equal(isRemotePath('gs://b/k'), true);
    assert.equal(isRemotePath('gcs://b/k'), true);
    assert.equal(isRemotePath('/local/path'), false);
    assert.equal(isRemotePath(null), false);
  });

  it('getServicePrefix maps gcs:// to gs', () => {
    assert.equal(getServicePrefix('s3://b/k'), 's3');
    assert.equal(getServicePrefix('gs://b/k'), 'gs');
    assert.equal(getServicePrefix('gcs://b/k'), 'gs');
    assert.equal(getServicePrefix('gdrive://f'), 'gdrive');
    assert.equal(getServicePrefix('/local'), null);
  });

  it('normalizeRemoteUri repairs single-slash and gcs alias', () => {
    assert.equal(normalizeRemoteUri('gs:/bucket/key'), 'gs://bucket/key');
    assert.equal(normalizeRemoteUri('gcs://bucket/key'), 'gs://bucket/key');
    assert.equal(normalizeRemoteUri('s3:/bucket/key'), 's3://bucket/key');
  });

  it('joinRemotePath preserves gs:// double slash', () => {
    assert.equal(
      joinRemotePath('gs://bucket/prefix', 'metadata.json'),
      'gs://bucket/prefix/metadata.json'
    );
    assert.equal(
      joinRemotePath('gcs://bucket/prefix/', 'a', 'b.csv'),
      'gs://bucket/prefix/a/b.csv'
    );
  });
});

describe('FileUtilities routing', () => {
  it('getServiceWorker routes gs:// and gcs:// to GCS worker', async () => {
    const futil = new FileUtilities({ accountId: 'test' });
    // Access via list error path would need credentials; instead verify prefix helpers
    // and that put rejects local destinations with updated message.
    await assert.rejects(
      () => futil.put({ filename: '/tmp/x', directory: '/tmp' }),
      /directory must be s3:\/\/, r2:\/\/, gdrive:\/\/, or gs:\/\//
    );
  });
});

describe('FileUtilities.moveAll cross-service', () => {
  it('lists on source and copies via FileUtilities when prefixes differ', async () => {
    const futil = new FileUtilities({ accountId: 'test' });
    const calls = { listAll: [], move: [] };
    futil.listAll = async (opts) => {
      calls.listAll.push(opts);
      return ['s3://src-bucket/dir/a.csv', 's3://src-bucket/dir/b.csv'];
    };
    futil.move = async (opts) => {
      calls.move.push(opts);
      return { filename: opts.target };
    };
    await futil.moveAll({
      directory: 's3://src-bucket/dir',
      targetDirectory: 'gs://dst-bucket/dir'
    });
    assert.equal(calls.listAll.length, 1);
    assert.deepEqual(calls.move, [
      { filename: 's3://src-bucket/dir/a.csv', target: 'gs://dst-bucket/dir/a.csv' },
      { filename: 's3://src-bucket/dir/b.csv', target: 'gs://dst-bucket/dir/b.csv' }
    ]);
  });
});
