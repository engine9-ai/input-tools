import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { describe, it, before, after } from 'node:test';
import { v7 as uuidv7 } from 'uuid';
import FileUtilities from '../file/FileUtilities.js';
import {
  DEFAULT_FORMAT,
  DEFAULT_PRIMARY_KEY,
  METADATA_FILENAME,
  directoryFromFilename,
  ensureUpdateFilename,
  isDatasetSideFile,
  isUpdateFile,
  loadDatasetMetadata,
  promoteUpdateFiles,
  resolveDatasetDirectory,
  updateFilePostfix,
  writeDatasetMetadata
} from '../datasetLayout.js';

const files = new FileUtilities({ accountId: 'test' });

describe('Dataset naming', () => {
  it('treats .update. in the basename as an update file', () => {
    assert.equal(isUpdateFile('0199a0c5-7c3a-7c11-8000-000000000001.update.csv.gz'), true);
    assert.equal(isUpdateFile('/tmp/foo.update.jsonl.gz'), true);
    assert.equal(isUpdateFile('2024-01-base.csv.gz'), false);
    assert.equal(isUpdateFile('update.csv.gz'), false);
    assert.equal(isUpdateFile('metadata.json'), false);
  });
  it('skips metadata, locks, and seen_records as side files', () => {
    assert.equal(isDatasetSideFile('metadata.json'), true);
    assert.equal(isDatasetSideFile('seen_records.txt'), true);
    assert.equal(isDatasetSideFile('seen_records.lock'), true);
    assert.equal(isDatasetSideFile('dir.sqlite-wal'), true);
    assert.equal(isDatasetSideFile('foo.error.json'), true);
    assert.equal(isDatasetSideFile('x.idv1.parquet'), true);
    assert.equal(isDatasetSideFile('base.csv.gz'), false);
    assert.equal(isDatasetSideFile('x.update.csv.gz'), false);
  });
  it('infers a directory from a local or remote filename', () => {
    assert.equal(directoryFromFilename('/var/data/people.csv'), '/var/data');
    assert.equal(directoryFromFilename('people.csv'), '.');
    assert.equal(directoryFromFilename('s3://bucket/acct/people.csv'), 's3://bucket/acct');
    assert.equal(directoryFromFilename('gs://bucket/a/b.csv.gz'), 'gs://bucket/a');
    assert.equal(directoryFromFilename('gcs://bucket/a/b.csv'), 'gs://bucket/a');
    assert.equal(directoryFromFilename('gdrive://folderId/file.csv'), 'gdrive://folderId');
    assert.equal(directoryFromFilename('r2://bucket/dir/file.jsonl.gz'), 'r2://bucket/dir');
  });
  it('resolves a table directory against a store path', () => {
    const rel = 'acct/plugins/pid/person/ab12/ab12cdef';
    assert.equal(resolveDatasetDirectory(rel, { storePath: '/var/store' }), `/var/store/${rel}`);
    assert.equal(resolveDatasetDirectory(rel, { storePath: '/var/store/' }), `/var/store/${rel}`);
    assert.equal(resolveDatasetDirectory(`/${rel}/`, { storePath: 's3://bucket/root' }), `/${rel}`);
    assert.equal(resolveDatasetDirectory(rel, { storePath: 's3://bucket/root' }), `s3://bucket/root/${rel}`);
    assert.equal(resolveDatasetDirectory(rel, { storePath: 'gcs://bucket/root' }), `gs://bucket/root/${rel}`);
    assert.equal(resolveDatasetDirectory('s3://bucket/a/b/', { storePath: '/var/store' }), 's3://bucket/a/b');
    assert.equal(resolveDatasetDirectory('gcs://bucket/a'), 'gs://bucket/a');
    assert.equal(resolveDatasetDirectory('gdrive://folderId/sub'), 'gdrive://folderId/sub');
    assert.equal(resolveDatasetDirectory('/'), '/');
    assert.throws(() => resolveDatasetDirectory(rel), /requires storePath/);
    assert.throws(() => resolveDatasetDirectory(rel, { storePath: '' }), /requires storePath/);
    assert.throws(() => resolveDatasetDirectory(''), /requires directory/);
    assert.throws(() => resolveDatasetDirectory(undefined, { storePath: '/var/store' }), /requires directory/);
  });
  it('builds and inserts .update. into filenames', () => {
    assert.equal(updateFilePostfix(), '.update.csv.gz');
    assert.equal(updateFilePostfix({ format: 'jsonl.gz' }), '.update.jsonl.gz');
    assert.equal(updateFilePostfix({ format: 'parquet' }), '.update.parquet');
    assert.equal(ensureUpdateFilename('abc.csv.gz'), 'abc.update.csv.gz');
    assert.equal(ensureUpdateFilename('/tmp/abc.update.csv.gz'), '/tmp/abc.update.csv.gz');
    assert.equal(ensureUpdateFilename('s3://b/p/n.jsonl.gz'), 's3://b/p/n.update.jsonl.gz');
    assert.equal(ensureUpdateFilename('plain'), 'plain.update.csv');
  });
});

describe('loadDatasetMetadata', () => {
  it('returns defaults when metadata.json is absent', async () => {
    const directory = path.join(os.tmpdir(), `e9-append-meta-missing-${Date.now()}-${process.pid}`);
    await fsp.mkdir(directory, { recursive: true });
    try {
      const meta = await loadDatasetMetadata(directory, files);
      assert.equal(meta.type, 'dataset');
      assert.equal(meta.primary_key, DEFAULT_PRIMARY_KEY);
      assert.equal(meta.format, DEFAULT_FORMAT);
      assert.equal(meta.directory, directory);
      assert.equal(meta.metadata_present, false);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('merges creator fields including input_id', async () => {
    const directory = path.join(os.tmpdir(), `e9-append-meta-present-${Date.now()}-${process.pid}`);
    await fsp.mkdir(directory, { recursive: true });
    try {
      await fsp.writeFile(
        path.join(directory, METADATA_FILENAME),
        JSON.stringify({
          type: 'table',
          description: 'Sample people',
          primary_key: 'email',
          format: 'jsonl.gz',
          input_id: 'in_123',
          extra: { owner: 'test' }
        })
      );
      const meta = await loadDatasetMetadata(directory, files);
      assert.equal(meta.primary_key, 'email');
      assert.equal(meta.format, 'jsonl.gz');
      assert.equal(meta.input_id, 'in_123');
      assert.equal(meta.metadata_present, true);
      assert.equal(meta.description, 'Sample people');
      assert.deepEqual(meta.extra, { owner: 'test' });
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
});

describe('writeDatasetMetadata', () => {
  it('merges and normalizes keys into metadata.json', async () => {
    const directory = path.join(os.tmpdir(), `e9-append-meta-write-${Date.now()}-${process.pid}`);
    await fsp.mkdir(directory, { recursive: true });
    try {
      await writeDatasetMetadata(directory, { primaryKey: 'email', input_id: 'a' }, files, {
        normalize: (o) => {
          const out = { ...o };
          if (out.primaryKey) {
            out.primary_key = out.primaryKey;
            delete out.primaryKey;
          }
          return out;
        }
      });
      const first = await loadDatasetMetadata(directory, files);
      assert.equal(first.primary_key, 'email');
      assert.equal(first.input_id, 'a');
      await writeDatasetMetadata(directory, { format: 'jsonl.gz' }, files);
      const second = await loadDatasetMetadata(directory, files);
      assert.equal(second.primary_key, 'email');
      assert.equal(second.format, 'jsonl.gz');
      assert.equal(second.input_id, 'a');
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
});

describe('promoteUpdateFiles', () => {
  const directory = path.join(os.tmpdir(), `e9-append-promote-${Date.now()}-${process.pid}`);
  before(async () => {
    await fsp.mkdir(directory, { recursive: true });
  });
  after(async () => {
    await fsp.rm(directory, { recursive: true, force: true });
  });

  it('moves a local update file into the table directory', async () => {
    const src = path.join(os.tmpdir(), `${uuidv7()}.update.csv.gz`);
    await fsp.writeFile(src, 'id,name\n1,a\n');
    const [moved] = await promoteUpdateFiles({
      fileWorker: files,
      files: [{ filename: src, records: 1 }],
      directory
    });
    assert.ok(moved.filename.startsWith(directory));
    assert.equal(isUpdateFile(moved.filename), true);
    await fsp.access(moved.filename);
    await assert.rejects(() => fsp.access(src), { code: 'ENOENT' });
  });
});
