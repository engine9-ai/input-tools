import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { describe, it, before, after } from 'node:test';
import FileUtilities from '../file/FileUtilities.js';
import {
  contentFingerprint,
  hashFingerprint,
  isSeenRecordsFile,
  SEEN_RECORDS_FILENAME,
  SEEN_RECORDS_LOCK_FILENAME,
  withUniqueRecords
} from '../writeUniqueRecords.js';
import { joinRemotePath } from '../file/tools.js';

const files = new FileUtilities({ accountId: 'test' });

describe('contentFingerprint', () => {
  it('hashes full row content with sorted keys', () => {
    const a = contentFingerprint({ b: 2, a: 1 }, { exclude: [] });
    const b = contentFingerprint({ a: 1, b: 2 }, { exclude: [] });
    const c = contentFingerprint({ a: 1, b: 3 }, { exclude: [] });
    assert.equal(a, b);
    assert.notEqual(a, c);
    assert.match(a, /^[a-f0-9]{32}$/);
  });
  it('excludes listed keys (default export_id)', () => {
    const a = contentFingerprint({ name: 'x', export_id: '1' });
    const b = contentFingerprint({ name: 'x', export_id: '2' });
    const c = contentFingerprint({ name: 'y', export_id: '1' });
    assert.equal(a, b);
    assert.notEqual(a, c);
  });
  it('treats same id with a new column as a non-dupe', () => {
    const a = contentFingerprint({ id: '1', name: 'A' }, { exclude: [] });
    const b = contentFingerprint({ id: '1', name: 'A', extra: 'col' }, { exclude: [] });
    assert.notEqual(a, b);
  });
  it('hashes unique_function material — never stores the raw value', () => {
    const byFn = contentFingerprint({ id: '1', name: 'A' }, { unique_function: (o) => o.id });
    const same = contentFingerprint({ id: '1', name: 'B' }, { unique_function: (o) => o.id });
    assert.equal(byFn, hashFingerprint('1'));
    assert.equal(byFn, same);
    assert.notEqual(byFn, '1');
    assert.match(byFn, /^[a-f0-9]{32}$/);
  });
});

describe('writeUniqueRecords', async () => {
  const directory = path.join(os.tmpdir(), `e9-write-unique-${Date.now()}-${process.pid}`);
  before(async () => {
    await fsp.mkdir(directory, { recursive: true });
  });
  after(async () => {
    await fsp.rm(directory, { recursive: true, force: true });
  });

  it('writes unseen content, then a second run of the same files writes nothing', async () => {
    const { filename: source } = await files.objectStreamToFile({
      stream: [
        { id: '1', name: 'One' },
        { id: '2', name: 'Two' },
        { id: '1', name: 'One' }
      ],
      targetFormat: 'jsonl'
    });
    const first = await files.writeUniqueRecords({
      directory,
      filename: source,
      target_format: 'jsonl',
      exclude: [],
      backfill: false
    });
    assert.equal(first.records, 2);
    assert.equal(first.seen, 1);
    assert.ok(first.file_array[0].filename.startsWith(directory));
    assert.equal(path.basename(first.seen_file), SEEN_RECORDS_FILENAME);
    const seenLines = (await fsp.readFile(first.seen_file, 'utf8')).trim().split('\n');
    assert.equal(seenLines.length, 2);
    for (const line of seenLines) assert.match(line, /^[a-f0-9]{32}$/);

    const second = await files.writeUniqueRecords({
      directory,
      filename: source,
      target_format: 'jsonl',
      exclude: [],
      backfill: false
    });
    assert.equal(second.records, 0);
    assert.equal(second.seen, 3);
    assert.equal(second.no_data, true);
    assert.equal(second.file_array[0].filename, null);
  });

  it('keeps same id when a new column makes content different', async () => {
    const sub = path.join(directory, 'new-col');
    await fsp.mkdir(sub, { recursive: true });
    const { filename: fileA } = await files.objectStreamToFile({
      stream: [{ id: '1', name: 'A' }],
      targetFormat: 'jsonl'
    });
    await files.writeUniqueRecords({
      directory: sub,
      filename: fileA,
      target_format: 'jsonl',
      exclude: [],
      backfill: false
    });
    const { filename: fileB } = await files.objectStreamToFile({
      stream: [{ id: '1', name: 'A', extra: 'yes' }],
      targetFormat: 'jsonl'
    });
    const again = await files.writeUniqueRecords({
      directory: sub,
      filename: fileB,
      target_format: 'jsonl',
      exclude: [],
      backfill: false
    });
    assert.equal(again.records, 1);
    assert.equal(again.seen, 0);
  });

  it('backfills from existing files in the directory', async () => {
    const sub = path.join(directory, 'backfill');
    await fsp.mkdir(sub, { recursive: true });
    await fsp.writeFile(path.join(sub, 'prior.jsonl'), `${JSON.stringify({ name: 'A', n: 1 })}\n`);
    const { filename: source } = await files.objectStreamToFile({
      stream: [
        { name: 'A', n: 1 },
        { name: 'C', n: 3 }
      ],
      targetFormat: 'jsonl'
    });
    const result = await files.writeUniqueRecords({
      directory: sub,
      filename: source,
      target_format: 'jsonl',
      exclude: [],
      backfill: true
    });
    assert.equal(result.records, 1);
    assert.equal(result.seen, 1);
    const { stream } = await files.fileToObjectStream({ filename: result.file_array[0].filename });
    assert.deepEqual(
      (await stream.toArray()).map((r) => r.name),
      ['C']
    );
  });

  it('accepts file_array and a stream', async () => {
    const sub = path.join(directory, 'array');
    await fsp.mkdir(sub, { recursive: true });
    const { filename: fileA } = await files.objectStreamToFile({
      stream: [{ name: 'A', n: 1 }],
      targetFormat: 'jsonl'
    });
    const { filename: fileB } = await files.objectStreamToFile({
      stream: [
        { name: 'A', n: 1 },
        { name: 'C', n: 3 }
      ],
      targetFormat: 'jsonl'
    });
    const result = await files.writeUniqueRecords({
      directory: sub,
      file_array: [{ filename: fileA }, { filename: fileB }],
      target_format: 'jsonl',
      exclude: [],
      backfill: false
    });
    assert.equal(result.records, 2);
    assert.equal(result.seen, 1);

    const streamed = await files.writeUniqueRecords({
      directory: sub,
      stream: [{ name: 'A', n: 1 }],
      target_format: 'jsonl',
      exclude: [],
      backfill: false
    });
    assert.equal(streamed.records, 0);
    assert.equal(streamed.seen, 1);
  });

  it('joins remote paths without using local path.join', () => {
    assert.equal(joinRemotePath('s3://bucket/dir/', SEEN_RECORDS_LOCK_FILENAME), 's3://bucket/dir/seen_records.lock');
  });

  it('waits for seen_records.lock then proceeds', async () => {
    const sub = path.join(directory, 'lock-wait');
    await fsp.mkdir(sub, { recursive: true });
    await files.write({
      filename: path.join(sub, SEEN_RECORDS_LOCK_FILENAME),
      content: JSON.stringify({ ts: new Date().toISOString(), pid: process.pid }),
      exclusive: true
    });
    const pending = files.writeUniqueRecords({
      directory: sub,
      stream: [{ id: `lock-wait-${Date.now()}`, name: 'Waited' }],
      target_format: 'jsonl',
      exclude: [],
      backfill: false,
      lock_timeout_ms: 2000
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    await files.remove({ filename: path.join(sub, SEEN_RECORDS_LOCK_FILENAME) });
    const result = await pending;
    assert.equal(result.records, 1);
  });

  it('errors if seen_records.lock is not acquired in time', async () => {
    const sub = path.join(directory, 'lock-timeout');
    await fsp.mkdir(sub, { recursive: true });
    await files.write({
      filename: path.join(sub, SEEN_RECORDS_LOCK_FILENAME),
      content: JSON.stringify({ ts: new Date().toISOString(), pid: process.pid }),
      exclusive: true
    });
    try {
      await assert.rejects(
        () =>
          files.writeUniqueRecords({
            directory: sub,
            stream: [{ id: 'lock-timeout', name: 'Nope' }],
            target_format: 'jsonl',
            exclude: [],
            backfill: false,
            lock_timeout_ms: 400
          }),
        /Could not acquire content dedupe lock/
      );
    } finally {
      await files.remove({ filename: path.join(sub, SEEN_RECORDS_LOCK_FILENAME) }).catch(() => {});
    }
  });

  it('hides index files from listing helpers', () => {
    const names = ['pull-1.jsonl', SEEN_RECORDS_FILENAME, SEEN_RECORDS_LOCK_FILENAME, 'seen_records.sqlite', 'data.csv'];
    assert.deepEqual(
      names.filter((n) => !isSeenRecordsFile(n)),
      ['pull-1.jsonl', 'data.csv']
    );
  });
});

describe('withUniqueRecords (export-style)', () => {
  it('backfills prior export rows excluding export_id, then filters a second stream', async () => {
    const directory = path.join(os.tmpdir(), `e9-export-session-${Date.now()}-${process.pid}`);
    await fsp.mkdir(directory, { recursive: true });
    try {
      const prior = path.join(directory, 'prior.export.csv');
      await fsp.writeFile(prior, 'export_id,person_id,name\ne1,1,Alice\ne2,2,Bob\n');
      const first = await withUniqueRecords(
        files,
        {
          directory,
          exclude: ['export_id'],
          backfill_filenames: [prior]
        },
        (session) => {
          const classified = session.classify([
            { person_id: '1', name: 'Alice' },
            { person_id: '2', name: 'Bob' },
            { person_id: '3', name: 'Carol' }
          ]);
          assert.equal(classified.seen.length, 2);
          assert.equal(classified.unseen.length, 1);
          assert.equal(classified.unseen[0].name, 'Carol');
          session.commit(classified.fingerprints);
          return { records: classified.unseen.length, deduplicated: classified.seen.length };
        }
      );
      assert.equal(first.records, 1);
      assert.equal(first.deduplicated, 2);

      const second = await withUniqueRecords(
        files,
        { directory, exclude: ['export_id'], backfill: true },
        (session) => {
          const classified = session.classify([
            { person_id: '3', name: 'Carol' },
            { person_id: '3', name: 'Carol', extra: 'new-col' }
          ]);
          return { unseen: classified.unseen.map((r) => r.extra || null), seen: classified.seen.length };
        }
      );
      assert.equal(second.seen, 1);
      assert.deepEqual(second.unseen, ['new-col']);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('writeUniqueRecords into a store twice mirrors export add_to_input_store dedupe', async () => {
    const directory = path.join(os.tmpdir(), `e9-export-write-${Date.now()}-${process.pid}`);
    await fsp.mkdir(directory, { recursive: true });
    try {
      const first = await files.writeUniqueRecords({
        directory,
        stream: [
          { export_id: 'e1', person_id: '1', name: 'Alice' },
          { export_id: 'e2', person_id: '2', name: 'Bob' }
        ],
        target_format: 'csv',
        file_extended_type: 'export',
        exclude: ['export_id'],
        backfill: false
      });
      assert.equal(first.records, 2);
      assert.ok(first.file_array[0].filename.includes('.export'));

      const second = await files.writeUniqueRecords({
        directory,
        stream: [
          { export_id: 'e3', person_id: '1', name: 'Alice' },
          { export_id: 'e4', person_id: '2', name: 'Bob' },
          { export_id: 'e5', person_id: '3', name: 'Carol' }
        ],
        target_format: 'csv',
        file_extended_type: 'export',
        exclude: ['export_id'],
        backfill: true,
        backfill_filter: '\\.export\\.csv$'
      });
      assert.equal(second.records, 1);
      assert.equal(second.seen, 2);
      const { stream } = await files.fileToObjectStream({ filename: second.file_array[0].filename });
      const written = await stream.toArray();
      assert.equal(written.length, 1);
      assert.equal(written[0].name, 'Carol');
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
});
