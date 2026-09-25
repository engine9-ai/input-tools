import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { describe, it } from 'node:test';
import { v7 as uuidv7 } from 'uuid';
import { Dataset, DatasetWriter, FileUtilities, ForEachEntry, sortDatasetFiles, datasetFileKind, datasetPostfix, ensureKindFilename } from '../index.js';
import { applyDatasetRow, flattenRow } from '../Dataset.js';

const files = new FileUtilities({ accountId: 'test' });

async function tempTable(metadata = { primary_key: 'id', format: 'csv.gz' }) {
  const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-table-'));
  if (metadata) await fsp.writeFile(path.join(directory, 'metadata.json'), JSON.stringify(metadata));
  return directory;
}
async function gunzipText(filename) {
  return zlib.gunzipSync(await fsp.readFile(filename)).toString('utf8');
}
function uuidAt(ms) {
  return uuidv7({ msecs: ms });
}

describe('Dataset file kinds', () => {
  it('classifies basenames', () => {
    assert.equal(datasetFileKind('base.csv.gz'), 'source');
    assert.equal(datasetFileKind('x.update.csv.gz'), 'update');
    assert.equal(datasetFileKind('x.delete.jsonl'), 'delete');
    assert.equal(datasetFileKind('x.append.csv'), 'append');
    assert.equal(datasetFileKind('x.timeline.csv.gz'), 'append');
  });
  it('builds postfixes and filenames per kind', () => {
    assert.equal(datasetPostfix({ kind: 'update' }), '.update.csv.gz');
    assert.equal(datasetPostfix({ kind: 'delete', format: 'jsonl' }), '.delete.jsonl');
    assert.equal(datasetPostfix({ kind: 'append', format: 'jsonl.gz' }), '.append.jsonl.gz');
    assert.equal(datasetPostfix({ kind: 'source' }), '.csv.gz');
    assert.equal(ensureKindFilename('a.csv', 'delete'), 'a.delete.csv');
    assert.equal(ensureKindFilename('a.timeline.csv', 'append'), 'a.timeline.csv');
    assert.equal(ensureKindFilename('a.csv', 'source'), 'a.csv');
    assert.throws(() => datasetPostfix({ kind: 'bogus' }), /Invalid Dataset file kind/);
  });
  it('sorts by uuidv7 time, then modifiedAt, then name', () => {
    const t0 = Date.UTC(2026, 0, 1);
    const sorted = sortDatasetFiles([
      { name: `${uuidAt(t0 + 5000)}.update.csv` },
      { name: 'zzz.csv', modifiedAt: new Date(t0 + 1000).toISOString() },
      { name: `${uuidAt(t0 + 2000)}.update.csv` },
      { name: 'aaa.csv', modifiedAt: new Date(t0 + 1000).toISOString() },
      { name: 'nodate.csv' }
    ]).map((f) => f.name.replace(/^[0-9a-f-]{36}/, 'uuid'));
    assert.deepEqual(sorted, ['nodate.csv', 'aaa.csv', 'zzz.csv', 'uuid.update.csv', 'uuid.update.csv']);
  });
});

describe('row merge rules', () => {
  it('sets dotted paths without wiping siblings and deep-merges objects', () => {
    const row = applyDatasetRow({ x: { y: { a: 'zxv' } }, n: { p: 1 } }, { 'x.y.z': 123, n: { q: 2 } });
    assert.deepEqual(row, { x: { y: { a: 'zxv', z: 123 } }, n: { p: 1, q: 2 } });
  });
  it('treats empty strings in CSV updates as no change, but nulls in jsonl as values', () => {
    assert.deepEqual(applyDatasetRow({ a: 1, b: 2 }, { a: '', b: 3 }, { kind: 'update', stringy: true }), { a: 1, b: 3 });
    assert.deepEqual(applyDatasetRow({ a: 1 }, { a: null }, { kind: 'update', stringy: false }), { a: null });
    assert.deepEqual(applyDatasetRow({ a: 1 }, { a: '' }, { kind: 'source', stringy: true }), { a: '' });
  });
  it('flattens nested objects to dotted keys for csv', () => {
    assert.deepEqual(flattenRow({ id: 1, x: { y: { z: 2 } }, arr: [1, 2], d: new Date(0) }), {
      id: 1,
      'x.y.z': 2,
      arr: '[1,2]',
      d: '1970-01-01T00:00:00.000Z'
    });
  });
});

describe('DatasetWriter', () => {
  it('unions columns across heterogeneous rows in csv and flattens nested values', async () => {
    const directory = await tempTable();
    try {
      const table = await Dataset.open(directory, { fileUtilities: files });
      const w = table.writer({ kind: 'update', format: 'csv' });
      w.push({ id: 1, status: 'ok' });
      w.push({ id: 2, error: 'bad', nested: { q: 1 } });
      const result = await w.end();
      assert.equal(result.records, 2);
      assert.equal(result.promoted, true);
      assert.equal(path.dirname(result.filename), directory);
      const lines = (await fsp.readFile(result.filename, 'utf8')).trim().split('\n');
      assert.equal(lines[0], 'id,status,error,nested.q');
      assert.equal(lines[1], '1,ok,,');
      assert.equal(lines[2], '2,,bad,1');
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
  it('writes gzipped jsonl as-is', async () => {
    const directory = await tempTable({ primary_key: 'id', format: 'jsonl.gz' });
    try {
      const table = await Dataset.open(directory, { fileUtilities: files });
      const w = table.writer();
      w.push({ id: 1, nested: { q: 1 }, n: null });
      const result = await w.end();
      assert.ok(result.filename.endsWith('.update.jsonl.gz'));
      assert.equal((await gunzipText(result.filename)).trim(), '{"id":1,"nested":{"q":1},"n":null}');
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
  it('does not promote empty files, and cleans up on abort', async () => {
    const directory = await tempTable();
    try {
      const table = await Dataset.open(directory, { fileUtilities: files });
      const empty = await table.writer().end();
      assert.equal(empty.records, 0);
      assert.equal(empty.filename, null);
      const w = table.writer();
      w.push({ id: 1 });
      await w.abort(new Error('boom'));
      await assert.rejects(() => w.end(), /boom/);
      const listing = (await fsp.readdir(directory)).filter((f) => f !== 'metadata.json');
      assert.deepEqual(listing, []);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
  it('enforces the primary key for update and delete rows, and requires a declared key', async () => {
    const directory = await tempTable({ primary_key: 'email' });
    const unkeyed = await tempTable(null);
    try {
      const table = await Dataset.open(directory, { fileUtilities: files });
      const w = table.writer({ kind: 'delete' });
      assert.throws(() => w.push({ id: 1 }), /missing primary key 'email'/);
      await assert.rejects(() => w.end(), /missing primary key 'email'/);
      const appendWriter = table.writer({ kind: 'append' });
      appendWriter.push({ anything: 1 });
      assert.equal((await appendWriter.end()).records, 1);

      const noMeta = await Dataset.open(unkeyed, { fileUtilities: files });
      assert.equal(noMeta.keyed, false);
      assert.throws(() => noMeta.writer({ kind: 'update' }), /No metadata.json/);
      const explicit = await Dataset.open(unkeyed, { fileUtilities: files, primary_key: 'person_id' });
      assert.equal(explicit.keyed, true);
      explicit.writer({ kind: 'update' });
      await explicit.ensureMetadata();
      assert.equal((await Dataset.open(unkeyed, { fileUtilities: files })).primaryKey, 'person_id');
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
      await fsp.rm(unkeyed, { recursive: true, force: true });
    }
  });
  it('writes scratch files without a directory', async () => {
    const w = new DatasetWriter({ fileUtilities: files, kind: 'source', postfix: '.to_resend.csv' });
    w.push({ a: 1 });
    const result = await w.end();
    try {
      assert.ok(result.filename.endsWith('.to_resend.csv'));
      assert.equal(result.promoted, false);
      assert.equal(result.kind, 'source');
    } finally {
      await fsp.unlink(result.filename);
    }
  });
});

describe('Dataset.read', () => {
  it('merges sources, updates, and deletes in date order across csv and jsonl', async () => {
    const directory = await tempTable({ primary_key: 'id', format: 'csv.gz' });
    try {
      const t0 = Date.UTC(2026, 0, 1);
      await fsp.writeFile(path.join(directory, 'base.csv'), 'id,email,x.y.a\n1,a@x.org,zxv\n2,b@x.org,\n3,c@x.org,\n');
      // a source without a uuidv7 name sorts by mtime; make it older than the updates
      await fsp.utimes(path.join(directory, 'base.csv'), new Date(t0), new Date(t0));
      await fsp.writeFile(
        path.join(directory, `${uuidAt(t0 + 1000)}.update.csv`),
        'id,status,x.y.z,email\n1,ok,123,\n2,error,,\n'
      );
      await fsp.writeFile(
        path.join(directory, `${uuidAt(t0 + 2000)}.update.jsonl`),
        '{"id":"1","email":null,"nested":{"q":1}}\n{"id":"4","email":"new@x.org"}\n{"status":"orphan-no-key"}\n'
      );
      await fsp.writeFile(path.join(directory, `${uuidAt(t0 + 3000)}.delete.jsonl`), '{"id":"3"}\n{"id":"999"}\n');
      // side files are ignored
      await fsp.writeFile(path.join(directory, 'seen_records.txt'), 'x');
      await fsp.writeFile(path.join(directory, 'x.idv1.parquet'), 'x');

      const table = await Dataset.open(directory, { fileUtilities: files });
      const listed = await table.listFiles();
      assert.deepEqual(
        listed.map((f) => f.kind),
        ['source', 'update', 'update', 'delete']
      );
      const { stream, records, deleted, size } = await table.read();
      const rows = await stream.toArray();
      assert.equal(records, 10);
      assert.equal(deleted, 1);
      assert.equal(size, 3);
      const byId = Object.fromEntries(rows.map((r) => [String(r.id), r]));
      assert.deepEqual(byId['1'], {
        id: '1',
        email: null,
        x: { y: { a: 'zxv', z: '123' } },
        status: 'ok',
        nested: { q: 1 }
      });
      assert.deepEqual(byId['2'], { id: '2', email: 'b@x.org', x: { y: { a: '' } }, status: 'error' });
      assert.equal(byId['3'], undefined);
      assert.deepEqual(byId['4'], { id: '4', email: 'new@x.org' });
      const onlyUpdates = await table.toArray({ kinds: ['update'] });
      assert.equal(onlyUpdates.length, 3);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
  it('keeps unkeyed source and append rows, and yields [] for a missing directory', async () => {
    const directory = await tempTable({ primary_key: 'id' });
    try {
      await fsp.writeFile(path.join(directory, 'a.csv'), 'id,v\n,x\n,y\n');
      await fsp.writeFile(path.join(directory, 'b.append.csv'), 'id,v\n,z\n');
      const table = await Dataset.open(directory, { fileUtilities: files });
      assert.deepEqual((await table.toArray()).map((r) => r.v), ['x', 'y', 'z']);
      const missing = await Dataset.open(path.join(directory, 'nope'), { fileUtilities: files });
      assert.deepEqual(await missing.listFiles(), []);
      assert.deepEqual(await missing.toArray(), []);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
  it('round-trips ForEachEntry table output through the reader', async () => {
    const directory = await tempTable({ primary_key: 'person_id', format: 'csv.gz' });
    try {
      const src = path.join(os.tmpdir(), `e9-src-${process.pid}-${Date.now()}.csv.gz`);
      await fsp.writeFile(src, zlib.gzipSync('person_id,email\n1,a@x.org\n2,b@x.org\n'));
      const table = await Dataset.open(directory, { fileUtilities: files });
      const forEach = new ForEachEntry({ accountId: 'test' });
      const { outputFiles } = await forEach.process({
        filename: src,
        bindings: { out: { path: 'output.dataset', options: { dataset: table } } },
        async transform({ batch, out }) {
          for (const p of batch) out.push(p.person_id === '1' ? { person_id: 1, status: 'ok' } : { person_id: 2, error: 'x' });
        }
      });
      assert.equal(outputFiles.out[0].records, 2);
      const rows = await table.toArray();
      assert.deepEqual(
        rows.map((r) => [r.person_id, r.status, r.error]),
        [
          ['1', 'ok', undefined],
          ['2', undefined, 'x']
        ]
      );
      await fsp.unlink(src);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
});
