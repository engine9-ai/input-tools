import nodetest from 'node:test';
import assert from 'node:assert';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { ForEachEntry, Dataset, FileUtilities, getTimelineEntryUUID } from '../../index.js';
const { describe, it } = nodetest;

const PLUGIN_ID = 'f9e1024d-21ac-473c-bac6-64796dd771dd';

async function tempDir(prefix) {
  return fsp.mkdtemp(path.join(os.tmpdir(), prefix));
}

describe('ForEachEntry', async () => {
  it('loops through 1000 packet people and writes scratch timeline and stream files', async () => {
    let counter = 0;
    const forEach = new ForEachEntry();
    const result = await forEach.process({
      packet: 'test/sample/1000_message.packet.zip',
      batchSize: 50,
      bindings: {
        timelineOutputFileStream: { path: 'output.timeline', options: { entry_type: 'EMAIL_DELIVERED' } },
        sampleOutputFileStream: { path: 'output.stream' }
      },
      async transform({ batch, timelineOutputFileStream, sampleOutputFileStream }) {
        batch.forEach((p) => {
          if (Math.random() > 0.9) {
            sampleOutputFileStream.push({ person_id: p.person_id || 1, email: p.email, entry_type: 'SAMPLE_OUTPUT' });
          }
          timelineOutputFileStream.push({ person_id: p.person_id || 1, email: p.email });
          counter += 1;
        });
      }
    });
    const timeline = result.outputFiles.timelineOutputFileStream[0];
    const sample = result.outputFiles.sampleOutputFileStream[0];
    assert.equal(timeline.records, 1000);
    assert.ok(sample.records > 0);
    assert.ok(path.basename(timeline.filename).endsWith('.timeline.csv.gz'));
    assert.equal(timeline.promoted, false, 'no directory: timeline stays a scratch file');
    assert.equal(sample.promoted, false, 'no directory: stream stays a scratch file');
    assert.ok(!path.basename(sample.filename).includes('.update.'), 'scratch output is not an update file');
    assert.equal(counter, 1000);
    assert.equal(result.records, 1000);
    const header = zlib.gunzipSync(await fsp.readFile(timeline.filename)).toString('utf8').split('\n')[0];
    assert.ok(header.split(',').includes('entry_type'), 'options.entry_type is applied as a default');
    await fsp.unlink(timeline.filename);
    await fsp.unlink(sample.filename);
  });

  it('reads csv.gz, jsonl, and plain csv inputs', async () => {
    const directory = await tempDir('e9-foreach-input-');
    try {
      const gz = path.join(directory, 'p.csv.gz');
      const jsonl = path.join(directory, 'p.jsonl');
      const csv = path.join(directory, 'p.csv');
      await fsp.writeFile(gz, zlib.gzipSync('person_id,email\n1,a@x.org\n2,b@x.org\n'));
      await fsp.writeFile(jsonl, '{"person_id":1,"email":"a@x.org"}\n{"person_id":2,"email":"b@x.org"}\n');
      await fsp.writeFile(csv, 'person_id,email\n1,a@x.org\n2,b@x.org\n');
      const forEach = new ForEachEntry({ accountId: 'test' });
      for (const filename of [gz, jsonl, csv]) {
        const seen = [];
        const result = await forEach.process({
          filename,
          bindings: {},
          async transform({ batch }) {
            seen.push(...batch.map((p) => p.email));
          }
        });
        assert.deepEqual(seen, ['a@x.org', 'b@x.org'], filename);
        assert.equal(result.records, 2);
      }
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('writes update and timeline files into an explicit table directory', async () => {
    const directory = await tempDir('e9-foreach-table-');
    await fsp.writeFile(path.join(directory, 'metadata.json'), JSON.stringify({ primary_key: 'id', format: 'csv.gz' }));
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      const result = await forEach.process({
        filename: 'test/sample/message/5_fake_people.csv',
        batchSize: 5,
        bindings: {
          updates: { path: 'output.stream', options: { directory } },
          timeline: { path: 'output.timeline', options: { directory, plugin_id: PLUGIN_ID } }
        },
        async transform({ batch, updates, timeline }) {
          for (const p of batch) {
            updates.push({ id: p.email, status: 'ok' });
            timeline.push({ person_id: 1, email: p.email, ts: '2026-01-15T12:00:00Z', entry_type: 'EMAIL_SEND' });
          }
        }
      });
      const updateFile = result.outputFiles.updates[0];
      const timelineFile = result.outputFiles.timeline[0];
      assert.equal(path.dirname(updateFile.filename), directory);
      assert.equal(path.dirname(timelineFile.filename), directory);
      assert.ok(path.basename(updateFile.filename).includes('.update.'));
      assert.ok(timelineFile.filename.endsWith('.timeline.csv.gz'));
      assert.ok(!path.basename(timelineFile.filename).includes('.update.'));
      assert.equal(updateFile.records, 5);
      assert.equal(timelineFile.records, 5);
      const buf = await fsp.readFile(updateFile.filename);
      assert.equal(buf[0], 0x1f);
      assert.equal(buf[1], 0x8b);
      const text = zlib.gunzipSync(await fsp.readFile(timelineFile.filename)).toString('utf8');
      const [header, first] = text.split('\n');
      const cols = header.split(',');
      const row = Object.fromEntries(first.split(',').map((v, i) => [cols[i], v]));
      const expected = getTimelineEntryUUID({
        person_id: 1,
        ts: '2026-01-15T12:00:00Z',
        entry_type: 'EMAIL_SEND',
        plugin_id: PLUGIN_ID
      });
      assert.equal(row.id, expected, 'timeline id is deterministic when the row allows it');
      const table = await Dataset.open(directory, { fileUtilities: new FileUtilities({ accountId: 'test' }) });
      const kinds = (await table.listFiles()).map((f) => f.kind).sort();
      assert.deepEqual(kinds, ['append', 'update']);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('does not infer a table directory from the input file', async () => {
    const directory = await tempDir('e9-foreach-noinfer-');
    const filename = path.join(directory, 'people.csv');
    await fsp.writeFile(filename, 'email\na@example.org\n');
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      const result = await forEach.process({
        filename,
        bindings: { resend: { path: 'output.stream', options: { postfix: '.to_resend.csv' } } },
        async transform({ batch, resend }) {
          for (const p of batch) resend.push({ email: p.email, status: 'ok' });
        }
      });
      const out = result.outputFiles.resend[0];
      assert.notEqual(path.dirname(out.filename), directory);
      assert.ok(out.filename.endsWith('.to_resend.csv'));
      assert.deepEqual(await fsp.readdir(directory), ['people.csv']);
      await fsp.unlink(out.filename);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('requires metadata or primary_key for table updates and enforces the key', async () => {
    const directory = await tempDir('e9-foreach-meta-');
    const filename = path.join(directory, 'people.csv');
    await fsp.writeFile(filename, 'email\na@example.org\n');
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      await assert.rejects(
        () =>
          forEach.process({
            filename,
            bindings: { updates: { path: 'output.dataset', options: { directory } } },
            async transform() {}
          }),
        (err) => err.message.includes('No metadata.json') && err.message.includes(directory)
      );
      await assert.rejects(
        () =>
          forEach.process({
            filename,
            bindings: { updates: { path: 'output.dataset', options: { directory, primary_key: 'id' } } },
            async transform({ batch, updates }) {
              for (const p of batch) updates.push({ email: p.email });
            }
          }),
        /missing primary key 'id'/
      );
      assert.deepEqual(await fsp.readdir(directory), ['people.csv'], 'failed runs leave nothing behind');
      await assert.rejects(
        () => forEach.process({ filename, bindings: { t: { path: 'output.dataset' } }, async transform() {} }),
        /requires options.dataset or options.directory/
      );
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('accepts person_id 0 on timeline rows and rejects missing person_id', async () => {
    const forEach = new ForEachEntry({ accountId: 'test' });
    const ok = await forEach.process({
      stream: [{ person_id: 0 }, { person_id: 5 }],
      bindings: { tl: { path: 'output.timeline' } },
      async transform({ batch, tl }) {
        for (const p of batch) tl.push({ person_id: Number(p.person_id), entry_type: 'EMAIL_SEND' });
      }
    });
    assert.equal(ok.outputFiles.tl[0].records, 2);
    await fsp.unlink(ok.outputFiles.tl[0].filename);
    await assert.rejects(
      () =>
        forEach.process({
          stream: [{ email: 'x' }],
          bindings: { tl: { path: 'output.timeline' } },
          async transform({ batch, tl }) {
            for (const p of batch) tl.push({ email: p.email });
          }
        }),
      /must have a person_id/
    );
  });

  it('can be reused for multiple process() calls', async () => {
    const forEach = new ForEachEntry({ accountId: 'test' });
    const run = () =>
      forEach.process({
        stream: [{ a: 1 }, { a: 2 }],
        bindings: { out: { path: 'output.stream' } },
        async transform({ batch, out }) {
          batch.forEach((r) => out.push(r));
        }
      });
    const a = await run();
    const b = await run();
    assert.equal(a.outputFiles.out[0].records, 2);
    assert.equal(b.outputFiles.out[0].records, 2);
    assert.notEqual(a.outputFiles.out[0].filename, b.outputFiles.out[0].filename);
    await fsp.unlink(a.outputFiles.out[0].filename);
    await fsp.unlink(b.outputFiles.out[0].filename);
  });
});
