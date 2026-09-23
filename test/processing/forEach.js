import nodetest from 'node:test';
import assert from 'node:assert';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import debug$0 from 'debug';
import { ForEachEntry } from '../../index.js';
const { describe, it } = nodetest;
const debug = debug$0('test/forEach');
describe('Test Person File For Each', async () => {
  it('forEachPerson Should loop through 1000 sample people', async () => {
    let counter = 0;
    const forEach = new ForEachEntry();
    const result = await forEach.process({
      packet: 'test/sample/1000_message.packet.zip',
      batchSize: 50,
      bindings: {
        timelineOutputFileStream: {
          path: 'output.timeline',
          options: {
            entry_type: 'ENTRY_OPTION'
          }
        },
        sampleOutputFileStream: {
          path: 'output.stream'
        }
      },
      async transform(props) {
        const { batch, timelineOutputFileStream, sampleOutputFileStream } = props;
        batch.forEach((p) => {
          if (Math.random() > 0.9) {
            sampleOutputFileStream.push({
              // for testing we don't need real person_ids
              person_id: p.person_id || Math.floor(Math.random() * 1000000),
              email: p.email,
              entry_type: 'SAMPLE_OUTPUT'
            });
          }
          timelineOutputFileStream.push({
            // for testing we don't need real person_ids
            person_id: p.person_id || Math.floor(Math.random() * 1000000),
            email: p.email,
            entry_type: 'EMAIL_DELIVERED'
          });
        });
        batch.forEach(() => {
          counter += 1;
        });
      }
    });
    assert(result.outputFiles?.timelineOutputFileStream?.[0]?.records);
    assert(result.outputFiles?.sampleOutputFileStream?.[0]?.records);
    assert(
      path.basename(result.outputFiles.timelineOutputFileStream[0].filename).endsWith('.timeline.csv.gz'),
      'timeline output should be a timeline file'
    );
    assert(
      !path.basename(result.outputFiles.timelineOutputFileStream[0].filename).includes('.update.'),
      'timeline output is not an update upload'
    );
    assert(
      path.basename(result.outputFiles.sampleOutputFileStream[0].filename).includes('.update.'),
      'stream output should be an update file'
    );
    assert.equal(counter, 1000, `Expected to loop through 1000 people, actual:${counter}`);
  });

  it('promotes output.stream update files into options.directory', async () => {
    const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-foreach-table-'));
    await fsp.writeFile(
      path.join(directory, 'metadata.json'),
      JSON.stringify({ primary_key: 'id', format: 'csv.gz' })
    );
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      const result = await forEach.process({
        filename: 'test/sample/message/5_fake_people.csv',
        batchSize: 5,
        bindings: {
          out: {
            path: 'output.stream',
            options: { directory, format: 'csv.gz' }
          }
        },
        async transform({ batch, out }) {
          for (const p of batch) {
            out.push({ id: p.email, email: p.email, status: 'ok' });
          }
        }
      });
      const written = result.outputFiles.out[0];
      assert.ok(written.filename.startsWith(directory));
      assert.ok(path.basename(written.filename).includes('.update.'));
      assert.equal(written.records, 5);
      await fsp.access(written.filename);
      const buf = await fsp.readFile(written.filename);
      assert.equal(buf[0], 0x1f);
      assert.equal(buf[1], 0x8b);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('writes update and timeline files next to the input file when directory is omitted', async () => {
    const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-foreach-infer-'));
    const filename = path.join(directory, 'people.csv');
    await fsp.copyFile('test/sample/message/5_fake_people.csv', filename);
    await fsp.writeFile(
      path.join(directory, 'metadata.json'),
      JSON.stringify({ primary_key: 'id', format: 'csv.gz' })
    );
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      const result = await forEach.process({
        filename,
        batchSize: 5,
        bindings: {
          updates: { path: 'output.stream' },
          timeline: { path: 'output.timeline' }
        },
        async transform({ batch, updates, timeline }) {
          for (const p of batch) {
            updates.push({ id: p.email, status: 'ok', email: p.email });
            timeline.push({
              person_id: 1,
              email: p.email,
              ts: '2026-01-15T12:00:00Z'
            });
          }
        }
      });
      const updateFile = result.outputFiles.updates[0];
      const timelineFile = result.outputFiles.timeline[0];
      assert.equal(path.dirname(updateFile.filename), directory);
      assert.equal(path.dirname(timelineFile.filename), directory);
      assert.ok(path.basename(updateFile.filename).includes('.update.'));
      assert.ok(!path.basename(updateFile.filename).includes('.timeline.'));
      assert.ok(timelineFile.filename.endsWith('.timeline.csv.gz'));
      assert.ok(!path.basename(timelineFile.filename).includes('.update.'));
      assert.equal(updateFile.records, 5);
      assert.equal(timelineFile.records, 5);
      const timelineText = zlib.gunzipSync(await fsp.readFile(timelineFile.filename)).toString('utf8');
      assert.match(timelineText.split('\n')[0], /(^|,)id(,|$)/);
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('errors with putMetadata when an update directory has no metadata.json', async () => {
    const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-foreach-noid-'));
    const filename = path.join(directory, 'people.csv');
    await fsp.writeFile(filename, 'email\na@example.org\n');
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      await assert.rejects(
        () =>
          forEach.process({
            filename,
            bindings: {
              updates: { path: 'output.stream' }
            },
            async transform({ batch, updates }) {
              for (const p of batch) updates.push({ email: p.email, status: 'ok' });
            }
          }),
        (err) =>
          err.message.includes('engine9 putMetadata --directory=') &&
          err.message.includes(directory) &&
          err.message.includes('--metadata=')
      );
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });

  it('requires the primary key when the inferred directory has metadata.json', async () => {
    const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-foreach-meta-'));
    const filename = path.join(directory, 'people.csv');
    await fsp.writeFile(filename, 'email\na@example.org\n');
    await fsp.writeFile(
      path.join(directory, 'metadata.json'),
      JSON.stringify({ primary_key: 'id', format: 'csv.gz' })
    );
    try {
      const forEach = new ForEachEntry({ accountId: 'test' });
      await assert.rejects(
        () =>
          forEach.process({
            filename,
            bindings: {
              updates: { path: 'output.stream' }
            },
            async transform({ batch, updates }) {
              for (const p of batch) updates.push({ email: p.email });
            }
          }),
        /missing primary key 'id'/
      );
    } finally {
      await fsp.rm(directory, { recursive: true, force: true });
    }
  });
  debug('Completed tests');
});
