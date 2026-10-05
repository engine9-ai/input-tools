import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { describe, it, before, after } from 'node:test';
import FileUtilities from '../file/FileUtilities.js';
import Dataset from '../Dataset.js';

const files = new FileUtilities({ accountId: 'test' });
let directory;

async function writeCsv(name, header, rows) {
  const filename = path.join(directory, name);
  await fsp.writeFile(filename, `${header}\n${rows.join('\n')}\n`);
  return filename;
}

describe('Dataset.batchUnprocessed', () => {
  before(async () => {
    directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-batch-unprocessed-'));
  });
  after(async () => {
    if (directory) await fsp.rm(directory, { recursive: true, force: true });
  });

  it('writes unseen emails once, then only new ones, and skips blank emails', async () => {
    await writeCsv('one.export.csv', 'email,Dev Recent Donors,export_id', [
      'a@example.com,y,1',
      ',y,2',
      'a@example.com,y,3'
    ]);
    const first = await Dataset.batchUnprocessed(directory, {
      fileUtilities: files,
      primary_key: 'email',
      columns: 'email,Dev Recent Donors'
    });
    assert.equal(first.no_data, false);
    assert.equal(first.records, 1);
    assert.match(first.filename, /\.csv$/);
    const firstText = await fsp.readFile(first.filename, 'utf8');
    assert.match(firstText, /email,Dev Recent Donors/);
    assert.match(firstText, /a@example.com,y/);
    assert.equal(firstText.includes('export_id'), false);

    const second = await Dataset.batchUnprocessed(directory, {
      fileUtilities: files,
      primary_key: 'email',
      columns: 'email,Dev Recent Donors'
    });
    assert.equal(second.no_data, true);
    assert.equal(second.records, 0);
    assert.equal(second.filename, null);

    await writeCsv('two.export.csv', 'email,Dev Recent Donors,export_id', ['b@example.com,y,4']);
    const third = await Dataset.batchUnprocessed(directory, {
      fileUtilities: files,
      primary_key: 'email',
      columns: 'email,Dev Recent Donors'
    });
    assert.equal(third.no_data, false);
    assert.equal(third.records, 1);
    const thirdText = await fsp.readFile(third.filename, 'utf8');
    assert.match(thirdText, /b@example.com,y/);
    assert.equal(thirdText.includes('a@example.com'), false);
  });

  it('applies update and delete files before choosing unprocessed keys', async () => {
    const mergedDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-batch-merged-'));
    try {
      await fsp.writeFile(
        path.join(mergedDir, 'base.csv'),
        'email,flag\na@example.com,n\nb@example.com,n\n'
      );
      await fsp.writeFile(path.join(mergedDir, 'fix.update.csv'), 'email,flag\na@example.com,y\n');
      await fsp.writeFile(path.join(mergedDir, 'drop.delete.csv'), 'email\nb@example.com\n');
      const result = await Dataset.batchUnprocessed(mergedDir, {
        fileUtilities: files,
        primary_key: 'email',
        columns: 'email,flag'
      });
      assert.equal(result.records, 1);
      const text = await fsp.readFile(result.filename, 'utf8');
      assert.match(text, /a@example.com,y/);
      assert.equal(text.includes('b@example.com'), false);
    } finally {
      await fsp.rm(mergedDir, { recursive: true, force: true });
    }
  });

  it('returns no_data without reading when no_data is set', async () => {
    const skipped = await Dataset.batchUnprocessed(directory, {
      fileUtilities: files,
      no_data: true
    });
    assert.equal(skipped.no_data, true);
    assert.equal(skipped.records, 0);
    const skippedString = await Dataset.batchUnprocessed({
      directory,
      fileUtilities: files,
      no_data: 'true'
    });
    assert.equal(skippedString.no_data, true);
  });

  it('defaults the key to id, and uses metadata.json primary_key when present', async () => {
    const idDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-batch-id-'));
    const metaDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'e9-batch-meta-'));
    try {
      await fsp.writeFile(
        path.join(idDir, 'people.csv'),
        'id,email\n1,a@example.com\n2,a@example.com\n,blank@example.com\n'
      );
      const byId = await Dataset.batchUnprocessed(idDir, { fileUtilities: files, columns: 'id,email' });
      assert.equal(byId.records, 2);
      const text = await fsp.readFile(byId.filename, 'utf8');
      assert.match(text, /^id,/);
      assert.match(text, /1,a@example.com/);
      assert.match(text, /2,a@example.com/);
      assert.equal(text.includes('blank@example.com'), false);
      const again = await Dataset.batchUnprocessed(idDir, { fileUtilities: files, columns: 'id,email' });
      assert.equal(again.no_data, true);

      await fsp.writeFile(path.join(metaDir, 'metadata.json'), JSON.stringify({ primary_key: 'email' }));
      await fsp.writeFile(path.join(metaDir, 'people.csv'), 'email,flag\na@example.com,y\na@example.com,y\n');
      const byMeta = await Dataset.batchUnprocessed(metaDir, { fileUtilities: files, columns: 'email,flag' });
      assert.equal(byMeta.records, 1);
      const metaText = await fsp.readFile(byMeta.filename, 'utf8');
      assert.match(metaText, /a@example.com,y/);
    } finally {
      await fsp.rm(idDir, { recursive: true, force: true });
      await fsp.rm(metaDir, { recursive: true, force: true });
    }
  });
});
