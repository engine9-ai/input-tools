import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import nodetest from 'node:test';
import { FileUtilities } from '../index.js';

const { describe, it, after } = nodetest;

describe('removeFiles', () => {
  const dirs = [];

  after(async () => {
    await Promise.all(dirs.map((dir) => fs.rm(dir, { recursive: true, force: true })));
  });

  async function tempDir() {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'remove-files-'));
    dirs.push(dir);
    return dir;
  }

  async function touch(dir, name) {
    const filename = path.join(dir, name);
    await fs.writeFile(filename, name);
    return filename;
  }

  it('removes a comma-delimited filenames list', async () => {
    const dir = await tempDir();
    const a = await touch(dir, 'a.txt');
    const b = await touch(dir, 'b.txt');
    const files = new FileUtilities({ accountId: 'test' });
    const result = await files.removeFiles({ filenames: `${a},${b}` });
    assert.deepEqual(result.removed.sort(), [a, b].sort());
    assert.equal(result.records, 2);
    await assert.rejects(fs.stat(a));
    await assert.rejects(fs.stat(b));
  });

  it('removes file_array entries and skips no_data', async () => {
    const dir = await tempDir();
    const a = await touch(dir, 'a.txt');
    const b = await touch(dir, 'b.txt');
    const files = new FileUtilities({ accountId: 'test' });
    const result = await files.removeFiles({
      file_array: [{ filename: a }, { filename: b, no_data: true }, a]
    });
    assert.deepEqual(result.removed, [a]);
    await assert.rejects(fs.stat(a));
    await fs.stat(b);
  });

  it('loads filenames from optionsFilename', async () => {
    const dir = await tempDir();
    const a = await touch(dir, 'a.txt');
    const b = await touch(dir, 'b.txt');
    const optionsFilename = path.join(dir, 'options.json');
    await fs.writeFile(optionsFilename, JSON.stringify({ filenames: [a, b] }));
    const files = new FileUtilities({ accountId: 'test' });
    const result = await files.removeFiles({ optionsFilename });
    assert.equal(result.records, 2);
    await assert.rejects(fs.stat(a));
    await assert.rejects(fs.stat(b));
    await fs.stat(optionsFilename);
  });

  it('returns no_data without removing when the loaded options say so', async () => {
    const dir = await tempDir();
    const a = await touch(dir, 'a.txt');
    const optionsFilename = path.join(dir, 'options.json');
    await fs.writeFile(optionsFilename, JSON.stringify({ no_data: true, filename: a }));
    const files = new FileUtilities({ accountId: 'test' });
    const result = await files.removeFiles({ options_filename: optionsFilename });
    assert.equal(result.no_data, true);
    assert.equal(result.records, 0);
    await fs.stat(a);
  });
});
