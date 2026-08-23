import nodetest from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { FileUtilities } from '../index.js';

const { it } = nodetest;

async function withTempDir(fn) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'rename-all-'));
  try {
    return await fn(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

it('renameAll applies replaceAll to file basenames', async () => {
  await withTempDir(async (dir) => {
    await fs.writeFile(path.join(dir, 'report.complete.csv'), 'a');
    await fs.writeFile(path.join(dir, 'notes.csv.complete'), 'b');
    await fs.writeFile(path.join(dir, 'keep.csv'), 'c');
    const futil = new FileUtilities({ accountId: 'test' });
    await futil.renameAll({ directory: dir, search: /\.complete/g, replace: '' });
    const names = (await fs.readdir(dir)).sort();
    assert.deepEqual(names, ['keep.csv', 'notes.csv', 'report.csv']);
  });
});

it('renameAll accepts a /pattern/flags string like worker CLI', async () => {
  await withTempDir(async (dir) => {
    await fs.writeFile(path.join(dir, 'foo.complete.csv'), 'a');
    const futil = new FileUtilities({ accountId: 'test' });
    await futil.renameAll({ directory: dir, search: '/\\.complete/g', replace: '' });
    const names = await fs.readdir(dir);
    assert.deepEqual(names, ['foo.csv']);
  });
});

it('renameAll treats a plain string search as a literal, like replaceAll', async () => {
  await withTempDir(async (dir) => {
    await fs.writeFile(path.join(dir, 'foo.complete.csv'), 'a');
    const futil = new FileUtilities({ accountId: 'test' });
    await futil.renameAll({ directory: dir, search: '.complete', replace: '' });
    const names = await fs.readdir(dir);
    assert.deepEqual(names, ['foo.csv']);
  });
});
