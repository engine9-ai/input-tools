import nodetest from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { FileUtilities } from '../index.js';

const { it } = nodetest;

it('json gunzips .json.gz before parse', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'file-json-gz-'));
  const filename = path.join(dir, 'sample.json.gz');
  try {
    const payload = { hello: 'world', n: 2 };
    await fs.writeFile(filename, zlib.gzipSync(Buffer.from(`${JSON.stringify(payload)}\n`, 'utf8')));
    const futil = new FileUtilities({ accountId: 'test' });
    assert.deepEqual(await futil.json({ filename }), payload);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
});
