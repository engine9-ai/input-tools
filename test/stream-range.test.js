import nodetest from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { FileUtilities } from '../index.js';

const { it } = nodetest;

it('stream honors inclusive start/end byte offsets', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'file-stream-range-'));
  const filename = path.join(dir, 'sample.txt');
  try {
    await fs.writeFile(filename, 'abcdefghij');
    const futil = new FileUtilities({ accountId: 'test' });
    const { stream } = await futil.stream({ filename, start: 2, end: 5 });
    const chunks = [];
    for await (const chunk of stream) chunks.push(Buffer.from(chunk));
    assert.equal(Buffer.concat(chunks).toString('utf8'), 'cdef');
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
});
