import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import nodetest from 'node:test';
import { FileUtilities, writeTempFile } from '../index.js';

const { describe, it } = nodetest;

describe('FileUtilities.move local to remote', () => {
  it('puts then unlinks the local source', async () => {
    const futil = new FileUtilities({ accountId: 'test' });
    const src = await writeTempFile({ accountId: 'test', content: 'payload', postfix: '.txt' });
    const putCalls = [];
    futil.put = async (opts) => {
      putCalls.push(opts);
      return { filename: opts.target };
    };
    const result = await futil.move({ filename: src.filename, target: 'gs://bucket/out.txt' });
    assert.equal(result.filename, 'gs://bucket/out.txt');
    assert.equal(putCalls.length, 1);
    assert.equal(putCalls[0].filename, src.filename);
    assert.equal(putCalls[0].target, 'gs://bucket/out.txt');
    await assert.rejects(() => fsp.access(src.filename), { code: 'ENOENT' });
  });

  it('copy puts and keeps the local source', async () => {
    const futil = new FileUtilities({ accountId: 'test' });
    const src = await writeTempFile({ accountId: 'test', content: 'payload', postfix: '.txt' });
    futil.put = async (opts) => ({ filename: opts.target });
    try {
      await futil.copy({ filename: src.filename, target: 'gs://bucket/out.txt' });
      await fsp.access(src.filename);
    } finally {
      await fsp.rm(src.filename, { force: true });
    }
  });
});
