import nodetest from 'node:test';
import assert from 'node:assert';
import debug$0 from 'debug';
import { create, getManifest } from '../../index.js';
const { describe, it } = nodetest;
const debug = debug$0('test/zip');
describe('Test Person Packet Creator', async () => {
  const pfile = './test/sample/message/5_fake_people.csv';
  it(`should create a zip file from directory ${process.cwd()} with path ${pfile}`, async () => {
    const out = await create({
      personFiles: [pfile],
      messageFiles: 'test/sample/message/message.json5'
    });
    debug('Successfully created:', out);
    assert.ok(out.bytes > 0, 'Packet is empty');
    const manifest = await getManifest({ packet: out.filename });
    assert.deepEqual(
      manifest.files.map((f) => f.path).sort(),
      ['message/message.json5', 'person/5_fake_people.csv']
    );
    return out;
  });
  it('should reject a missing file', async () => {
    await assert.rejects(create({ personFiles: ['./test/sample/does-not-exist.csv'] }));
  });
  it('should retrieve a manifest', async () => {
    const manifest = await getManifest({ packet: './test/sample/5_message.packet.zip' });
    assert.equal(typeof manifest, 'object', 'Manifest is not an object');
    assert.equal(manifest.accountId, 'engine9', 'Manifest does not have an accountId=engine9');
  });
});
