import nodetest from 'node:test';
import assert from 'node:assert';
import debug$0 from 'debug';
import promises from 'node:timers/promises';
import { ForEachEntry } from '../../index.js';
const { describe, it } = nodetest;
const debug = debug$0('test:big-data');
const { setTimeout } = promises;
describe('big-data message: forEachPerson', async () => {
  it('message: forEachPerson should loop through 1000000 sample people', async () => {
    const messageContent = [];
    let counter = 0;
    const forEach = new ForEachEntry();
    const output = await forEach.process({
      // packet: '../1000000_person_message.packet.zip',
      filename: '../1000000_person_message.packet/person/1000000_fake_people.csv',
      batchSize: 10000,
      concurrency: 1000,
      progress: debug,
      bindings: {
        timelineOutputStream: { path: 'output.timeline' },
        message: { path: 'file', filename: '../1000000_person_message.packet/message/message.json5' },
        handlebars: { path: 'handlebars' }
      },
      async transform({ batch, message, handlebars, timelineOutputStream }) {
        //const id = uuidv7();
        //debug(`Processing batch of ${batch.length} - ${id}`);
        if (!message?.content?.text) throw new Error(`Sample message has no content.text:${JSON.stringify(message)}`);
        const template = handlebars.compile(message.content.text);
        batch.forEach((person) => {
          messageContent.push(template(person));
        });
        batch.forEach((p) => {
          const o = {
            person_id: p.person_id,
            email: p.email,
            entry_type: 'EMAIL_DELIVERED'
          };
          counter += 1;
          //if (counter % 10000 === 0) debug(`*** Processed ${counter} items, last person_id=${p.person_id}`, o);
          timelineOutputStream.push(o);
        });
        // debug(`Processed batch of size ${batch.length}`);
        await setTimeout(Math.random() * 3000);
        //debug(`Completed processing ${id}`);
      }
    });
    debug(output);
    assert.equal(counter, 1000000, `Expected to loop through 1000000 people, actual:${counter}`);
  });
  debug('Completed all tests');
});
