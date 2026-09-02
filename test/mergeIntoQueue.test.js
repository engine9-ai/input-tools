import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { keyFromFields, mergeIntoQueue } from '../mergeIntoQueue.js';

describe('mergeIntoQueue', () => {
  it('keyFromFields supports normalizeField', () => {
    const normalizeField = (field, value) =>
      field === 'email' && typeof value === 'string' ? value.trim().toLowerCase() : (value ?? '');
    assert.equal(
      keyFromFields({ email: '  Ada@Example.com ', person_id: 1 }, ['email', 'person_id'], { normalizeField }),
      keyFromFields({ email: 'ada@example.com', person_id: 1 }, ['email', 'person_id'], { normalizeField })
    );
  });

  it('merges duplicate keys when merge is provided', () => {
    const queue = [];
    const merge = (a, b) => ({ ...a, ...b, status: b.status ?? a.status });
    mergeIntoQueue(queue, { person_id: 1, email: 'a@b.com', status: 'Unsubscribed' }, {
      keyFields: ['email', 'person_id'],
      merge
    });
    mergeIntoQueue(queue, { person_id: 1, email: 'a@b.com', status: 'Subscribed' }, {
      keyFields: ['email', 'person_id'],
      merge
    });
    assert.equal(queue.length, 1);
    assert.equal(queue[0].status, 'Subscribed');
  });

  it('throws on duplicate keys without merge', () => {
    const queue = [];
    mergeIntoQueue(queue, { id: 1, person_id: 9 }, { keyFields: ['id'] });
    assert.throws(
      () => mergeIntoQueue(queue, { id: 1, person_id: 10 }, { keyFields: ['id'] }),
      /Duplicate item in queue/
    );
  });
});
