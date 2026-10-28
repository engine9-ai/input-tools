import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bool, chunkArray, getStringArray, isValidDate, ObjectError, parseJSONValue, relativeDate, stableStringify } from '../values.js';
import * as root from '../index.js';

test('values.js is the one copy the package root exports', () => {
  for (const [name, fn] of Object.entries({ bool, chunkArray, getStringArray, isValidDate, ObjectError, parseJSONValue, relativeDate, stableStringify })) {
    assert.equal(root[name], fn, name);
  }
});

test('parseJSONValue: text or object in, object out; fallback for blank or bad text', () => {
  assert.deepEqual(parseJSONValue('{"a":1}'), { a: 1 });
  const obj = { a: 1 };
  assert.equal(parseJSONValue(obj), obj);
  assert.equal(parseJSONValue(null), null);
  assert.deepEqual(parseJSONValue('', []), []);
  assert.deepEqual(parseJSONValue('not json', {}), {});
});

test('stableStringify sorts keys and drops undefined and functions, as JSON.stringify does', () => {
  assert.equal(stableStringify({ b: 1, a: { d: [1, undefined], c: 2 }, e: undefined, f: () => 1 }), '{"a":{"c":2,"d":[1,null]},"b":1}');
  assert.equal(stableStringify({ a: 1, b: 2 }), stableStringify({ b: 2, a: 1 }));
  assert.equal(stableStringify(undefined), 'null');
});

test('chunkArray, getStringArray, bool', () => {
  assert.deepEqual(chunkArray([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(chunkArray([], 2), []);
  assert.deepEqual(getStringArray(' a, b ,,c'), ['a', 'b', 'c']);
  assert.deepEqual(getStringArray(null), []);
  assert.equal(bool('yes'), true);
  assert.equal(bool('0'), false);
  assert.equal(bool('', true), true);
});

test('relativeDate and isValidDate', () => {
  const base = new Date('2026-10-28T12:00:00Z');
  assert.equal(relativeDate('-1d', base).toISOString(), '2026-10-27T12:00:00.000Z');
  assert.equal(relativeDate('none'), null);
  assert.ok(isValidDate(relativeDate('2026-01-01')));
  assert.equal(isValidDate(new Date('nope')), false);
  assert.throws(() => relativeDate('nope'), /Invalid Date/);
});

test('ObjectError keeps the fields of its object', () => {
  const e = new ObjectError({ message: 'bad', code: 'X', status: 422 });
  assert.equal(e.message, 'bad');
  assert.equal(e.code, 'X');
  assert.equal(e.status, 422);
});
