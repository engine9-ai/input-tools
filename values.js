/*
  Small value helpers shared by @engine9/input-tools, @engine9/core,
  @engine9/schemas, and the engine9 server. This is the one copy: core's
  lib/utilities.js re-exports these.

  Portable: no filesystem, no cloud SDKs, only dayjs. Safe to deep-import in a
  Cloudflare Worker (`@engine9/input-tools/values.js`).
*/
import dayjs from 'dayjs';

function isValidDate(d) {
  // we WANT to use isNaN, not the Number.isNaN -- we're checking the date type
  return d instanceof Date && !isNaN(d);
}
/* 'yes', 'true', 't', '1', and truthy non-strings are true; null, undefined, and '' give the default (false). */
function bool(x, _defaultVal) {
  const defaultVal = _defaultVal === undefined ? false : _defaultVal;
  if (x === undefined || x === null || x === '') return defaultVal;
  if (typeof x !== 'string') return !!x;
  if (x === '1') return true; // 0 will return false, but '1' is true
  const y = x.toLowerCase();
  return !!(y.indexOf('y') + 1) || !!(y.indexOf('t') + 1);
}
/* 'a, b' or ['a', 'b'] to a trimmed list with no blanks. */
function getStringArray(s, nonZeroLength) {
  let a = s || [];
  if (typeof a === 'number') a = String(a);
  if (typeof a === 'string') a = [a];
  if (typeof s === 'string') a = s.split(',');
  a = a.map((x) => x.toString().trim()).filter(Boolean);
  if (nonZeroLength && a.length === 0) a = [0];
  return a;
}
/* A Date from a Date, epoch ms, 'now', '-7d', '+1M.start.month', or a date string; null for '' or 'none'. */
function relativeDate(s, _initialDate) {
  let initialDate = _initialDate;
  if (!s || s === 'none') return null;
  if (typeof s.getMonth === 'function') return s;
  // We actually want a double equals here to test strings as well
  if (parseInt(s, 10) == s) {
    const r = new Date(parseInt(s, 10));
    if (!isValidDate(r)) throw new Error(`Invalid integer date:${s}`);
    return r;
  }
  if (initialDate) {
    initialDate = new Date(initialDate);
  } else {
    initialDate = new Date();
  }
  let r = s.match(/^([+-]{1})([0-9]+)([YyMwdhms]{1})([.a-z]*)$/);
  if (r) {
    let period = null;
    switch (r[3]) {
      case 'Y':
      case 'y':
        period = 'years';
        break;
      case 'M':
        period = 'months';
        break;
      case 'w':
        period = 'weeks';
        break;
      case 'd':
        period = 'days';
        break;
      case 'h':
        period = 'hours';
        break;
      case 'm':
        period = 'minutes';
        break;
      case 's':
        period = 'seconds';
        break;
      default:
        period = 'minutes';
        break;
    }
    let d = dayjs(initialDate);
    if (r[1] === '+') {
      d = d.add(parseInt(r[2], 10), period);
    } else {
      d = d.subtract(parseInt(r[2], 10), period);
    }
    if (!isValidDate(d.toDate())) throw new Error(`Invalid date configuration:${r}`);
    if (r[4]) {
      const opts = r[4].split('.').filter(Boolean);
      if (opts[0] === 'start') d = d.startOf(opts[1] || 'day');
      else if (opts[0] === 'end') d = d.endOf(opts[1] || 'day');
      else throw new Error(`Invalid relative date,unknown options:${r[4]}`);
    }
    return d.toDate();
  }
  if (s === 'now') {
    r = dayjs(new Date()).toDate();
    return r;
  }
  r = dayjs(new Date(s)).toDate();
  if (!isValidDate(r)) throw new Error(`Invalid Date: ${s}`);
  return r;
}

/*
  A JSON column's value as an object. SQLite and D1 return JSON columns as
  text, MySQL as objects. null, '', and text that is not JSON give fallback.
*/
function parseJSONValue(value, fallback = null) {
  if (value == null || value === '') return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}
/* JSON with object keys sorted, so equal values stringify (and hash) equally. Functions and undefined are dropped, as JSON.stringify does. */
function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((v) => (v === undefined || typeof v === 'function' ? 'null' : stableStringify(v))).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value)
      .filter((k) => value[k] !== undefined && typeof value[k] !== 'function')
      .sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}
/* [1,2,3,4,5], 2 -> [[1,2],[3,4],[5]] */
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) chunks.push(array.slice(i, i + size));
  return chunks;
}
/*
An error that can take an object as a constructor, that can be dereferenced later.
The object should have a 'message' property for the parent error.
*/
class ObjectError extends Error {
  constructor(data) {
    if (typeof data === 'string') {
      // normal behavior
      super(data);
    } else if (typeof data === 'object') {
      super(data.message);
      Object.keys(data).forEach((k) => {
        this[k] = data[k];
      });
      this.status = data.status;
    } else {
      super('(No error message)');
    }
  }
}

export { bool, chunkArray, getStringArray, isValidDate, ObjectError, parseJSONValue, relativeDate, stableStringify };
