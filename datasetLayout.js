/**
 * Directory-local Dataset convention (lakehouse-style, no catalog DB). A Dataset is a
 * directory of immutable files -- distinct from a warehouse (SQL) table.
 *
 * A Dataset is one self-contained directory: optional metadata.json plus data
 * files in any input-tools format. A data file's *kind* is read from its
 * basename:
 *
 *   source  — no token; a full row set (base load)
 *   append  — `.append.` or `.timeline.`; insert-only rows (timeline entries)
 *   update  — `.update.`; upsert rows keyed by metadata primary_key
 *   delete  — `.delete.`; tombstones keyed by primary_key
 *
 * Writers (Dataset / ForEachEntry) emit local files then promote them into the
 * directory — remote stores cannot append in place. `Dataset.read()` merges the
 * files in date order. Files are immutable once promoted.
 */
import fsp from 'node:fs/promises';
import path from 'node:path';
import JSON5 from 'json5';
import { validate as uuidIsValid, version as uuidVersion } from 'uuid';
import { isRemotePath, joinRemotePath, normalizeRemoteUri } from './file/tools.js';
import { isSeenRecordsFile } from './writeUniqueRecords.js';
import { getUUIDTimestamp } from './uuidTools.js';

export const DEFAULT_PRIMARY_KEY = 'id';
export const DEFAULT_FORMAT = 'csv.gz';
export const DEFAULT_DATASET_TYPE = 'dataset';
export const METADATA_FILENAME = 'metadata.json';
export const UPDATE_TOKEN = '.update.';
export const DELETE_TOKEN = '.delete.';
export const APPEND_TOKEN = '.append.';
export const TIMELINE_TOKEN = '.timeline.';
export const DATASET_KINDS = Object.freeze(['source', 'append', 'update', 'delete']);
const KIND_TOKENS = { update: UPDATE_TOKEN, delete: DELETE_TOKEN, append: APPEND_TOKEN };

export function basenameOf(name) {
  const s = String(name || '');
  const i = Math.max(s.lastIndexOf('/'), s.lastIndexOf('\\'));
  return i >= 0 ? s.slice(i + 1) : s;
}

function isNotFoundError(e) {
  const codes = [e?.name, e?.Code, e?.code];
  const status = e?.$metadata?.httpStatusCode ?? e?.statusCode ?? e?.status;
  const message = String(e?.message || '');
  return (
    status === 404 ||
    codes.some((c) => c === 'NotFound' || c === 'NoSuchKey' || c === 'ENOENT') ||
    message.includes('ENOENT') ||
    message.includes('NoSuchKey') ||
    /no file named metadata\.json/i.test(message)
  );
}

export function isUpdateFile(name) {
  return basenameOf(name).includes(UPDATE_TOKEN);
}

export function isDeleteFile(name) {
  return basenameOf(name).includes(DELETE_TOKEN);
}

export function isAppendFile(name) {
  const base = basenameOf(name);
  return base.includes(APPEND_TOKEN) || base.includes(TIMELINE_TOKEN);
}

/** Kind of a Dataset data file from its basename: 'update' | 'delete' | 'append' | 'source'. */
export function datasetFileKind(name) {
  if (isDeleteFile(name)) return 'delete';
  if (isUpdateFile(name)) return 'update';
  if (isAppendFile(name)) return 'append';
  return 'source';
}

export function assertDatasetFileKind(kind) {
  if (!DATASET_KINDS.includes(kind)) {
    throw new Error(`Invalid Dataset file kind '${kind}', expected one of ${DATASET_KINDS.join(', ')}`);
  }
  return kind;
}

/**
 * Sort key for Dataset files: the uuidv7 timestamp when the basename starts with one,
 * else the listed modifiedAt, else 0. Ties are broken by basename.
 */
export function datasetFileSortTime(file) {
  const base = basenameOf(typeof file === 'string' ? file : file?.filename || file?.name);
  const head = base.slice(0, 36);
  if (uuidIsValid(head) && uuidVersion(head) === 7) return getUUIDTimestamp(head).getTime();
  const m = typeof file === 'object' ? file?.modifiedAt || file?.modified_at : null;
  if (m) {
    const t = new Date(m).getTime();
    if (!Number.isNaN(t)) return t;
  }
  return 0;
}

/** Ascending order by datasetFileSortTime, then basename. Returns a new array. */
export function sortDatasetFiles(files) {
  return [...(files || [])]
    .map((f) => ({ f, t: datasetFileSortTime(f), n: basenameOf(typeof f === 'string' ? f : f?.filename || f?.name) }))
    .sort((a, b) => a.t - b.t || (a.n < b.n ? -1 : a.n > b.n ? 1 : 0))
    .map(({ f }) => f);
}

export function isDatasetSideFile(name) {
  const base = basenameOf(name).toLowerCase();
  if (!base) return true;
  if (isSeenRecordsFile(base)) return true;
  if (base === METADATA_FILENAME || base.endsWith('.metadata.json5') || base.endsWith('.metadata.json')) {
    return true;
  }
  if (base.endsWith('.error.json') || base.endsWith('.idv1.parquet')) return true;
  if (base.endsWith('.lock') || base.endsWith('.sqlite') || base.endsWith('.sqlite-wal') || base.endsWith('.sqlite-shm')) {
    return true;
  }
  return false;
}

/**
 * Infer writer format from metadata `format` or a filename/postfix.
 * `.update.` tokens and extra labels (e.g. `.timeline.`) are ignored.
 */
export function parseDatasetFormat(format) {
  let s = String(format || DEFAULT_FORMAT)
    .trim()
    .toLowerCase();
  if (s.startsWith('.')) s = s.slice(1);
  s = s.replace(/(^|\.)update\./g, '$1');
  const gzip = s.endsWith('.gz');
  const core = gzip ? s.slice(0, -3).replace(/\.$/, '') : s;
  let targetFormat = 'csv';
  if (core === 'parquet' || core.endsWith('.parquet') || core.includes('parquet')) targetFormat = 'parquet';
  else if (core === 'jsonl' || core.endsWith('.jsonl') || core.includes('jsonl')) targetFormat = 'jsonl';
  else if (core === 'json' || core.endsWith('.json')) targetFormat = 'jsonl';
  else if (core === 'csv' || core.endsWith('.csv') || core.includes('csv')) targetFormat = 'csv';
  else if (core) targetFormat = core.split('.').pop() || 'csv';
  const normalized = gzip ? `${targetFormat}.gz` : targetFormat;
  return {
    format: normalized,
    targetFormat,
    gzip,
    postfix: gzip ? `${UPDATE_TOKEN}${targetFormat}.gz` : `${UPDATE_TOKEN}${targetFormat}`
  };
}

export function updateFilePostfix({ format } = {}) {
  return parseDatasetFormat(format || DEFAULT_FORMAT).postfix;
}

/** Postfix for a new file of `kind`, e.g. `.update.csv.gz`, `.delete.jsonl`, `.append.csv.gz`, `.csv.gz`. */
export function datasetPostfix({ kind = 'update', format } = {}) {
  assertDatasetFileKind(kind);
  const { format: normalized } = parseDatasetFormat(format || DEFAULT_FORMAT);
  const token = KIND_TOKENS[kind];
  return token ? `${token}${normalized}` : `.${normalized}`;
}

export function ensureUpdatePostfix(postfix) {
  return ensureKindPostfix(postfix, 'update');
}

export function ensureKindPostfix(postfix, kind = 'update') {
  assertDatasetFileKind(kind);
  const raw = postfix == null || postfix === '' ? datasetPostfix({ kind }) : String(postfix);
  const withDot = raw.startsWith('.') ? raw : `.${raw}`;
  const token = KIND_TOKENS[kind];
  if (!token || datasetFileKind(`x${withDot}`) === kind) return withDot;
  return `${token.slice(0, -1)}${withDot}`;
}

/**
 * Insert the kind token after the first basename segment unless it is already present.
 * `abc.csv.gz` + update → `abc.update.csv.gz`; `plain` + delete → `plain.delete.csv`.
 */
export function ensureKindFilename(localPathOrBasename, kind = 'update') {
  assertDatasetFileKind(kind);
  const full = String(localPathOrBasename || '');
  const base = basenameOf(full);
  if (!base) throw new Error('ensureKindFilename requires a filename');
  const token = KIND_TOKENS[kind];
  if (!token) return full;
  if (datasetFileKind(base) === kind) return full;
  const dir = full.slice(0, full.length - base.length);
  const firstDot = base.indexOf('.');
  const next = firstDot < 0 ? `${base}${token}csv` : `${base.slice(0, firstDot)}${token}${base.slice(firstDot + 1)}`;
  return `${dir}${next}`;
}

export function ensureUpdateFilename(localPathOrBasename) {
  return ensureKindFilename(localPathOrBasename, 'update');
}

/**
 * Parent directory of a local or remote file path.
 * `s3://`, `r2://`, `gs://`, `gcs://`, and `gdrive://` keep their scheme.
 * A bare filename returns `.`.
 */
export function directoryFromFilename(filename) {
  const full = normalizeRemoteUri(String(filename || '').trim());
  if (!full) return '';
  const base = basenameOf(full);
  if (!base) return '';
  const dir = full.slice(0, full.length - base.length).replace(/[/\\]+$/, '');
  return dir || '.';
}

/**
 * Resolve a Dataset directory the way a host should hand it to writers.
 * Remote URIs (`s3://`, `r2://`, `gs://`, `gcs://`, `gdrive://`) and absolute local paths pass
 * through unchanged (trailing slashes trimmed, `gcs://` normalized to `gs://`).
 * A relative path is joined under `storePath` with `joinRemotePath`, so a store root of
 * `s3://bucket/root` and `acct/plugins/<pid>/person/ab12/<input_id>` yields one remote directory.
 */
export function resolveDatasetDirectory(directory, { storePath } = {}) {
  const raw = normalizeRemoteUri(String(directory ?? '').trim());
  if (!raw) throw new Error('resolveDatasetDirectory requires directory');
  if (isRemotePath(raw)) return raw.replace(/[/\\]+$/, '');
  if (path.isAbsolute(raw)) {
    const trimmed = raw.replace(/[/\\]+$/, '');
    return trimmed || raw;
  }
  const store = storePath == null ? '' : String(storePath).trim();
  if (!store) {
    throw new Error(`resolveDatasetDirectory: relative directory '${raw}' requires storePath (e.g. ENGINE9_STORED_INPUT_PATH)`);
  }
  return joinRemotePath(store, raw);
}

/**
 * Merge (default) or replace metadata.json in a Dataset directory.
 * `directory` is any local or remote directory (`s3://`, `r2://`, `gs://`, …), same as an input store.
 * Join a relative path with `joinRemotePath(storePath, rel)` first. Absolute paths and remote URIs pass through.
 * Optional `normalize(obj)` runs on existing and incoming objects before merge (e.g. snake_case keys).
 */
export async function writeDatasetMetadata(directory, metadata, fileWorker, { merge = true, normalize } = {}) {
  if (!directory) throw new Error('writeDatasetMetadata requires directory');
  if (!fileWorker || typeof fileWorker.write !== 'function') {
    throw new Error('writeDatasetMetadata requires fileWorker with write()');
  }
  if (metadata == null || typeof metadata !== 'object' || Array.isArray(metadata)) {
    throw new Error('writeDatasetMetadata requires a metadata object');
  }
  const map = typeof normalize === 'function' ? normalize : (o) => o;
  const filename = joinRemotePath(directory, METADATA_FILENAME);
  let existing = {};
  if (merge) {
    try {
      if (typeof fileWorker.json !== 'function') {
        throw new Error('writeDatasetMetadata merge requires fileWorker.json()');
      }
      const parsed = await fileWorker.json({ filename });
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) existing = parsed;
    } catch (e) {
      if (!isNotFoundError(e)) throw e;
    }
  }
  const next = { ...map(existing), ...map(metadata) };
  await fileWorker.write({ filename, content: JSON.stringify(next, null, 4) });
  return next;
}

function defaultMetadata(directory) {
  return {
    type: DEFAULT_DATASET_TYPE,
    primary_key: DEFAULT_PRIMARY_KEY,
    format: DEFAULT_FORMAT,
    directory: directory || null,
    metadata_present: false
  };
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

/** Shell command that writes metadata.json for a Dataset directory. */
export function putMetadataCommand(directory) {
  return `engine9 putMetadata --directory=${shellQuote(directory)} --metadata='{"primary_key":"<column>","format":"csv.gz"}'`;
}

/**
 * Error for an update write into a directory that has no metadata.json.
 */
export function missingDatasetMetadataError(directory) {
  return new Error(`No metadata.json in ${directory}. Run ${putMetadataCommand(directory)} to create it.`);
}

/**
 * Read optional metadata.json; missing file yields Dataset defaults.
 * Extra creator fields (including input_id) are passed through.
 */
export async function loadDatasetMetadata(directory, fileWorker) {
  const defaults = defaultMetadata(directory);
  if (!directory) return defaults;
  const filename = joinRemotePath(directory, METADATA_FILENAME);
  if (!isRemotePath(directory)) {
    try {
      await fsp.access(filename);
    } catch (e) {
      if (isNotFoundError(e)) return defaults;
      throw e;
    }
  }
  let parsed;
  try {
    if (fileWorker && typeof fileWorker.json === 'function') {
      parsed = await fileWorker.json({ filename });
    } else if (isRemotePath(directory)) {
      throw new Error('loadDatasetMetadata requires fileWorker for remote directories');
    } else {
      parsed = JSON5.parse(await fsp.readFile(filename, 'utf8'));
    }
  } catch (e) {
    if (isNotFoundError(e)) return defaults;
    throw e;
  }
  if (parsed == null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`Invalid ${METADATA_FILENAME} in ${directory}: expected an object`);
  }
  return {
    ...defaults,
    ...parsed,
    type: parsed.type || defaults.type,
    primary_key: parsed.primary_key || parsed.primaryKey || defaults.primary_key,
    format: parsed.format || defaults.format,
    directory,
    metadata_present: true
  };
}

/**
 * Move local (or already-written) files into the Dataset directory.
 * Uses FileUtilities.move so s3://, r2://, and gs:// targets get a put, not an append.
 * `kind` (default 'update') is stamped into the basename when missing; `asUpdate: false`
 * is a legacy alias for `kind: 'source'` (name kept as-is).
 */
export async function promoteUpdateFiles({ fileWorker, files, directory, asUpdate = true, kind } = {}) {
  if (!directory) throw new Error('promoteUpdateFiles requires directory');
  if (!fileWorker || typeof fileWorker.move !== 'function') {
    throw new Error('promoteUpdateFiles requires fileWorker with move()');
  }
  const resolvedKind = kind || (asUpdate ? 'update' : 'source');
  assertDatasetFileKind(resolvedKind);
  const out = [];
  for (const file of files || []) {
    if (!file?.filename) {
      out.push(file);
      continue;
    }
    const named = ensureKindFilename(file.filename, resolvedKind);
    const target = joinRemotePath(directory, basenameOf(named));
    const moved = await fileWorker.move({ filename: file.filename, target });
    out.push({ ...file, filename: moved.filename });
  }
  return out;
}
