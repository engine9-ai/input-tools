/**
 * Directory-local append-table convention (lakehouse-style, no catalog DB).
 *
 * A table is one self-contained directory: optional metadata.json, N source
 * files in any input-tools format, and update files whose basename contains
 * `.update.`. Writers (ForEachEntry) emit local update files then promote
 * them into the directory — remote stores cannot append in place.
 */
import fsp from 'node:fs/promises';
import JSON5 from 'json5';
import { isRemotePath, joinRemotePath, normalizeRemoteUri } from './file/tools.js';
import { isSeenRecordsFile } from './writeUniqueRecords.js';

export const DEFAULT_PRIMARY_KEY = 'id';
export const DEFAULT_FORMAT = 'csv.gz';
export const DEFAULT_TABLE_TYPE = 'table';
export const METADATA_FILENAME = 'metadata.json';
export const UPDATE_TOKEN = '.update.';

function basenameOf(name) {
  const s = String(name || '');
  const i = Math.max(s.lastIndexOf('/'), s.lastIndexOf('\\'));
  return i >= 0 ? s.slice(i + 1) : s;
}

function isNotFoundError(e) {
  const code = e?.name || e?.Code || e?.code;
  const status = e?.$metadata?.httpStatusCode ?? e?.statusCode ?? e?.status;
  const message = String(e?.message || '');
  return (
    status === 404 ||
    code === 'NotFound' ||
    code === 'NoSuchKey' ||
    code === 'ENOENT' ||
    message.includes('ENOENT') ||
    message.includes('NoSuchKey') ||
    /no file named metadata\.json/i.test(message)
  );
}

export function isUpdateFile(name) {
  return basenameOf(name).includes(UPDATE_TOKEN);
}

export function isTableSideFile(name) {
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
export function parseTableFormat(format) {
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
  return parseTableFormat(format || DEFAULT_FORMAT).postfix;
}

export function ensureUpdatePostfix(postfix) {
  const raw = postfix == null || postfix === '' ? updateFilePostfix() : String(postfix);
  const withDot = raw.startsWith('.') ? raw : `.${raw}`;
  if (withDot.includes(UPDATE_TOKEN)) return withDot;
  return `.update${withDot}`;
}

export function ensureUpdateFilename(localPathOrBasename) {
  const full = String(localPathOrBasename || '');
  const base = basenameOf(full);
  if (!base) throw new Error('ensureUpdateFilename requires a filename');
  if (isUpdateFile(base)) return full;
  const dir = full.slice(0, full.length - base.length);
  const firstDot = base.indexOf('.');
  const next =
    firstDot < 0
      ? `${base}${UPDATE_TOKEN}csv`
      : `${base.slice(0, firstDot)}${UPDATE_TOKEN}${base.slice(firstDot + 1)}`;
  return `${dir}${next}`;
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

function defaultMetadata(directory) {
  return {
    type: DEFAULT_TABLE_TYPE,
    primary_key: DEFAULT_PRIMARY_KEY,
    format: DEFAULT_FORMAT,
    directory: directory || null,
    metadata_present: false
  };
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

/** Shell command that writes metadata.json for a table directory. */
export function putMetadataCommand(directory) {
  return `engine9 putMetadata --directory=${shellQuote(directory)} --metadata='{"primary_key":"<column>","format":"csv.gz"}'`;
}

/**
 * Error for an update write into a directory that has no metadata.json.
 */
export function missingTableMetadataError(directory) {
  return new Error(`No metadata.json in ${directory}. Run ${putMetadataCommand(directory)} to create it.`);
}

/**
 * Read optional metadata.json; missing file yields table defaults.
 * Extra creator fields (including input_id) are passed through.
 */
export async function loadTableMetadata(directory, fileWorker) {
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
      throw new Error('loadTableMetadata requires fileWorker for remote directories');
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
 * Move local (or already-written) update files into the table directory.
 * Uses FileUtilities.move so s3://, r2://, and gs:// targets get a put, not an append.
 */
export async function promoteUpdateFiles({ fileWorker, files, directory, asUpdate = true } = {}) {
  if (!directory) throw new Error('promoteUpdateFiles requires directory');
  if (!fileWorker || typeof fileWorker.move !== 'function') {
    throw new Error('promoteUpdateFiles requires fileWorker with move()');
  }
  const out = [];
  for (const file of files || []) {
    if (!file?.filename) {
      out.push(file);
      continue;
    }
    const named = asUpdate ? ensureUpdateFilename(file.filename) : file.filename;
    const targetName = basenameOf(named);
    const target = joinRemotePath(directory, targetName);
    const moved = await fileWorker.move({ filename: file.filename, target });
    out.push({ ...file, filename: moved.filename });
  }
  return out;
}
