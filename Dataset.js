/**
 * Dataset — a directory of immutable files read as one upsertable row set.
 * Deliberately not called a "table": `options.table` elsewhere in engine9 and frakture names a
 * warehouse (SQL) table; a Dataset is always a directory path.
 *
 *   const dataset = await Dataset.open(directory, { fileUtilities });
 *   const w = dataset.writer({ kind: 'update' });  // or 'append' | 'delete' | 'source'
 *   w.push({ id: 1, status: 'ok' });
 *   const { filename, records } = await w.end();     // local temp → promoted into directory
 *   const rows = await dataset.toArray();          // merged view of every data file
 *
 * Merge rules (Dataset.read):
 *   - Files are applied ascending by uuidv7 basename timestamp, else modifiedAt, else name.
 *   - source / append rows are inserted; rows without a primary key get a per-file synthetic key.
 *   - update rows upsert by primary key. Keys omitted from an update leave prior values.
 *     In CSV update files an empty string also means "no change" (CSV cannot express absence).
 *     In JSONL updates `null` sets null.
 *   - delete rows remove the primary key.
 *   - Dotted keys (`x.y.z`) set nested paths without wiping siblings; nested objects deep-merge.
 *
 * Writers: CSV output unions the columns of every row (two-pass through a local spool), and
 * flattens nested objects to dotted keys so writer and reader agree. JSONL writes rows as-is.
 */
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import zlib from 'node:zlib';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { stringify } from 'csv';
import debug$0 from 'debug';
import FileUtilities from './file/FileUtilities.js';
import { CSV_STRINGIFY_OPTIONS, getTempFilename, joinRemotePath, normalizeRemoteUri } from './file/tools.js';
import {
  DEFAULT_FORMAT,
  DEFAULT_PRIMARY_KEY,
  assertDatasetFileKind,
  basenameOf,
  ensureKindFilename,
  ensureKindPostfix,
  isDatasetSideFile,
  loadDatasetMetadata,
  missingDatasetMetadataError,
  parseDatasetFormat,
  resolveDatasetDirectory,
  sortDatasetFiles,
  datasetFileKind,
  datasetPostfix,
  writeDatasetMetadata
} from './datasetLayout.js';

const debug = debug$0('@engine9/input-tools:dataset');

const STRINGY_FORMATS = new Set(['csv', 'txt', 'xlsx', 'xls']);

function isPlainObject(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date) && !Buffer.isBuffer(v);
}

export function hasPrimaryKey(row, primaryKey) {
  const v = row?.[primaryKey];
  return v !== undefined && v !== null && v !== '';
}

function isNotFoundError(e) {
  const codes = [e?.name, e?.Code, e?.code];
  const status = e?.$metadata?.httpStatusCode ?? e?.statusCode ?? e?.status;
  return status === 404 || codes.some((c) => c === 'NotFound' || c === 'NoSuchKey' || c === 'ENOENT');
}

/** Deep-merge plain objects; other values from `source` replace. Returns `target`. */
export function deepMergeRows(target, source) {
  for (const [k, v] of Object.entries(source)) {
    if (v === undefined) continue;
    if (isPlainObject(v) && isPlainObject(target[k])) deepMergeRows(target[k], v);
    else target[k] = isPlainObject(v) ? deepMergeRows({}, v) : v;
  }
  return target;
}

/** Set `obj.a.b.c = value` for parts ['a','b','c'], creating objects and deep-merging object leaves. */
export function setDottedPath(obj, parts, value) {
  let cur = obj;
  for (let i = 0; i < parts.length - 1; i += 1) {
    const p = parts[i];
    if (!isPlainObject(cur[p])) cur[p] = {};
    cur = cur[p];
  }
  const leaf = parts[parts.length - 1];
  if (isPlainObject(value) && isPlainObject(cur[leaf])) deepMergeRows(cur[leaf], value);
  else cur[leaf] = isPlainObject(value) ? deepMergeRows({}, value) : value;
  return obj;
}

/**
 * Apply one row onto an existing merged row (or a fresh object).
 * `stringy` marks CSV-like sources where '' cannot be distinguished from absent.
 */
export function applyDatasetRow(existing, row, { kind = 'source', stringy = false } = {}) {
  const out = existing || {};
  for (const [k, v] of Object.entries(row)) {
    if (v === undefined) continue;
    if (kind === 'update' && stringy && v === '') continue;
    if (k.includes('.')) {
      setDottedPath(out, k.split('.'), v);
    } else if (isPlainObject(v) && isPlainObject(out[k])) {
      deepMergeRows(out[k], v);
    } else {
      out[k] = isPlainObject(v) ? deepMergeRows({}, v) : v;
    }
  }
  return out;
}

/** Nested plain objects → dotted keys; arrays → JSON; Dates → ISO. Used for CSV output. */
export function flattenRow(row, prefix = '', out = {}) {
  for (const [k, v] of Object.entries(row)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (isPlainObject(v)) flattenRow(v, key, out);
    else if (Array.isArray(v)) out[key] = JSON.stringify(v);
    else if (v instanceof Date) out[key] = v.toISOString();
    else out[key] = v;
  }
  return out;
}

class LineSplit extends Transform {
  constructor() {
    super({ readableObjectMode: true });
    this.buffer = '';
  }
  _transform(chunk, enc, cb) {
    this.buffer += chunk.toString();
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop();
    for (const line of lines) if (line) this.push(line);
    cb();
  }
  _flush(cb) {
    if (this.buffer) this.push(this.buffer);
    cb();
  }
}

/**
 * Object writer for one Dataset file. `push(row)` validates synchronously and throws on a bad
 * row; `end()` finalizes the local file, promotes it into the Dataset directory (when one is
 * set), and resolves `{ filename, records, kind }`. Zero-record files are not promoted.
 */
export class DatasetWriter {
  constructor({
    directory = null,
    fileUtilities,
    accountId,
    kind = 'update',
    format,
    primaryKey = DEFAULT_PRIMARY_KEY,
    requirePrimaryKey,
    filename,
    postfix,
    promote = true,
    keepEmpty,
    validate,
    prepare
  } = {}) {
    this.directory = directory || null;
    this.fileUtilities = fileUtilities || new FileUtilities({ accountId });
    this.accountId = accountId || this.fileUtilities.accountId;
    this.kind = assertDatasetFileKind(kind);
    this.primaryKey = primaryKey || DEFAULT_PRIMARY_KEY;
    this.requirePrimaryKey =
      requirePrimaryKey === undefined ? this.kind === 'update' || this.kind === 'delete' : Boolean(requirePrimaryKey);
    this.promote = Boolean(promote && this.directory);
    this.keepEmpty = keepEmpty === undefined ? !this.promote : Boolean(keepEmpty);
    this.validate = typeof validate === 'function' ? validate : null;
    this.prepare = typeof prepare === 'function' ? prepare : null;
    const parsed = parseDatasetFormat(postfix || format || DEFAULT_FORMAT);
    if (parsed.targetFormat !== 'csv' && parsed.targetFormat !== 'jsonl') {
      throw new Error(`DatasetWriter writes csv or jsonl (optionally gzipped), not ${parsed.targetFormat}`);
    }
    this.format = parsed.format;
    this.targetFormat = parsed.targetFormat;
    this.gzip = parsed.gzip;
    this.postfix = postfix ? ensureKindPostfix(postfix, this.kind) : datasetPostfix({ kind: this.kind, format: parsed.format });
    this.requestedFilename = filename || null;
    this.records = 0;
    this.columns = new Map();
    this.error = null;
    this.started = null;
    this.ended = false;
    this.source = new Readable({ objectMode: true, read() {} });
  }

  async _start() {
    if (this.started) return this.started;
    this.started = (async () => {
      let local = this.requestedFilename || (await getTempFilename({ accountId: this.accountId, postfix: this.postfix }));
      local = ensureKindFilename(local, this.kind);
      this.localFilename = local;
      const counter = new Transform({
        objectMode: true,
        transform: (row, enc, cb) => {
          this.records += 1;
          cb(null, row);
        }
      });
      if (this.targetFormat === 'jsonl') {
        const streams = [
          this.source,
          counter,
          new Transform({
            objectMode: true,
            transform(row, enc, cb) {
              cb(null, `${JSON.stringify(row)}\n`);
            }
          })
        ];
        if (this.gzip) streams.push(zlib.createGzip());
        streams.push(fs.createWriteStream(local));
        this.pipelinePromise = pipeline(streams);
      } else {
        // CSV: spool flattened rows as JSONL, union columns, then stringify on end().
        this.spoolFilename = `${local}.spool.jsonl`;
        const columns = this.columns;
        this.pipelinePromise = pipeline([
          this.source,
          counter,
          new Transform({
            objectMode: true,
            transform(row, enc, cb) {
              const flat = flattenRow(row);
              for (const k of Object.keys(flat)) if (!columns.has(k)) columns.set(k, columns.size);
              cb(null, `${JSON.stringify(flat)}\n`);
            }
          }),
          fs.createWriteStream(this.spoolFilename)
        ]);
      }
      this.pipelinePromise.catch((e) => {
        if (!this.error) this.error = e;
      });
    })();
    return this.started;
  }

  /** Validate and queue one row. Throws synchronously on an invalid row. Returns the row written. */
  push(row) {
    if (row === null || row === undefined) return null;
    if (this.ended) throw new Error('DatasetWriter: push after end()');
    if (typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('DatasetWriter: rows must be plain objects');
    }
    let out = row;
    try {
      if (this.prepare) out = this.prepare(row) || row;
      if (this.requirePrimaryKey && !hasPrimaryKey(out, this.primaryKey)) {
        throw new Error(`Invalid ${this.kind} row, missing primary key '${this.primaryKey}'`);
      }
      if (this.validate) {
        const ok = this.validate(out);
        if (ok === false) throw new Error(`Invalid ${this.kind} row rejected by validator`);
      }
    } catch (e) {
      if (!this.error) this.error = e;
      throw e;
    }
    if (!this.started) this._start();
    this.source.push(out);
    return out;
  }

  write(row) {
    return this.push(row);
  }

  async _finishCsv() {
    const columns = [...this.columns.keys()];
    const streams = [fs.createReadStream(this.spoolFilename), new LineSplit()];
    streams.push(
      new Transform({
        objectMode: true,
        transform(line, enc, cb) {
          cb(null, JSON.parse(line));
        }
      })
    );
    streams.push(stringify({ ...CSV_STRINGIFY_OPTIONS, columns }));
    if (this.gzip) streams.push(zlib.createGzip());
    streams.push(fs.createWriteStream(this.localFilename));
    try {
      if (this.records > 0) await pipeline(streams);
      else await fsp.writeFile(this.localFilename, this.gzip ? zlib.gzipSync('') : '');
    } finally {
      await fsp.unlink(this.spoolFilename).catch(() => {});
    }
  }

  /** Stop writing, discard local files, and make end() reject with `reason`. */
  async abort(reason) {
    if (!this.error) this.error = reason instanceof Error ? reason : new Error(String(reason || 'DatasetWriter aborted'));
    if (this.ended) return;
    this.ended = true;
    if (this.started) {
      this.source.destroy();
      await this.pipelinePromise?.catch(() => {});
    }
    await Promise.all(
      [this.localFilename, this.spoolFilename].filter(Boolean).map((f) => fsp.unlink(f).catch(() => {}))
    );
  }

  /** Finish the file; promote when a directory is set. Resolves `{ filename, records, kind, promoted }`. */
  async end() {
    if (this.ended) {
      if (this.result) return this.result;
      throw this.error || new Error('DatasetWriter: end() already failed');
    }
    this.ended = true;
    try {
      await this._start();
      this.source.push(null);
      await this.pipelinePromise;
      if (this.error) throw this.error;
      if (this.targetFormat === 'csv') await this._finishCsv();
    } catch (e) {
      if (!this.error) this.error = e;
      await Promise.all(
        [this.localFilename, this.spoolFilename].filter(Boolean).map((f) => fsp.unlink(f).catch(() => {}))
      );
      throw this.error;
    }
    const result = { filename: this.localFilename, records: this.records, kind: this.kind, promoted: false };
    if (this.records === 0 && !this.keepEmpty) {
      await fsp.unlink(this.localFilename).catch(() => {});
      result.filename = null;
      this.result = result;
      return result;
    }
    if (this.promote) {
      const target = joinRemotePath(this.directory, basenameOf(this.localFilename));
      debug('promoting %s -> %s (%d records)', this.localFilename, target, this.records);
      const moved = await this.fileUtilities.move({ filename: this.localFilename, target });
      result.filename = moved?.filename || target;
      result.promoted = true;
    }
    this.result = result;
    return result;
  }
}

export default class Dataset {
  constructor({ directory, fileUtilities, accountId, metadata, primaryKey, format } = {}) {
    if (!directory) throw new Error('Dataset requires directory');
    this.directory = directory;
    this.fileUtilities = fileUtilities || new FileUtilities({ accountId });
    this.accountId = accountId || this.fileUtilities.accountId;
    this.metadata = metadata || { primary_key: DEFAULT_PRIMARY_KEY, format: DEFAULT_FORMAT, metadata_present: false };
    this.explicitPrimaryKey = primaryKey || null;
    this.explicitFormat = format || null;
  }

  /**
   * Open a Dataset directory. `directory` may be relative when `storePath` is given.
   * `primary_key` / `format` override metadata.json for this handle only (use putMetadata to persist).
   */
  static async open(directory, { fileUtilities, accountId, storePath, primary_key, primaryKey, format } = {}) {
    const files = fileUtilities || new FileUtilities({ accountId });
    if (!directory) throw new Error('Dataset.open requires directory');
    const resolved = storePath
      ? resolveDatasetDirectory(directory, { storePath })
      : normalizeRemoteUri(String(directory).trim()).replace(/(.)[/\\]+$/, '$1');
    const metadata = await loadDatasetMetadata(resolved, files);
    return new Dataset({
      directory: resolved,
      fileUtilities: files,
      accountId,
      metadata,
      primaryKey: primary_key || primaryKey || null,
      format: format || null
    });
  }

  get primaryKey() {
    return this.explicitPrimaryKey || this.metadata?.primary_key || DEFAULT_PRIMARY_KEY;
  }

  get format() {
    return parseDatasetFormat(this.explicitFormat || this.metadata?.format || DEFAULT_FORMAT).format;
  }

  get metadataPresent() {
    return Boolean(this.metadata?.metadata_present);
  }

  /** Whether upserts/deletes may be written: metadata.json declares a key, or one was passed to open(). */
  get keyed() {
    return this.metadataPresent || Boolean(this.explicitPrimaryKey);
  }

  /** Write (merge by default) metadata.json and refresh this handle. */
  async putMetadata(fields = {}, { merge = true, normalize } = {}) {
    const next = {
      ...(fields.primary_key || fields.primaryKey ? {} : { primary_key: this.primaryKey }),
      ...(fields.format ? {} : { format: this.format }),
      ...fields
    };
    if (next.primaryKey) {
      next.primary_key = next.primaryKey;
      delete next.primaryKey;
    }
    await writeDatasetMetadata(this.directory, next, this.fileUtilities, { merge, normalize });
    this.metadata = await loadDatasetMetadata(this.directory, this.fileUtilities);
    return this.metadata;
  }

  /** Ensure metadata.json exists with at least primary_key and format. No-op when present. */
  async ensureMetadata(fields = {}) {
    if (this.metadataPresent && !Object.keys(fields).length) return this.metadata;
    return this.putMetadata(fields);
  }

  /**
   * Data files in apply order: `[{ filename, name, kind, format, modifiedAt }]`.
   * Side files (metadata.json, seen_records*, locks, .idv1.parquet, …) are excluded.
   */
  async listFiles({ kinds } = {}) {
    let listing;
    try {
      listing = await this.fileUtilities.list({ directory: this.directory });
    } catch (e) {
      if (isNotFoundError(e)) return [];
      throw e;
    }
    const wanted = kinds ? new Set([].concat(kinds).map(assertDatasetFileKind)) : null;
    const files = (listing || [])
      .filter((f) => f && f.type !== 'directory' && f.name && !String(f.name).includes('/'))
      .filter((f) => !isDatasetSideFile(f.name))
      .map((f) => {
        const name = basenameOf(f.name);
        const kind = datasetFileKind(name);
        return {
          filename: joinRemotePath(this.directory, name),
          name,
          kind,
          format: parseDatasetFormat(name).targetFormat,
          modifiedAt: f.modifiedAt || null
        };
      })
      .filter((f) => !wanted || wanted.has(f.kind));
    return sortDatasetFiles(files);
  }

  /** Raw rows across files in apply order. Yields `{ row, file }`. */
  async *scan({ kinds, files: fileList } = {}) {
    const files = fileList || (await this.listFiles({ kinds }));
    for (const file of files) {
      const { stream } = await this.fileUtilities.fileToObjectStream({ filename: file.filename });
      for await (const row of stream) {
        yield { row, file };
      }
    }
  }

  /**
   * Merge every data file into one Map keyed by primary key. Returns
   * `{ rows: Map, files, records, deleted }` where `records` counts rows read.
   */
  async merge({ kinds, files: fileList } = {}) {
    const files = fileList || (await this.listFiles({ kinds }));
    const pk = this.primaryKey;
    const rows = new Map();
    let records = 0;
    let deleted = 0;
    for (let fi = 0; fi < files.length; fi += 1) {
      const file = files[fi];
      const stringy = STRINGY_FORMATS.has(file.format);
      const { stream } = await this.fileUtilities.fileToObjectStream({ filename: file.filename });
      let ri = 0;
      for await (const row of stream) {
        records += 1;
        ri += 1;
        if (!row || typeof row !== 'object') continue;
        const keyed = hasPrimaryKey(row, pk);
        if (file.kind === 'delete') {
          if (keyed && rows.delete(String(row[pk]))) deleted += 1;
          continue;
        }
        if (file.kind === 'update') {
          if (!keyed) continue; // cannot target a row
          const key = String(row[pk]);
          rows.set(key, applyDatasetRow(rows.get(key), row, { kind: 'update', stringy }));
          continue;
        }
        const key = keyed ? String(row[pk]) : `\u0000${fi}:${ri}`;
        rows.set(key, applyDatasetRow(rows.get(key), row, { kind: file.kind, stringy }));
      }
    }
    return { rows, files, records, deleted };
  }

  /** Merged rows as an object-mode Readable: `{ stream, files, records, deleted }`. */
  async read(options = {}) {
    const { rows, files, records, deleted } = await this.merge(options);
    return { stream: Readable.from(rows.values()), files, records, deleted, size: rows.size };
  }

  /** Merged rows as an array. */
  async toArray(options = {}) {
    const { rows } = await this.merge(options);
    return [...rows.values()];
  }

  /**
   * Writer for one new file of `kind`. Upsert/delete kinds require a declared primary key
   * (metadata.json or `primary_key` passed to open) unless `requirePrimaryKey: false`.
   */
  writer({ kind = 'update', format, filename, postfix, requirePrimaryKey, validate, prepare, promote = true } = {}) {
    assertDatasetFileKind(kind);
    const needsKey = requirePrimaryKey === undefined ? kind === 'update' || kind === 'delete' : Boolean(requirePrimaryKey);
    if (needsKey && !this.keyed) throw missingDatasetMetadataError(this.directory);
    return new DatasetWriter({
      directory: this.directory,
      fileUtilities: this.fileUtilities,
      accountId: this.accountId,
      kind,
      format: format || this.format,
      primaryKey: this.primaryKey,
      requirePrimaryKey: needsKey,
      filename,
      postfix,
      promote,
      validate,
      prepare
    });
  }
}
