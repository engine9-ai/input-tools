/**
 * Directory-local content dedupe for inbound stores and outbound exports.
 *
 * seen_records.txt stores only fingerprints: one lowercase hex MD5 per line
 * (RFC 1321, Node crypto.createHash('md5'), UTF-8). Never raw ids. MD5 is the
 * checksum used for non-security content identity (S3 ETag / Content-MD5);
 * 128 bits is a compact TEXT key if this index is later loaded into SQLite.
 *
 * Default material (same id + new column is a non-dupe):
 *   JSON.stringify(sorted makeStrings(omit(record, exclude)))
 *   exclude defaults to ['export_id']
 *
 * unique_function is an optional content projection. Return identity *material*
 * (a string, or a JSON-serializable value) — not a hash. We always MD5 it with
 * the same algorithm so the seen file stays uniform.
 *
 * Hashes commit only after the filtered file is persisted. Parallel runs take
 * seen_records.lock via FileUtilities.write({ exclusive }).
 */
import { createHash } from 'node:crypto';
import fsp from 'node:fs/promises';
import nodestream from 'node:stream';
import debug$0 from 'debug';
import JSON5 from 'json5';
import { bool, isRemotePath, joinRemotePath, makeStrings } from './file/tools.js';

const { Transform } = nodestream;
const debug = debug$0('@engine9/input-tools:writeUniqueRecords');

export const SEEN_RECORDS_FILENAME = 'seen_records.txt';
export const SEEN_RECORDS_LOCK_FILENAME = 'seen_records.lock';
export const SEEN_RECORDS_LOCK_TIMEOUT_MS = 5 * 60 * 1000;
export const SEEN_RECORDS_LOCK_STALE_MS = 30 * 60 * 1000;

const DEFAULT_EXCLUDE = ['export_id'];
const INDEX_FILES = new Set([
  SEEN_RECORDS_FILENAME,
  SEEN_RECORDS_LOCK_FILENAME,
  'seen_records.sqlite',
  'seen_records.sqlite-wal',
  'seen_records.sqlite-shm'
]);

export function isSeenRecordsFile(name) {
  return INDEX_FILES.has(String(name || '').split('/').pop());
}

/** RFC 1321 MD5, lowercase hex. The only form stored in seen_records.txt. */
export const FINGERPRINT_HASH = 'md5';

export function hashFingerprint(material) {
  return createHash(FINGERPRINT_HASH).update(String(material), 'utf8').digest('hex');
}

function uniqueFunctionMaterial(value) {
  if (value == null || value === '') return '';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function basename(name) {
  return String(name || '').split('/').pop();
}

function skipBackfillName(name) {
  const base = basename(name).toLowerCase();
  if (isSeenRecordsFile(base)) return true;
  if (base === 'metadata.json' || base.endsWith('.metadata.json5') || base.endsWith('.metadata.json')) return true;
  if (base.endsWith('.error.json') || base.endsWith('.idv1.parquet')) return true;
  if (base.endsWith('.lock') || base.endsWith('.sqlite') || base.endsWith('.sqlite-wal') || base.endsWith('.sqlite-shm')) {
    return true;
  }
  return false;
}

export function contentFingerprint(record, opts = {}) {
  if (typeof opts.unique_function === 'function') {
    const material = uniqueFunctionMaterial(opts.unique_function(makeStrings(record || {})));
    return material ? hashFingerprint(material) : '';
  }
  const exclude = new Set((opts.exclude == null ? DEFAULT_EXCLUDE : [].concat(opts.exclude)).map(String));
  const strings = makeStrings(record || {});
  const body = {};
  for (const k of Object.keys(strings).sort()) {
    if (!exclude.has(k)) body[k] = strings[k];
  }
  return hashFingerprint(JSON.stringify(body));
}

function fingerprintOpts(opts = {}) {
  return {
    exclude: opts.exclude == null ? [...DEFAULT_EXCLUDE] : [].concat(opts.exclude).map(String),
    unique_function: opts.unique_function
  };
}

function isAlreadyExistsError(e) {
  const code = e?.code || e?.Code || e?.name;
  const status = e?.$metadata?.httpStatusCode ?? e?.statusCode ?? e?.status;
  return (
    code === 'EEXIST' ||
    code === 'PreconditionFailed' ||
    code === 'ConditionalRequestConflict' ||
    status === 412 ||
    /precondition|condition not met|412/i.test(String(e?.message || ''))
  );
}

function isNotFoundError(e) {
  const code = e?.name || e?.Code || e?.code;
  const status = e?.$metadata?.httpStatusCode;
  const message = String(e?.message || '');
  return (
    status === 404 ||
    code === 'NotFound' ||
    code === 'NoSuchKey' ||
    code === 'ENOENT' ||
    message.includes('ENOENT') ||
    message.includes('NoSuchKey')
  );
}

async function readText(fileWorker, filename) {
  try {
    if (!isRemotePath(filename)) {
      return await fsp.readFile(filename, 'utf8');
    }
    const { stream } = await fileWorker.stream({ filename });
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    return Buffer.concat(chunks.map((c) => (Buffer.isBuffer(c) ? c : Buffer.from(c)))).toString('utf8');
  } catch (e) {
    if (isNotFoundError(e)) return '';
    throw e;
  }
}

async function acquireLock(fileWorker, directory, timeoutMs) {
  const lockPath = joinRemotePath(directory, SEEN_RECORDS_LOCK_FILENAME);
  if (String(directory).startsWith('gdrive://')) {
    throw new Error('writeUniqueRecords lock does not support gdrive:// directories');
  }
  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await fileWorker.write({
        filename: lockPath,
        content: JSON.stringify({ ts: new Date().toISOString(), pid: process.pid }),
        exclusive: true
      });
      return async () => {
        try {
          await fileWorker.remove({ filename: lockPath });
        } catch (e) {
          debug(`lock release failed for ${lockPath}: ${e?.message || e}`);
        }
      };
    } catch (e) {
      if (!isAlreadyExistsError(e)) throw e;
      try {
        const raw = await readText(fileWorker, lockPath);
        const ts = Date.parse(JSON.parse(raw || '{}').ts);
        if (Number.isFinite(ts) && Date.now() - ts > SEEN_RECORDS_LOCK_STALE_MS) {
          await fileWorker.remove({ filename: lockPath }).catch(() => {});
          continue;
        }
      } catch {
        /* lock may have been released between write and read */
      }
      if (Date.now() >= deadline) {
        throw new Error(`Could not acquire content dedupe lock at ${lockPath} after ${timeoutMs}ms`);
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
}

async function listBackfillFiles(fileWorker, directory, filter) {
  let files;
  try {
    files = await fileWorker.list({ directory });
  } catch (e) {
    if (e?.code === 'ENOENT' || isNotFoundError(e)) return [];
    throw e;
  }
  const re = filter ? (filter instanceof RegExp ? filter : new RegExp(filter)) : null;
  return (files || [])
    .map((file) => file.name || file)
    .filter((name) => !skipBackfillName(name) && (!re || re.test(name)))
    .map((name) => joinRemotePath(directory, name));
}

async function loadSeen(fileWorker, seenPath) {
  const text = await readText(fileWorker, seenPath);
  return new Set(text.split(/\r?\n/).map((s) => s.trim()).filter(Boolean));
}

/**
 * Lock the directory, load (and optionally backfill) seen hashes, then run `fn(session)`.
 * `session.classify` does not persist; `session.commit` marks hashes dirty; `close` writes
 * seen_records.txt. Export uses this so FOE CSVs stay in their original stringify path.
 */
export async function withUniqueRecords(fileWorker, opts, fn) {
  const { directory } = opts || {};
  if (!directory) throw new Error('withUniqueRecords requires directory');
  if (!isRemotePath(directory)) await fsp.mkdir(directory, { recursive: true });
  const seenPath = joinRemotePath(directory, SEEN_RECORDS_FILENAME);
  const fpOpts = fingerprintOpts(opts);
  const timeoutMs =
    opts.lock_timeout_ms === undefined || opts.lock_timeout_ms === null || opts.lock_timeout_ms === ''
      ? SEEN_RECORDS_LOCK_TIMEOUT_MS
      : parseInt(opts.lock_timeout_ms, 10);
  const release = await acquireLock(fileWorker, directory, timeoutMs);
  const seen = await loadSeen(fileWorker, seenPath);
  let dirty = false;
  try {
    if (bool(opts.backfill, true) && seen.size === 0) {
      const backfillFiles = opts.backfill_filenames || (await listBackfillFiles(fileWorker, directory, opts.backfill_filter));
      if (backfillFiles.length && typeof fileWorker.getUniqueSet === 'function') {
        const { uniqueSet } = await fileWorker.getUniqueSet({
          filenames: backfillFiles,
          uniqueFunction: (row) => contentFingerprint(row, fpOpts),
          onFileRead: opts.on_backfill_file
        });
        for (const id of uniqueSet) {
          if (id) seen.add(id);
        }
        dirty = seen.size > 0;
      }
    }
    const pending = new Set();
    const session = {
      directory,
      seen_file: seenPath,
      classify(records) {
        const unseen = [];
        const seenRows = [];
        const fingerprints = [];
        for (const record of records || []) {
          if (record?._is_placeholder) continue;
          const fp = contentFingerprint(record, fpOpts);
          if (!fp) {
            unseen.push(record);
            continue;
          }
          if (pending.has(fp) || seen.has(fp)) {
            seenRows.push(record);
            continue;
          }
          pending.add(fp);
          unseen.push(record);
          fingerprints.push(fp);
        }
        return { unseen, seen: seenRows, fingerprints };
      },
      commit(fingerprints) {
        for (const row of fingerprints || []) {
          const id = typeof row === 'string' ? row : row?.id;
          if (!id) continue;
          pending.delete(id);
          if (!seen.has(id)) {
            seen.add(id);
            dirty = true;
          }
        }
      },
      discard(fingerprints) {
        for (const row of fingerprints || []) {
          const id = typeof row === 'string' ? row : row?.id;
          if (id) pending.delete(id);
        }
      }
    };
    return await fn(session);
  } finally {
    try {
      if (dirty) {
        await fileWorker.write({
          filename: seenPath,
          content: `${[...seen].join('\n')}\n`
        });
      }
    } finally {
      await release();
    }
  }
}

/**
 * Read files or a stream, drop rows already seen in directory/seen_records.txt,
 * write only unseen rows into that directory, then persist hashes.
 */
export async function writeUniqueRecords(fileWorker, opts = {}) {
  let options = { ...opts };
  if (options.options_filename) {
    const loaded = await fileWorker.json({ filename: options.options_filename });
    options = { ...opts, ...loaded };
  }
  if (bool(options.no_data, false)) {
    return { ...options, no_data: true, records: 0, seen: 0, file_array: [] };
  }
  const { directory } = options;
  if (!directory) throw new Error('writeUniqueRecords requires directory');
  let arr = options.file_array;
  if (typeof arr === 'string') arr = JSON5.parse(arr);
  if (options.filename) arr = [options];
  else if (options.stream && !arr) arr = [options];
  if (!arr) throw new Error('writeUniqueRecords requires filename, file_array, or stream');
  if (!Array.isArray(arr)) arr = [arr];
  arr = arr.map((o) => (typeof o === 'string' ? { filename: o } : o)).filter((o) => !bool(o.no_data, false));
  const seenPath = joinRemotePath(directory, SEEN_RECORDS_FILENAME);
  if (arr.length === 0) {
    return { directory, seen_file: seenPath, no_data: true, records: 0, seen: 0, file_array: [] };
  }
  return withUniqueRecords(fileWorker, options, async (session) => {
    const output = [];
    let fileCounter = 0;
    for (const item of arr) {
      const filename = item.filename || options.filename;
      if (!filename && !item.stream && !options.stream) {
        throw new Error('writeUniqueRecords file_array items require filename or stream');
      }
      fileCounter += 1;
      const sourceLabel = filename || `stream-${fileCounter}`;
      if (typeof fileWorker.progress === 'function') {
        fileWorker.progress(`writeUniqueRecords: file ${fileCounter}/${arr.length} ${sourceLabel}`);
      }
      debug(`writeUniqueRecords ${fileCounter}/${arr.length} ${sourceLabel}`);
      const { stream } = await fileWorker.fileToObjectStream({
        filename,
        stream: item.stream || (!filename ? options.stream : undefined),
        sourcePostfix: item.source_postfix ?? options.source_postfix,
        format: item.format ?? options.format,
        encoding: item.encoding ?? options.encoding
      });
      let seenCount = 0;
      const toCommit = [];
      const filter = new Transform({
        objectMode: true,
        transform(row, enc, cb) {
          try {
            const rows = Array.isArray(row) ? row : [row];
            if (rows[0]?._is_placeholder) return cb();
            const classified = session.classify(rows);
            seenCount += classified.seen.length;
            toCommit.push(...classified.fingerprints);
            for (const rec of classified.unseen) this.push(rec);
            cb();
          } catch (e) {
            cb(e);
          }
        }
      });
      const written = await fileWorker.objectStreamToFile({
        stream: stream.pipe(filter),
        targetFormat: options.target_format || 'csv',
        gzip: bool(options.gzip, false),
        fileExtendedType: options.file_extended_type || 'unique',
        accountId: fileWorker.accountId
      });
      const records = written.records || 0;
      if (records === 0) {
        await fsp.unlink(written.filename).catch(() => {});
        session.discard(toCommit);
        output.push({ source_filename: filename || null, filename: null, records: 0, seen: seenCount });
        continue;
      }
      const targetName = options.target_filename
        ? basename(options.target_filename)
        : basename(written.filename);
      const moved = await fileWorker.move({
        filename: written.filename,
        target: joinRemotePath(directory, targetName)
      });
      session.commit(toCommit);
      output.push({ source_filename: filename || null, filename: moved.filename, records, seen: seenCount });
    }
    const records = output.reduce((a, b) => a + b.records, 0);
    const seen = output.reduce((a, b) => a + b.seen, 0);
    return {
      directory,
      seen_file: seenPath,
      file_array: output,
      records,
      seen,
      no_data: records === 0
    };
  });
}

writeUniqueRecords.metadata = {
  description:
    'Write previously unseen records (content hash by default) from one or many files into directory. Lookup is directory/seen_records.txt. Hashes commit only after a successful write.',
  options: {
    directory: {
      required: true,
      description: 'Directory for seen_records.txt and newly written unique-record files'
    },
    filename: { description: 'A single source file (csv, jsonl, parquet, …)' },
    file_array: {
      description: 'One or many source files, same shape as idFiles (objects with filename, or path strings)'
    },
    stream: { description: 'In-memory records instead of a file' },
    target_format: { description: "Output format: 'csv' (default) or 'jsonl'" },
    gzip: { description: 'Gzip output files (default false)' },
    exclude: {
      description:
        'Keys omitted from the default full-row MD5 material (default export_id). Use for write-time stamps that would otherwise make every row unique.'
    },
    unique_function: {
      description:
        'Optional content projection: (makeStrings(record)) => material. Return a string (or JSON value), not a hash — the result is always MD5 hex, same as the default path. Do not use this to key on id alone.'
    },
    backfill: {
      description: 'When true (default), seed an empty seen file from existing tabular files in directory'
    },
    backfill_filter: { description: 'Optional regex applied to basenames when discovering backfill files' },
    backfill_filenames: { description: 'Explicit file list to seed from (skips directory discovery)' },
    lock_timeout_ms: {
      description: `Wait this long to acquire seen_records.lock (default ${SEEN_RECORDS_LOCK_TIMEOUT_MS}). Stale locks older than ${SEEN_RECORDS_LOCK_STALE_MS}ms are stolen. Works for local, s3://, r2://, and gs:// via FileUtilities.write({ exclusive }).`
    },
    no_data: { description: 'When true at top level or on a file_array item, skip processing' },
    options_filename: { description: 'Load options from a JSON file when the payload is large' },
    file_extended_type: { description: "Temp file name token (default 'unique')" },
    target_filename: { description: 'Optional basename for the written file inside directory' }
  }
};
