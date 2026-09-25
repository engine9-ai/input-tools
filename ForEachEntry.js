/**
 * ForEachEntry — batch a person/entry file (csv, csv.gz, jsonl, parquet, xlsx, or a packet)
 * through an async transform, with named output bindings the transform can push rows into.
 *
 * Output bindings (`bindings.<name> = { path, options }`):
 *   output.dataset { dataset | directory, kind = 'update', format, primary_key, storePath, postfix, filename }
 *                   Writes one file of `kind` into a Dataset (a directory of immutable files — not a
 *                   warehouse table) and promotes it there.
 *   output.stream   Scratch file in the temp dir (no directory inference). With `options.directory`
 *                   it behaves like output.dataset with kind 'update'.
 *   output.timeline Timeline entries (kind 'append'); requires person_id, defaults ts, assigns a
 *                   deterministic id when the row allows it. Promoted when `options.directory` is set.
 *   file            getFile(binding) result.      handlebars   the shared handlebars instance.
 *
 * Every output binding value has `push(row)`. `process()` resolves `{ outputFiles, records, batches }`
 * where `outputFiles.<name>` is `[{ filename, records, kind, promoted }]`.
 */
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { throttle } from 'throttle-debounce';
import parallelTransform from 'parallel-transform';
import debug$0 from 'debug';
import handlebars from 'handlebars';
import { v7 as uuidv7 } from 'uuid';
import FileUtilities from './file/FileUtilities.js';
import { getBatchTransform, getFile } from './file/tools.js';
import Dataset, { DatasetWriter, hasPrimaryKey } from './Dataset.js';
import { canComputeTimelineEntryUUID, getTimelineEntryUUID } from './uuidTools.js';
import { DEFAULT_FORMAT, parseDatasetFormat } from './datasetLayout.js';

const debug = debug$0('@engine9/input-tools');
const debugThrottle = throttle(1000, debug, { noLeading: false, noTrailing: false });

const OUTPUT_PATHS = new Set(['output.dataset', 'output.stream', 'output.timeline']);

function timelinePrepare({ defaults = {}, assignRandomIds }) {
  return (row) => {
    if (typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('Invalid timeline data push, must be an object');
    }
    if (defaults.entry_type && !row.entry_type && (row.entry_type_id === undefined || row.entry_type_id === null)) {
      row.entry_type = defaults.entry_type;
    }
    if (defaults.plugin_id && !row.plugin_id) row.plugin_id = defaults.plugin_id;
    if (row.person_id === undefined || row.person_id === null || row.person_id === '') {
      throw new Error('Invalid timeline data push, must have a person_id, even if 0');
    }
    if (!row.ts) row.ts = new Date().toISOString();
    if (!hasPrimaryKey(row, 'id')) {
      if (canComputeTimelineEntryUUID(row)) {
        try {
          row.id = getTimelineEntryUUID(row);
        } catch (e) {
          debug('deterministic timeline id failed, falling back to uuidv7: %s', e.message);
          if (assignRandomIds) row.id = uuidv7();
        }
      } else if (assignRandomIds) {
        row.id = uuidv7();
      }
    }
    return row;
  };
}

class ForEachEntry {
  constructor({ accountId, fileUtilities } = {}) {
    this.accountId = accountId;
    this.fileUtilities = fileUtilities || new FileUtilities({ accountId });
  }

  async _openDataset(options = {}) {
    if (options.dataset instanceof Dataset) return options.dataset;
    if (options.dataset) throw new Error('binding options.dataset must be a Dataset instance');
    if (!options.directory) return null;
    return Dataset.open(options.directory, {
      fileUtilities: this.fileUtilities,
      accountId: this.accountId,
      storePath: options.storePath || options.store_path,
      primary_key: options.primary_key || options.primaryKey,
      format: options.format
    });
  }

  /** Build the writer for one output binding. */
  async _outputWriter(bindingName, binding) {
    const options = binding.options || {};
    const isTimeline = binding.path === 'output.timeline';
    const dataset = await this._openDataset(options);
    const format = options.format || dataset?.format || DEFAULT_FORMAT;
    const parsedFormat = parseDatasetFormat(format).format;

    if (isTimeline) {
      const prepare = timelinePrepare({
        defaults: { entry_type: options.entry_type, plugin_id: options.plugin_id },
        assignRandomIds: Boolean(dataset)
      });
      const postfix = options.postfix || `.timeline.${parsedFormat}`;
      if (dataset) {
        return dataset.writer({ kind: 'append', format, postfix, filename: options.filename, prepare, requirePrimaryKey: false });
      }
      return new DatasetWriter({
        fileUtilities: this.fileUtilities,
        accountId: this.accountId,
        kind: 'append',
        format,
        postfix,
        filename: options.filename,
        prepare,
        requirePrimaryKey: false,
        promote: false
      });
    }

    const kind = options.kind || 'update';
    if (dataset) {
      return dataset.writer({
        kind,
        format,
        postfix: options.postfix,
        filename: options.filename,
        requirePrimaryKey: options.requirePrimaryKey ?? options.require_primary_key,
        validate: options.validate
      });
    }
    if (binding.path === 'output.dataset') {
      throw new Error(`Binding ${bindingName}: output.dataset requires options.dataset or options.directory`);
    }
    // Scratch output: temp file, no promotion, no key enforcement, name kept as requested.
    return new DatasetWriter({
      fileUtilities: this.fileUtilities,
      accountId: this.accountId,
      kind: options.kind || 'source',
      format,
      postfix: options.postfix,
      filename: options.filename,
      requirePrimaryKey: Boolean(options.primary_key || options.primaryKey),
      primaryKey: options.primary_key || options.primaryKey,
      promote: false,
      validate: options.validate
    });
  }

  async _inputStream({ filename, packet, stream, input = {} }) {
    if (stream) return (await this.fileUtilities.fileToObjectStream({ stream })).stream;
    if (filename) {
      debug(`Processing file ${filename}`);
      return (await this.fileUtilities.fileToObjectStream({ filename, ...input })).stream;
    }
    if (packet) {
      debug(`Processing person file from packet ${packet}`);
      return (await this.fileUtilities.stream({ packet, type: input.type || 'person' })).stream;
    }
    throw new Error('process requires filename, packet, or stream');
  }

  async process({
    packet,
    filename,
    stream,
    input,
    progress,
    transform: userTransform,
    batchSize = 500,
    concurrency = 10,
    bindings = {}
  }) {
    if (typeof userTransform !== 'function') throw new Error('async transform function is required');
    if (userTransform.length > 1) throw new Error('transform should be an async function that accepts one argument');
    const inStream = await this._inputStream({ filename, packet, stream, input });

    let progressThrottle = () => {};
    if (typeof progress === 'function') {
      const startTime = Date.now();
      progressThrottle = throttle(
        2000,
        ({ records, batches }) => {
          const perMinute = ((records * 60 * 1000) / Math.max(1, Date.now() - startTime)).toFixed(1);
          progress({ records, message: `Processed ${records} across ${batches} batches,${perMinute} records/minute` });
        },
        { noLeading: false, noTrailing: false }
      );
    }

    const transformArguments = {};
    const writers = {};
    for (const [bindingName, binding] of Object.entries(bindings)) {
      if (!binding?.path) throw new Error(`Invalid binding: path is required for binding ${bindingName}`);
      if (OUTPUT_PATHS.has(binding.path)) {
        const writer = await this._outputWriter(bindingName, binding);
        writers[bindingName] = writer;
        transformArguments[bindingName] = writer;
      } else if (binding.path === 'file') {
        transformArguments[bindingName] = await getFile(binding);
      } else if (binding.path === 'handlebars') {
        transformArguments[bindingName] = handlebars;
      } else {
        throw new Error(`Unsupported binding path for binding ${bindingName}: ${binding.path}`);
      }
    }

    let records = 0;
    let batches = 0;
    try {
      await pipeline(
        inStream,
        getBatchTransform({ batchSize }).transform,
        parallelTransform(concurrency, (batch, cb) => {
          userTransform({ ...transformArguments, batch })
            .then((d) => {
              batches += 1;
              records += batch?.length || 0;
              progressThrottle({ records, batches });
              debugThrottle(`Processed ${batches} batches for a total of ${records} records`);
              cb(null, d ?? {});
            })
            .catch(cb);
        }),
        new Writable({
          objectMode: true,
          write(batch, enc, cb) {
            cb();
          }
        })
      );
    } catch (e) {
      await Promise.all(Object.values(writers).map((w) => w.abort(e)));
      throw e;
    }
    debug('Completed all batches');
    const outputFiles = {};
    const names = Object.keys(writers);
    const ended = await Promise.allSettled(names.map((n) => writers[n].end()));
    const failed = ended.find((r) => r.status === 'rejected');
    if (failed) {
      await Promise.all(names.map((n) => writers[n].abort(failed.reason)));
      throw failed.reason;
    }
    names.forEach((n, i) => {
      outputFiles[n] = [ended[i].value];
    });
    return { outputFiles, records, batches };
  }
}
export default ForEachEntry;
