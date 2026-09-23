import fs from 'node:fs';
import zlib from 'node:zlib';
import nodestream from 'node:stream';
import promises from 'node:stream/promises';
import { throttle } from 'throttle-debounce';
import parallelTransform from 'parallel-transform';
import debug$0 from 'debug';
import { Mutex } from 'async-mutex';
import { stringify, parse } from 'csv';
import handlebars from 'handlebars';
import { v7 as uuidv7 } from 'uuid';
import ValidatingReadable from './ValidatingReadable.js';
import FileUtilities from './file/FileUtilities.js';
import { getTempFilename, getBatchTransform, getFile, streamPacket, CSV_STRINGIFY_OPTIONS } from './file/tools.js';
import {
  DEFAULT_FORMAT,
  DEFAULT_PRIMARY_KEY,
  directoryFromFilename,
  ensureUpdateFilename,
  missingTableMetadataError,
  ensureUpdatePostfix,
  loadTableMetadata,
  parseTableFormat,
  promoteUpdateFiles,
  updateFilePostfix
} from './appendTable.js';
const { Transform, Writable } = nodestream;
const { pipeline } = promises;
const debug = debug$0('@engine9/input-tools');
const debugThrottle = throttle(1000, debug, { noLeading: false, noTrailing: false });

function hasPrimaryKey(data, primaryKey) {
  const v = data?.[primaryKey];
  return v !== undefined && v !== null && v !== '';
}

class ForEachEntry {
  constructor({ accountId } = {}) {
    this.fileUtilities = new FileUtilities({ accountId });
  }
  getOutputStream({
    name,
    filename,
    postfix,
    format,
    directory,
    primaryKey = DEFAULT_PRIMARY_KEY,
    requirePrimaryKey = false,
    isTimeline = false,
    validatorFunction = () => true
  }) {
    this.outputStreams = this.outputStreams || {};
    if (this.outputStreams[name]?.items) return this.outputStreams[name].items;
    this.outputStreams[name] = this.outputStreams[name] || {
      mutex: new Mutex()
    };
    return this.outputStreams[name].mutex.runExclusive(async () => {
      const parsed = parseTableFormat(postfix || format || DEFAULT_FORMAT);
      if (parsed.targetFormat !== 'csv' && parsed.targetFormat !== 'jsonl') {
        throw new Error(
          `ForEachEntry output writes csv or jsonl (optionally gzipped), not ${parsed.targetFormat}`
        );
      }
      const resolvedPostfix = isTimeline
        ? postfix || `.timeline.${parsed.format}`
        : ensureUpdatePostfix(postfix || parsed.postfix);
      let f = filename || (await getTempFilename({ postfix: resolvedPostfix }));
      if (!isTimeline) f = ensureUpdateFilename(f);
      const fileInfo = {
        filename: f,
        records: 0
      };
      debug(`Output file requested ${name}, writing output to: ${fileInfo.filename}`);
      const outputStream = new ValidatingReadable(
        {
          objectMode: true
        },
        (data) => {
          if (!data) return true;
          if (typeof data !== 'object') throw new Error('Invalid output data push, must be an object');
          if (!hasPrimaryKey(data, primaryKey)) {
            if (isTimeline && primaryKey === 'id' && directory) {
              data.id = uuidv7();
            } else if (requirePrimaryKey) {
              throw new Error(`Invalid append-table row, missing primary key '${primaryKey}'`);
            }
          }
          return validatorFunction(data);
        }
      );
      outputStream._read = () => {};
      const writeStream = fs.createWriteStream(fileInfo.filename);
      const finishWritingOutputPromise = new Promise((resolve, reject) => {
        writeStream
          .on('finish', () => {
            resolve();
          })
          .on('error', (err) => {
            reject(err);
          });
        outputStream.on('error', reject);
      });
      // Validation can reject this before process() awaits it. A handler now
      // keeps that from becoming an unhandled rejection; the await still throws.
      finishWritingOutputPromise.catch(() => {});
      this.outputStreams[name].items = {
        stream: outputStream,
        promises: [finishWritingOutputPromise],
        files: [fileInfo],
        directory
      };
      let out = outputStream.pipe(
        new Transform({
          objectMode: true,
          transform(o, enc, cb) {
            fileInfo.records += 1;
            cb(null, o);
          }
        })
      );
      if (parsed.targetFormat === 'jsonl') {
        out = out.pipe(
          new Transform({
            objectMode: true,
            transform(d, encoding, cb) {
              cb(null, `${JSON.stringify(d)}\n`);
            }
          })
        );
      } else {
        out = out.pipe(stringify(CSV_STRINGIFY_OPTIONS));
      }
      if (parsed.gzip) out = out.pipe(zlib.createGzip());
      out.pipe(writeStream);
      return this.outputStreams[name].items;
    });
  }
  async process({
    packet,
    filename,
    progress,
    transform: userTransform,
    batchSize = 500,
    concurrency = 10,
    bindings = {}
  }) {
    let inStream = null;
    if (filename) {
      debug(`Processing file ${filename}`);
      inStream = (await this.fileUtilities.stream({ filename })).stream;
    } else if (packet) {
      debug(`Processing person file from packet ${packet}`);
      inStream = (await streamPacket({ packet, type: 'person' })).stream;
    }
    if (typeof userTransform !== 'function') throw new Error('async transform function is required');
    if (userTransform.length > 1) throw new Error('transform should be an async function that accepts one argument');
    let progressThrottle = () => {};
    if (typeof progress === 'function') {
      const startTime = new Date().getTime();
      progressThrottle = throttle(
        2000,
        function ({ records, batches }) {
          let message = `Processed ${records} across ${batches} batches,${(
            (records * 60 * 1000) /
            (new Date().getTime() - startTime)
          ).toFixed(1)} records/minute`;
          progress({ records, message });
        },
        { noLeading: false, noTrailing: false }
      );
    }
    let records = 0;
    let batches = 0;
    const outputFiles = {};
    const transformArguments = {};
    // An array of promises that must be completed, such as writing to disk
    let bindingPromises = [];
    // new Streams may be created, and they have to be completed when the file is completed
    const newStreams = [];
    const promoteDirs = {};
    const bindingNames = Object.keys(bindings);
    const inferredDirectory = filename ? directoryFromFilename(filename) : null;
    await Promise.all(
      bindingNames.map(async (bindingName) => {
        const binding = bindings[bindingName];
        if (!binding.path) throw new Error(`Invalid binding: path is required for binding ${bindingName}`);
        if (binding.path === 'output.timeline' || binding.path === 'output.stream') {
          const isTimeline = binding.path === 'output.timeline';
          const explicitDirectory = binding.options?.directory || null;
          const directory = explicitDirectory || inferredDirectory || null;
          let metadata = {
            primary_key: DEFAULT_PRIMARY_KEY,
            format: DEFAULT_FORMAT,
            metadata_present: false
          };
          if (directory) {
            metadata = await loadTableMetadata(directory, this.fileUtilities);
          }
          if (!isTimeline && directory && !metadata.metadata_present) {
            throw missingTableMetadataError(directory);
          }
          const format = binding.options?.format || metadata.format || DEFAULT_FORMAT;
          const primaryKey = binding.options?.primary_key || metadata.primary_key || DEFAULT_PRIMARY_KEY;
          const basePostfix = updateFilePostfix({ format });
          const postfix =
            binding.options?.postfix || (isTimeline ? `.timeline.${parseTableFormat(format).format}` : basePostfix);
          const {
            stream: streamImpl,
            promises,
            files
          } = await this.getOutputStream({
            name: bindingName,
            filename: binding.options?.filename,
            postfix,
            format,
            directory,
            primaryKey,
            requirePrimaryKey:
              !isTimeline &&
              Boolean(directory || binding.options?.primary_key || metadata.metadata_present),
            isTimeline,
            validatorFunction: isTimeline
              ? (data) => {
                  if (!data) return true;
                  if (typeof data !== 'object') throw new Error('Invalid timeline data push, must be an object');
                  if (!data.person_id) throw new Error('Invalid timeline data push, must have a person_id, even if 0');
                  if (!data.ts) data.ts = new Date().toISOString();
                  return true;
                }
              : () => true
          });
          newStreams.push(streamImpl);
          transformArguments[bindingName] = streamImpl;
          bindingPromises = bindingPromises.concat(promises || []);
          outputFiles[bindingName] = files;
          if (directory) promoteDirs[bindingName] = { directory, asUpdate: !isTimeline };
        } else if (binding.path === 'file') {
          transformArguments[bindingName] = await getFile(binding);
        } else if (binding.path === 'handlebars') {
          transformArguments[bindingName] = handlebars;
        } else {
          throw new Error(`Unsupported binding path for binding ${bindingName}: ${binding.path}`);
        }
      })
    );
    await pipeline(
      inStream,
      parse({
        relax: true,
        skip_empty_lines: true,
        max_limit_on_data_read: 10000000,
        columns: true
      }),
      getBatchTransform({ batchSize }).transform,
      parallelTransform(concurrency, (batch, cb) => {
        userTransform({ ...transformArguments, batch })
          .then((d) => {
            batches += 1;
            records += batch?.length || 0;
            progressThrottle({ records, batches });
            debugThrottle(`Processed ${batches} batches for a total of ${records} outbound records`);
            cb(null, d);
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
    debug('Completed all batches');
    newStreams.forEach((s) => s.push(null));
    await Promise.all(bindingPromises);
    for (const [bindingName, target] of Object.entries(promoteDirs)) {
      outputFiles[bindingName] = await promoteUpdateFiles({
        fileWorker: this.fileUtilities,
        files: outputFiles[bindingName],
        directory: target.directory,
        asUpdate: target.asUpdate
      });
    }
    return { outputFiles };
  }
}
export default ForEachEntry;
