import fs from 'node:fs';
import nodestream from 'node:stream';
import promises from 'node:stream/promises';
import { throttle } from 'throttle-debounce';
import parallelTransform from 'parallel-transform';
import debug$0 from 'debug';
import { Mutex } from 'async-mutex';
import { stringify, parse } from 'csv';
import handlebars from 'handlebars';
import ValidatingReadable from './ValidatingReadable.js';
import FileUtilities from './file/FileUtilities.js';
import { getTempFilename, getBatchTransform, getFile, streamPacket } from './file/tools.js';
const { Transform, Writable } = nodestream;
const { pipeline } = promises;
const debug = debug$0('@engine9/input-tools');
const debugThrottle = throttle(1000, debug, { noLeading: false, noTrailing: false });
class ForEachEntry {
  constructor({ accountId } = {}) {
    this.fileUtilities = new FileUtilities({ accountId });
  }
  getOutputStream({ name, filename, postfix = '.timeline.csv', validatorFunction = () => true }) {
    this.outputStreams = this.outputStreams || {};
    if (this.outputStreams[name]?.items) return this.outputStreams[name].items;
    this.outputStreams[name] = this.outputStreams[name] || {
      mutex: new Mutex()
    };
    return this.outputStreams[name].mutex.runExclusive(async () => {
      let f = filename || (await getTempFilename({ postfix }));
      const fileInfo = {
        filename: f,
        records: 0
      };
      debug(`Output file requested ${name}, writing output to to: ${fileInfo.filename}`);
      const outputStream = new ValidatingReadable(
        {
          objectMode: true
        },
        validatorFunction
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
      });
      this.outputStreams[name].items = {
        stream: outputStream,
        promises: [finishWritingOutputPromise],
        files: [fileInfo]
      };
      outputStream
        .pipe(
          new Transform({
            objectMode: true,
            transform(o, enc, cb) {
              fileInfo.records += 1;
              cb(null, o);
            }
          })
        )
        .pipe(stringify({ header: true }))
        .pipe(writeStream);
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
    const bindingNames = Object.keys(bindings);
    await Promise.all(
      bindingNames.map(async (bindingName) => {
        const binding = bindings[bindingName];
        if (!binding.path) throw new Error(`Invalid binding: path is required for binding ${bindingName}`);
        if (binding.path === 'output.timeline') {
          const {
            stream: streamImpl,
            promises,
            files
          } = await this.getOutputStream({
            name: bindingName,
            postfix: binding.options?.postfix || '.timeline.csv',
            validatorFunction: (data) => {
              if (!data) return true;
              if (typeof data !== 'object') throw new Error('Invalid timeline data push, must be an object');
              // Is this necessary?
              if (!data.person_id) throw new Error('Invalid timeline data push, must have a person_id, even if 0');
              if (!data.ts) data.ts = new Date().toISOString();
              return true;
            }
          });
          newStreams.push(streamImpl);
          transformArguments[bindingName] = streamImpl;
          bindingPromises = bindingPromises.concat(promises || []);
          outputFiles[bindingName] = files;
        } else if (binding.path === 'output.stream') {
          const {
            stream: streamImpl,
            promises,
            files
          } = await this.getOutputStream({
            name: bindingName,
            postfix: binding.options?.postfix || '.timeline.csv'
          });
          newStreams.push(streamImpl);
          transformArguments[bindingName] = streamImpl;
          bindingPromises = bindingPromises.concat(promises || []);
          outputFiles[bindingName] = files;
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
    return { outputFiles };
  }
}
export default ForEachEntry;
