import fs from 'node:fs';
import path from 'node:path';
import dayjs from 'dayjs';
import debug$0 from 'debug';
import unzipper from 'unzipper';
import { v4 as uuidv4, v5 as uuidv5, v7 as uuidv7, validate as uuidIsValid } from 'uuid';
import archiver from 'archiver';
import handlebars from 'handlebars';
import FileUtilities from './file/FileUtilities.js';
import { inferCredentialsScheme } from './file/credentials.js';
import tools from './file/tools.js';
import ForEachEntry from './ForEachEntry.js';
import Dataset, { DatasetWriter } from './Dataset.js';
import { TIMELINE_ENTRY_TYPES } from './timelineTypes.js';
import {
  UNIX_MS_MIN,
  getPluginUUID,
  getInputUUID,
  getVersionedUUID,
  getUUIDTimestamp,
  getEntryTypeId,
  getEntryType,
  getTimelineEntryUUID,
  canComputeTimelineEntryUUID
} from './uuidTools.js';
import { checkUnicode, collectInvalidUnicodeValues, cleanUnicodeValues } from './checkUnicode.js';
import { keyFromFields, mergeIntoQueue } from './mergeIntoQueue.js';
import {
  contentFingerprint,
  hashFingerprint,
  FINGERPRINT_HASH,
  isSeenRecordsFile,
  writeUniqueRecords,
  withUniqueRecords,
  SEEN_RECORDS_FILENAME,
  SEEN_RECORDS_LOCK_FILENAME
} from './writeUniqueRecords.js';
import {
  DEFAULT_FORMAT,
  DEFAULT_PRIMARY_KEY,
  METADATA_FILENAME,
  DATASET_KINDS,
  isUpdateFile,
  isDeleteFile,
  isAppendFile,
  datasetFileKind,
  sortDatasetFiles,
  isDatasetSideFile,
  loadDatasetMetadata,
  directoryFromFilename,
  resolveDatasetDirectory,
  writeDatasetMetadata,
  missingDatasetMetadataError,
  updateFilePostfix,
  datasetPostfix,
  ensureUpdateFilename,
  ensureKindFilename,
  promoteUpdateFiles
} from './datasetLayout.js';
const debug = debug$0('@engine9/input-tools');

const {
  appendFileStatus,
  bool,
  getBatchTransform,
  getDebatchTransform,
  getFile,
  getManifest,
  getPacketFiles,
  getServicePrefix,
  getStringArray,
  downloadFile,
  getTempFilename,
  getTempDir,
  isRemotePath,
  isValidDate,
  joinRemotePath,
  normalizeRemoteUri,
  parseJSON5,
  relativeDate,
  streamPacket,
  makeStrings,
  writeTempFile
} = tools;

const RELATIVE_DATE_RE = /^([+-])([0-9]+)([YyMwdhms])([.a-z]*)$/;

function looksLikeDateOrRelativeDateLiteral(value) {
  if (typeof value !== 'string' || !value) return false;
  if (value === 'now' || value === 'none') return true;
  if (/^[0-9+-]/.test(value)) return true;
  return RELATIVE_DATE_RE.test(value);
}

function unquotedDateHelperError(value) {
  const display = typeof value === 'string' ? value : String(value);
  return new Error(
    `Handlebars {{date}} argument ${display} must be quoted (e.g. {{date '${display}'}}). Unquoted date and relative-date values are parsed as Handlebars expressions or context paths, not date literals.`
  );
}

function assertDateHelperArgQuoted(param) {
  if (param.type === 'StringLiteral') return;
  if (param.type === 'NumberLiteral') {
    throw unquotedDateHelperError(param.original ?? param.value);
  }
  if (param.type === 'PathExpression' && param.parts.length === 1 && !param.data) {
    const { original } = param;
    if (looksLikeDateOrRelativeDateLiteral(original)) {
      throw unquotedDateHelperError(original);
    }
  }
}

function walkHandlebarsNodes(nodes, visitor) {
  if (!nodes) return;
  for (const node of nodes) {
    visitor(node);
    if (node.type === 'BlockStatement') {
      walkHandlebarsNodes(node.program?.body, visitor);
      walkHandlebarsNodes(node.inverse?.body, visitor);
    }
  }
}

function validateDateHelperTemplate(ast) {
  walkHandlebarsNodes(ast.body, (node) => {
    if (node.type !== 'MustacheStatement') return;
    const { path, params } = node;
    if (path.type !== 'PathExpression' || path.parts.join('.') !== 'date' || params.length === 0) return;
    assertDateHelperArgQuoted(params[0]);
  });
}

function assertQuotedDateValue(dateObject) {
  if (typeof dateObject === 'number' && Math.abs(dateObject) < UNIX_MS_MIN) {
    throw unquotedDateHelperError(dateObject);
  }
}

function getFormattedDate(dateObject, format = 'ISO8601') {
  assertQuotedDateValue(dateObject);
  const d = relativeDate(dateObject);
  if (!d) return '';
  if (!format || format === 'ISO8601') return d.toISOString();
  return dayjs(d).format(format);
}
handlebars.registerHelper('date', (d, f) => {
  let format;
  if (typeof f === 'string') format = f;
  return getFormattedDate(d, format);
});

const handlebarsCompile = handlebars.compile.bind(handlebars);
handlebars.compile = function compileWithDateValidation(template, options) {
  validateDateHelperTemplate(handlebars.parse(template, options));
  return handlebarsCompile(template, options);
};
handlebars.registerHelper('json', (d) => JSON.stringify(d));
handlebars.registerHelper('uuid', () => uuidv7());
handlebars.registerHelper('percent', (a, b) => `${((100 * a) / b).toFixed(2)}%`);

/** Handlebars appends an options object (`hash`, `data`, …) as the last argument to every helper. */
function stripHandlebarsHelperOptions(args) {
  if (args.length === 0) return args;
  const last = args[args.length - 1];
  if (last && typeof last === 'object' && 'hash' in last && 'data' in last) {
    return args.slice(0, -1);
  }
  return args;
}

/** First truthy value, or the last argument (so `{{or overrides.end ''}}` can default to empty string). */
handlebars.registerHelper('or', (...args) => {
  const values = stripHandlebarsHelperOptions(args);
  for (const v of values) {
    if (v) return v;
  }
  return values.length > 0 ? values[values.length - 1] : '';
});

/** First falsy value, or the last argument when all are truthy. */
handlebars.registerHelper('and', (...args) => {
  const values = stripHandlebarsHelperOptions(args);
  for (const v of values) {
    if (!v) return v;
  }
  return values.length > 0 ? values[values.length - 1] : '';
});
async function list(_path) {
  const directory = await unzipper.Open.file(_path);
  return new Promise((resolve, reject) => {
    directory.files[0].stream().pipe(fs.createWriteStream('firstFile')).on('error', reject).on('finish', resolve);
  });
}
async function extract(_path, _file) {
  const directory = await unzipper.Open(_path);
  // return directory.files.map((f) => f.path);
  const file = directory.files.find((d) => d.path === _file);
  const tempFilename = await getTempFilename({ source: _file });
  return new Promise((resolve, reject) => {
    file.stream().pipe(fs.createWriteStream(tempFilename)).on('error', reject).on('finish', resolve);
  });
}
function appendFiles(existingFiles, _newFiles, options) {
  const newFiles = getStringArray(_newFiles);
  if (newFiles.length === 0) return;
  let { type, dateCreated } = options || {};
  if (!type) type = 'unknown';
  if (!dateCreated) dateCreated = new Date().toISOString();
  let arr = newFiles;
  if (!Array.isArray(newFiles)) arr = [arr];
  arr.forEach((p) => {
    const item = {
      type,
      originalFilename: '',
      isNew: true,
      dateCreated
    };
    if (typeof p === 'string') {
      item.originalFilename = path.resolve(process.cwd(), p);
    } else {
      item.originalFilename = path.resolve(process.cwd(), item.originalFilename);
    }
    const file = item.originalFilename.split(path.sep).pop();
    item.path = `${type}/${file}`;
    const existingFile = existingFiles.find((f) => f.path === item.path);
    if (existingFile) throw new Error('Error adding files, duplicate path found for path:', +item.path);
    existingFiles.push(item);
  });
}
async function create(options) {
  const {
    accountId = 'engine9',
    pluginId = '',
    target = '', // target filename, creates one if not specified
    messageFiles = [], // file with contents of message, used for delivery
    personFiles = [], // files with data on people
    timelineFiles = [], // activity entry
    statisticsFiles = [] // files with aggregate statistics
  } = options;
  if (options.peopleFiles) throw new Error('Unknown option: peopleFiles, did you mean personFiles?');
  const files = [];
  const dateCreated = new Date().toISOString();
  appendFiles(files, messageFiles, { type: 'message', dateCreated });
  appendFiles(files, personFiles, { type: 'person', dateCreated });
  appendFiles(files, timelineFiles, { type: 'timeline', dateCreated });
  appendFiles(files, statisticsFiles, { type: 'statistics', dateCreated });
  const zipFilename = target || (await getTempFilename({ postfix: '.packet.zip' }));
  const manifest = {
    accountId,
    source: {
      pluginId
    },
    dateCreated,
    files
  };
  // create a file to stream archive data to.
  const output = fs.createWriteStream(zipFilename);
  const archive = archiver('zip', {
    zlib: { level: 9 } // Sets the compression level.
  });
  return new Promise((resolve, reject) => {
    debug(`Setting up write stream to ${zipFilename}`);
    // listen for all archive data to be written
    // 'close' event is fired only when a file descriptor is involved
    output.on('close', () => {
      debug('archiver has been finalized and the output file descriptor has closed, calling success');
      debug(zipFilename);
      return resolve({
        filename: zipFilename,
        bytes: archive.pointer()
      });
    });
    // This event is fired when the data source is drained no matter what was the data source.
    // It is not part of this library but rather from the NodeJS Stream API.
    // @see: https://nodejs.org/api/stream.html#stream_event_end
    output.on('end', () => {
      // debug('end event -- Data has been drained');
    });
    // warnings could be file not founds, etc, but we error even on those
    archive.on('warning', (err) => {
      reject(err);
    });
    // good practice to catch this error explicitly
    archive.on('error', (err) => {
      reject(err);
    });
    archive.pipe(output);
    files.forEach(({ path: name, originalFilename }) => archive.file(originalFilename, { name }));
    files.forEach((f) => {
      delete f.originalFilename;
      delete f.isNew;
    });
    archive.append(Buffer.from(JSON.stringify(manifest, null, 4), 'utf8'), { name: 'manifest.json' });
    archive.finalize();
  });
}
function getDateRangeArray(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  const result = [];
  const msInDay = 24 * 60 * 60 * 1000;
  function addDays(date, days) {
    const d = new Date(date);
    d.setDate(d.getDate() + days);
    return d;
  }
  function addMonths(date, months) {
    const d = new Date(date);
    d.setMonth(d.getMonth() + months);
    return d;
  }
  function addYears(date, years) {
    const d = new Date(date);
    d.setFullYear(d.getFullYear() + years);
    return d;
  }
  const diffDays = Math.floor((end - start) / msInDay);
  const diffMonths = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
  const diffYears = end.getFullYear() - start.getFullYear();
  let current = new Date(start);
  let stepFn;
  if (diffDays < 10) {
    stepFn = (date) => addDays(date, 1);
  } else if (diffDays < 32) {
    stepFn = (date) => addDays(date, 3);
  } else if (diffMonths < 4) {
    stepFn = (date) => addDays(date, 7);
  } else if (diffYears < 2) {
    stepFn = (date) => addMonths(date, 1);
  } else if (diffYears < 4) {
    stepFn = (date) => addMonths(date, 3);
  } else {
    stepFn = (date) => addYears(date, 1);
  }
  while (current <= end) {
    result.push(new Date(current));
    const next = stepFn(current);
    if (next > end) break;
    current = next;
  }
  // Ensure the last date is exactly the end date
  if (result.length === 0 || result[result.length - 1].getTime() !== end.getTime()) {
    result.push(new Date(end));
  }
  return result;
}
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
export { appendFileStatus };
export { bool };
export { checkUnicode };
export { collectInvalidUnicodeValues };
export { cleanUnicodeValues };
export { create };
export { list };
export { downloadFile };
export { extract };
export { ForEachEntry };
export { Dataset };
export { DatasetWriter };
export { FileUtilities };
export { getBatchTransform };
export { getDateRangeArray };
export { getDebatchTransform };
export { getEntryType };
export { getEntryTypeId };
export { getFile };
export { getManifest };
export { getStringArray };
export { getTempDir };
export { getTempFilename };
export { getTimelineEntryUUID };
export { canComputeTimelineEntryUUID };
export { getPacketFiles };
export { getPluginUUID };
export { getInputUUID };
export { getVersionedUUID };
export { getUUIDTimestamp };
export { handlebars };
export { getServicePrefix };
export { inferCredentialsScheme };
export { isRemotePath };
export { isValidDate };
export { joinRemotePath };
export { keyFromFields };
export { mergeIntoQueue };
export { makeStrings };
export { normalizeRemoteUri };
export { ObjectError };
export { parseJSON5 };
export { relativeDate };
export { streamPacket };
export { TIMELINE_ENTRY_TYPES };
export { writeTempFile };
export { writeUniqueRecords };
export { withUniqueRecords };
export { contentFingerprint };
export { hashFingerprint };
export { FINGERPRINT_HASH };
export { isSeenRecordsFile };
export { SEEN_RECORDS_FILENAME };
export { SEEN_RECORDS_LOCK_FILENAME };
export { DEFAULT_FORMAT };
export { DEFAULT_PRIMARY_KEY };
export { METADATA_FILENAME };
export { DATASET_KINDS };
export { isUpdateFile };
export { isDeleteFile };
export { isAppendFile };
export { datasetFileKind };
export { sortDatasetFiles };
export { isDatasetSideFile };
export { loadDatasetMetadata };
export { directoryFromFilename };
export { resolveDatasetDirectory };
export { writeDatasetMetadata };
export { missingDatasetMetadataError };
export { updateFilePostfix };
export { datasetPostfix };
export { ensureUpdateFilename };
export { ensureKindFilename };
export { promoteUpdateFiles };
export { uuidIsValid };
export { uuidv4 };
export { uuidv5 };
export { uuidv7 };
export default {
  appendFileStatus,
  bool,
  checkUnicode,
  collectInvalidUnicodeValues,
  cleanUnicodeValues,
  create,
  list,
  downloadFile,
  extract,
  ForEachEntry,
  Dataset,
  DatasetWriter,
  FileUtilities,
  getBatchTransform,
  getDateRangeArray,
  getDebatchTransform,
  getEntryType,
  getEntryTypeId,
  getFile,
  getManifest,
  getServicePrefix,
  getStringArray,
  getTempDir,
  getTempFilename,
  getTimelineEntryUUID,
  canComputeTimelineEntryUUID,
  getPacketFiles,
  getPluginUUID,
  getInputUUID,
  getVersionedUUID,
  getUUIDTimestamp,
  handlebars,
  inferCredentialsScheme,
  isRemotePath,
  isValidDate,
  joinRemotePath,
  keyFromFields,
  mergeIntoQueue,
  makeStrings,
  normalizeRemoteUri,
  ObjectError,
  parseJSON5,
  relativeDate,
  streamPacket,
  TIMELINE_ENTRY_TYPES,
  writeTempFile,
  writeUniqueRecords,
  withUniqueRecords,
  contentFingerprint,
  hashFingerprint,
  FINGERPRINT_HASH,
  isSeenRecordsFile,
  SEEN_RECORDS_FILENAME,
  SEEN_RECORDS_LOCK_FILENAME,
  DEFAULT_FORMAT,
  DEFAULT_PRIMARY_KEY,
  METADATA_FILENAME,
  DATASET_KINDS,
  isUpdateFile,
  isDeleteFile,
  isAppendFile,
  datasetFileKind,
  sortDatasetFiles,
  isDatasetSideFile,
  loadDatasetMetadata,
  directoryFromFilename,
  resolveDatasetDirectory,
  writeDatasetMetadata,
  missingDatasetMetadataError,
  updateFilePostfix,
  datasetPostfix,
  ensureUpdateFilename,
  ensureKindFilename,
  promoteUpdateFiles,
  uuidIsValid,
  uuidv4,
  uuidv5,
  uuidv7
};
