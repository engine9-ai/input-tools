import fs from 'node:fs';
import path from 'node:path';
import dayjs from 'dayjs';
import debug$0 from 'debug';
import unzipper from 'unzipper';
import { v4 as uuidv4, v5 as uuidv5, v7 as uuidv7, validate as uuidIsValid } from 'uuid';
import archiver from 'archiver';
import handlebars from 'handlebars';
import FileUtilities from './file/FileUtilities.js';
import tools from './file/tools.js';
import ForEachEntry from './ForEachEntry.js';
import { TIMELINE_ENTRY_TYPES } from './timelineTypes.js';
const debug = debug$0('@engine9/input-tools');

const {
  appendPostfix,
  bool,
  getBatchTransform,
  getDebatchTransform,
  getFile,
  getManifest,
  getPacketFiles,
  getStringArray,
  downloadFile,
  getTempFilename,
  getTempDir,
  isValidDate,
  parseJSON5,
  relativeDate,
  streamPacket,
  makeStrings,
  writeTempFile
} = tools;
function getFormattedDate(dateObject, format = 'MMM DD,YYYY') {
  let d = dateObject;
  if (d === 'now') d = new Date();
  if (d) return dayjs(d).format(format);
  return '';
}
handlebars.registerHelper('date', (d, f) => {
  let format;
  if (typeof f === 'string') format = f;
  return getFormattedDate(d, format);
});
handlebars.registerHelper('json', (d) => JSON.stringify(d));
handlebars.registerHelper('uuid', () => uuidv7());
handlebars.registerHelper('percent', (a, b) => `${((100 * a) / b).toFixed(2)}%`);
handlebars.registerHelper('or', (a, b, c) => a || b || c);
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
function intToByteArray(_v) {
  // we want to represent the input as a 8-bytes array
  const byteArray = [0, 0, 0, 0, 0, 0, 0, 0];
  let v = _v;
  for (let index = 0; index < byteArray.length; index += 1) {
    const byte = v & 0xff;
    byteArray[index] = byte;
    v = (v - byte) / 256;
  }
  return byteArray;
}
function getPluginUUID(uniqueNamespaceLikeDomainName, valueWithinNamespace) {
  // Random custom namespace for plugins -- not intended for cryptographically secure, just a unique namespace:
  return uuidv5(`${uniqueNamespaceLikeDomainName}::${valueWithinNamespace}`, 'f9e1024d-21ac-473c-bac6-64796dd771dd');
}
function getInputUUID(a, b) {
  let pluginId = a;
  let remoteInputId = b;
  if (typeof a === 'object') {
    pluginId = a.pluginId;
    remoteInputId = a.remoteInputId;
  }
  if (!pluginId) throw new Error('getInputUUID: Cowardly rejecting a blank plugin_id');
  if (!uuidIsValid(pluginId)) throw new Error(`Invalid pluginId:${pluginId}, should be a UUID`);
  const rid = (remoteInputId || '').trim();
  if (!rid) throw new Error('getInputUUID: Cowardly rejecting a blank remote_input_id, set a default');
  // Random custom namespace for inputs -- not secure, just a namespace:
  // 3d0e5d99-6ba9-4fab-9bb2-c32304d3df8e
  return uuidv5(`${pluginId}:${rid}`, '3d0e5d99-6ba9-4fab-9bb2-c32304d3df8e');
}
const timestampMatch = /^\d{13}$/;
function dateFromString(s) {
  if (typeof s === 'number') return new Date(s);
  if (typeof s === 'string') {
    if (s.match(timestampMatch)) return new Date(parseInt(s));
  }
  return new Date(s);
}
function getVersionedUUID(date, reqUuid) {
  /* optional date and input UUID */
  const uuid = reqUuid || uuidv7();
  const bytes = Buffer.from(uuid.replace(/-/g, ''), 'hex');
  if (date !== undefined) {
    const d = dateFromString(date);
    // isNaN behaves differently than Number.isNaN -- we're actually going for the
    // attempted conversion here
    if (isNaN(d)) throw new Error(`getVersionedUUID got an invalid date:${date || '<blank>'}`);
    const dateBytes = intToByteArray(d.getTime()).reverse();
    dateBytes.slice(2, 8).forEach((b, i) => {
      bytes[i] = b;
    });
  }
  const result = uuidv4({ random: bytes });
  return result.substring(0, 14) + 'e' + result.substring(15, 19) + '1' + result.substring(20);
}
/* Returns a date from a given uuid (assumed to be a v7, otherwise the results are ... weird */
function getUUIDTimestamp(uuid) {
  const ts = parseInt(`${uuid}`.replace(/-/g, '').slice(0, 12), 16);
  return new Date(ts);
}
function getEntryTypeId(o, { defaults = {} } = {}) {
  let id = o.entry_type_id || defaults.entry_type_id;
  if (id) return id;
  const etype = o.entry_type || defaults.entry_type;
  if (!etype) {
    throw new Error('No entry_type, nor entry_type_id specified, specify one to generate a timeline suitable ID');
  }
  id = TIMELINE_ENTRY_TYPES[etype];
  if (id === undefined) throw new Error(`Invalid entry_type: ${etype}`);
  return id;
}
function getEntryType(o, defaults = {}) {
  let etype = o.entry_type || defaults.entry_type;
  if (etype) return etype;
  const id = o.entry_type_id || defaults.entry_type_id;
  etype = TIMELINE_ENTRY_TYPES[id];
  if (etype === undefined) throw new Error(`Invalid entry_type: ${etype}`);
  return etype;
}
const requiredTimelineEntryFields = ['ts', 'entry_type_id', 'plugin_id', 'person_id'];
function getTimelineEntryUUID(inputObject, { defaults = {} } = {}) {
  const o = { ...defaults, ...inputObject };
  /*
        Outside systems CAN specify a unique UUID as remote_entry_uuid,
        which will be used for updates, etc.
        If not, it will be generated using whatever info we have
      */
  if (o.remote_entry_uuid) {
    if (!uuidIsValid(o.remote_entry_uuid)) throw new Error('Invalid remote_entry_uuid, it must be a UUID');
    return o.remote_entry_uuid;
  }
  /*
          Outside systems CAN specify a unique remote_entry_id
          If not, it will be generated using whatever info we have
        */
  if (o.remote_entry_id) {
    if (!o.plugin_id)
      throw new Error('Error generating timeline entry uuid -- remote_entry_id specified, but no plugin_id');
    if (!uuidIsValid(o.plugin_id))
      throw new Error(`Invalid plugin_id:'${o.plugin_id}', type ${typeof o.plugin_id} -- should be a uuid`);
    try {
      const uuid = uuidv5(String(o.remote_entry_id), o.plugin_id);
      // Change out the ts to match the v7 sorting.
      // But because outside specified remote_entry_uuid
      // may not match this standard, uuid sorting isn't guaranteed
      return getVersionedUUID(o.ts, uuid);
    } catch (e) {
      debug('Error getting uuid with object:', o);
      throw e;
    }
  }
  o.entry_type_id = getEntryTypeId(o);
  const missing = requiredTimelineEntryFields.filter((d) => o[d] === undefined); // 0 could be an entry type value
  if (missing.length > 0) throw new Error(`Missing required fields to append an entry_id:${missing.join(',')}`);
  const ts = new Date(o.ts);
  // isNaN behaves differently than Number.isNaN -- we're actually going for the
  // attempted conversion here
  if (isNaN(ts)) throw new Error(`getTimelineEntryUUID got an invalid date:${o.ts || '<blank>'}`);
  const idString = `${ts.toISOString()}-${o.person_id}-${o.entry_type_id}-${o.source_code_id || 0}`;
  if (!uuidIsValid(o.plugin_id)) {
    throw new Error(`Invalid plugin_id:'${o.plugin_id}', type ${typeof o.plugin_id} -- should be a uuid`);
  }
  const uuid = uuidv5(idString, o.plugin_id);
  // Change out the ts to match the v7 sorting.
  // But because outside specified remote_entry_uuid
  // may not match this standard, uuid sorting isn't guaranteed
  return getVersionedUUID(ts, uuid);
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
export { appendPostfix };
export { bool };
export { create };
export { list };
export { downloadFile };
export { extract };
export { ForEachEntry };
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
export { getPacketFiles };
export { getPluginUUID };
export { getInputUUID };
export { getVersionedUUID };
export { getUUIDTimestamp };
export { handlebars };
export { isValidDate };
export { makeStrings };
export { ObjectError };
export { parseJSON5 };
export { relativeDate };
export { streamPacket };
export { TIMELINE_ENTRY_TYPES };
export { writeTempFile };
export { uuidIsValid };
export { uuidv4 };
export { uuidv5 };
export { uuidv7 };
export default {
  appendPostfix,
  bool,
  create,
  list,
  downloadFile,
  extract,
  ForEachEntry,
  FileUtilities,
  getBatchTransform,
  getDateRangeArray,
  getDebatchTransform,
  getEntryType,
  getEntryTypeId,
  getFile,
  getManifest,
  getStringArray,
  getTempDir,
  getTempFilename,
  getTimelineEntryUUID,
  getPacketFiles,
  getPluginUUID,
  getInputUUID,
  getVersionedUUID,
  getUUIDTimestamp,
  handlebars,
  isValidDate,
  makeStrings,
  ObjectError,
  parseJSON5,
  relativeDate,
  streamPacket,
  TIMELINE_ENTRY_TYPES,
  writeTempFile,
  uuidIsValid,
  uuidv4,
  uuidv5,
  uuidv7
};
