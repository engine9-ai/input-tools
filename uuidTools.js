/**
 * Deterministic and time-sortable UUID helpers for plugins, inputs, and timeline entries.
 * Kept separate from index.js so stream tooling (ForEachEntry, Table) can import them
 * without a circular import.
 */
import debug$0 from 'debug';
import { v4 as uuidv4, v5 as uuidv5, v7 as uuidv7, validate as uuidIsValid } from 'uuid';
import { TIMELINE_ENTRY_TYPES } from './timelineTypes.js';

const debug = debug$0('@engine9/input-tools');

export const UNIX_MS_MIN = 1e11;

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

export function getPluginUUID(uniqueNamespaceLikeDomainName, valueWithinNamespace) {
  // Random custom namespace for plugins -- not intended for cryptographically secure, just a unique namespace:
  return uuidv5(`${uniqueNamespaceLikeDomainName}::${valueWithinNamespace}`, 'f9e1024d-21ac-473c-bac6-64796dd771dd');
}

export function getInputUUID(a, b) {
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

export function dateFromString(s) {
  if (typeof s === 'number') return new Date(s);
  if (typeof s === 'string' && /^\d+$/.test(s)) {
    const n = Number(s);
    if (n >= UNIX_MS_MIN) return new Date(n);
  }
  return new Date(s);
}

export function getVersionedUUID(date, reqUuid) {
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
  //The version MUST be a supported UUID number, and the variant matters as well - 8,9,a,b
  return result.substring(0, 14) + '1' + result.substring(15, 19) + '8' + result.substring(20);
}

/* Returns a date from a given uuid (assumed to be a v7, otherwise the results are ... weird */
export function getUUIDTimestamp(uuid) {
  const ts = parseInt(`${uuid}`.replace(/-/g, '').slice(0, 12), 16);
  return new Date(ts);
}

export function getEntryTypeId(o, { defaults = {} } = {}) {
  let id = o.entry_type_id ?? defaults.entry_type_id;
  if (id !== undefined && id !== null) return id;
  const etype = o.entry_type || defaults.entry_type;
  if (!etype) {
    debug('Invalid input:', o, { defaults });
    throw new Error('No entry_type, nor entry_type_id specified, specify one to generate a timeline suitable ID');
  }
  id = TIMELINE_ENTRY_TYPES[etype];
  if (id === undefined) throw new Error(`Invalid entry_type: ${etype}`);
  return id;
}

export function getEntryType(o, defaults = {}) {
  let etype = o.entry_type || defaults.entry_type;
  if (etype) return etype;
  const id = o.entry_type_id ?? defaults.entry_type_id;
  etype = TIMELINE_ENTRY_TYPES[id];
  if (etype === undefined) throw new Error(`Invalid entry_type: ${etype}`);
  return etype;
}

const requiredTimelineEntryFields = ['ts', 'entry_type_id', 'plugin_id', 'person_id'];

export function getTimelineEntryUUID(inputObject, { defaults = {} } = {}) {
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
  const ts = dateFromString(o.ts);
  // isNaN behaves differently than Number.isNaN -- we're actually going for the
  // attempted conversion here
  if (isNaN(ts)) throw new Error(`getTimelineEntryUUID got an invalid date:${o.ts || '<blank>'}`);
  // Per-row input_id / message_id disambiguates entries that share ts/person/entry_type/source_code
  // (e.g. email opens on different messages). Only use values from inputObject, not defaults.
  const rowInputId = inputObject.message_id ?? inputObject.input_id;
  const inputSuffix =
    rowInputId !== undefined && rowInputId !== null && rowInputId !== '' ? `-${rowInputId}` : '';
  const idString = `${ts.toISOString()}-${o.person_id}-${o.entry_type_id}-${o.source_code_id || 0}${inputSuffix}`;
  if (!uuidIsValid(o.plugin_id)) {
    throw new Error(`Invalid plugin_id:'${o.plugin_id}', type ${typeof o.plugin_id} -- should be a uuid`);
  }
  const uuid = uuidv5(idString, o.plugin_id);
  // Change out the ts to match the v7 sorting.
  // But because outside specified remote_entry_uuid
  // may not match this standard, uuid sorting isn't guaranteed
  return getVersionedUUID(ts, uuid);
}

/**
 * True when a timeline row carries enough to compute a deterministic id via getTimelineEntryUUID:
 * a remote_entry_uuid, or a valid plugin_id with remote_entry_id, or plugin_id + ts + person_id + entry type.
 */
export function canComputeTimelineEntryUUID(row) {
  if (!row || typeof row !== 'object') return false;
  if (row.remote_entry_uuid) return uuidIsValid(row.remote_entry_uuid);
  if (!row.plugin_id || !uuidIsValid(row.plugin_id)) return false;
  if (row.remote_entry_id) return true;
  if (row.ts === undefined || row.ts === null || row.ts === '') return false;
  if (row.person_id === undefined || row.person_id === null || row.person_id === '') return false;
  const hasType = (row.entry_type_id !== undefined && row.entry_type_id !== null) || Boolean(row.entry_type);
  return hasType;
}
