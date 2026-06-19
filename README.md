# @engine9/input-tools

Cross-environment utilities for reading, writing, and processing Engine9-style input packets (zip manifests, CSV/JSON streams, timeline IDs, and templating). Intended for CLIs, workers, and third-party integrations — not tied to server-only storage or optional analytics engines.

```javascript
import inputTools from '@engine9/input-tools';
// or named imports:
import { relativeDate, handlebars, getTimelineEntryUUID } from '@engine9/input-tools';
```

## Scope

This package stays **portable across environments**. It should not depend on server-only storage layout, optional analytics engines, or products that not every consumer installs. For example, **DuckDB** paths and related defaults live in the Engine9 **server** (or your app), not here.

---

## Handlebars

The package exports a shared [`handlebars`](https://handlebarsjs.com/) instance with Engine9 helpers registered. `handlebars.compile` validates `{{date}}` templates at compile time: literal date arguments must be quoted (see below).

### Helpers

| Helper | Description |
| ------ | ----------- |
| `date` | Parse a relative or ISO date string via `relativeDate`, then format. Default format is ISO8601. Second argument is an optional [dayjs format string](https://day.js.org/docs/en/display/format). |
| `or` | First truthy argument, or the last argument (so `{{or overrides.end ''}}` can default to empty string). |
| `and` | First falsy argument, or the last when all are truthy. |
| `json` | `JSON.stringify` the value. |
| `uuid` | New UUID v7 string. |
| `percent` | `(a / b * 100).toFixed(2) + '%'`. |

### Examples

**Static relative date** — quote the literal so Handlebars does not treat `-365d` as an expression:

```handlebars
{{date '-30d'}}
{{date '-7d' 'YYYY-MM-DD'}}
```

**Export definition with override + default** — nest `or` inside `date` so the chosen value is parsed before it lands in SQL. Used in export definition EQL (see `server/utilities/exportDefinitionMerge.js`):

```javascript
{
  eql: 'person.modified_at >= \'{{date (or overrides.start "-365d")}}\''
}
```

When `export({ start: '-90d' })` is called, Handlebars resolves to an ISO timestamp. When `start` is omitted, the default `-365d` is used. For an optional exclusive end:

```javascript
{
  eql: 'timeline.ts < \'{{date (or overrides.end "")}}\''
}
```

Empty `overrides.end` yields an empty string from `date` (no date).

**Other helpers:**

```handlebars
{{or overrides.plugin_id "default-plugin-id"}}
{{json someObject}}
{{uuid}}
{{percent 42 100}}
```

---

## Date and time

### `relativeDate(s, initialDate?)`

Parse rolling windows, ISO strings, unix ms, or `now` / `none`.

| Input | Result |
| ----- | ------ |
| `-30d`, `+7d`, `-12h`, `-1M`, `-2w` | Offset from `initialDate` or now |
| `-30d.start.day`, `-1M.end.month` | Offset then snap to start/end of period |
| `now` | Current time |
| `none`, `''`, `null`, `undefined` | `null` |
| Integer ≥ 1e11 | Unix ms timestamp |
| ISO / parseable date string | Parsed `Date` |

Also available as transform binding `tools.relativeDate` in Engine9 workers.

### `isValidDate(d)`

Returns true when `d` is a `Date` with a valid time value.

### `getDateRangeArray(startDate, endDate)`

Returns an array of `Date` values between two bounds, with step size chosen from the span (daily → yearly).

---

## Input packets (zip)

Engine9 packets are zip files with a root `manifest.json` and typed member files (`person/`, `timeline/`, etc.).

### `create(options)`

Build a packet zip. Options include `accountId`, `pluginId`, `target` (output path), and file lists: `messageFiles`, `personFiles`, `timelineFiles`, `statisticsFiles`. Returns `{ filename, bytes }`.

### `list(path)` / `extract(path, file)`

Low-level zip helpers (list / extract one member to a temp file).

### `getPacketFiles({ packet })`

Open a packet from a local path or `s3://` URI; returns an unzipper directory handle.

### `getManifest({ packet })`

Read and parse `manifest.json` from a packet.

### `getFile({ packet, filename, type })`

Load one manifest member as a string or parsed JSON/JSON5 (by extension).

### `streamPacket({ packet, type })`

Return `{ stream, path }` for a single manifest member by type (`person`, `timeline`, etc.).

### `downloadFile({ packet, type })`

Stream one typed member to a temp file; returns `{ filename }`.

### `ForEachEntry`

Class for batch-processing packet contents with transforms, bindings, and optional CSV output streams. Construct with `{ accountId }`, then call `process({ packet, transform, batchSize, concurrency, bindings, ... })`.

---

## Files and streams

### `FileUtilities`

Worker-style class (`new FileUtilities({ accountId })`) for local and remote paths (`s3://`, `r2://`, `gdrive://`): read/write, CSV/JSON/JSON5/Parquet/XLSX streaming, glob, copy, etc. Used heavily by Engine9 `FileWorker`.

### `getTempDir({ accountId })` / `getTempFilename(options)` / `writeTempFile({ content, postfix, ... })`

Account-scoped temp directories and unique filenames (`prefix`, `postfix`, `targetFilename`, `source`).

### `getBatchTransform({ batchSize })` / `getDebatchTransform()`

Node transform streams that collect rows into batches or split batches into rows (object mode).

### `appendFileStatus(filename, postfix)`

Insert a status segment into a filename before the extension (e.g. `.complete`).

### `downloadFile`

See [Input packets](#input-packets-zip).

---

## Timeline and entry types

### `TIMELINE_ENTRY_TYPES`

Map of entry type name → numeric `entry_type_id` (e.g. `EMAIL_SEND`, `TRANSACTION`, `CRM_ORIGIN`). Re-exported from `timelineTypes.js`.

### `getEntryTypeId(o, { defaults })` / `getEntryType(o, defaults)`

Resolve `entry_type_id` ↔ `entry_type` label on a row object.

### `getTimelineEntryUUID(inputObject, { defaults })`

Deterministic timeline row UUID from `remote_entry_uuid`, `remote_entry_id`, or `(ts, person_id, entry_type_id, source_code_id, …)` plus `plugin_id`.

---

## UUIDs

| Export | Purpose |
| ------ | ------- |
| `uuidv4`, `uuidv5`, `uuidv7` | Re-exported from `uuid` |
| `uuidIsValid` | Re-exported UUID validator |
| `getPluginUUID(namespace, valueWithinNamespace)` | Stable plugin id from namespace + value |
| `getInputUUID({ pluginId, remoteInputId })` | Stable input id from plugin + remote input name |
| `getVersionedUUID(date, reqUuid?)` | Embed timestamp into UUID bytes for sortable ids |
| `getUUIDTimestamp(uuid)` | Read timestamp back from a versioned UUID |

---

## Parsing and coercion

| Export | Purpose |
| ------ | ------- |
| `bool(x, defaultVal?)` | Parse booleans from strings (`y`, `t`, `1`, etc.) |
| `getStringArray(s, nonZeroLength?)` | Normalize string / number / array to string array |
| `parseJSON5(o, defaultVal?)` | Parse JSON5 string or pass through objects |
| `makeStrings(o)` | Shallow-copy object with all values stringified |

---

## Unicode validation

### `checkUnicode(value, options?)`

Validate (and optionally repair) identifier-like strings to printable ASCII. Smart quotes map to `'`.

### `collectInvalidUnicodeValues(rows, fields, options?)`

Scan an array of row objects for invalid values in named fields.

---

## Errors

### `ObjectError`

`Error` subclass that copies arbitrary properties from an error object (e.g. HTTP `status`).

---

## Default export

The default export is an object containing all named exports above for `import inputTools from '@engine9/input-tools'`.

---

## Related packages

- **Engine9 server** — workers bind `tools.relativeDate`, `tools.handlebars`, and `FileUtilities` for imports/exports.
- **Export definitions** — Handlebars merges in `server/utilities/exportDefinitionMerge.js`; use `{{date (or overrides.start "-30d")}}` in raw EQL conditions.
