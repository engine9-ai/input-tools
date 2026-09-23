# @engine9/input-tools

Cross-environment utilities for reading, writing, and processing engine9-style input packets (zip manifests, CSV/JSON streams, timeline IDs, and templating). Intended for CLIs, workers, and third-party integrations — not tied to server-only storage or optional analytics engines.

```javascript
import inputTools from '@engine9/input-tools';
// or named imports:
import {
  ForEachEntry,
  FileUtilities,
  joinRemotePath,
  getTempDir,
  getInputUUID,
  loadTableMetadata,
  promoteUpdateFiles
} from '@engine9/input-tools';
```

## Scope

This package stays **portable across environments**. It should not depend on server-only storage layout, optional analytics engines, or products that not every consumer installs. For example, **DuckDB** paths and related defaults live in the engine9 **server** (or your app), not here.

---

## Handlebars

The package exports a shared [`handlebars`](https://handlebarsjs.com/) instance with engine9 helpers registered. `handlebars.compile` validates `{{date}}` templates at compile time: literal date arguments must be quoted (see below).

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

Also available as transform binding `tools.relativeDate` in engine9 workers.

### `isValidDate(d)`

Returns true when `d` is a `Date` with a valid time value.

### `getDateRangeArray(startDate, endDate)`

Returns an array of `Date` values between two bounds, with step size chosen from the span (daily → yearly).

---

## Input packets (zip)

engine9 packets are zip files with a root `manifest.json` and typed member files (`person/`, `timeline/`, etc.).

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

Class for batch-processing packet contents with transforms, bindings, and optional output streams. Construct with `{ accountId }`, then call `process({ packet, transform, batchSize, concurrency, bindings, ... })`.

`output.stream` writes a local `{uuidv7}.update.csv.gz` file, then moves it. `output.timeline` writes `{uuidv7}.timeline.csv.gz` (override either with `options.postfix` or `options.format`). Timeline files are not update uploads, so the name does not contain `.update.`. Remote stores cannot append in place.

The destination is `bindings.<name>.options.directory` when that is set. When it is omitted and `process` was given `filename`, the destination is that file's parent directory (`directoryFromFilename` — local paths and `s3://`, `r2://`, `gs://`, `gdrive://`). Packet input has no parent, so those outputs stay in the temp file unless `directory` is set.

`output.timeline` requires `person_id` and defaults `ts`. Timeline rows missing `id` get a uuidv7 when a destination directory is used. `output.stream` into a directory requires `metadata.json` there. If it is missing, the error includes a command: `engine9 putMetadata --directory='<directory>' --metadata='{"primary_key":"<column>","format":"csv.gz"}'`. The primary key on each update row is that file's `primary_key`.

See [Append tables](#append-tables) for how other libraries should pick `directory` and write updates.

---

## Append tables

A table is one self-contained directory. There is no catalog database; metadata, sources, and updates all live together. This package is the portable contract for that directory — CLIs, plugins, and other libraries should depend on these helpers rather than inventing a parallel layout.

```text
<table-dir>/
  metadata.json                 # optional
  2024-01-base.csv.gz           # source (no .update. in the basename)
  0199a0c5-....update.csv.gz    # update (MUST contain .update.)
```

Rule: The directory **is** the table. Pass that path around; do not reconstruct it from row contents.

### Using from another library

Typical writer (batch a source file, push rows, land an update file in the table directory):

```javascript
import {
  ForEachEntry,
  FileUtilities,
  joinRemotePath,
  loadTableMetadata
} from '@engine9/input-tools';

const files = new FileUtilities({ accountId });
const directory = joinRemotePath(base, 'people'); // see "Choosing a directory"
const meta = await loadTableMetadata(directory, files);

const foreach = new ForEachEntry({ accountId });
const { outputFiles } = await foreach.process({
  filename: sourceCsv,
  bindings: {
    out: {
      path: 'output.stream',
      options: {
        directory,
        format: meta.format,           // default csv.gz
        primary_key: meta.primary_key  // default id
      }
    }
  },
  async transform({ batch, out }) {
    for (const row of batch) {
      out.push({ id: row.id, status: 'ok', 'x.y.z': 123 });
    }
  }
});
// outputFiles.out[0].filename is inside directory after promote
```

Omit `directory` when the files should sit next to the file you just read. `output.stream` and `output.timeline` can be used together. Timeline files are named `{uuid}.timeline.csv.gz`. An update file requires `metadata.json` in that directory, with `primary_key` set to the row identity column.

```javascript
const { outputFiles } = await foreach.process({
  filename: sourceCsv,
  bindings: {
    updates: { path: 'output.stream' },
    timeline: { path: 'output.timeline' }
  },
  async transform({ batch, updates, timeline }) {
    for (const row of batch) {
      updates.push({ id: row.id, status: 'ok' });
      timeline.push({
        person_id: row.person_id,
        ts: row.ts,
        entry_type_id: row.entry_type_id
      });
    }
  }
});
// both files are in the parent directory of sourceCsv
```

`output.timeline` requires `person_id` and defaults `ts`. Timeline rows missing `id` get a uuidv7. `output.stream` rows need the `primary_key` declared in `metadata.json`.

Do this:

- Write **local** then `move` into the directory (`ForEachEntry` does this; or call `promoteUpdateFiles` yourself). Object stores cannot append in place.
- Put `.update.` in every update basename (`ensureUpdateFilename` / `updateFilePostfix`).
- Include `metadata.json` `primary_key` on every update row. Extra fields (status, nested dotted keys) are allowed.
- Use `joinRemotePath` for every path join. Node `path.join` collapses `gs://` / `s3://`.
- Treat `loadTableMetadata` defaults as authoritative when `metadata.json` is absent.

Do not:

- Stream-append to an `s3://`, `r2://`, or `gs://` key.
- Mix side files (`metadata.json`, `seen_records*`, locks) into a reader’s data list — use `isTableSideFile`.
- Guess an engine9 input-store path when the host can give you a directory (see below).
- Use `getTempDir` as a durable table. It is a daily scratch folder.

Without `ForEachEntry`, write a local `{uuidv7}.update.csv.gz` (or jsonl), then:

```javascript
import { promoteUpdateFiles, ensureUpdateFilename } from '@engine9/input-tools';

await promoteUpdateFiles({
  fileWorker: files,
  files: [{ filename: ensureUpdateFilename(localPath), records }],
  directory
});
```

Exported helpers: `loadTableMetadata`, `directoryFromFilename`, `isUpdateFile`, `isTableSideFile`, `updateFilePostfix`, `ensureUpdateFilename`, `promoteUpdateFiles`, plus `DEFAULT_PRIMARY_KEY`, `DEFAULT_FORMAT`, `METADATA_FILENAME`.

### Choosing a directory

Pick **one** durable directory per table and reuse it. Resolution, in order:

| Situation | How to get `directory` |
| --- | --- |
| `ForEachEntry` is reading a file | Omit `options.directory`. The parent of `filename` is used. |
| Host already has the table | Use that path as-is. Prefer this when the table directory is chosen separately from the input file. |
| engine9 account worker | `await accountWorker.getStoreDirectory({ inputId })` (server). Honors `input.data_path` when set. **Not** in this package — input-tools has no `store_path` or SQL. |
| Host cannot run the worker | Ask the host to pass `directory` (and optionally `input_id`). Do not invent `{store_path}/…` in a plugin if the host can resolve it. |
| Standalone table (no warehouse input) | `joinRemotePath(base, …)` under a location **you** own. Examples: `joinRemotePath('/var/data', 'tables', 'people')` or `joinRemotePath('s3://bucket/app', accountId, 'people')`. |
| Tests / one-off scratch | `await getTempDir({ accountId })` then a subfolder via `joinRemotePath`. Scratch is not a lake. |

Optional: after you have `directory`, `await files.list({ directory })` (create it on first write if missing). Write `metadata.json` with `FileUtilities.write` when you want a declared primary key / format / `input_id`.

If you must compose an engine9-style input store without `getStoreDirectory`, the usual layout is:

```text
joinRemotePath(storePath, accountId, 'plugins', pluginId, inputType, inputId.slice(0, 4), inputId)
```

`inputId` must be `getInputUUID({ pluginId, remoteInputId })`, not a random UUID. Prefer `getStoreDirectory` whenever SQL is available — `data_path` overrides this layout.

### `metadata.json` (optional)

When present, marks the directory as a table-style store. Extra fields pass through.

| Field | Default / notes |
| --- | --- |
| `type` | `"table"` |
| `description` | Human text |
| `primary_key` | `"id"` |
| `format` | Preferred extension for new update files, e.g. `csv.gz`, `jsonl.gz`, `parquet` |
| `input_id` | Often set; not required |

Absent metadata: treat the directory as a table with `primary_key: "id"` and `format: "csv.gz"`. `ForEachEntry` currently writes **csv** or **jsonl** (optionally gzipped), not parquet.

### Files

- **Sources** — any input-tools-supported tabular files whose basename does **not** contain `.update.`.
- **Updates** — basename contains `.update.` (e.g. `{uuidv7}.update.csv.gz`). Rows are keyed by `primary_key`. Update rows MAY add columns/fields that do not exist on the sources.
- **Side files** — skip `metadata.json`, `seen_records*`, locks, sqlite sidecars, `.error.json`, `.idv1.parquet`.

### Reading a table (contract — not implemented here)

input-tools does **not** ship a merged reader. Server, DuckDB, or a future helper should:

1. List data files (exclude side files). Optionally prefer `metadata.format` when filtering.
2. Sort **ascending by date** by default: use the uuidv7 timestamp when the basename starts with a uuidv7 (`getUUIDTimestamp`), else file mtime, else lexicographic name.
3. Stream each file in that order (`FileUtilities.fileToObjectStream` / `stream`). Upsert into a map keyed by `primary_key`.
4. **Merge:** later files update the same key. Apply fields with **dotted notation** as the default for nested objects:
   - Column/key `x.y.z` with value `123` sets `obj.x.y.z = 123` and must **not** wipe sibling `obj.x.y.a`.
   - Example: existing `{ x: { y: { a: 'zxv' } } }` plus update `x.y.z=123` yields `{ x: { y: { a: 'zxv', z: 123 } } }`.
   - Keys omitted from an update leave prior values. Explicit null/empty handling is implementation-defined for later readers.
5. Final state per key is the merged object after the last file.

---

## Files and streams

### `FileUtilities`

Worker-style class (`new FileUtilities({ accountId })`) for local and remote paths (`s3://`, `r2://`, `gdrive://`, `gs://`): read/write, CSV/JSON/JSON5/Parquet/XLSX streaming, glob, copy, etc. Used heavily by engine9 `FileWorker`.

**Remote URI schemes**

| Scheme | Backend | Auth |
| ------ | ------- | ---- |
| `s3://bucket/key` | AWS S3 | Default AWS credential chain, or a key file via `credentials` (`accessKeyId` / `secretAccessKey`) |
| `r2://bucket/key` | Cloudflare R2 | `CLOUDFLARE_R2_*` env vars, or a key file via `credentials` (`account_id`, `accessKeyId`, `secretAccessKey`) |
| `gdrive://{folderId}/{file}` | Google Drive | `GOOGLE_APPLICATION_CREDENTIALS` JSON key **with** `subject_to_impersonate`, or the same JSON via `credentials` |
| `gs://bucket/key` (alias `gcs://`) | Google Cloud Storage | Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS` or gcloud ADC), or a GCS service-account JSON via `credentials`. Does **not** require `subject_to_impersonate` — the same key file can serve Drive (with that field) and GCS (as the service account itself). |

`new FileUtilities({ accountId, credentials })`: `credentials` is a local path, object-store URI, or already-parsed JSON object. A URI is read with **bot/default** credentials (bootstrap), then applied only to the destination scheme inferred from the file. Cross-service copy (e.g. local/`s3://` → `gs://`) keeps default auth on the source.

Cross-service `copy` / `move` (e.g. `s3://…` → `gs://…`) downloads to a temp file then uploads. Same-service copies use native APIs. `transform` can take a remote `targetFilename` (`gs://…`); it writes a local temp file then `put`s to the destination.

### `isRemotePath` / `getServicePrefix` / `joinRemotePath` / `normalizeRemoteUri`

Shared helpers for detecting remote URIs and joining path segments without collapsing `gs://` (Node `path.join` would turn it into `gs:/`).

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

- **engine9 server** — workers bind `tools.relativeDate`, `tools.handlebars`, and `FileUtilities` for imports/exports. Account workers resolve input-store directories with `getStoreDirectory({ inputId })`; pass that path into append-table writers as `directory`.
- **Export definitions** — Handlebars merges in `server/utilities/exportDefinitionMerge.js`; use `{{date (or overrides.start "-30d")}}` in raw EQL conditions.
