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
  loadDatasetMetadata,
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

Batch a file through an async transform with named output bindings. Construct with `{ accountId }` (optionally `fileUtilities`), then call `process({ filename | packet | stream, transform, batchSize, concurrency, progress, bindings })`. Input may be `csv`, `csv.gz`, `jsonl`, `parquet`, `xlsx`, a packet (`type` defaults to `person`), or an in-memory array / object stream. An instance may be reused. Resolves `{ outputFiles, records, batches }`; `outputFiles.<name>` is `[{ filename, records, kind, promoted }]`.

| Binding `path` | Writes | Destination |
| --- | --- | --- |
| `output.dataset` | One [dataset](#datasets) file of `options.kind` (`update` default, or `append` / `delete` / `source`) | `options.dataset` (a `Dataset`) or `options.directory` (+ optional `storePath`, `primary_key`, `format`). Required. |
| `output.stream` | Scratch file `{uuidv7}{postfix}` (default `.csv.gz`) | Temp dir. With `options.directory` it becomes `output.dataset` kind `update`. |
| `output.timeline` | Timeline entries, `{uuidv7}.timeline.{format}` (kind `append`) | Temp dir, or `options.directory`. |
| `file` | — | `getFile(binding)` result is passed to the transform. |
| `handlebars` | — | The shared handlebars instance. |

Every output binding value has `push(row)`; an invalid row throws inside the transform and the run aborts, discarding partial files. Dataset files with zero rows are not promoted (`filename: null`).

`output.timeline` requires `person_id` (0 is valid) and defaults `ts`. `options.entry_type` / `options.plugin_id` are applied as row defaults. A row without `id` gets a deterministic `getTimelineEntryUUID` when it carries `plugin_id`, `ts`, `person_id`, and an entry type; otherwise it gets a random uuidv7 only when written to a directory.

The destination is **never inferred** from the input file's parent — pass `directory` or `dataset`. Upserts (`update` / `delete`) require a declared primary key: `metadata.json` in the directory, or `primary_key` in the binding options. Missing both raises an error that includes `engine9 putMetadata --directory='<directory>' --metadata='{"primary_key":"<column>","format":"csv.gz"}'`.

See [Datasets](#datasets) for choosing `directory` and reading the result.

---

## Datasets

A **Dataset** is one self-contained directory of **immutable** files read as one row set. It is deliberately not called a "table": everywhere else in engine9 and frakture, `table` / `options.table` means a warehouse (SQL) table. A Dataset is always a directory path. There is no catalog database; metadata, sources, updates, and deletes all live together. This package is the portable contract for that directory — CLIs, plugins, and other libraries should depend on `Dataset` rather than inventing a parallel layout.

```text
<dataset-dir>/
  metadata.json                      # optional: primary_key, format, …
  2024-01-base.csv.gz                # source  (no token) — full rows
  0199a0c5-….append.jsonl.gz         # append  (.append. or .timeline.) — insert-only rows
  0199a0c6-….update.csv.gz           # update  (.update.) — upsert by primary_key
  0199a0c7-….delete.jsonl            # delete  (.delete.) — tombstones by primary_key
```

Rule: The directory **is** the Dataset. Pass that path around; do not reconstruct it from row contents.

### `Dataset`

```javascript
import { Dataset, FileUtilities } from '@engine9/input-tools';

const files = new FileUtilities({ accountId });
const dataset = await Dataset.open(directory, { fileUtilities: files });
// or: Dataset.open('acct/plugins/<pid>/person/ab12/<input_id>', { fileUtilities, storePath })
// or: Dataset.open(directory, { fileUtilities, primary_key: 'person_id', format: 'jsonl.gz' })

dataset.primaryKey;          // metadata.json primary_key, or the open() override, default 'id'
dataset.format;              // 'csv.gz' | 'csv' | 'jsonl.gz' | 'jsonl'
dataset.keyed;               // true when upserts may be written
await dataset.ensureMetadata();            // write metadata.json (primary_key, format) if absent
await dataset.putMetadata({ input_id });   // merge extra fields

// write
const w = dataset.writer({ kind: 'update' });   // 'append' | 'delete' | 'source'
w.push({ person_id: 1, status: 'ok', 'x.y.z': 123 });
w.push({ person_id: 2, error: 'bad' });        // sparse rows are fine
const { filename, records, promoted } = await w.end();   // local temp → moved into directory

// read
const files = await dataset.listFiles();       // [{ filename, name, kind, format, modifiedAt }] in apply order
const rows = await dataset.toArray();          // merged rows
const { stream, size, deleted } = await dataset.read({ kinds: ['source', 'update', 'delete'] });
for await (const { row, file } of dataset.scan()) { /* raw rows in apply order */ }
```

Writers require a declared primary key for `update` / `delete` (`metadata.json` or `primary_key` passed to `open`). `DatasetWriter` can also be used without a directory for scratch files (`new DatasetWriter({ fileUtilities, kind: 'source', postfix: '.error.csv' })`).

**CSV writers union the columns of every row** (two-pass through a local spool) and flatten nested objects to dotted keys, so a row pushed with `error` after rows without it still lands in the file. JSONL writers write rows as-is. Zero-row files are discarded, not promoted.

### Merge rules (`Dataset.read` / `toArray`)

1. Data files are applied ascending by the uuidv7 timestamp in the basename, else `modifiedAt`, else basename (`sortDatasetFiles`). Side files (`metadata.json`, `seen_records*`, locks, `.error.json`, `.idv1.parquet`) are skipped (`isDatasetSideFile`).
2. `source` and `append` rows are inserted (and upsert if they repeat a key). Rows without a primary key are kept under a per-file synthetic key.
3. `update` rows upsert by primary key. Keys **omitted** from the row leave prior values. In CSV update files an empty string also means "no change" (CSV cannot express absence); in JSONL `null` sets null. Update rows without a key are ignored.
4. `delete` rows remove the key.
5. Dotted keys set nested paths: existing `{ x: { y: { a: 'zxv' } } }` plus `x.y.z=123` yields `{ x: { y: { a: 'zxv', z: 123 } } }`. Nested objects (JSONL) deep-merge.
6. Keys are compared as strings, so `1` (JSONL) and `"1"` (CSV) are the same row; the merged value keeps the type of the last writer.

The merge is held in memory — this is the "light" reader for datasets of up to a few million rows. Engines (DuckDB, ClickHouse) can apply the same rules over `listFiles()` for larger datasets. Timeline appends keyed by `id` normally belong in their own Dataset; when they share a directory with a `person_id`-keyed Dataset, read with `kinds: ['source', 'update', 'delete']`.

### Using from `ForEachEntry`

```javascript
const dataset = await Dataset.open(directory, { fileUtilities: files });
const foreach = new ForEachEntry({ accountId, fileUtilities: files });
const { outputFiles } = await foreach.process({
  filename: sourceFile,                       // csv, csv.gz, jsonl, parquet, …
  bindings: {
    results: { path: 'output.dataset', options: { dataset, kind: 'update' } },
    timeline: { path: 'output.timeline', options: { directory: timelineDir, plugin_id } }
  },
  async transform({ batch, results, timeline }) {
    for (const row of batch) {
      results.push({ person_id: row.person_id, status: 'ok' });
      timeline.push({ person_id: row.person_id, ts: row.ts, entry_type: 'EMAIL_SEND' });
    }
  }
});
```

Do this:

- Write **local** then `move` into the directory (`Dataset.writer` does this; or call `promoteUpdateFiles({ kind })` yourself). Object stores cannot append in place.
- Stamp the kind token into every basename (`datasetPostfix` / `ensureKindFilename`).
- Include the primary key on every `update` / `delete` row. Extra fields (status, nested dotted keys) are allowed.
- Use `joinRemotePath` / `resolveDatasetDirectory` for every path join. Node `path.join` collapses `gs://` / `s3://`.

Do not:

- Stream-append to an `s3://`, `r2://`, or `gs://` key, or rewrite a promoted file.
- Mix side files into a reader's data list — use `Dataset.listFiles()` or `isDatasetSideFile`.
- Guess an engine9 input-store path when the host can give you a directory (see below).
- Use `getTempDir` as a durable Dataset. It is a daily scratch folder.

Lower-level helpers: `loadDatasetMetadata`, `writeDatasetMetadata`, `directoryFromFilename`, `resolveDatasetDirectory`, `datasetFileKind`, `isUpdateFile`, `isDeleteFile`, `isAppendFile`, `isDatasetSideFile`, `sortDatasetFiles`, `datasetPostfix`, `updateFilePostfix`, `ensureKindFilename`, `ensureUpdateFilename`, `promoteUpdateFiles`, plus `DEFAULT_PRIMARY_KEY`, `DEFAULT_FORMAT`, `METADATA_FILENAME`, `DATASET_KINDS`.

### Choosing a directory

Pick **one** durable directory per Dataset and reuse it. Resolution, in order:

| Situation | How to get `directory` |
| --- | --- |
| Results belong next to the input file | `directoryFromFilename(filename)` — pass it explicitly; `ForEachEntry` no longer infers it. |
| Host already has the Dataset | Use that path as-is. Prefer this when the Dataset directory is chosen separately from the input file. |
| Relative path under a store root | `resolveDatasetDirectory(directory, { storePath })` — joins a relative path under the store root with `joinRemotePath` (`s3://`, `r2://`, `gs://`, `gdrive://`, or a local directory) and passes absolute paths and remote URIs through unchanged (`gcs://` normalized to `gs://`). Throws when a relative path is given without `storePath`. |
| engine9 account worker | `await accountWorker.getStoreDirectory({ inputId })` (server). Honors `input.data_path` when set. **Not** in this package — input-tools has no `store_path` or SQL. |
| Host cannot run the worker | Ask the host to pass `directory` (and optionally `input_id`). Do not invent `{store_path}/…` in a plugin if the host can resolve it. |
| Standalone Dataset (no warehouse input) | `joinRemotePath(base, …)` under a location **you** own. Examples: `joinRemotePath('/var/data', 'tables', 'people')` or `joinRemotePath('s3://bucket/app', accountId, 'people')`. |
| Tests / one-off scratch | `await getTempDir({ accountId })` then a subfolder via `joinRemotePath`. Scratch is not a lake. |

Optional: after you have `directory`, `await files.list({ directory })` (create it on first write if missing). Write `metadata.json` with `writeDatasetMetadata(directory, fields, files)` (merge by default; pass `{ merge: false }` to replace). Hosts that need key normalization (e.g. camelCase → snake_case) can pass `{ normalize }`.

If you must compose an engine9-style input store without `getStoreDirectory`, the usual layout is:

```text
joinRemotePath(storePath, accountId, 'plugins', pluginId, inputType, inputId.slice(0, 4), inputId)
```

`inputId` must be `getInputUUID({ pluginId, remoteInputId })`, not a random UUID. Prefer `getStoreDirectory` whenever SQL is available — `data_path` overrides this layout.

### `metadata.json` (optional)

When present, marks the directory as a Dataset. Extra fields pass through.

| Field | Default / notes |
| --- | --- |
| `type` | `"dataset"` (legacy `"table"` accepted) |
| `description` | Human text |
| `primary_key` | `"id"` |
| `format` | Preferred extension for new update files, e.g. `csv.gz`, `jsonl.gz`, `parquet` |
| `input_id` | Often set; not required |

Absent metadata: reads treat the directory as a Dataset with `primary_key: "id"` and `format: "csv.gz"`; upsert writes require a key (metadata or `primary_key` option). Writers produce **csv** or **jsonl** (optionally gzipped); readers also accept parquet and xlsx sources.

### Files

- **Sources** — any input-tools-supported tabular file whose basename has no kind token.
- **Appends** — basename contains `.append.` or `.timeline.`; insert-only rows.
- **Updates** — basename contains `.update.` (e.g. `{uuidv7}.update.csv.gz`). Rows are keyed by `primary_key` and MAY add columns/fields that do not exist on the sources.
- **Deletes** — basename contains `.delete.`; only the `primary_key` column is needed.
- **Side files** — `metadata.json`, `seen_records*`, locks, sqlite sidecars, `.error.json`, `.idv1.parquet` are never data.

Reading is implemented by `Dataset.read` (see [Merge rules](#merge-rules-datasetread--toarray)); engines that read the directory directly should follow the same rules.

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

- **engine9 server** — workers bind `tools.relativeDate`, `tools.handlebars`, and `FileUtilities` for imports/exports. Account workers resolve input-store directories with `getStoreDirectory({ inputId })`; pass that path into Dataset writers as `directory`.
- **Export definitions** — Handlebars merges in `server/utilities/exportDefinitionMerge.js`; use `{{date (or overrides.start "-30d")}}` in raw EQL conditions.
