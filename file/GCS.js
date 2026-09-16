import debug$0 from 'debug';
import fs from 'node:fs';
import withDb from 'mime-type/with-db';
import { Storage } from '@google-cloud/storage';
import { getTempFilename, getFilePostfix, normalizeListDepth, relativeDate } from './tools.js';
import { gcsClientConfig } from './credentials.js';

const debug = debug$0('@engine9/input/GCS');
const { mimeType: mime } = withDb;

function Worker() {
  this.prefix = 'gs';
}

/**
 * Parse gs:// or gcs:// URI into bucket + key.
 * Canonical form is gs://bucket/object/key; gcs:// is accepted as an alias.
 */
export function getParts(filename) {
  if (!filename) throw new Error(`Invalid filename: ${filename}`);
  let uri = String(filename);
  if (uri.startsWith('gcs://')) uri = `gs://${uri.slice('gcs://'.length)}`;
  if (!uri.startsWith('gs://')) {
    throw new Error(`Invalid filename, must start with gs:// or gcs://: ${filename}`);
  }
  const parts = uri.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).join('/');
  if (!Bucket) throw new Error(`Invalid gs:// path, missing bucket: ${filename}`);
  return { Bucket, Key };
}

function toGsUri(bucket, key) {
  if (!key) return `gs://${bucket}`;
  return `gs://${bucket}/${key}`;
}

Worker.prototype.getClient = function () {
  if (!this.client) {
    if (this.resolvedCredentials) {
      this.client = new Storage(gcsClientConfig(this.resolvedCredentials));
    } else {
      // Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS or gcloud ADC).
      // Unlike Google Drive, do not require subject_to_impersonate.
      this.client = new Storage();
    }
  }
  return this.client;
};

Worker.prototype.getFile = function (filename) {
  const storage = this.getClient();
  const { Bucket, Key } = getParts(filename);
  return storage.bucket(Bucket).file(Key);
};

Worker.prototype.getMetadata = async function ({ filename }) {
  const file = this.getFile(filename);
  const [metadata] = await file.getMetadata();
  return metadata;
};
Worker.prototype.getMetadata.metadata = {
  options: {
    filename: {}
  }
};

Worker.prototype.stream = async function ({ filename, start, end }) {
  const { Bucket, Key } = getParts(filename);
  try {
    debug(`Streaming file gs://${Bucket}/${Key}`);
    const file = this.getFile(filename);
    const streamOpts = {};
    if (Number.isInteger(start) && start >= 0) streamOpts.start = start;
    if (Number.isInteger(end) && end >= 0) streamOpts.end = end;
    return { stream: file.createReadStream(streamOpts) };
  } catch (e) {
    debug(`Could not stream filename:${filename}`);
    throw e;
  }
};
Worker.prototype.stream.metadata = {
  options: {
    filename: {}
  }
};

Worker.prototype.copy = async function ({ filename, target }) {
  if (!filename?.startsWith('gs://') && !filename?.startsWith('gcs://')) {
    throw new Error('Cowardly not copying a file not from gs:// -- use put instead');
  }
  if (!target?.startsWith('gs://') && !target?.startsWith('gcs://')) {
    throw new Error('gcs copy requires a gs:// or gcs:// target');
  }
  const src = this.getFile(filename);
  const { Bucket, Key } = getParts(target);
  debug(`Copying ${filename} to ${JSON.stringify({ Bucket, Key })}`);
  const [copied] = await src.copy(this.getClient().bucket(Bucket).file(Key));
  return copied;
};
Worker.prototype.copy.metadata = {
  options: {
    filename: {},
    target: {}
  }
};

Worker.prototype.move = async function ({ filename, target }) {
  await this.copy({ filename, target });
  await this.remove({ filename });
  return { filename: target.startsWith('gcs://') ? `gs://${target.slice('gcs://'.length)}` : target };
};
Worker.prototype.move.metadata = {
  options: {
    filename: {},
    target: {}
  }
};

Worker.prototype.remove = async function ({ filename }) {
  const file = this.getFile(filename);
  await file.delete({ ignoreNotFound: true });
  return { removed: filename };
};
Worker.prototype.remove.metadata = {
  options: {
    filename: {}
  }
};

Worker.prototype.download = async function ({ filename }) {
  const fileName = filename.split('/').pop();
  const localPath = await getTempFilename({ targetFilename: fileName });
  const { Bucket, Key } = getParts(filename);
  debug(`Downloading ${fileName} to ${localPath}`);
  await this.getClient().bucket(Bucket).file(Key).download({ destination: localPath });
  const { size } = await fs.promises.stat(localPath);
  return { size, filename: localPath };
};
Worker.prototype.download.metadata = {
  options: {
    filename: {}
  }
};

Worker.prototype.put = async function (options) {
  const { filename, directory } = options;
  if (!filename) throw new Error('Local filename required');
  if (!directory?.startsWith('gs://') && !directory?.startsWith('gcs://')) {
    throw new Error(`directory path must start with gs:// or gcs://, is ${directory}`);
  }
  const file = options.file || filename.split('/').pop();
  let dir = directory;
  if (dir.startsWith('gcs://')) dir = `gs://${dir.slice('gcs://'.length)}`;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key: prefix } = getParts(dir);
  const Key = [prefix, file].filter(Boolean).join('/');
  const ContentType = mime.lookup(file) || 'application/octet-stream';
  debug(`Putting ${filename} to ${JSON.stringify({ Bucket, Key, ContentType })}`);
  await this.getClient().bucket(Bucket).upload(filename, {
    destination: Key,
    contentType: ContentType
  });
  return { filename: toGsUri(Bucket, Key) };
};
Worker.prototype.put.metadata = {
  options: {
    filename: {},
    directory: { description: 'Directory to put file, e.g. gs://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' }
  }
};

Worker.prototype.write = async function (options) {
  const { directory, file, content, exclusive } = options;
  if (!directory?.startsWith('gs://') && !directory?.startsWith('gcs://')) {
    throw new Error('directory must start with gs:// or gcs://');
  }
  if (!file) throw new Error('file is required');
  let dir = directory;
  if (dir.startsWith('gcs://')) dir = `gs://${dir.slice('gcs://'.length)}`;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key: prefix } = getParts(dir);
  const Key = [prefix, file].filter(Boolean).join('/');
  const ContentType = mime.lookup(file) || 'application/octet-stream';
  debug(`Writing content of length ${content?.length} to ${JSON.stringify({ Bucket, Key })}`);
  const gcsFile = this.getClient().bucket(Bucket).file(Key);
  const saveOpts = {
    contentType: ContentType,
    resumable: false
  };
  if (exclusive) {
    // Create only if the object does not exist (generation 0).
    saveOpts.preconditionOpts = { ifGenerationMatch: 0 };
  }
  await gcsFile.save(content, saveOpts);
  return { filename: toGsUri(Bucket, Key) };
};
Worker.prototype.write.metadata = {
  options: {
    directory: { description: 'Directory to put file, e.g. gs://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' },
    content: { description: 'Contents of file' }
  }
};

Worker.prototype.list = async function ({ directory, start, end, raw, depth: depthOpt, postfix }) {
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  if (dir.startsWith('gcs://')) dir = `gs://${dir.slice('gcs://'.length)}`;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key: rootPrefix } = getParts(dir);
  const bucket = this.getClient().bucket(Bucket);
  const maxDepth = normalizeListDepth(depthOpt);

  const relToRoot = (keyOrPrefix) => {
    const normalized = keyOrPrefix.replace(/\/$/, '');
    if (!rootPrefix) return normalized;
    if (normalized.length <= rootPrefix.length) return '';
    return normalized.slice(rootPrefix.length + 1);
  };

  if (!maxDepth) {
    const Prefix = rootPrefix ? `${rootPrefix}/` : '';
    const [files, , apiResponse] = await bucket.getFiles({
      prefix: Prefix,
      delimiter: '/',
      autoPaginate: false
    });
    const prefixes = apiResponse?.prefixes || [];
    if (raw) return files;
    const filteredFiles = (files || []).filter((f) => {
      const key = f.name;
      if (key === Prefix || key === rootPrefix) return false;
      if (postfix && !key.endsWith(postfix)) return false;
      return true;
    });
    const output = []
      .concat(
        prefixes.map((p) => ({
          name: relToRoot(p),
          type: 'directory'
        })).filter((d) => d.name)
      )
      .concat(
        filteredFiles
          .filter((f) => {
            const LastModified = f.metadata?.updated || f.metadata?.timeCreated;
            if (start && LastModified && new Date(LastModified) < start) return false;
            if (end && LastModified && new Date(LastModified) > end) return false;
            return true;
          })
          .map((f) => ({
            name: relToRoot(f.name),
            type: 'file',
            size: f.metadata?.size != null ? parseInt(f.metadata.size, 10) : undefined,
            modifiedAt: f.metadata?.updated
              ? new Date(f.metadata.updated).toISOString()
              : undefined
          }))
          .filter((d) => d.name)
      );
    return output;
  }

  if (raw) {
    throw new Error('list raw output is not supported together with depth');
  }

  const output = [];

  async function listLevel(currentPrefix) {
    const prefixParam = currentPrefix === '' ? '' : `${currentPrefix}/`;
    let pageToken;
    const allPrefixes = [];
    const allFiles = [];
    do {
      const [files, , apiResponse] = await bucket.getFiles({
        prefix: prefixParam,
        delimiter: '/',
        autoPaginate: false,
        pageToken
      });
      allFiles.push(...(files || []));
      allPrefixes.push(...(apiResponse?.prefixes || []));
      pageToken = apiResponse?.nextPageToken;
    } while (pageToken);

    for (const p of allPrefixes) {
      const subPrefix = p.replace(/\/$/, '');
      const rel = relToRoot(p);
      if (!rel) continue;
      const segCount = rel.split('/').length;
      if (segCount > maxDepth) continue;
      output.push({ name: rel, type: 'directory' });
      if (segCount < maxDepth) {
        await listLevel(subPrefix);
      }
    }
    for (const f of allFiles) {
      const Key = f.name;
      const rel = relToRoot(Key);
      if (!rel) continue;
      if (rel.split('/').length > maxDepth) continue;
      if (postfix && !rel.endsWith(postfix)) continue;
      const LastModified = f.metadata?.updated || f.metadata?.timeCreated;
      if (start && LastModified && new Date(LastModified) < start) continue;
      if (end && LastModified && new Date(LastModified) > end) continue;
      output.push({
        name: rel,
        type: 'file',
        size: f.metadata?.size != null ? parseInt(f.metadata.size, 10) : undefined,
        modifiedAt: LastModified ? new Date(LastModified).toISOString() : undefined
      });
    }
  }

  await listLevel(rootPrefix);
  return output;
};
Worker.prototype.list.metadata = {
  options: {
    directory: { required: true },
    postfix: {
      description: 'Only include files whose key ends with this string'
    },
    depth: {
      description:
        'If set, recursively list objects and prefixes up to this key depth (relative to directory); omit for a single-level listing only'
    }
  }
};

Worker.prototype.analyzeDirectory = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  if (dir.startsWith('gcs://')) dir = `gs://${dir.slice('gcs://'.length)}`;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key } = getParts(dir);
  const bucket = this.getClient().bucket(Bucket);
  let Prefix = '';
  if (Key) Prefix = `${Key}/`;
  const dirsSeen = new Set();
  let fileCount = 0;
  let firstModified = null;
  let lastModified = null;
  let firstTime = null;
  let lastTime = null;
  const postfixCounts = Object.create(null);
  let pageToken;
  do {
    const [files, , apiResponse] = await bucket.getFiles({
      prefix: Prefix,
      autoPaginate: false,
      pageToken
    });
    for (const f of files || []) {
      const objectKey = f.name;
      let rel = Prefix ? (objectKey.startsWith(Prefix) ? objectKey.slice(Prefix.length) : objectKey) : objectKey;
      if (!rel) continue;
      const isFolderMarker = rel.endsWith('/');
      const parts = rel.replace(/\/$/, '').split('/').filter(Boolean);
      for (let i = 0; i < parts.length - 1; i++) {
        dirsSeen.add(parts.slice(0, i + 1).join('/'));
      }
      if (isFolderMarker) {
        if (parts.length) dirsSeen.add(parts.join('/'));
        continue;
      }
      fileCount++;
      const postfix = getFilePostfix(objectKey);
      postfixCounts[postfix] = (postfixCounts[postfix] || 0) + 1;
      const LastModified = f.metadata?.updated || f.metadata?.timeCreated;
      if (!LastModified) continue;
      const mtime = new Date(LastModified).getTime();
      const modifiedAt = new Date(LastModified).toISOString();
      const filename = toGsUri(Bucket, objectKey);
      if (firstTime === null || mtime < firstTime) {
        firstTime = mtime;
        firstModified = { filename, modifiedAt };
      }
      if (lastTime === null || mtime > lastTime) {
        lastTime = mtime;
        lastModified = { filename, modifiedAt };
      }
    }
    pageToken = apiResponse?.nextPageToken;
  } while (pageToken);
  return {
    fileCount,
    directoryCount: dirsSeen.size,
    postfixCounts,
    firstModified: fileCount ? firstModified : null,
    lastModified: fileCount ? lastModified : null
  };
};
Worker.prototype.analyzeDirectory.metadata = {
  options: {
    directory: { required: true }
  }
};

Worker.prototype.listAll = async function (options) {
  const { directory } = options;
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  if (dir.startsWith('gcs://')) dir = `gs://${dir.slice('gcs://'.length)}`;
  const start = options.start && relativeDate(options.start);
  const end = options.end && relativeDate(options.end);
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key } = getParts(dir);
  const bucket = this.getClient().bucket(Bucket);
  const files = [];
  let Prefix = null;
  if (Key) Prefix = `${Key}/`;
  let pageToken;
  do {
    debug(`Listing gs://${Bucket}/${Prefix || ''} pageToken=${pageToken || ''}`);
    const [pageFiles, , apiResponse] = await bucket.getFiles({
      prefix: Prefix || undefined,
      autoPaginate: false,
      pageToken
    });
    for (const f of pageFiles || []) {
      if (f.name.endsWith('/')) continue;
      const LastModified = f.metadata?.updated || f.metadata?.timeCreated;
      if (start && LastModified && new Date(LastModified) < start) continue;
      if (end && LastModified && new Date(LastModified) > end) continue;
      files.push(toGsUri(Bucket, f.name));
    }
    pageToken = apiResponse?.nextPageToken;
  } while (pageToken);
  return files;
};
Worker.prototype.listAll.metadata = {
  options: {
    directory: { required: true }
  }
};

Worker.prototype.moveAll = async function ({ directory, targetDirectory }) {
  if (!directory || !targetDirectory) throw new Error('directory and targetDirectory required');
  const files = await this.listAll({ directory });
  let srcDir = directory;
  if (srcDir.startsWith('gcs://')) srcDir = `gs://${srcDir.slice('gcs://'.length)}`;
  let dstDir = targetDirectory;
  if (dstDir.startsWith('gcs://')) dstDir = `gs://${dstDir.slice('gcs://'.length)}`;
  while (srcDir.slice(-1) === '/') srcDir = srcDir.slice(0, -1);
  while (dstDir.slice(-1) === '/') dstDir = dstDir.slice(0, -1);
  const configs = files.map((d) => ({
    filename: d,
    target: d.replace(srcDir, dstDir)
  }));
  const pLimit = await import('p-limit');
  const limitedMethod = pLimit.default(10);
  return Promise.all(configs.map(({ filename, target }) => limitedMethod(async () => this.move({ filename, target }))));
};
Worker.prototype.moveAll.metadata = {
  options: {
    directory: { required: true },
    targetDirectory: { required: true }
  }
};

Worker.prototype.stat = async function ({ filename }) {
  if (!filename) throw new Error('filename is required');
  const [metadata] = await this.getFile(filename).getMetadata();
  const modifiedAt = metadata.updated ? new Date(metadata.updated) : null;
  const createdAt = metadata.timeCreated ? new Date(metadata.timeCreated) : modifiedAt;
  const size = metadata.size != null ? parseInt(metadata.size, 10) : undefined;
  return {
    createdAt,
    modifiedAt,
    contentType: metadata.contentType,
    size
  };
};
Worker.prototype.stat.metadata = {
  options: {
    filename: {}
  }
};

export default Worker;
