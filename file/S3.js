import debug$0 from 'debug';
import fs from 'node:fs';
import withDb from 'mime-type/with-db';
import clientS3 from '@aws-sdk/client-s3';
import { getTempFilename, relativeDate } from './tools.js';
const debug = debug$0('@engine9/input/S3');
const { mimeType: mime } = withDb;
const {
  S3Client,
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  GetObjectAttributesCommand,
  PutObjectCommand,
  ListObjectsV2Command
} = clientS3;
function Worker() {
  this.prefix = 's3';
}
function getParts(filename) {
  if (!filename) throw new Error(`Invalid filename: ${filename}`);
  if (!filename.startsWith('r2://') && !filename.startsWith('s3://')) {
    throw new Error(`Invalid filename, must start with r2:// or s3://: ${filename}`);
  }
  const parts = filename.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).join('/');
  return { Bucket, Key };
}
Worker.prototype.getClient = function () {
  if (!this.client) this.client = new S3Client({});
  return this.client;
};
Worker.prototype.getMetadata = async function ({ filename }) {
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);
  const resp = await s3Client.send(
    new GetObjectAttributesCommand({
      Bucket,
      Key,
      ObjectAttributes: ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    })
  );
  return resp;
};
Worker.prototype.getMetadata.metadata = {
  options: {
    filename: {}
  }
};
Worker.prototype.stream = async function ({ filename }) {
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  try {
    debug(`Streaming file ${Key}`);
    const response = await s3Client.send(command);
    return { stream: response.Body };
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
  if (filename.startsWith('s3://') || filename.startsWith('r2://')) {
    //we're fine
  } else {
    throw new Error('Cowardly not copying a file not from s3 -- use put instead');
  }
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(target);
  debug(`Copying ${filename} to ${JSON.stringify({ Bucket, Key })}}`);
  const command = new CopyObjectCommand({
    CopySource: filename.slice(4), // remove the s3:/
    Bucket,
    Key
  });
  return s3Client.send(command);
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
  return { filename: target };
};
Worker.prototype.move.metadata = {
  options: {
    filename: {},
    target: {}
  }
};
Worker.prototype.remove = async function ({ filename }) {
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);
  const command = new DeleteObjectCommand({ Bucket, Key });
  return s3Client.send(command);
};
Worker.prototype.remove.metadata = {
  options: {
    filename: {}
  }
};
Worker.prototype.download = async function ({ filename }) {
  const file = filename.split('/').pop();
  const localPath = await getTempFilename({ targetFilename: file });
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  debug(`Downloading ${file} to ${localPath}`);
  const response = await s3Client.send(command);
  const fileStream = fs.createWriteStream(localPath);
  response.Body.pipe(fileStream);
  return new Promise((resolve, reject) => {
    fileStream.on('finish', async () => {
      const { size } = await fs.promises.stat(localPath);
      resolve({ size, filename: localPath });
    });
    fileStream.on('error', reject);
  });
};
Worker.prototype.download.metadata = {
  options: {
    filename: {}
  }
};
Worker.prototype.put = async function (options) {
  const { filename, directory } = options;
  if (!filename) throw new Error('Local filename required');
  if (directory?.indexOf('s3://') !== 0 && directory?.indexOf('r2://') !== 0)
    throw new Error(`directory path must start with s3:// or r2://, is ${directory}`);
  const file = options.file || filename.split('/').pop();
  const parts = directory.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).filter(Boolean).concat(file).join('/');
  const Body = fs.createReadStream(filename);
  const ContentType = mime.lookup(file);
  debug(`Putting ${filename} to ${JSON.stringify({ Bucket, Key, ContentType })}}`);
  const s3Client = this.getClient();
  const command = new PutObjectCommand({
    Bucket,
    Key,
    Body,
    ContentType
  });
  return s3Client.send(command);
};
Worker.prototype.put.metadata = {
  options: {
    filename: {},
    directory: { description: 'Directory to put file, e.g. s3://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' }
  }
};
Worker.prototype.write = async function (options) {
  const { directory, file, content } = options;
  if (!directory?.indexOf('s3://') === 0) throw new Error('directory must start with s3://');
  const parts = directory.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).filter(Boolean).concat(file).join('/');
  const Body = content;
  debug(`Writing content of length ${content.length} to ${JSON.stringify({ Bucket, Key })}}`);
  const s3Client = this.getClient();
  const ContentType = mime.lookup(file);
  const command = new PutObjectCommand({
    Bucket,
    Key,
    Body,
    ContentType
  });
  return s3Client.send(command);
};
Worker.prototype.write.metadata = {
  options: {
    directory: { description: 'Directory to put file, e.g. s3://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' },
    content: { description: 'Contents of file' }
  }
};
Worker.prototype.list = async function ({ directory, start, end, raw }) {
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key: Prefix } = getParts(dir);
  const s3Client = this.getClient();
  const command = new ListObjectsV2Command({
    Bucket,
    Prefix: `${Prefix}/`,
    Delimiter: '/'
  });
  const { Contents: files, CommonPrefixes } = await s3Client.send(command);
  if (raw) return files;
  // debug('Prefixes:', { CommonPrefixes });
  const output = []
    .concat(
      (CommonPrefixes || []).map((f) => ({
        name: f.Prefix.slice(Prefix.length + 1, -1),
        type: 'directory'
      }))
    )
    .concat(
      (files || [])
        .filter(({ LastModified }) => {
          if (start && new Date(LastModified) < start) {
            return false;
          } else if (end && new Date(LastModified) > end) {
            return false;
          } else {
            return true;
          }
        })
        .map(({ Key, Size, LastModified }) => ({
          name: Key.slice(Prefix.length + 1),
          type: 'file',
          size: Size,
          modifiedAt: new Date(LastModified).toISOString()
        }))
    );
  return output;
};
Worker.prototype.list.metadata = {
  options: {
    directory: { required: true }
  }
};
/* List everything with the prefix */
Worker.prototype.listAll = async function (options) {
  const { directory } = options;
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  const start = options.start && relativeDate(options.start);
  const end = options.end && relativeDate(options.end);
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key } = getParts(dir);
  const s3Client = this.getClient();
  const files = [];
  let ContinuationToken = null;
  let Prefix = null;
  if (Key) Prefix = `${Key}/`;
  do {
    const command = new ListObjectsV2Command({
      Bucket,
      Prefix,
      ContinuationToken
      // Delimiter: '/',
    });
    debug(`Sending List command with prefix ${Prefix} with ContinuationToken ${ContinuationToken}`);
    const result = await s3Client.send(command);
    const newFiles =
      result.Contents?.filter(({ LastModified }) => {
        if (start && new Date(LastModified) < start) {
          return false;
        } else if (end && new Date(LastModified) > end) {
          return false;
        } else {
          return true;
        }
      })?.map((d) => `${this.prefix}://${Bucket}/${d.Key}`) || [];
    debug(`Retrieved ${newFiles.length} new files, total ${files.length},sample ${newFiles.slice(0, 3).join(',')}`);
    files.push(...newFiles);
    ContinuationToken = result.NextContinuationToken;
  } while (ContinuationToken);
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
  const configs = files.map((d) => ({
    filename: d,
    target: d.replace(directory, targetDirectory)
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
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);
  const command = new HeadObjectCommand({ Bucket, Key });
  const response = await s3Client.send(command);
  const {
    // "AcceptRanges": "bytes",
    ContentLength, // : "3191",
    ContentType, // : "image/jpeg",
    // ETag": "\"6805f2cfc46c0f04559748bb039d69ae\"",
    LastModified // : "2016-12-15T01:19:41.000Z",
    // Metadata": {},
    // VersionId": "null"
  } = response;
  const modifiedAt = new Date(LastModified);
  const createdAt = modifiedAt; // Same for S3
  const size = parseInt(ContentLength, 10);
  return {
    createdAt,
    modifiedAt,
    contentType: ContentType,
    size
  };
};
Worker.prototype.stat.metadata = {
  options: {
    filename: {}
  }
};
export default Worker;
