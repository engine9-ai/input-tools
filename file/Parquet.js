import {
  asyncBufferFromFile,
  parquetMetadataAsync,
  parquetReadObjects,
  parquetSchema
} from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { HeadObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import nodestream from 'node:stream';
import debug$0 from 'debug';
import clientS3 from '@aws-sdk/client-s3';
import FileWorker from './FileUtilities.js';

const { Readable } = nodestream;
const debug = debug$0('Parquet');
const { S3Client } = clientS3;

const READ_BATCH_SIZE = 10000;
const readOptions = { compressors, rowFormat: 'object' };

function Worker() {}

/** @param {import('@aws-sdk/client-s3').S3Client} client */
async function asyncBufferFromS3(client, { Bucket, Key }) {
  const head = await client.send(new HeadObjectCommand({ Bucket, Key }));
  const byteLength = Number(head.ContentLength ?? 0);
  return {
    byteLength,
    async slice(start, end = byteLength) {
      const rangeEnd = Math.max(start, (end ?? byteLength) - 1);
      const resp = await client.send(
        new GetObjectCommand({ Bucket, Key, Range: `bytes=${start}-${rangeEnd}` })
      );
      const bytes = await resp.Body.transformToByteArray();
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    }
  };
}

async function openFile(filename) {
  if (filename.indexOf('s3://') === 0) {
    const client = new S3Client({});
    const parts = filename.split('/');
    const file = await asyncBufferFromS3(client, {
      Bucket: parts[2],
      Key: parts.slice(3).join('/')
    });
    return openBuffer(file);
  }
  const file = await asyncBufferFromFile(filename);
  return openBuffer(file);
}

async function openBuffer(file) {
  const metadata = await parquetMetadataAsync(file, { compressors });
  return new HyparquetReader(file, metadata);
}

function mapElementType(element) {
  const logical = element.logical_type?.type || element.converted_type;
  if (logical === 'UTF8' || logical === 'STRING' || logical === 'JSON') return 'UTF8';
  if (logical === 'TIMESTAMP' || logical === 'TIMESTAMP_MILLIS') return 'TIMESTAMP_MILLIS';
  if (logical === 'DATE') return 'TIMESTAMP_MILLIS';
  if (logical === 'DECIMAL') return 'DOUBLE';
  if (logical === 'UUID') return 'BYTE_ARRAY';
  switch (element.type) {
    case 'BOOLEAN':
      return 'BOOLEAN';
    case 'INT32':
      return 'INT32';
    case 'INT64':
      return 'INT64';
    case 'FLOAT':
      return 'FLOAT';
    case 'DOUBLE':
      return 'DOUBLE';
    case 'BYTE_ARRAY':
      return 'UTF8';
    case 'FIXED_LEN_BYTE_ARRAY':
      return 'BYTE_ARRAY';
    default:
      return 'UTF8';
  }
}

function buildSchema(metadata) {
  const tree = parquetSchema(metadata);
  const fieldList = (tree.children || []).map((child) => ({
    name: child.element.name,
    type: mapElementType(child.element),
    originalType: child.element.type
  }));
  return { fieldList };
}

function normalizeRow(row) {
  const out = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] = normalizeValue(value);
  }
  return out;
}

function normalizeValue(value) {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value;
  if (typeof value === 'bigint') {
    const n = Number(value);
    if (n > 1e15 || n < -1e15) return value;
    return n;
  }
  if (typeof value === 'object' && typeof value.millis === 'bigint') {
    return new Date(Number(value.millis));
  }
  return value;
}

class HyparquetReader {
  constructor(file, metadata) {
    this.file = file;
    this._fileMetadata = metadata;
    this._rowGroups = metadata.row_groups;
    this._schema = buildSchema(metadata);
    this.metadata = { num_rows: Number(metadata.num_rows) };
  }

  getSchema() {
    return this._schema;
  }

  getFileMetaData() {
    return this._fileMetadata;
  }

  getRowGroups() {
    return this._rowGroups;
  }

  getCursor(columns) {
    const file = this.file;
    const totalRows = this.metadata.num_rows;
    let rowIndex = 0;
    let buffer = [];
    let bufferPos = 0;

    const baseOpts = {
      ...readOptions,
      file,
      columns: columns?.length ? columns : undefined
    };

    return {
      async next() {
        if (bufferPos < buffer.length) {
          return normalizeRow(buffer[bufferPos++]);
        }
        if (rowIndex >= totalRows) return null;

        const batchEnd = Math.min(rowIndex + READ_BATCH_SIZE, totalRows);
        buffer = await parquetReadObjects({
          ...baseOpts,
          rowStart: rowIndex,
          rowEnd: batchEnd
        });
        rowIndex = batchEnd;
        bufferPos = 0;
        if (!buffer.length) return null;
        return normalizeRow(buffer[bufferPos++]);
      }
    };
  }

  async close() {
    /* hyparquet holds no persistent handles */
  }
}

Worker.prototype.meta = async function (options) {
  const reader = await openFile(options.filename);
  try {
    const schema = reader.getSchema();
    return {
      schema,
      records: reader.metadata.num_rows
    };
  } finally {
    await reader.close();
  }
};
Worker.prototype.meta.metadata = {
  options: {
    path: {}
  }
};

Worker.prototype.schema = async function (options) {
  const reader = await openFile(options.filename);
  try {
    return reader.getSchema();
  } finally {
    await reader.close();
  }
};
Worker.prototype.schema.metadata = {
  options: {
    path: {}
  }
};

function cleanColumnName(name) {
  return name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
}

Worker.prototype.stream = async function (options) {
  const reader = await openFile(options.filename);
  let columns;
  if (options.columns) {
    const { fieldList } = reader.getSchema();
    columns = [];
    let requestedColumns = options.columns;
    if (typeof options.columns === 'string') {
      requestedColumns = options.columns.split(',').map((d) => d.trim());
    } else {
      requestedColumns = options.columns.map((d) => (d.name ? d.name.trim() : d.trim()));
    }
    requestedColumns.forEach((c) => {
      const matchingCols = fieldList
        .filter((f) => f.name === c || cleanColumnName(f.name) === cleanColumnName(c))
        .map((f) => f.name);
      columns = columns.concat(matchingCols);
    });
  }
  let limit = 0;
  if (parseInt(options.limit, 10) === options.limit) {
    limit = parseInt(options.limit, 10);
  }

  debug(
    `Reading parquet file ${options.filename} with columns ${columns?.join(',')} and limit ${limit}`
  );
  const cursor = reader.getCursor(columns);
  let counter = 0;
  const start = new Date().getTime();
  const stream = new Readable({
    objectMode: true,
    async read() {
      const token = await cursor.next();
      if (token) {
        counter += 1;
        if (limit && counter > limit) {
          debug(`Reached limit of ${limit}, stopping`);
          this.push(null);
          await reader.close();
          return;
        }
        if (counter % 10000 === 0) {
          const m = process.memoryUsage().heapTotal;
          const end = new Date().getTime();
          debug(
            `Read ${counter} ${(counter * 1000) / (end - start)}/sec, Node reported memory usage: ${m / 1024 / 1024} MBs`
          );
        }
        this.push(token);
      } else {
        await reader.close();
        this.push(null);
      }
    }
  });
  return { stream };
};
Worker.prototype.stream.metadata = {
  options: {
    path: {}
  }
};

Worker.prototype.toFile = async function (options) {
  const { stream } = await this.stream(options);
  const fworker = new FileWorker(this);
  return fworker.objectStreamToFile({ ...options, stream });
};
Worker.prototype.toFile.metadata = {
  options: {
    path: {}
  }
};

Worker.prototype.stats = async function (options) {
  const reader = await openFile(options.filename);
  try {
    return {
      schema: reader.getSchema(),
      fileMetadata: reader.getFileMetaData(),
      rowGroups: reader.getRowGroups()
    };
  } finally {
    await reader.close();
  }
};
Worker.prototype.stats.metadata = {
  options: {
    path: {}
  }
};

export default Worker;
