import { google } from 'googleapis';
import fs from 'node:fs';
import nodestream from 'node:stream';
import debug$0 from 'debug';
import withDb from 'mime-type/with-db';
import { getTempFilename, getFilePostfix, normalizeListDepth, relativeDate } from './tools.js';

const debug = debug$0('@engine9/input/GoogleDrive');
const { mimeType: mime } = withDb;
const { Readable } = nodestream;
const fsp = fs.promises;

function Worker() {
    this.prefix = 'gdrive';
}

/*
  Parse a gdrive:// URL into its parts.
  The path is NOT a delimited path -- the folder identifier is always a single
  Google Drive parent folder id.  The allowed forms are:
    gdrive://{folderId}
    gdrive://{folderId}/
    gdrive://{folderId}/{file}
*/
function getParts(filename) {
    if (!filename) throw new Error(`Invalid filename: ${filename}`);
    if (!filename.startsWith('gdrive://')) {
        throw new Error(`Invalid filename, must start with gdrive://: ${filename}`);
    }
    const rest = filename.slice('gdrive://'.length);
    const slash = rest.indexOf('/');
    let folderId;
    let file;
    if (slash === -1) {
        folderId = rest;
        file = '';
    } else {
        folderId = rest.slice(0, slash);
        file = rest.slice(slash + 1);
    }
    if (!folderId) throw new Error(`Invalid gdrive path, missing folderId: ${filename}`);
    if (file.includes('/')) {
        throw new Error(
            `Invalid gdrive path ${filename}; the folder must be a single parent folder id, not a delimited path`
        );
    }
    return { folderId, file };
}

function getFolderId(directory) {
    if (!directory) throw new Error('directory is required');
    // Accept either a raw folder id or a gdrive:// URL.
    if (directory.startsWith('gdrive://')) {
        const { folderId, file } = getParts(directory.replace(/\/$/, ''));
        if (file) {
            throw new Error(
                `directory must be a gdrive folder (gdrive://{folderId}), not a file path: ${directory}`
            );
        }
        return folderId;
    }
    if (directory.includes('/')) {
        throw new Error(
            `directory must be a single Google Drive folder id or gdrive://{folderId}, got: ${directory}`
        );
    }
    return directory;
}

function toDate(v) {
    if (!v) return null;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
}

Worker.prototype.setAuth = async function () {
    const keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    if (!keyFile) {
        throw new Error(
            'GOOGLE_APPLICATION_CREDENTIALS env var is not set; point it at a service-account JSON key file that also contains a "subject_to_impersonate" field'
        );
    }
    let raw;
    try {
        raw = await fsp.readFile(keyFile);
    } catch (e) {
        throw new Error(
            `Could not read GOOGLE_APPLICATION_CREDENTIALS file at ${keyFile}: ${e.message}`
        );
    }
    let settings;
    try {
        settings = JSON.parse(raw);
    } catch (e) {
        throw new Error(
            `GOOGLE_APPLICATION_CREDENTIALS file at ${keyFile} is not valid JSON: ${e.message}`
        );
    }
    if (!settings.subject_to_impersonate)
        throw new Error(`You should include subject_to_impersonate in file ${keyFile}`);
    const auth = new google.auth.GoogleAuth({
        clientOptions: {
            subject: settings.subject_to_impersonate
        },
        keyFile,
        scopes: ['https://www.googleapis.com/auth/drive']
    });
    google.options({ auth });
};

Worker.prototype.getClient = async function () {
    if (!this.client) {
        await this.setAuth();
        this.client = google.drive({ version: 'v3' });
    }
    return this.client;
};

/*
  Resolve a filename inside a folder to its Drive file id.
  Returns the first non-trashed match, or null if none is found.
*/
Worker.prototype.findFileId = async function ({ folderId, file }) {
    if (!folderId) throw new Error('folderId is required');
    if (!file) throw new Error('file is required');
    const drive = await this.getClient();
    const escaped = file.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    const q = `'${folderId}' in parents and name = '${escaped}' and trashed = false`;
    const resp = await drive.files.list({
        q,
        pageSize: 2,
        fields: 'files(id, name, mimeType, size, modifiedTime, createdTime)',
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
    });
    const files = resp.data?.files || [];
    if (files.length === 0) return null;
    if (files.length > 1) {
        debug(`Warning: multiple files named ${file} found in folder ${folderId}; using the first`);
    }
    return files[0];
};

/*
  List all files in a folder as Drive metadata (paginated).
*/
Worker.prototype._listFolderRaw = async function ({ folderId, pageSize = 200 }) {
    const drive = await this.getClient();
    const q = `'${folderId}' in parents and trashed = false`;
    const files = [];
    let pageToken;
    do {
        const resp = await drive.files.list({
            q,
            pageSize,
            pageToken,
            fields:
                'nextPageToken, files(id, name, mimeType, size, modifiedTime, createdTime)',
            supportsAllDrives: true,
            includeItemsFromAllDrives: true
        });
        files.push(...(resp.data?.files || []));
        pageToken = resp.data?.nextPageToken;
    } while (pageToken);
    return files;
};

Worker.prototype.list = async function ({ directory, path, start, end, postfix, raw, depth }) {
    const folderId = getFolderId(directory || path);
    const maxDepth = normalizeListDepth(depth);
    if (maxDepth && maxDepth > 1) {
        throw new Error(
            'gdrive list does not support depth>1: a gdrive directory is a single parent folder id'
        );
    }
    const files = await this._listFolderRaw({ folderId });
    if (raw) return files;
    const FOLDER_MIME = 'application/vnd.google-apps.folder';
    return files
        .filter((f) => {
            const modifiedAt = toDate(f.modifiedTime);
            if (start && modifiedAt && modifiedAt < start) return false;
            if (end && modifiedAt && modifiedAt > end) return false;
            if (postfix && f.mimeType !== FOLDER_MIME && !f.name.endsWith(postfix)) return false;
            return true;
        })
        .map((f) => ({
            name: f.name,
            type: f.mimeType === FOLDER_MIME ? 'directory' : 'file',
            size: f.size ? parseInt(f.size, 10) : undefined,
            modifiedAt: f.modifiedTime ? new Date(f.modifiedTime).toISOString() : undefined
        }));
};
Worker.prototype.list.metadata = {
    options: {
        directory: { description: 'Google Drive folder id or gdrive://{folderId}' },
        path: { description: 'Deprecated alias for directory' },
        postfix: { description: 'Only include files whose name ends with this string' },
        raw: { description: 'Return the raw Drive file metadata instead of the normalized list' }
    }
};

Worker.prototype.listAll = async function (options) {
    const { directory } = options;
    const folderId = getFolderId(directory);
    const start = options.start ? relativeDate(options.start) : null;
    const end = options.end ? relativeDate(options.end) : null;
    const files = await this._listFolderRaw({ folderId });
    const FOLDER_MIME = 'application/vnd.google-apps.folder';
    return files
        .filter((f) => f.mimeType !== FOLDER_MIME)
        .filter((f) => {
            const modifiedAt = toDate(f.modifiedTime);
            if (start && modifiedAt && modifiedAt < start) return false;
            if (end && modifiedAt && modifiedAt > end) return false;
            return true;
        })
        .map((f) => `${this.prefix}://${folderId}/${f.name}`);
};
Worker.prototype.listAll.metadata = {
    options: {
        directory: { required: true },
        start: {},
        end: {}
    }
};

Worker.prototype.analyzeDirectory = async function ({ directory }) {
    const folderId = getFolderId(directory);
    const files = await this._listFolderRaw({ folderId });
    const FOLDER_MIME = 'application/vnd.google-apps.folder';
    const postfixCounts = Object.create(null);
    let fileCount = 0;
    let directoryCount = 0;
    let firstModified = null;
    let lastModified = null;
    let firstTime = null;
    let lastTime = null;
    for (const f of files) {
        if (f.mimeType === FOLDER_MIME) {
            directoryCount += 1;
            continue;
        }
        fileCount += 1;
        const p = getFilePostfix(f.name);
        postfixCounts[p] = (postfixCounts[p] || 0) + 1;
        const modifiedAt = toDate(f.modifiedTime);
        if (!modifiedAt) continue;
        const mtime = modifiedAt.getTime();
        const filename = `${this.prefix}://${folderId}/${f.name}`;
        const iso = modifiedAt.toISOString();
        if (firstTime === null || mtime < firstTime) {
            firstTime = mtime;
            firstModified = { filename, modifiedAt: iso };
        }
        if (lastTime === null || mtime > lastTime) {
            lastTime = mtime;
            lastModified = { filename, modifiedAt: iso };
        }
    }
    return {
        fileCount,
        directoryCount,
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

Worker.prototype.stream = async function ({ filename }) {
    const { folderId, file } = getParts(filename);
    if (!file) throw new Error(`filename must include a file name: ${filename}`);
    const drive = await this.getClient();
    const match = await this.findFileId({ folderId, file });
    if (!match) throw new Error(`No file named ${file} found in folder ${folderId}`);
    debug(`Streaming file gdrive://${folderId}/${file} (id=${match.id})`);
    const response = await drive.files.get(
        { fileId: match.id, alt: 'media', supportsAllDrives: true },
        { responseType: 'stream' }
    );
    return { stream: response.data };
};
Worker.prototype.stream.metadata = {
    options: {
        filename: {}
    }
};

Worker.prototype.download = async function ({ filename }) {
    const { folderId, file } = getParts(filename);
    if (!file) throw new Error(`filename must include a file name: ${filename}`);
    const localPath = await getTempFilename({ targetFilename: file });
    const { stream } = await this.stream({ filename });
    debug(`Downloading gdrive://${folderId}/${file} to ${localPath}`);
    const fileStream = fs.createWriteStream(localPath);
    stream.pipe(fileStream);
    return new Promise((resolve, reject) => {
        fileStream.on('finish', async () => {
            const { size } = await fs.promises.stat(localPath);
            resolve({ size, filename: localPath });
        });
        fileStream.on('error', reject);
        stream.on('error', reject);
    });
};
Worker.prototype.download.metadata = {
    options: {
        filename: {}
    }
};

Worker.prototype._upload = async function ({ folderId, file, mimeType, body }) {
    const drive = await this.getClient();
    const existing = await this.findFileId({ folderId, file });
    if (existing) {
        const resp = await drive.files.update({
            fileId: existing.id,
            supportsAllDrives: true,
            media: { mimeType, body },
            fields: 'id, name, parents, size, modifiedTime'
        });
        return resp.data;
    }
    const resp = await drive.files.create({
        supportsAllDrives: true,
        requestBody: {
            name: file,
            parents: [folderId]
        },
        media: { mimeType, body },
        fields: 'id, name, parents, size, modifiedTime'
    });
    return resp.data;
};

Worker.prototype.put = async function (options) {
    const { filename, directory } = options;
    if (!filename) throw new Error('Local filename required');
    const folderId = getFolderId(directory);
    const file = options.file || filename.split('/').pop();
    const mimeType = mime.lookup(file) || 'application/octet-stream';
    debug(`Putting ${filename} to gdrive://${folderId}/${file} (mime=${mimeType})`);
    return this._upload({
        folderId,
        file,
        mimeType,
        body: fs.createReadStream(filename)
    });
};
Worker.prototype.put.metadata = {
    options: {
        filename: { description: 'Local file to upload' },
        directory: {
            description: 'Google Drive folder id or gdrive://{folderId} to put the file into'
        },
        file: { description: 'Name of the file in Drive; defaults to the local filename' }
    }
};

Worker.prototype.write = async function (options) {
    const { directory, file, content } = options;
    if (!file) throw new Error('file is required');
    const folderId = getFolderId(directory);
    const mimeType = mime.lookup(file) || 'application/octet-stream';
    debug(`Writing content of length ${content?.length} to gdrive://${folderId}/${file}`);
    const body = typeof content === 'string' || Buffer.isBuffer(content)
        ? Readable.from(content)
        : content;
    return this._upload({ folderId, file, mimeType, body });
};
Worker.prototype.write.metadata = {
    options: {
        directory: { description: 'Google Drive folder id or gdrive://{folderId}' },
        file: { description: 'Name of the file in Drive' },
        content: { description: 'Contents of file (string, Buffer, or stream)' }
    }
};

Worker.prototype.remove = async function ({ filename }) {
    const { folderId, file } = getParts(filename);
    if (!file) throw new Error(`filename must include a file name: ${filename}`);
    const drive = await this.getClient();
    const match = await this.findFileId({ folderId, file });
    if (!match) {
        debug(`No file named ${file} found in folder ${folderId}; nothing to remove`);
        return { removed: filename };
    }
    await drive.files.delete({ fileId: match.id, supportsAllDrives: true });
    return { removed: filename };
};
Worker.prototype.remove.metadata = {
    options: {
        filename: {}
    }
};

Worker.prototype.copy = async function ({ filename, target }) {
    if (!filename?.startsWith('gdrive://'))
        throw new Error('gdrive copy requires a gdrive:// source -- use put for local files');
    if (!target?.startsWith('gdrive://'))
        throw new Error('gdrive copy requires a gdrive:// target');
    const { folderId: srcFolderId, file: srcFile } = getParts(filename);
    const { folderId: dstFolderId, file: dstFile } = getParts(target);
    if (!srcFile || !dstFile)
        throw new Error('gdrive copy requires both source and target to include a file name');
    const drive = await this.getClient();
    const match = await this.findFileId({ folderId: srcFolderId, file: srcFile });
    if (!match) throw new Error(`No file named ${srcFile} found in folder ${srcFolderId}`);
    const existing = await this.findFileId({ folderId: dstFolderId, file: dstFile });
    if (existing) {
        // Overwrite by removing existing destination file first.
        await drive.files.delete({ fileId: existing.id, supportsAllDrives: true });
    }
    const resp = await drive.files.copy({
        fileId: match.id,
        supportsAllDrives: true,
        requestBody: {
            name: dstFile,
            parents: [dstFolderId]
        },
        fields: 'id, name, parents, size, modifiedTime'
    });
    return resp.data;
};
Worker.prototype.copy.metadata = {
    options: {
        filename: {},
        target: {}
    }
};

Worker.prototype.move = async function ({ filename, target }) {
    if (!filename?.startsWith('gdrive://'))
        throw new Error('gdrive move requires a gdrive:// source');
    if (!target?.startsWith('gdrive://'))
        throw new Error('gdrive move requires a gdrive:// target');
    const { folderId: srcFolderId, file: srcFile } = getParts(filename);
    const { folderId: dstFolderId, file: dstFile } = getParts(target);
    if (!srcFile || !dstFile)
        throw new Error('gdrive move requires both source and target to include a file name');
    const drive = await this.getClient();
    const match = await this.findFileId({ folderId: srcFolderId, file: srcFile });
    if (!match) throw new Error(`No file named ${srcFile} found in folder ${srcFolderId}`);
    // If the destination already has a file with the same name, remove it first
    // so the rename/reparent doesn't collide.
    const existing = await this.findFileId({ folderId: dstFolderId, file: dstFile });
    if (existing && existing.id !== match.id) {
        await drive.files.delete({ fileId: existing.id, supportsAllDrives: true });
    }
    const resp = await drive.files.update({
        fileId: match.id,
        supportsAllDrives: true,
        addParents: dstFolderId,
        removeParents: srcFolderId,
        requestBody: { name: dstFile },
        fields: 'id, name, parents, size, modifiedTime'
    });
    return { filename: target, ...resp.data };
};
Worker.prototype.move.metadata = {
    options: {
        filename: {},
        target: {}
    }
};

Worker.prototype.moveAll = async function ({ directory, targetDirectory }) {
    if (!directory || !targetDirectory)
        throw new Error('directory and targetDirectory required');
    const files = await this.listAll({ directory });
    const srcFolderPrefix = `${this.prefix}://${getFolderId(directory)}/`;
    const dstFolderPrefix = targetDirectory.startsWith('gdrive://')
        ? `${targetDirectory.replace(/\/$/, '')}/`
        : `${this.prefix}://${getFolderId(targetDirectory)}/`;
    const configs = files.map((f) => ({
        filename: f,
        target: f.replace(srcFolderPrefix, dstFolderPrefix)
    }));
    const pLimit = await import('p-limit');
    const limitedMethod = pLimit.default(10);
    return Promise.all(
        configs.map(({ filename, target }) =>
            limitedMethod(async () => this.move({ filename, target }))
        )
    );
};
Worker.prototype.moveAll.metadata = {
    options: {
        directory: { required: true },
        targetDirectory: { required: true }
    }
};

Worker.prototype.stat = async function ({ filename }) {
    if (!filename) throw new Error('filename is required');
    const { folderId, file } = getParts(filename);
    if (!file) throw new Error(`filename must include a file name: ${filename}`);
    const match = await this.findFileId({ folderId, file });
    if (!match) throw new Error(`No file named ${file} found in folder ${folderId}`);
    const modifiedAt = toDate(match.modifiedTime);
    const createdAt = toDate(match.createdTime) || modifiedAt;
    const size = match.size ? parseInt(match.size, 10) : undefined;
    return {
        createdAt,
        modifiedAt,
        contentType: match.mimeType,
        size
    };
};
Worker.prototype.stat.metadata = {
    options: {
        filename: {}
    }
};

Worker.prototype.getMetadata = async function ({ filename }) {
    if (!filename) throw new Error('filename is required');
    const { folderId, file } = getParts(filename);
    if (!file) throw new Error(`filename must include a file name: ${filename}`);
    const match = await this.findFileId({ folderId, file });
    if (!match) throw new Error(`No file named ${file} found in folder ${folderId}`);
    return match;
};
Worker.prototype.getMetadata.metadata = {
    options: {
        filename: {}
    }
};

export default Worker;
