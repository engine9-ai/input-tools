import mimeTypes from 'mime-types';

/** Content type for a file name, or undefined when the extension is not known. */
export default function lookup(file) {
  return mimeTypes.lookup(file) || undefined;
}
