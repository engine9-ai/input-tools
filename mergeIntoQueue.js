/**
 * Merge an item into an array queue, combining with an earlier item that shares the same key.
 * Caller supplies merge(existing, incoming) for field-level semantics.
 */

export function keyFromFields(item, keyFields, { normalizeField } = {}) {
  return keyFields
    .map((field) => {
      const value = item[field];
      if (normalizeField) return normalizeField(field, value);
      return value ?? '';
    })
    .join('\0');
}

/**
 * @param {Array} queue - mutable array (created by caller if needed)
 * @param {object} item
 * @param {object} options
 * @param {(row: object) => string} [options.key] - full key function
 * @param {string[]} [options.keyFields] - shorthand when key is field projection
 * @param {(field: string, value: unknown) => unknown} [options.normalizeField]
 * @param {(existing: object, incoming: object) => object} [options.merge]
 * @param {string} [options.label] - used in duplicate-without-merge errors
 */
export function mergeIntoQueue(queue, item, { key, keyFields, normalizeField, merge, label = 'queue' } = {}) {
  if (!Array.isArray(queue)) {
    throw new Error('mergeIntoQueue requires an array queue');
  }
  const getKey =
    key ||
    (keyFields?.length
      ? (row) => keyFromFields(row, keyFields, { normalizeField })
      : null);
  if (!getKey) {
    throw new Error('mergeIntoQueue requires key or keyFields');
  }
  const itemKey = getKey(item);
  const idx = queue.findIndex((existing) => getKey(existing) === itemKey);
  if (idx < 0) {
    queue.push(item);
    return;
  }
  if (!merge) {
    throw new Error(
      `Duplicate item in ${label} (key=${itemKey.replace(/\0/g, ',')}); provide merge to combine duplicates`
    );
  }
  queue[idx] = merge(queue[idx], item);
}
