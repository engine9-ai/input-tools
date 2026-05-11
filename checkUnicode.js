/**
 * Printable ASCII validation for identifier-like strings (U+0020–U+007E).
 * Internal typo repair maps mathematical / fullwidth characters to ASCII when `clean` is true.
 */

const PRINTABLE_ASCII_MIN = 0x20;
const PRINTABLE_ASCII_MAX = 0x7e;
const ALLOWED_NON_ASCII_CODE_POINTS = new Set([
  0x2018, // LEFT SINGLE QUOTATION MARK
  0x2019, // RIGHT SINGLE QUOTATION MARK
  0x2026 // HORIZONTAL ELLIPSIS
]);

const INVISIBLE_DELETE_WHEN_REPAIR = new Set([
  0x200b, // ZWSP
  0x200c, // ZWNJ
  0x200d, // ZWJ
  0xfeff, // BOM
  0x2060, // WORD JOINER
  0x00ad // SOFT HYPHEN
]);

/** Unicode Mathematical Alphanumeric Symbols: contiguous Latin letter clusters (26 UC + 26 lc each). */
const LATIN_LETTER_52_BLOCK_STARTS = [
  0x1d400, // bold
  0x1d434, // italic
  0x1d468, // bold italic
  0x1d5a0, // sans-serif
  0x1d5d4, // sans-serif bold
  0x1d608, // sans-serif italic
  0x1d63c, // sans-serif bold italic
  0x1d670 // monospace
];

/** Mathematical digit “styles”: ten consecutive decimal digits per block. */
const DIGIT_BLOCK_STARTS = [0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6];

/**
 * @param {number} cp
 * @param {number} blockStart
 * @returns {string | null}
 */
function mapLatin52(cp, blockStart) {
  if (cp < blockStart || cp > blockStart + 51) return null;
  const off = cp - blockStart;
  if (off < 26) return String.fromCharCode(0x41 + off);
  return String.fromCharCode(0x61 + off - 26);
}

/**
 * @param {number} cp
 * @returns {string | null}
 */
function mapDigit10(cp, base) {
  if (cp < base || cp > base + 9) return null;
  return String(cp - base);
}

/**
 * @param {number} cp
 * @returns {string | null}
 */
function typoReplacementCodePoint(cp) {
  // Normalize common unicode dash variants to ASCII hyphen-minus.
  if (cp === 0x2010 || cp === 0x2011 || cp === 0x2012 || cp === 0x2013 || cp === 0x2014 || cp === 0x2015) {
    return '-';
  }
  if (cp === 0x2212) return '-'; // MINUS SIGN
  if (cp === 0xfffd) return ' '; // REPLACEMENT CHARACTER (often shown as) -> ASCII space
  for (const base of DIGIT_BLOCK_STARTS) {
    const d = mapDigit10(cp, base);
    if (d !== null) return d;
  }
  for (const start of LATIN_LETTER_52_BLOCK_STARTS) {
    const ch = mapLatin52(cp, start);
    if (ch !== null) return ch;
  }
  if (cp >= 0xff01 && cp <= 0xff5e) return String.fromCharCode(cp - 0xfee0);
  if (cp >= 0xff21 && cp <= 0xff3a) return String.fromCharCode(0x41 + (cp - 0xff21));
  if (cp >= 0xff41 && cp <= 0xff5a) return String.fromCharCode(0x61 + (cp - 0xff41));
  if (cp >= 0xff10 && cp <= 0xff19) return String.fromCharCode(0x30 + (cp - 0xff10));
  return null;
}

/**
 * Apply common typo / homoglyph repairs toward ASCII identifiers.
 * @param {string} s
 * @returns {string}
 */
/**
 * After NFKD + mark stripping, map anything still outside printable ASCII (except the small
 * allowlist used for validation) to `_`, so {@link checkUnicode} with `clean: true` can succeed.
 * NO-BREAK SPACE maps to ordinary space.
 * @param {string} s
 * @returns {string}
 */
function replaceNonAsciiAllowedWithUnderscore(s) {
  let result = '';
  for (let i = 0; i < s.length; ) {
    const cp = s.codePointAt(i);
    const w = cp > 0xffff ? 2 : 1;
    if (cp === 0x00a0) {
      result += ' ';
    } else if (cp >= PRINTABLE_ASCII_MIN && cp <= PRINTABLE_ASCII_MAX) {
      result += String.fromCodePoint(cp);
    } else if (ALLOWED_NON_ASCII_CODE_POINTS.has(cp)) {
      result += String.fromCodePoint(cp);
    } else {
      result += '_';
    }
    i += w;
  }
  return result;
}

function applyReplaceCommonTypos(s) {
  let out = '';
  for (let i = 0; i < s.length; ) {
    const cp = s.codePointAt(i);
    const w = cp > 0xffff ? 2 : 1;
    if (INVISIBLE_DELETE_WHEN_REPAIR.has(cp)) {
      i += w;
      continue;
    }
    const repl = typoReplacementCodePoint(cp);
    if (repl !== null) out += repl;
    else out += String.fromCodePoint(cp);
    i += w;
  }
  const normalized = out.normalize('NFKD').replace(/\p{M}/gu, '');
  return replaceNonAsciiAllowedWithUnderscore(normalized);
}

/**
 * @param {number} cp
 * @returns {string}
 */
function classifyInvalidUnicodeCodePoint(cp) {
  if (cp < 0x20 || cp === 0x7f) return 'non_printable_ascii';
  if (cp >= 0x1d400 && cp <= 0x1d7ff) return 'mathematical_alphanumeric_symbol';
  if (cp >= 0xff00 && cp <= 0xffef) return 'fullwidth_or_halfwidth_form';
  if (
    (cp >= 0x0300 && cp <= 0x036f) ||
    (cp >= 0x1ab0 && cp <= 0x1aff) ||
    (cp >= 0x1dc0 && cp <= 0x1dff) ||
    (cp >= 0xfe20 && cp <= 0xfe2f)
  ) {
    return 'combining_mark';
  }
  if (INVISIBLE_DELETE_WHEN_REPAIR.has(cp)) return 'zero_width_or_invisible';
  return 'non_ascii_unicode';
}

/**
 * First printable-ASCII violation in `s`, or null if all characters are in range.
 * @param {string} s
 * @returns {{ index: number, codePoint: number, char: string, reason: string } | null}
 */
function findPrintableAsciiViolation(s) {
  for (let i = 0; i < s.length; ) {
    const cp = s.codePointAt(i);
    const len = cp > 0xffff ? 2 : 1;
    if (cp < PRINTABLE_ASCII_MIN || (cp > PRINTABLE_ASCII_MAX && !ALLOWED_NON_ASCII_CODE_POINTS.has(cp))) {
      return {
        index: i,
        codePoint: cp,
        char: String.fromCodePoint(cp),
        reason: classifyInvalidUnicodeCodePoint(cp)
      };
    }
    i += len;
  }
  return null;
}

const DEFAULT_MAX_SAMPLE_VALUE_LEN = 200;

/**
 * @typedef {{ maxLength?: number, clean?: boolean }} CheckUnicodeOptions
 */

/**
 * Validate `value` for printable ASCII (and optional max length). Used internally by {@link checkUnicode}
 * and {@link collectInvalidUnicodeValues}.
 *
 * When `clean` is true: trim surrounding whitespace, replace common Unicode typos/homoglyphs,
 * map any remaining non-ASCII (outside the small punctuation allowlist) to `_` (NBSP → space),
 * trim again (so replacement characters mapped to space do not leave stray leading/trailing spaces),
 * then truncate to `maxLength` if given, then validate.
 *
 * @param {unknown} value
 * @param {CheckUnicodeOptions} [options]
 * @returns {{ ok: true, value: string | null | undefined } | { ok: false, violation: Record<string, unknown>, pendingValue: string, rawValue: string }}
 */
function runUnicodeCheck(value, options = {}) {
  const { maxLength, clean = false } = options;
  if (value === null) return { ok: true, value: null };
  if (value === undefined) return { ok: true, value: undefined };
  const rawValue = String(value);
  let s = rawValue;
  if (clean) {
    s = s.trim();
    s = applyReplaceCommonTypos(s);
    s = s.trim();
    if (maxLength != null && s.length > maxLength) {
      s = s.slice(0, maxLength);
    }
  }
  const asciiViolation = findPrintableAsciiViolation(s);
  if (asciiViolation) {
    return {
      ok: false,
      violation: asciiViolation,
      pendingValue: s,
      rawValue
    };
  }
  if (!clean && maxLength != null && s.length > maxLength) {
    return {
      ok: false,
      violation: {
        reason: 'max_length_exceeded',
        length: s.length,
        maxLength
      },
      pendingValue: s,
      rawValue
    };
  }
  return { ok: true, value: s };
}

/**
 * Check whether `value` satisfies printable ASCII (U+0020–U+007E), optionally enforcing `maxLength`.
 *
 * @param {unknown} value
 * @param {CheckUnicodeOptions} [options]
 * @param {boolean} [options.clean=false] If true, applies common typo replacement then truncates to
 *   `maxLength` when specified, then validates.
 * @returns {{ ok: true, value: string | null | undefined } | { ok: false, violation: Record<string, unknown>, pendingValue: string, rawValue: string }}
 */
export function checkUnicode(value, options = {}) {
  return runUnicodeCheck(value, options);
}

/**
 * Scan rows for values that fail {@link checkUnicode}.
 *
 * @param {Array<Record<string, unknown>>} rows
 * @param {{
 *   field?: string,
 *   pick?: (row: Record<string, unknown>) => unknown,
 *   maxSamples?: number,
 *   maxSampleValueLen?: number,
 * } & CheckUnicodeOptions} [options]
 * @returns {{ count: number, samples: Array<{ value: string, violation: Record<string, unknown> }> }}
 */
export function collectInvalidUnicodeValues(rows, options = {}) {
  const {
    field = 'source_code',
    pick,
    maxSamples = 5,
    maxSampleValueLen = DEFAULT_MAX_SAMPLE_VALUE_LEN,
    ...checkOpts
  } = options;
  const getVal = pick || ((row) => row[field]);
  let count = 0;
  const samples = [];
  /** @type {Set<string>} */
  const seen = new Set();

  if (!Array.isArray(rows)) return { count: 0, samples: [] };

  for (const row of rows) {
    const raw = getVal(row);
    const r = runUnicodeCheck(raw, checkOpts);
    if (r.ok) continue;
    count += 1;
    if (samples.length >= maxSamples) continue;
    const display = String(raw);
    if (seen.has(display)) continue;
    seen.add(display);
    samples.push({
      value: display.length > maxSampleValueLen ? `${display.slice(0, maxSampleValueLen)}…` : display,
      violation: r.violation
    });
  }

  return { count, samples };
}
