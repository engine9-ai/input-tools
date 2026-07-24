import assert from 'node:assert';
import nodetest from 'node:test';
import { checkUnicode, collectInvalidUnicodeValues, cleanUnicodeValues } from '../checkUnicode.js';

const { describe, it } = nodetest;

/** @param {unknown} input @param {string} expected @param {object} [opts] */
function assertCleansTo(input, expected, opts = { clean: true }) {
  const r = checkUnicode(input, opts);
  assert.strictEqual(r.ok, true, `expected ok for ${JSON.stringify(input)}`);
  assert.strictEqual(r.value, expected);
  if (r.violation) assert.strictEqual(r.cleaned, true);
}

describe('checkUnicode', () => {
  it('accepts null, undefined, empty, printable ASCII', () => {
    assert.deepStrictEqual(checkUnicode(null), { ok: true, value: null });
    assert.deepStrictEqual(checkUnicode(undefined), { ok: true, value: undefined });
    assert.deepStrictEqual(checkUnicode(''), { ok: true, value: '' });
    assert.deepStrictEqual(checkUnicode('TXT_OLOGY_ACW_KSDM_20250611_FR'), {
      ok: true,
      value: 'TXT_OLOGY_ACW_KSDM_20250611_FR'
    });
  });

  it('rejects non-printable ASCII when clean is false', () => {
    const r = checkUnicode('ABC\tDEF');
    assert.strictEqual(r.ok, false);
    assert.strictEqual(r.violation.reason, 'non_printable_ascii');
  });

  it('maps right single quotation mark U+2019 to ASCII apostrophe', () => {
    assert.deepStrictEqual(checkUnicode('O\u2019Brien'), { ok: true, value: "O'Brien" });
  });

  it('maps left single quotation mark U+2018 to ASCII apostrophe', () => {
    assert.deepStrictEqual(checkUnicode('\u2018quoted'), { ok: true, value: "'quoted" });
  });

  it('accepts horizontal ellipsis U+2026', () => {
    assert.deepStrictEqual(checkUnicode('wait\u2026now'), { ok: true, value: 'wait\u2026now' });
  });

  it('rejects mathematical alphanumeric unless clean:true', () => {
    const boldK = '\u{1D40A}'; // MATHEMATICAL BOLD CAPITAL K
    const boldThree = '\u{1D7EF}'; // MATHEMATICAL SANS-SERIF BOLD DIGIT THREE
    assert.strictEqual(checkUnicode(`PRE_${boldK}_${boldThree}`).ok, false);
    assert.strictEqual(checkUnicode(`X${boldK}Y`).ok, false);
    assertCleansTo(`PRE_${boldK}_${boldThree}`, 'PRE_K_3');
  });

  it('clean:true strips combining marks (NFKD + Mn)', () => {
    assertCleansTo('caf\u00e9_GOOD', 'cafe_GOOD');
  });

  it('clean:true maps Latin accented letters to ASCII (ó and similar)', () => {
    assertCleansTo('C\u00f3rdoba', 'Cordoba');
    assertCleansTo('jalape\u00f1o', 'jalapeno');
    assertCleansTo('M\u00fcller', 'Muller');
    assertCleansTo('fa\u00e7ade', 'facade');
  });

  it('clean:true maps Latin letters that do not NFKD to a single ASCII letter', () => {
    assertCleansTo('S\u00f8ren', 'Soren');
    assertCleansTo('E\u00e6r', 'Eaer');
    assertCleansTo('gro\u00df', 'gross');
    assertCleansTo('c\u0153ur', 'coeur');
  });

  it('clean:true removes invisible characters', () => {
    assertCleansTo('AB\u200BCD', 'ABCD');
  });

  it('clean:true maps fullwidth Latin to ASCII', () => {
    assertCleansTo(`X\uFF2BY`, 'XKY');
  });

  it('clean:true maps unicode dashes to ASCII hyphen', () => {
    assertCleansTo('A\u2013B', 'A-B');
    assertCleansTo('A\u2014B', 'A-B');
  });

  it('clean:true maps replacement character U+FFFD to ASCII space', () => {
    assertCleansTo('A\uFFFDB', 'A B');
    assertCleansTo('foo\uFFFDbar', 'foo bar');
    assertCleansTo('\uFFFDtrim', 'trim');
  });

  it('clean:true trims surrounding whitespace and tabs', () => {
    assertCleansTo('\t  ABC_123 \t', 'ABC_123');
  });

  it('rejects maxLength when clean is false', () => {
    const r = checkUnicode('abcd', { maxLength: 3 });
    assert.strictEqual(r.ok, false);
    assert.strictEqual(r.violation.reason, 'max_length_exceeded');
  });

  it('clean:true truncates to maxLength after typo repair and reports violation', () => {
    assert.deepStrictEqual(checkUnicode('abcdefghij', { clean: true, maxLength: 4 }), {
      ok: true,
      value: 'abcd',
      cleaned: true,
      violation: { reason: 'max_length_exceeded', length: 10, maxLength: 4 }
    });
  });

  it('clean:true reports unicode violation when repairing look-alikes', () => {
    const boldK = '\u{1D40A}';
    const r = checkUnicode(`PRE_${boldK}`, { clean: true, maxLength: 180 });
    assert.strictEqual(r.ok, true);
    assert.strictEqual(r.value, 'PRE_K');
    assert.strictEqual(r.cleaned, true);
    assert.strictEqual(r.violation.reason, 'mathematical_alphanumeric_symbol');
  });

  it('cleanUnicodeValues mutates rows and samples length/unicode cleanups', () => {
    const long = `${'a'.repeat(200)} https://example.com/x`;
    const boldK = '\u{1D40A}';
    const batch = [
      { source_code: 'ok' },
      { source_code: long },
      { source_code: long },
      { source_code: `BAD_${boldK}` }
    ];
    const { count, samples, failures } = cleanUnicodeValues(batch, { maxLength: 180, maxSamples: 5 });
    assert.strictEqual(failures.length, 0);
    assert.strictEqual(count, 3);
    assert.strictEqual(batch[1].source_code.length, 180);
    assert.strictEqual(batch[3].source_code, 'BAD_K');
    assert.strictEqual(samples.length, 2);
    assert.strictEqual(samples[0].violation.reason, 'max_length_exceeded');
    assert.strictEqual(samples[1].violation.reason, 'mathematical_alphanumeric_symbol');
    assert.strictEqual(samples[0].cleaned.length, 180);
  });

  it('collectInvalidUnicodeValues counts rows and dedupes samples by value', () => {
    const boldK = '\u{1D40A}';
    const batch = [
      { source_code: 'ok' },
      { source_code: `BAD_${boldK}` },
      { source_code: `BAD_${boldK}` },
      { source_code: '\uFF21' }
    ];
    const { count, samples } = collectInvalidUnicodeValues(batch, {
      maxSamples: 5,
      clean: false
    });
    assert.strictEqual(count, 3);
    assert.strictEqual(samples.length, 2);
    assert.ok(samples.every((s) => s.violation && typeof s.value === 'string'));
  });

  it('clean:true maps U+1D7FB to ASCII digit (monospace five)', () => {
    assertCleansTo('\u{1D7FB}', '5');
  });

  it('clean:true maps Latin-1 / symbol punctuation outside ASCII to underscore', () => {
    assertCleansTo('a\u00b1b', 'a_b');
    assertCleansTo('90\u00b0', '90_');
    assertCleansTo('x\u00d7y', 'x_y');
    assertCleansTo('x\u00f7y', 'x_y');
    assertCleansTo('a\u{1F600}b', 'a_b');
  });

  it('clean:true maps NBSP to ordinary space', () => {
    assertCleansTo('a\u00a0b', 'a b');
  });
});
