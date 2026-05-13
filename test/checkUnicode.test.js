import assert from 'node:assert';
import nodetest from 'node:test';
import { checkUnicode, collectInvalidUnicodeValues } from '../checkUnicode.js';

const { describe, it } = nodetest;

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
    assert.deepStrictEqual(checkUnicode(`PRE_${boldK}_${boldThree}`, { clean: true }), {
      ok: true,
      value: 'PRE_K_3'
    });
  });

  it('clean:true strips combining marks (NFKD + Mn)', () => {
    assert.deepStrictEqual(checkUnicode('caf\u00e9_GOOD', { clean: true }), {
      ok: true,
      value: 'cafe_GOOD'
    });
  });

  it('clean:true removes invisible characters', () => {
    assert.deepStrictEqual(checkUnicode('AB\u200BCD', { clean: true }), { ok: true, value: 'ABCD' });
  });

  it('clean:true maps fullwidth Latin to ASCII', () => {
    assert.deepStrictEqual(checkUnicode(`X\uFF2BY`, { clean: true }), { ok: true, value: 'XKY' });
  });

  it('clean:true maps unicode dashes to ASCII hyphen', () => {
    assert.deepStrictEqual(checkUnicode('A\u2013B', { clean: true }), { ok: true, value: 'A-B' });
    assert.deepStrictEqual(checkUnicode('A\u2014B', { clean: true }), { ok: true, value: 'A-B' });
  });

  it('clean:true maps replacement character U+FFFD to ASCII space', () => {
    assert.deepStrictEqual(checkUnicode('A\uFFFDB', { clean: true }), { ok: true, value: 'A B' });
    assert.deepStrictEqual(checkUnicode('foo\uFFFDbar', { clean: true }), { ok: true, value: 'foo bar' });
    assert.deepStrictEqual(checkUnicode('\uFFFDtrim', { clean: true }), { ok: true, value: 'trim' });
  });

  it('clean:true trims surrounding whitespace and tabs', () => {
    assert.deepStrictEqual(checkUnicode('\t  ABC_123 \t', { clean: true }), {
      ok: true,
      value: 'ABC_123'
    });
  });

  it('rejects maxLength when clean is false', () => {
    const r = checkUnicode('abcd', { maxLength: 3 });
    assert.strictEqual(r.ok, false);
    assert.strictEqual(r.violation.reason, 'max_length_exceeded');
  });

  it('clean:true truncates to maxLength after typo repair', () => {
    assert.deepStrictEqual(checkUnicode('abcdefghij', { clean: true, maxLength: 4 }), {
      ok: true,
      value: 'abcd'
    });
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
    assert.deepStrictEqual(checkUnicode('\u{1D7FB}', { clean: true }), { ok: true, value: '5' });
  });

  it('clean:true maps Latin-1 / symbol punctuation outside ASCII to underscore', () => {
    assert.deepStrictEqual(checkUnicode('a\u00b1b', { clean: true }), { ok: true, value: 'a_b' });
    assert.deepStrictEqual(checkUnicode('90\u00b0', { clean: true }), { ok: true, value: '90_' });
    assert.deepStrictEqual(checkUnicode('x\u00d7y', { clean: true }), { ok: true, value: 'x_y' });
    assert.deepStrictEqual(checkUnicode('x\u00f7y', { clean: true }), { ok: true, value: 'x_y' });
    assert.deepStrictEqual(checkUnicode('a\u{1F600}b', { clean: true }), { ok: true, value: 'a_b' });
  });

  it('clean:true maps NBSP to ordinary space', () => {
    assert.deepStrictEqual(checkUnicode('a\u00a0b', { clean: true }), { ok: true, value: 'a b' });
  });
});
