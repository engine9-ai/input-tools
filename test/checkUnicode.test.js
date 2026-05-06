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
});
