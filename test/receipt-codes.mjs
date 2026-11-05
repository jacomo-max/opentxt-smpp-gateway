import assert from 'node:assert/strict';
import { refusalStatusFor, rejectCodeFor, errField, networkErrorTlv, _parse } from '../src/receipt-codes.js';
const fb = _parse('otxt01:0x1fff'), rj = _parse('otxt01:0x45');
// Lexico local refusals use the agreed 0x1FFF, never vendor-range codes.
for (const s of [0x451, 0x452, 0x453, 0x457]) assert.equal(refusalStatusFor('OTXT01', s, fb), 0x1fff);
// Standard codes and other binds untouched.
assert.equal(refusalStatusFor('OTXT01', 0x58, fb), 0x58);
assert.equal(refusalStatusFor('OTXT04', 0x451, fb), 0x451);
// Lexico REJECTD: hex err field + TLV; other binds keep legacy receipt.
assert.equal(rejectCodeFor('OTXT01', rj), 0x45);
assert.equal(rejectCodeFor('OTXT04', rj), null);
assert.equal(errField(0x45), '045');
assert.deepEqual([...networkErrorTlv(0x45)], [3, 0, 0x45]);
assert.notEqual(rejectCodeFor('OTXT01', rj), 0); // never ASMSC_OK
console.log('receipt-codes ok');
