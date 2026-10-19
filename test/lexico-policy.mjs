import assert from 'node:assert/strict';
import { strictSubmitFor, submitFailureStatus } from '../src/submit-policy.js';
assert.equal(strictSubmitFor('OTXT01'), true);
assert.equal(strictSubmitFor('otxt01'), true);
assert.equal(strictSubmitFor('OTXT04'), true);
assert.equal(strictSubmitFor('OTXT06'), false);
assert.equal(submitFailureStatus({status:503,error:{code:'temporarily_unavailable'}}), 0x45);
for (const status of [0x451,0x452,0x453,0x454,0x455]) assert.equal(submitFailureStatus({status:422,error:{smpp:{command_status:status}}}),status);
console.log('PASS Lexico strict admission and exact safety/temporary refusal codes');
