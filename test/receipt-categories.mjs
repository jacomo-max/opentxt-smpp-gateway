import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import smpp from 'smpp';
import {receiptFieldsFor} from '../src/receipt-categories.js';
import {rejectCodeFor,rejectStatFor,errField,networkErrorTlv} from '../src/receipt-codes.js';
const source=fs.readFileSync(new URL('../src/index.js',import.meta.url),'utf8');
const start=source.indexOf('async function sendOneReceipt(');
const end=source.indexOf('\nfunction sendMoMessage',start);
assert.ok(start>0 && end>start);
let captured=[];
const context=vm.createContext({receiptFieldsFor,rejectCodeFor,rejectStatFor,errField,networkErrorTlv,Date,Promise,setTimeout,clearTimeout,log:()=>{},pickReceiveSession:()=>({otxtSessionId:'test',deliver_sm(fields,cb){const pdu=new smpp.PDU('deliver_sm',fields);captured.push(smpp.PDU.fromBuffer(pdu.toBuffer()));cb({command_status:0});}})});
vm.runInContext(source.slice(start,end),context);
for(const [system,state,code,expected,tlv] of [['OTXT01','REJECTD','001','8191',[3,31,255]],['OTXT01','UNDELIV','001','000',[3,0,0]],['OTXT01','DELIVRD','000','000',null],['OTXT04','UNDELIV','001','001',null]]){
 await context.sendOneReceipt({systemId:system,smppMessageId:'isolated-test',to:'+15555550101',sourceAddr:'+15555550102'},state,code);
 const p=captured.at(-1);const text=p.short_message.message.toString();assert.match(text,new RegExp(`err:${expected} text:`));assert.match(text,new RegExp(`stat:${state==='REJECTD'&&system==='OTXT01'?'UNDELIV':state}`));
 if(tlv)assert.deepEqual([...p.network_error_code],tlv);else assert.equal(p.network_error_code,undefined);
 assert.equal(p.message_state,state==='DELIVRD'?2:5);
}
console.log('PASS: actual sendOneReceipt serializes both Lexico categories and preserves delivered/other-account receipts');
