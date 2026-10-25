// Backend admits a text but answers after the gateway's timeout: the gateway
// must find the backend-recorded mapping and answer success, not "failed".
import http from 'node:http';
import {spawn} from 'node:child_process';
import assert from 'node:assert/strict';
import smpp from 'smpp';
let seen=null;
const api=http.createServer(async(req,res)=>{
  let body='';for await(const c of req)body+=c;
  if(req.url==='/api-send-sms'){seen=req.headers;await new Promise(r=>setTimeout(r,1500));res.writeHead(202);return res.end('{"id":"late"}');}
  if(req.url.startsWith('/smpp-message-map?')){res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify({found:true,request_id:'log-1',system_id:'OTXT01'}));}
  res.end('{}');
});
await new Promise(r=>api.listen(18092,'127.0.0.1',r));
const env={...process.env,SMPP_ACCOUNTS:JSON.stringify([{system_id:'OTXT01',password:'testonly',api_key:'otxt_test'}]),GATEWAY_WEBHOOK_TOKEN:'t',OPENTXT_API_BASE_URL:'http://127.0.0.1:18092',SMPP_PORT:'12795',PORT:'18093',SEND_TIMEOUT_MS:'300'};
const child=spawn(process.execPath,['src/index.js'],{cwd:new URL('..',import.meta.url),env,stdio:['ignore','pipe','pipe']});
let logs='';child.stdout.on('data',x=>logs+=x);child.stderr.on('data',x=>logs+=x);
try{
  await new Promise((ok,bad)=>{let n=0;const t=setInterval(()=>{if(logs.includes('listening on :12795')){clearInterval(t);ok();}else if(++n>100){clearInterval(t);bad(Error('startup'));}},50);});
  const s=smpp.connect('smpp://127.0.0.1:12795');
  await new Promise((ok,bad)=>s.bind_transceiver({system_id:'OTXT01',password:'testonly'},p=>p.command_status?bad(Error('bind')):ok()));
  const r=await new Promise(ok=>s.submit_sm({destination_addr:'15555550101',short_message:'hello',registered_delivery:1},ok));
  assert.equal(r.command_status,0);assert.ok(r.message_id);
  assert.ok(Number(seen['x-otxt-submit-received-at'])>0);assert.equal(seen['x-otxt-smpp-registered'],'1');
  console.log('PASS: timed-out but admitted submit answers success with its message ID');
  s.close();
}finally{child.kill('SIGKILL');api.close();}
