import http from 'node:http';
import {spawn} from 'node:child_process';
import assert from 'node:assert/strict';
import smpp from 'smpp';
let requests=[];
const api=http.createServer(async(req,res)=>{
  let body='';for await(const chunk of req)body+=chunk;
  if(req.url==='/api-send-sms'){
    const data=JSON.parse(body); requests.push({headers:req.headers,body:data});
    await new Promise(r=>setTimeout(r, data.message.includes('load') ? 40 : 900));
    const allowed=data.message.includes('allowed');
    res.writeHead(allowed ? 202 : 422,{'Content-Type':'application/json'});
    res.end(JSON.stringify(allowed ? {data:{id:'test-accepted-id'}} : {error:{code:'link_blocked',message:'Blocked link',smpp:{command_status:0x453}}}));
  }else{res.end('{}');}
});
await new Promise(r=>api.listen(18082,'127.0.0.1',r));
const env={...process.env,SMPP_ACCOUNTS:JSON.stringify([{system_id:'OTXT04',password:'testonly',api_key:'otxt_test'},{system_id:'OTXT06',password:'testonly',api_key:'otxt_test'}]),GATEWAY_WEBHOOK_TOKEN:'test-only',OPENTXT_API_BASE_URL:'http://127.0.0.1:18082',SMPP_PORT:'12775',PORT:'18083',SUBMIT_ACK_MS:'100'};
const child=spawn(process.execPath,['src/index.js'],{cwd:new URL("..", import.meta.url),env,stdio:['ignore','pipe','pipe']});
let logs='';child.stdout.on('data',x=>logs+=x);child.stderr.on('data',x=>logs+=x);
await new Promise((resolve,reject)=>{let n=0;const t=setInterval(()=>{if(logs.includes('listening on :12775')){clearInterval(t);resolve();}else if(++n>100){clearInterval(t);reject(Error('gateway startup failed'));}},50);});
try{
 const session=smpp.connect('smpp://127.0.0.1:12775');
 session.on('error',e=>console.log('session error',e.message));
 const deadline=setTimeout(()=>{console.log('timeout',logs);child.kill('SIGKILL');process.exit(1);},60000);deadline.unref();
 await new Promise((resolve,reject)=>session.bind_transceiver({system_id:'OTXT04',password:'testonly'},p=>p.command_status?reject(Error('bind failed')):resolve()));
 let dlrs=0;session.on('deliver_sm',p=>{dlrs++;session.send(p.response());});
 const start=Date.now();
 const result=await new Promise(resolve=>session.submit_sm({destination_addr:'15555550101',short_message:'blocked test',registered_delivery:1},resolve));
 assert.equal(result.command_status,0x453);assert.ok(Date.now()-start>=850);assert.equal(dlrs,0);
 assert.equal(requests[0].headers['x-otxt-no-queue'],'1');assert.equal(requests[0].headers['x-otxt-via'],'smpp');
 console.log('PASS: real SMPP delayed refusal, no premature success, no synthetic receipt, strict no-queue header');
 const submit=fields=>new Promise(resolve=>session.submit_sm({destination_addr:'15555550101',registered_delivery:1,...fields},resolve));
 const part=(ref,seq,text)=>({short_message:text,sar_msg_ref_num:ref,sar_total_segments:2,sar_segment_seqnum:seq});
 const before=requests.length;
 let firstResponded=false;
 const first=submit(part(100,1,'blocked ')).then(r=>{firstResponded=true;return r;});
 await new Promise(r=>setTimeout(r,200));
 assert.equal(firstResponded,false); assert.equal(requests.length,before);
 const second=submit(part(100,2,'multipart'));
 for(const r of await Promise.all([first,second])) {assert.equal(r.command_status,0x453);assert.ok(!r.message_id);}
 assert.equal(requests.length,before+1);assert.equal(requests.at(-1).body.message,'blocked multipart');assert.equal(dlrs,0);
 console.log('PASS: both multipart submits rejected before success; complete text checked once');
 const good=await Promise.all([submit(part(101,2,'multipart')),submit(part(101,1,'allowed '))]);
 assert.ok(good.every(r=>r.command_status===0 && r.message_id));assert.notEqual(good[0].message_id,good[1].message_id);
 assert.equal(requests.at(-1).body.message,'allowed multipart');
 console.log('PASS: out-of-order allowed multipart assembled once with distinct part IDs');
 const beforeMissing=requests.length;
 const missing=await submit(part(102,1,'missing'));
 assert.equal(missing.command_status,smpp.ESME_RSUBMITFAIL);assert.ok(!missing.message_id);assert.equal(requests.length,beforeMissing);assert.equal(dlrs,0);
 console.log('PASS: incomplete multipart rejected with no backend send or receipt');
 const startLoad=Date.now(); const loadBefore=requests.length;
 const load=await Promise.all(Array.from({length:1200},(_,i)=>submit({short_message:`blocked load ${i}`})));
 assert.equal(load.length,1200);assert.ok(load.every(r=>r.command_status===0x453 && !r.message_id));
 assert.equal(requests.length-loadBefore,1200);assert.equal(dlrs,0);
 console.log(`PASS: 1,200 concurrent submits, exact refusals, no premature success/receipts, ${Date.now()-startLoad}ms`);
 clearTimeout(deadline);
 session.close();
}finally{child.kill('SIGKILL');api.close();}
