import http from 'node:http';
import {spawn} from 'node:child_process';
import assert from 'node:assert/strict';
import smpp from 'smpp';
let requests=[];
const api=http.createServer(async(req,res)=>{
  let body='';for await(const chunk of req)body+=chunk;
  if(req.url==='/api-send-sms'){
    requests.push(req.headers);
    await new Promise(r=>setTimeout(r,900));
    res.writeHead(422,{'Content-Type':'application/json'});
    res.end(JSON.stringify({error:{code:'link_blocked',message:'Blocked link',smpp:{command_status:0x453}}}));
  }else{res.end('{}');}
});
await new Promise(r=>api.listen(18082,'127.0.0.1',r));
const env={...process.env,SMPP_ACCOUNTS:JSON.stringify([{system_id:'OTXT04',password:'testonly',api_key:'otxt_test'},{system_id:'OTXT06',password:'testonly',api_key:'otxt_test'}]),GATEWAY_WEBHOOK_TOKEN:'test-only',OPENTXT_API_BASE_URL:'http://127.0.0.1:18082',SMPP_PORT:'12775',PORT:'18083',SUBMIT_ACK_MS:'100'};
const child=spawn(process.execPath,['src/index.js'],{cwd:new URL('../',import.meta.url).pathname,env,stdio:['ignore','pipe','pipe']});
let logs='';child.stdout.on('data',x=>logs+=x);child.stderr.on('data',x=>logs+=x);
await new Promise((resolve,reject)=>{let n=0;const t=setInterval(()=>{if(logs.includes('listening on :12775')){clearInterval(t);resolve();}else if(++n>100){clearInterval(t);reject(Error('gateway startup failed'));}},50);});
try{
 const session=smpp.connect('smpp://127.0.0.1:12775');
 session.on('error',e=>console.log('session error',e.message));
 const deadline=setTimeout(()=>{console.log('timeout',logs);child.kill('SIGKILL');process.exit(1);},10000);deadline.unref();
 await new Promise((resolve,reject)=>session.bind_transceiver({system_id:'OTXT04',password:'testonly'},p=>p.command_status?reject(Error('bind failed')):resolve()));
 let dlrs=0;session.on('deliver_sm',p=>{dlrs++;session.send(p.response());});
 const start=Date.now();
 const result=await new Promise(resolve=>session.submit_sm({destination_addr:'15555550101',short_message:'blocked test',registered_delivery:1},resolve));
 assert.equal(result.command_status,0x453);assert.ok(Date.now()-start>=850);assert.equal(dlrs,0);
 assert.equal(requests[0]['x-otxt-no-queue'],'1');assert.equal(requests[0]['x-otxt-via'],'smpp');
 console.log('PASS: real SMPP delayed refusal, no premature success, no synthetic receipt, strict no-queue header');
 session.close();
}finally{child.kill('SIGKILL');api.close();}
