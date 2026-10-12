'use strict';
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const {WebSocketServer,WebSocket}=require('ws');
const PORT=Number(process.env.PORT)||3000,MAX=100;
const footballCooldown=new Map(),businessCooldown=new Map();
const catalog={cars:[{name:'City Mini',price:0},{name:'Lagos Cruiser',price:850000},{name:'Royal SUV',price:4200000},{name:'Golden Executive',price:18500000}],homes:[{name:'Agege Flat',price:2500000},{name:'Ikeja Duplex',price:18000000},{name:'Lekki Villa',price:95000000}],businesses:[{name:'Suya Stand',price:250000,income:1500},{name:'Mini Market',price:2200000,income:7000},{name:'Tech Hub',price:12000000,income:25000}]};
const dataDir=path.join(__dirname,'data');fs.mkdirSync(dataDir,{recursive:true});const saveFile=path.join(dataDir,'players.json');let records={};try{records=JSON.parse(fs.readFileSync(saveFile,'utf8'))}catch{};
function persist(){const tmp=saveFile+'.tmp';fs.writeFileSync(tmp,JSON.stringify(records));fs.renameSync(tmp,saveFile)}
const online=new Map();const limit=(n,a,b)=>Math.min(b,Math.max(a,n));
const send=(ws,obj)=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))};
function publicPlayer(p){return {id:p.id,name:p.name,x:p.x,z:p.z,angle:p.angle,car:p.car,driving:p.driving,skin:p.skin,shirt:p.shirt}}
function broadcast(obj,except){const payload=JSON.stringify(obj);for(const ws of online.keys())if(ws!==except&&ws.readyState===WebSocket.OPEN)ws.send(payload)}
const server=http.createServer((req,res)=>{let url;try{url=new URL(req.url,'http://localhost')}catch{res.writeHead(400).end();return}if(url.pathname==='/vendor/three.module.js'){const f=path.join(__dirname,'node_modules','three','build','three.module.js');return fs.readFile(f,(err,buf)=>{if(err){res.writeHead(503).end('Run npm install to install Three.js');return}res.writeHead(200,{'Content-Type':'text/javascript; charset=utf-8','Cache-Control':'no-store'});res.end(buf)})}const filename=url.pathname==='/'?'index.html':url.pathname.slice(1);if(!['index.html','game.js'].includes(filename)){res.writeHead(404).end();return}const file=path.join(__dirname,'public',filename);fs.readFile(file,(err,buf)=>{if(err){res.writeHead(404).end();return}res.writeHead(200,{'Content-Type':filename.endsWith('.js')?'text/javascript; charset=utf-8':'text/html; charset=utf-8','Cache-Control':'no-store'});res.end(buf)})});
const wss=new WebSocketServer({server,maxPayload:2048});
wss.on('connection',ws=>{if(online.size>=MAX){send(ws,{type:'error',message:'Server full (100/100). Try later.'});ws.close(1013,'Server full');return}let player=null;let lastMove=Date.now(),lastJob=0;ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch{return}if(!m||typeof m!=='object')return;
if(m.type==='join'&&!player){const token=typeof m.token==='string'&&/^[a-f0-9]{48}$/.test(m.token)?m.token:crypto.randomBytes(24).toString('hex');if([...online.values()].some(p=>p.token===token)){send(ws,{type:'error',message:'This account is already online.'});return}let r=records[token];if(!r){r={name:'Player',money:1500000,bank:0,cars:[0],homes:[],businesses:[],skin:0,shirt:0,x:0,z:0};records[token]=r;persist()}r.name=String(m.name||r.name).replace(/[^a-zA-Z0-9 _-]/g,'').slice(0,18).trim()||'Player';player={...r,id:crypto.randomUUID(),token,angle:0,car:0,driving:false};online.set(ws,player);send(ws,{type:'welcome',token,self:player,others:[...online.values()].filter(p=>p!==player).map(publicPlayer),catalog,maxPlayers:MAX});broadcast({type:'joined',player:publicPlayer(player)},ws);broadcast({type:'count',count:online.size});return}
if(!player)return;
if(m.type==='move'){const now=Date.now();if(now-lastMove<40)return;const x=Number(m.x),z=Number(m.z),angle=Number(m.angle);if(![x,z,angle].every(Number.isFinite))return;const dt=limit((now-lastMove)/1000,.04,.6);const dist=Math.hypot(x-player.x,z-player.z);if(dist>Math.max(3,(player.driving?22:9)*dt+2))return;player.x=limit(x,-95,95);player.z=limit(z,-95,95);player.angle=angle;lastMove=now;broadcast({type:'moved',player:publicPlayer(player)},ws);return}
if(m.type==='drive'){const index=Number(m.index);if(!Number.isInteger(index)||!player.cars.includes(index))return;player.car=index;player.driving=!!m.enabled;send(ws,{type:'state',self:player});broadcast({type:'moved',player:publicPlayer(player)},ws);return}
if(m.type==='buy'){const kind=m.kind,index=Number(m.index),items=catalog[kind];if(!items||!Number.isInteger(index)||!items[index])return;const owned=kind==='cars'?'cars':kind==='homes'?'homes':'businesses';if(player[owned].includes(index)){send(ws,{type:'notice',message:'Already owned'});return}const price=items[index].price;if(player.money<price){send(ws,{type:'notice',message:'Not enough cash'});return}player.money-=price;player[owned].push(index);persistPlayer();send(ws,{type:'state',self:player});return}
if(m.type==='bank'){const amount=Number(m.amount);if(!Number.isSafeInteger(amount)||amount<=0||amount>1000000000)return;if(m.action==='deposit'&&player.money>=amount){player.money-=amount;player.bank+=amount}else if(m.action==='withdraw'&&player.bank>=amount){player.bank-=amount;player.money+=amount}else{return}persistPlayer();send(ws,{type:'state',self:player});return}
if(m.type==='job'){const now=Date.now();if(now-lastJob<30000){send(ws,{type:'notice',message:'Next delivery available in '+Math.ceil((30000-(now-lastJob))/1000)+'s'});return}lastJob=now;player.money+=35000;persistPlayer();send(ws,{type:'state',self:player});send(ws,{type:'notice',message:'Delivery complete! +₦35,000'});return}
if(m.type==='phone_message'||m.type==='phone_call'||m.type==='phone_transfer'){
 const target=[...online.entries()].find(([sock,p])=>p.id===m.to);
 if(!target||target[0]===ws){send(ws,{type:'notice',message:'Player is offline or unavailable'});return}
 const [recipientSocket,recipient]=target;
 if(m.type==='phone_message'){
  const message=String(m.text||'').trim().slice(0,240);if(!message)return;
  send(recipientSocket,{type:'phone_message',from:player.name,text:message});return;
 }
 if(m.type==='phone_call'){
  send(recipientSocket,{type:'phone_alert',message:'📞 '+player.name+' is requesting a call (voice not enabled)'});return;
 }
 const amount=Number(m.amount);
 if(!Number.isSafeInteger(amount)||amount<=0||amount>100000000||player.bank<amount){send(ws,{type:'notice',message:'Invalid amount or insufficient bank balance'});return}
 player.bank-=amount;recipient.bank+=amount;
 records[recipient.token]={...records[recipient.token],bank:recipient.bank};
 persistPlayer();send(ws,{type:'state',self:player});send(recipientSocket,{type:'state',self:recipient});
 send(ws,{type:'notice',message:'Transferred ₦'+amount.toLocaleString()+' to '+recipient.name});
 send(recipientSocket,{type:'phone_alert',message:'Received ₦'+amount.toLocaleString()+' from '+player.name});return;
}
if(m.type==='football'){
 const now=Date.now(), last=footballCooldown.get(player.token)||0;
 if(now-last<20000){send(ws,{type:'notice',message:'Training cooldown: '+Math.ceil((20000-(now-last))/1000)+' seconds'});return}
 footballCooldown.set(player.token,now);
 const skill=Number(m.skill);if(!Number.isInteger(skill)||skill<0||skill>2)return;
 const goal=crypto.randomInt(100)<(skill===0?45:skill===1?60:70);
 const prize=goal?75000:12000;player.money+=prize;
 player.goals=(player.goals||0)+(goal?1:0);player.matches=(player.matches||0)+1;
 persistPlayer();send(ws,{type:'state',self:player});send(ws,{type:'football_result',goal,prize,goals:player.goals,matches:player.matches});return;
}
if(m.type==='business_claim'){
 const now=Date.now(),last=businessCooldown.get(player.token)||0;
 if(!player.businesses.length){send(ws,{type:'notice',message:'Buy a business first'});return}
 if(now-last<60000){send(ws,{type:'notice',message:'Business payout available in '+Math.ceil((60000-(now-last))/1000)+' seconds'});return}
 businessCooldown.set(player.token,now);
 const earnings=player.businesses.reduce((sum,i)=>sum+(catalog.businesses[i]?.income||0),0);
 player.bank+=earnings;persistPlayer();send(ws,{type:'state',self:player});send(ws,{type:'notice',message:'Business earnings: ₦'+earnings.toLocaleString()+' paid to bank'});return;
}
if(m.type==='customize'){const skin=Number(m.skin),shirt=Number(m.shirt);if(!Number.isInteger(skin)||skin<0||skin>4||!Number.isInteger(shirt)||shirt<0||shirt>5)return;player.skin=skin;player.shirt=shirt;persistPlayer();send(ws,{type:'state',self:player});broadcast({type:'moved',player:publicPlayer(player)},ws);return}
function persistPlayer(){records[player.token]={name:player.name,money:player.money,bank:player.bank,cars:player.cars,homes:player.homes,businesses:player.businesses,skin:player.skin,shirt:player.shirt,x:player.x,z:player.z,goals:player.goals||0,matches:player.matches||0};persist()}
});ws.on('close',()=>{if(!player)return;records[player.token]={name:player.name,money:player.money,bank:player.bank,cars:player.cars,homes:player.homes,businesses:player.businesses,skin:player.skin,shirt:player.shirt,x:player.x,z:player.z,goals:player.goals||0,matches:player.matches||0};persist();online.delete(ws);broadcast({type:'left',id:player.id});broadcast({type:'count',count:online.size})})});
server.listen(PORT,'0.0.0.0',()=>console.log('BASSPASS multiplayer running on port '+PORT+' (capacity 100)'));
