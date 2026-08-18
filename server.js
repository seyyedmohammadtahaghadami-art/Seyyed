require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");

const PORT = Number(process.env.PORT || 8080);
const POLL_MS = Math.max(1200, Number(process.env.POLL_MS || 1800));
const REQUEST_TIMEOUT_MS = Math.max(3000, Number(process.env.REQUEST_TIMEOUT_MS || 10000));
const GATEWAY = (process.env.TSE_GATEWAY || "https://webgw.tse.ir").replace(/\/+$/, "");
const CDN = (process.env.TSETMC_CDN || "https://cdn.tsetmc.com").replace(/\/+$/, "");
const CUSTOM = (process.env.CUSTOM_MARKET_URL || "").trim();
const API_KEY = (process.env.CUSTOM_API_KEY || "").trim();
const AUTH_KEY = (process.env.AUTH_KEY || "").trim();
const STALE_AFTER_MS = Math.max(5000, Number(process.env.STALE_AFTER_MS || 15000));
const SOURCE_MIN_ROWS = Math.max(1, Number(process.env.SOURCE_MIN_ROWS || 20));
const RATE_LIMIT_PER_MINUTE = Math.max(30, Number(process.env.RATE_LIMIT_PER_MINUTE || 240));
const VERSION = "8.0.0";
const SSE_MAX_CLIENTS = Math.max(10, Number(process.env.SSE_MAX_CLIENTS || 200));
const SOURCE_TARGET_MS = Math.max(500, Number(process.env.SOURCE_TARGET_MS || 3500));

const app = express();
app.disable("x-powered-by");
const server = http.createServer(app);
server.requestTimeout = Math.max(10000, REQUEST_TIMEOUT_MS + 5000);
server.headersTimeout = Math.max(12000, REQUEST_TIMEOUT_MS + 7000);
server.keepAliveTimeout = 5000;
const wss = new WebSocket.Server({ server, path: "/ws", perMessageDeflate: { threshold: 1024, serverNoContextTakeover: true, clientNoContextTakeover: true } });

app.use((req,res,next)=>{
  const id = req.get("X-Request-ID") || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2,8)}`;
  req.requestId=id; res.set("X-Request-ID",id);
  res.set("X-Content-Type-Options","nosniff");
  res.set("X-Frame-Options","SAMEORIGIN");
  res.set("Referrer-Policy","no-referrer");
  next();
});

const rateBuckets = new Map();
app.use((req,res,next)=>{
  if (req.path.startsWith("/api/") || req.path === "/ws") {
    const ip=(req.headers["x-forwarded-for"]||req.socket.remoteAddress||"unknown").toString().split(",")[0].trim();
    const now=Date.now(), bucket=rateBuckets.get(ip);
    if (!bucket || now-bucket.started>=60000) rateBuckets.set(ip,{started:now,count:1});
    else { bucket.count++; if(bucket.count>RATE_LIMIT_PER_MINUTE) return res.status(429).json({error:"rate limit exceeded",requestId:req.requestId}); }
  }
  next();
});
setInterval(()=>{ const cutoff=Date.now()-120000; for(const [k,v] of rateBuckets) if(v.started<cutoff) rateBuckets.delete(k); },60000).unref();

app.use(express.json({ limit: "256kb" }));
app.use(express.static(path.join(__dirname, "../public"), { maxAge: 0 }));

const clients = new Set();
const sseClients = new Set();
const cache = new Map();
const MAX_WS_CLIENTS = Math.max(10, Number(process.env.MAX_WS_CLIENTS || 200));

const bootAt = Date.now();

const state = {
  rows: [],
  source: null,
  updatedAt: null,
  latencyMs: null,
  error: null,
  cycle: 0,
  stale: true,
  sourceEvidence: null
};

const aiHistory = [];
const AI_HISTORY_LIMIT = 240;

function clamp(v, a=-100, b=100) { return Math.max(a, Math.min(b, Number(v) || 0)); }
function avg(a) { const x=a.filter(Number.isFinite); return x.length ? x.reduce((s,v)=>s+v,0)/x.length : 0; }
function median(a) { const x=a.filter(Number.isFinite).sort((a,b)=>a-b); if(!x.length)return 0; const m=Math.floor(x.length/2); return x.length%2?x[m]:(x[m-1]+x[m])/2; }
function std(a, m=avg(a)) { const x=a.filter(Number.isFinite); if(x.length<2)return 0; return Math.sqrt(avg(x.map(v=>(v-m)**2))); }
function z(v, a) { const m=avg(a), d=std(a,m); return d ? (v-m)/d : 0; }
function percentile(v,a) { const x=a.filter(Number.isFinite).sort((a,b)=>a-b); if(!x.length)return 50; let n=0; for(const q of x) if(q<=v)n++; return n/x.length*100; }

function robustZ(v, values) {
  const x = values.filter(Number.isFinite).sort((a,b)=>a-b);
  if (x.length < 8) return z(v, x);
  const q1=x[Math.floor((x.length-1)*0.25)], q3=x[Math.floor((x.length-1)*0.75)];
  const iqr=q3-q1; return iqr ? (v-median(x))/(iqr/1.349) : 0;
}

function aiAnalyzeMarket(rows) {
  const r=rows||[];
  if(!r.length)return {score:0,confidence:0,regime:'داده کافی نیست',breadth:0,liquidity:0,risk:100,momentum:0,participation:0,flow:0,alerts:[],top:[],dataQuality:0,evidence:[]};
  const pcts=r.map(x=>num(x.pct)), vols=r.map(x=>num(x.volumeRatio)).filter(v=>v>0), vals=r.map(x=>num(x.value)).filter(v=>v>0), powers=r.map(x=>num(x.buyerPower)).filter(v=>v>0), flows=r.map(x=>num(x.moneyFlow));
  const up=r.filter(x=>x.pct>0).length, down=r.filter(x=>x.pct<0).length, flat=r.length-up-down;
  const active=r.filter(x=>x.last||x.close).length, breadth=(up-down)/r.length*100, advRatio=(up+down)?(up-down)/(up+down):0;
  const flowScore=clamp(avg(flows.map(v=>robustZ(v,flows)))*18,-100,100);
  const powerScore=clamp(avg(r.filter(x=>x.buyerPower>0).map(x=>robustZ(x.buyerPower,powers)))*16,-100,100);
  const volumeAnomaly=vols.length?clamp(avg(vols.map(v=>clamp((v-1)*35,-50,80))),-100,100):0;
  const priceScore=clamp(avg(pcts.map(v=>robustZ(v,pcts)))*14,-100,100);
  const participation=clamp(active/r.length*100,0,100), liquidity=clamp(percentile(avg(vals),vals),0,100), dispersion=std(pcts);
  const score=clamp(breadth*.34+priceScore*.18+flowScore*.20+powerScore*.12+volumeAnomaly*.08+(advRatio*20)*.08);
  const completeness=(r.filter(x=>x.pct!==0).length*.25+r.filter(x=>x.value>0).length*.20+r.filter(x=>x.volume>0).length*.20+r.filter(x=>x.buyerPower>0).length*.20+r.filter(x=>x.moneyFlow!==0).length*.15)/Math.max(1,r.length);
  const confidence=Math.round(clamp(30+completeness*60+Math.min(12,Math.abs(score)*.12)-Math.min(18,dispersion*1.7),10,97));
  let regime='متعادل'; if(score>=35)regime='قدرت مثبت'; else if(score>=14)regime='تمایل مثبت'; else if(score<=-35)regime='قدرت منفی'; else if(score<=-14)regime='تمایل منفی';
  const risk=Math.round(clamp(30+dispersion*9+Math.abs(score)*.08+Math.max(0,50-participation)*.25,5,96));
  const alerts=[], abnormal=r.filter(x=>x.volumeRatio>=3&&Math.abs(x.pct)>=1).length, strongFlow=r.filter(x=>Math.abs(x.moneyFlow)>0&&Math.abs(robustZ(x.moneyFlow,flows))>=2).length;
  if(abnormal)alerts.push(`${abnormal} نماد با حجم غیرعادی و حرکت قیمت هم‌زمان شناسایی شد`);
  if(strongFlow)alerts.push(`${strongFlow} نماد دارای جریان نقدی پرت نسبت به توزیع بازار است`);
  if(breadth>=35)alerts.push('عرض بازار به شکل معنادار به نفع نمادهای مثبت است');
  if(breadth<=-35)alerts.push('عرض بازار به شکل معنادار به نفع نمادهای منفی است');
  if(dispersion>=4)alerts.push('پراکندگی بازدهی بالاست؛ ریسک نوسان افزایش یافته');
  if(participation<65)alerts.push('پوشش داده کامل نیست؛ اعتماد تحلیل کاهش داده شده است');
  if(!alerts.length)alerts.push('هشدار ساختاری مهمی از داده فعلی استخراج نشد');
  const top=[...r].map(x=>({...x,aiScore:aiScore(x,r)})).sort((a,b)=>b.aiScore-a.aiScore).slice(0,12);
  return {score:Math.round(score),confidence,regime,breadth:+breadth.toFixed(1),liquidity:Math.round(liquidity),risk,participation:Math.round(participation),flow:Math.round(flowScore),up,down,flat,alerts,top,dataQuality:Math.round(completeness*100),evidence:[`عرض بازار ${breadth.toFixed(1)}٪`,`جریان نقدی ${flowScore.toFixed(0)}`,`پراکندگی ${dispersion.toFixed(2)}`,`پوشش داده ${Math.round(completeness*100)}٪`],concentration:Math.round(vals.length?clamp(Math.max(...vals)/(avg(vals)*Math.max(1,vals.length))*100,0,100):0)};
}

function aiScore(x, rows) {
  const pcts=rows.map(q=>num(q.pct)), vals=rows.map(q=>num(q.value)).filter(v=>v>0), vols=rows.map(q=>num(q.volumeRatio)).filter(v=>v>0), powers=rows.map(q=>num(q.buyerPower)).filter(v=>v>0), flows=rows.map(q=>num(q.moneyFlow));
  const trend=clamp(robustZ(num(x.pct),pcts)*16,-32,32), volume=clamp(robustZ(num(x.volumeRatio),vols)*12,-22,26), flow=clamp(robustZ(num(x.moneyFlow),flows)*14,-28,28), power=clamp(robustZ(num(x.buyerPower),powers)*14,-24,24), liquidity=clamp((percentile(num(x.value),vals)-50)*.35,-18,18);
  const range=x.high>x.low&&x.last?clamp(((x.last-x.low)/(x.high-x.low)-.5)*24,-12,12):0;
  return Math.round(clamp(trend+volume+flow+power+liquidity+range,-100,100));
}

function aiSnapshot(rows){const a=aiAnalyzeMarket(rows);aiHistory.push({at:Date.now(),score:a.score,breadth:a.breadth,flow:a.flow,risk:a.risk});if(aiHistory.length>AI_HISTORY_LIMIT)aiHistory.splice(0,aiHistory.length-AI_HISTORY_LIMIT);return a;}

const sourceState = {
  official: { ok: 0, fail: 0, lastOk: null, lastError: null, cooldownUntil: 0 },
  cdn: { ok: 0, fail: 0, lastOk: null, lastError: null, cooldownUntil: 0 },
  custom: { ok: 0, fail: 0, lastOk: null, lastError: null, cooldownUntil: 0 }
};

function auth(req) {
  return !AUTH_KEY || req.get("X-Auth-Key") === AUTH_KEY;
}
function guard(req, res, next) {
  if (!auth(req)) return res.status(401).json({ error: "unauthorized" });
  next();
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function num(v) {
  if (v === null || v === undefined || v === "") return 0;
  if (typeof v === "object" && v !== null && "value" in v) return num(v.value);
  if (typeof v === "string") {
    const cleaned = v.replace(/,/g, "").replace(/٪/g, "").trim();
    if (cleaned === "-" || cleaned === "—") return 0;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : 0;
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}
function pick(o, keys) {
  for (const k of keys) {
    const v = o?.[k];
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return 0;
}
function arr(j, keys = []) {
  if (Array.isArray(j)) return j;
  if (!j || typeof j !== "object") return [];
  for (const k of keys) if (Array.isArray(j?.[k])) return j[k];
  if (Array.isArray(j.Items)) return j.Items;
  if (Array.isArray(j.items)) return j.items;
  return [];
}
function unwrap(j) {
  if (!j || typeof j !== "object") return j;
  const keys = [
    "marketwatch", "marketWatch", "closingPriceInfo", "closingPriceDaily",
    "closingPriceChartData", "bestLimits", "clientType", "clientTypeAllDto",
    "instrumentSearch", "instrumentIdentity", "instrumentInfo", "marketOverview",
    "sectorSummeries", "sectorSummaries", "indexB1", "instEffect", "trade",
    "tradeIntraDay", "msg", "preparedData", "instrumentCalendar"
  ];
  for (const k of keys) if (j[k] !== undefined) return j[k];
  return j;
}

function normalizeRow(v) {
  const insCode = String(pick(v, ["insCode", "InsCode", "instrumentId", "InstrumentId", "inscode"]) || "");
  const symbol = String(pick(v, [
    "instrumentName", "lVal18AFC", "lVal18", "symbol", "ticker", "Symbol",
    "instrument_Name", "InstrumentName"
  ]) || "");
  const name = String(pick(v, [
    "companyNamePersian", "lVal30", "name", "company", "title", "Name",
    "company_Name_Persian"
  ]) || "");

  const last = num(pick(v, ["lastPrice", "pDrCotVal", "pl", "last", "price", "LastPrice", "lastprice"]));
  const close = num(pick(v, ["closingPrice", "pClosing", "pc", "close", "ClosePrice", "closingprice"]));
  const yesterday = num(pick(v, ["yesterdayPrice", "priceYesterday", "py", "previousPrice", "yesterdayprice"]));
  const change = num(pick(v, ["lastPriceChange", "priceChange", "change", "Change", "lastpricechange"])) || (last - yesterday);
  const pct = num(pick(v, ["lastPricePercent", "priceChangePercent", "changePercent", "pct", "percent", "Percent", "lastpricepercent"]))
    || (yesterday ? change / yesterday * 100 : 0);

  return {
    insCode,
    isin: String(pick(v, ["cIsin", "isin", "ISIN", "instrumentIsin"]) || ""),
    symbol, name, last, close, yesterday, change, pct,
    open: num(pick(v, ["openPrice", "pFirst", "pf", "open", "firstPrice", "firstprice"])),
    high: num(pick(v, ["maxPrice", "priceMax", "pMax", "high", "max", "maxprice"])),
    low: num(pick(v, ["minPrice", "priceMin", "pMin", "low", "min", "minprice"])),
    volume: num(pick(v, ["tradeVolume", "qTotTran5J", "volume", "vol", "Volume", "tradevolume", "qTotTran"])),
    value: num(pick(v, ["tradeValue", "qTotCap", "value", "Value", "tradevalue"])),
    trades: num(pick(v, ["tradeCount", "zTotTran", "count", "trades", "tradecount"])),
    baseVolume: num(pick(v, ["baseVolume", "baseVol", "bvol", "basevol"])),
    eps: num(pick(v, ["eps", "EPS", "estimatedEPS"])),
    pe: num(pick(v, ["pe", "PE", "sectorPE"])),
    market: String(pick(v, ["marketname", "marketName", "market", "flow"]) || ""),
    industry: String(pick(v, ["industryname", "industryName", "industry", "industryNamePersian"]) || ""),
    state: String(pick(v, ["statename", "stateName", "state", "stateTitle"]) || ""),
    buy_I_Volume: num(pick(v, ["buy_I_Volume", "buyIVolume"])),
    buy_N_Volume: num(pick(v, ["buy_N_Volume", "buyNVolume"])),
    sell_I_Volume: num(pick(v, ["sell_I_Volume", "sellIVolume"])),
    sell_N_Volume: num(pick(v, ["sell_N_Volume", "sellNVolume"]))
  };
}

function enrich(r) {
  const buyI = num(r.buy_I_Volume), sellI = num(r.sell_I_Volume);
  const buyN = num(r.buy_N_Volume), sellN = num(r.sell_N_Volume);
  const buyVol = buyI + buyN, sellVol = sellI + sellN;
  return {
    ...r,
    buy_I_Volume: buyI, buy_N_Volume: buyN,
    sell_I_Volume: sellI, sell_N_Volume: sellN,
    moneyFlow: buyI - sellI,
    netLegalFlow: buyN - sellN,
    buyerPower: sellI > 0 ? buyI / sellI : (sellVol > 0 ? buyVol / sellVol : 0),
    volumeRatio: r.baseVolume > 0 ? r.volume / r.baseVolume : 0,
    valuePerTrade: r.trades > 0 ? r.value / r.trades : 0
  };
}

async function httpJson(url, headers = {}, timeout = REQUEST_TIMEOUT_MS, options = {}) {
  let last; const attempts=Math.max(1,Number(options.attempts||2));
  for(let attempt=0;attempt<attempts;attempt++){
    const ctrl=new AbortController(), timer=setTimeout(()=>ctrl.abort(),timeout);
    try{
      const res=await fetch(url,{signal:ctrl.signal,headers:{"User-Agent":`IranMarketIndustrial/${VERSION}`,"Accept":"application/json,text/plain,*/*","Referer":"https://www.tsetmc.com/","Origin":"https://www.tsetmc.com","Accept-Encoding":"gzip, deflate, br",...headers}});
      const text=await res.text(); if(!res.ok)throw new Error(`HTTP ${res.status}`); if(!text.trim())throw new Error('empty response');
      try{return JSON.parse(text)}catch{throw new Error(/مسدود|دسترسی شما|General Error/i.test(text)?'source blocked':'non-json response')}
    }catch(e){last=e;if(attempt<attempts-1)await sleep(180*(attempt+1));}finally{clearTimeout(timer)}
  } throw last||new Error('request failed');
}

async function sourceCall(name, fn) {
  const st = sourceState[name];
  if (st.cooldownUntil > Date.now()) throw new Error(`${name} cooldown`);
  const started = Date.now();
  try {
    const data = await fn();
    st.ok++; st.fail = 0; st.lastOk = new Date().toISOString();
    st.lastError = null; st.cooldownUntil = 0;
    return { data, ms: Date.now() - started };
  } catch (e) {
    st.fail++; st.lastError = e.message;
    if (st.fail >= 3) st.cooldownUntil = Date.now() + Math.min(60000, 5000 * st.fail);
    throw e;
  }
}

function normalizeMarketPayload(j) {
  return arr(unwrap(j), ["data", "stocks", "items", "marketwatch"])
    .map(normalizeRow)
    .filter(x => x.insCode && x.symbol && (x.last || x.close));
}

function mergeClientType(rows, clientPayload) {
  const clients = arr(unwrap(clientPayload), ["clientTypeAllDto"]);
  const map = new Map();
  for (const c of clients) {
    const id = String(pick(c, ["insCode", "InsCode", "instrumentId"]) || "");
    if (id) map.set(id, c);
  }
  return rows.map(r => {
    const c = map.get(String(r.insCode));
    return enrich(c ? { ...r, ...normalizeRow(c) } : r);
  });
}

async function fetchOfficial() {
  const u = `${GATEWAY}/InstrumentProvider/api/v1/MarketWatch/MarketWatchCash/fa`;
  return normalizeMarketPayload(await httpJson(u));
}

async function fetchCdn() {
  const u = `${CDN}/api/ClosingPrice/GetMarketWatch?market=0&industrialGroup=&paperTypes[0]=1&paperTypes[1]=2&paperTypes[2]=3&paperTypes[3]=4&paperTypes[4]=5&paperTypes[5]=6&paperTypes[6]=7&paperTypes[7]=8&paperTypes[8]=9&showTraded=true&withBestLimits=false&hEven=0&RefID=0`;
  const [market, client] = await Promise.all([
    httpJson(u),
    cdn("/api/ClientType/GetClientTypeAll", 4500)
  ]);
  return mergeClientType(normalizeMarketPayload(market), client);
}

// Prefer the official cash-market snapshot and enrich it with bulk CDN client-type data.
async function fetchComposite() {
  const [official, cdnResult] = await Promise.allSettled([
    sourceCall("official", fetchOfficial),
    sourceCall("cdn", fetchCdn)
  ]);
  const off = official.status === "fulfilled" ? official.value : null;
  const cdnR = cdnResult.status === "fulfilled" ? cdnResult.value : null;
  if (off && off.data.length >= SOURCE_MIN_ROWS) {
    const flowMap = new Map((cdnR?.data || []).map(x => [String(x.insCode), x]));
    const rows = off.data.map(r => {
      const f = flowMap.get(String(r.insCode));
      return enrich(f ? {...r, buy_I_Volume:f.buy_I_Volume, buy_N_Volume:f.buy_N_Volume,
        sell_I_Volume:f.sell_I_Volume, sell_N_Volume:f.sell_N_Volume} : r);
    });
    return {name:"official+cdn", rows, ms:Math.max(off.ms, cdnR?.ms||0),
      evidence:{officialRows:off.data.length, cdnRows:cdnR?.data.length||0}};
  }
  if (cdnR && cdnR.data.length >= SOURCE_MIN_ROWS)
    return {name:"cdn", rows:cdnR.data, ms:cdnR.ms, evidence:{officialRows:0,cdnRows:cdnR.data.length}};
  const errors=[];
  if (official.status==="rejected") errors.push(`official: ${official.reason?.message||"failed"}`);
  if (cdnResult.status==="rejected") errors.push(`cdn: ${cdnResult.reason?.message||"failed"}`);
  throw new Error(errors.join(" | ") || "no healthy market source");
}

async function fetchCustom() {
  if (!CUSTOM) throw new Error("custom source not configured");
  return normalizeMarketPayload(await httpJson(CUSTOM, API_KEY ? {
    "Authorization": `Bearer ${API_KEY}`, "X-API-Key": API_KEY
  } : {}));
}

function sourceRank(name,rows,ms){if(!rows.length)return-Infinity;const coverage=rows.filter(x=>x.last||x.close).length/rows.length;const fields=rows.reduce((n,x)=>n+(x.value>0?1:0)+(x.volume>0?1:0)+(x.pct!==0?1:0),0)/(rows.length*3);const speed=1/(1+Math.max(0,ms)/2500);const preferred=name==='official'?4:name==='cdn'?3:2;return preferred+coverage*5+fields*4+speed;}

async function fetchMarket(){
  const started=Date.now();
  const candidates=[];
  const jobs=[sourceCall("official",fetchOfficial), sourceCall("cdn",fetchCdn)];
  if(CUSTOM) jobs.push(sourceCall("custom",fetchCustom));
  const results=await Promise.allSettled(jobs);
  for(let i=0;i<results.length;i++){
    const r=results[i];
    if(r.status!=="fulfilled" || !r.value?.data?.length) continue;
    const name=i===0?"official":i===1?"cdn":"custom";
    const rows=r.value.data.map(enrich);
    candidates.push({name,rows,ms:r.value.ms,rank:sourceRank(name,rows,r.value.ms)});
  }
  if(!candidates.length){
    state.error=results.map((r,i)=>r.status==="rejected"?`${i===0?"official":i===1?"cdn":"custom"}: ${r.reason?.message||"failed"}`:null).filter(Boolean).join(" | ")||"no healthy market source";
    state.stale=Boolean(state.updatedAt && Date.now()-Date.parse(state.updatedAt)>STALE_AFTER_MS);
    broadcast(); return false;
  }
  candidates.sort((a,b)=>b.rank-a.rank);
  const best=candidates[0];
  state.rows=best.rows;
  state.source=best.name + (candidates.length>1?`+${candidates.slice(1).map(x=>x.name).join("+")}`:"");
  state.updatedAt=new Date().toISOString();
  state.latencyMs=Math.max(1,Date.now()-started);
  state.error=null; state.stale=false; state.cycle++;
  state.sourceEvidence={
    selected:best.name,
    candidates:candidates.map(x=>({name:x.name,rows:x.rows.length,latencyMs:x.ms,rank:+x.rank.toFixed(2)})),
    targetMs:SOURCE_TARGET_MS
  };
  aiSnapshot(state.rows); broadcast(); return true;
}

function broadcast() {
  const payload = JSON.stringify({
    type: "market",
    rows: state.rows,
    meta: {
      source: state.source, updatedAt: state.updatedAt,
      latencyMs: state.latencyMs, error: state.error, cycle: state.cycle,
    stale: state.stale, sourceEvidence: state.sourceEvidence, serverTime: Date.now()
    }
  });
  for (const ws of clients) if (ws.readyState === WebSocket.OPEN) {
    try {
      if (!ws.subscription?.symbols) ws.send(payload);
      else {
        const selected=state.rows.filter(r=>ws.subscription.symbols.includes(String(r.insCode)) || ws.subscription.symbols.includes(String(r.symbol)));
        ws.send(JSON.stringify({type:"market",rows:selected,meta:{source:state.source,updatedAt:state.updatedAt,latencyMs:state.latencyMs,error:state.error,cycle:state.cycle,stale:state.stale,filtered:true,serverTime:Date.now()}}));
      }
    } catch {}
  }
  for (const client of sseClients) {
    try { client.res.write(`event: market\ndata: ${payload}\n\n`); } catch { sseClients.delete(client); }
  }
}

function cacheGet(key, ttl) {
  const x = cache.get(key);
  return x && Date.now() - x.at < ttl ? x.data : null;
}
function cacheSet(key, data) {
  cache.set(key, { at: Date.now(), data });
  if (cache.size > 300) {
    const oldest = [...cache.entries()].sort((a,b) => a[1].at - b[1].at).slice(0, 50);
    for (const [k] of oldest) cache.delete(k);
  }
  return data;
}
async function cdn(pathname, ttl = 1000) {
  const key = "cdn:" + pathname;
  const cached = cacheGet(key, ttl);
  if (cached !== null) return cached;
  const data = unwrap(await httpJson(CDN + pathname));
  return cacheSet(key, data);
}
async function getJsonWithFallback(paths, ttl = 1000) {
  let last;
  for (const p of paths) {
    try { return await cdn(p, ttl); } catch (e) { last = e; }
  }
  throw last || new Error("request failed");
}

app.get("/api/connection", guard, (req,res)=>{
  const now=Date.now();
  const sources=Object.fromEntries(Object.entries(sourceState).map(([name,v])=>{
    const age=v.lastOk?now-Date.parse(v.lastOk):null;
    const health=v.ok+v.fail===0?0:Math.round((v.ok/Math.max(1,v.ok+v.fail))*100);
    return [name,{ok:v.ok,fail:v.fail,health,lastOk:v.lastOk,ageMs:age,lastError:v.lastError,cooldown:v.cooldownUntil>now}];
  }));
  const fresh=Boolean(state.updatedAt && now-Date.parse(state.updatedAt)<=STALE_AFTER_MS);
  res.json({version:VERSION,online:true,fresh,stale:state.stale,source:state.source,updatedAt:state.updatedAt,ageMs:state.updatedAt?now-Date.parse(state.updatedAt):null,latencyMs:state.latencyMs,rows:state.rows.length,error:state.error,clients:clients.size,sseClients:sseClients.size,sources,sourceEvidence:state.sourceEvidence,requestId:req.requestId});
});

app.get("/api/stream", guard, (req,res)=>{
  if(sseClients.size>=SSE_MAX_CLIENTS) return res.status(503).json({error:"stream capacity reached"});
  res.status(200).set({"Content-Type":"text/event-stream; charset=utf-8","Cache-Control":"no-cache, no-transform","Connection":"keep-alive","X-Accel-Buffering":"no"});
  res.flushHeaders?.();
  const client={res,at:Date.now()}; sseClients.add(client);
  res.write(`event: ready\ndata: ${JSON.stringify({version:VERSION,serverTime:Date.now()})}\n\n`);
  res.write(`event: market\ndata: ${JSON.stringify({type:"market",rows:state.rows,meta:{source:state.source,updatedAt:state.updatedAt,latencyMs:state.latencyMs,error:state.error,cycle:state.cycle,stale:state.stale}})}\n\n`);
  const ping=setInterval(()=>{try{res.write(`event: ping\ndata: ${Date.now()}\n\n`)}catch{}},15000); ping.unref();
  req.on("close",()=>{clearInterval(ping);sseClients.delete(client);});
});

app.get("/api/ready", guard, (req,res)=>{
  const fresh=Boolean(state.updatedAt && Date.now()-Date.parse(state.updatedAt)<=STALE_AFTER_MS);
  const ready=state.rows.length>=SOURCE_MIN_ROWS && fresh && !state.error;
  res.status(ready?200:503).json({ready,version:VERSION,source:state.source,rows:state.rows.length,updatedAt:state.updatedAt,stale:state.stale,error:state.error,requestId:req.requestId});
});

app.get("/api/market", guard, (req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({ rows: state.rows, meta: {
    source: state.source, updatedAt: state.updatedAt,
    latencyMs: state.latencyMs, error: state.error, cycle: state.cycle,
    stale: state.stale, sourceEvidence: state.sourceEvidence
  }});
});

app.get("/api/health", guard, (req, res) => {
  res.json({
    ok:true, version:VERSION, uptimeSec:Math.round((Date.now()-bootAt)/1000),
    source: state.source, updatedAt: state.updatedAt, latencyMs: state.latencyMs,
    items: state.rows.length, error: state.error, stale: state.stale, clients:clients.size, sseClients:sseClients.size,
    memoryMb:Math.round(process.memoryUsage().rss/1024/1024), sourceEvidence: state.sourceEvidence,
    cyclesPerMinute: Math.round((state.cycle / Math.max(1,(Date.now()-bootAt)/60000))*10)/10,
    sources: Object.fromEntries(Object.entries(sourceState).map(([k,v]) => [k, {
      ok:v.ok, fail:v.fail, lastOk:v.lastOk, lastError:v.lastError,
      cooldown:v.cooldownUntil > Date.now()
    }])), requestId:req.requestId
  });
});

app.get("/api/indices", guard, async (req,res) => {
  try {
    const [b, f] = await Promise.all([
      cdn("/api/Index/GetIndexB1LastAll/SelectedIndexes/1", 1500),
      cdn("/api/Index/GetIndexB1LastAll/SelectedIndexes/2", 1500)
    ]);
    res.json({ bourse: arr(b, ["indexB1"]), farabourse: arr(f, ["indexB1"]) });
  } catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/overview", guard, async (req,res) => {
  try {
    const [a,b] = await Promise.all([
      cdn("/api/MarketData/GetMarketOverview/1", 1200),
      cdn("/api/MarketData/GetSectorsSummary", 3000)
    ]);
    res.json({ overview: a, sectors: arr(b, ["sectorSummeries","sectorSummaries"]) });
  } catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/search", guard, async (req,res) => {
  const q = String(req.query.q || "").trim();
  if (!q) return res.json([]);
  if (!/^[\u0600-\u06FF\w ._-]{1,80}$/u.test(q)) return res.status(400).json({error:"invalid query"});
  try {
    const j = await cdn(`/api/Instrument/GetInstrumentSearch/${encodeURIComponent(q)}`, 5000);
    res.json(arr(j, ["instrumentSearch"]).slice(0, 20).map(x => ({
      insCode: String(x.insCode || ""),
      symbol: x.lVal18AFC || x.lVal18 || "",
      name: x.lVal30 || "",
      flow: x.flow || "",
      isin: x.cIsin || ""
    })));
  } catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/symbol/:insCode", guard, async (req,res) => {
  const id = String(req.params.insCode);
  if (!/^\d{8,25}$/.test(id)) return res.status(400).json({error:"invalid insCode"});
  try {
    const results = await Promise.allSettled([
      cdn(`/api/ClosingPrice/GetClosingPriceInfo/${id}`, 800),
      cdn(`/api/Instrument/GetInstrumentIdentity/${id}`, 10000),
      cdn(`/api/BestLimits/${id}`, 700),
      cdn(`/api/ClientType/GetClientType/${id}/1/0`, 1200),
      cdn(`/api/ClosingPrice/GetClosingPriceDailyList/${id}/0`, 15000),
      cdn(`/api/Instrument/GetInstrumentInfo/${id}`, 10000)
    ]);
    const val = i => results[i].status === "fulfilled" ? results[i].value : null;
    res.json({
      info: val(0), identity: val(1), limits: val(2), clientType: val(3),
      history: arr(val(4), ["closingPriceDaily"]),
      instrumentInfo: val(5)
    });
  } catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/symbol/:insCode/chart", guard, async (req,res) => {
  const id = String(req.params.insCode);
  if (!/^\d{8,25}$/.test(id)) return res.status(400).json({error:"invalid insCode"});
  try {
    const j = await cdn(`/api/ClosingPrice/GetChartData/${id}/D`, 15000);
    res.json(arr(j, ["closingPriceChartData"]));
  } catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/messages", guard, async (req,res) => {
  try { res.json(arr(await cdn(`/api/Msg/GetMsgByFlow/0/30`, 3000), ["msg"])); }
  catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/codal", guard, async (req,res) => {
  try { res.json(arr(await cdn(`/api/Codal/GetPreparedData/30`, 10000), ["preparedData"])); }
  catch(e) { res.status(502).json({error:e.message}); }
});

app.get("/api/trade-top/:category/:flow/:top", guard, async (req,res) => {
  const {category,flow,top}=req.params;
  if(!/^[A-Za-z]+$/.test(category)||!/^[0-9]{1,2}$/.test(flow)||!/^[0-9]{1,3}$/.test(top)) return res.status(400).json({error:"invalid parameters"});
  try { res.json(arr(await cdn(`/api/ClosingPrice/GetTradeTop/${category}/${flow}/${top}`,5000),["tradeTop"])); }
  catch(e){res.status(502).json({error:e.message});}
});
app.get("/api/shareholders/:insCode", guard, async (req,res) => {
  const id=String(req.params.insCode); if(!/^[0-9]{8,25}$/.test(id)) return res.status(400).json({error:"invalid insCode"});
  try { res.json(arr(await cdn(`/api/Shareholder/GetInstrumentShareHolderLast/${id}`,10000),["shareHolder"])); }
  catch(e){res.status(502).json({error:e.message});}
});
app.get("/api/symbol/:insCode/client-history", guard, async (req,res) => {
  const id=String(req.params.insCode); if(!/^[0-9]{8,25}$/.test(id)) return res.status(400).json({error:"invalid insCode"});
  try { res.json(arr(await cdn(`/api/ClientType/GetClientTypeHistory/${id}`,15000),["clientType"])); }
  catch(e){res.status(502).json({error:e.message});}
});

app.get("/api/ai/market", guard, (req,res) => {
  const analysis=aiAnalyzeMarket(state.rows);
  const prev=aiHistory.length>1 ? aiHistory[aiHistory.length-2] : null;
  analysis.momentum=prev ? Math.round((analysis.score-prev.score)*0.7 + (analysis.breadth-(prev.breadth||0))*0.3) : 0;
  analysis.history=aiHistory.slice(-60);
  res.json({updatedAt:state.updatedAt,source:state.source,...analysis});
});

app.get("/api/ai/symbol/:insCode", guard, (req,res) => {
  const id=String(req.params.insCode);
  const x=state.rows.find(r=>String(r.insCode)===id);
  if(!x) return res.status(404).json({error:"symbol not in live market"});
  const score=aiScore(x,state.rows);
  const reasons=[];
  if(x.pct>=2) reasons.push('شتاب قیمت مثبت'); else if(x.pct<=-2) reasons.push('شتاب قیمت منفی');
  if(x.volumeRatio>=2) reasons.push('حجم بالاتر از مبنا');
  if(x.buyerPower>=1.5) reasons.push('قدرت خریدار بالاتر است'); else if(x.buyerPower>0&&x.buyerPower<=0.7) reasons.push('قدرت فروشنده بالاتر است');
  if(x.moneyFlow>0) reasons.push('جریان نقدی مثبت'); else if(x.moneyFlow<0) reasons.push('جریان نقدی منفی');
  if(x.high>x.low&&x.last){const pos=(x.last-x.low)/(x.high-x.low);if(pos>=.8)reasons.push('قیمت نزدیک سقف روز');else if(pos<=.2)reasons.push('قیمت نزدیک کف روز');}
  let label='خنثی'; if(score>=55)label='قدرت بالا'; else if(score>=25)label='تمایل مثبت'; else if(score<=-55)label='فشار بالا'; else if(score<=-25)label='تمایل منفی';
  const confidence=Math.round(clamp(35+Math.abs(score)*.35+reasons.length*7,20,96));
  res.json({insCode:id,symbol:x.symbol,score,label,confidence,reasons,evidence:reasons.length,metrics:{pct:x.pct,volumeRatio:x.volumeRatio,buyerPower:x.buyerPower,moneyFlow:x.moneyFlow,value:x.value}});
});

app.use((req,res)=>{
  res.status(404).json({error:"not found",requestId:req.requestId});
});

app.use((err,req,res,next)=>{
  console.error("request error",req.requestId,err);
  if(res.headersSent) return next(err);
  res.status(500).json({error:"internal server error",requestId:req.requestId});
});

function wsAuthorized(req) {
  if (!AUTH_KEY) return true;
  const token = new URL(req.url, "http://localhost").searchParams.get("key");
  return token === AUTH_KEY;
}

wss.on("connection", (ws, req) => {
  if (clients.size >= MAX_WS_CLIENTS) { ws.close(1013, "server busy"); return; }
  if (!wsAuthorized(req)) {
    ws.close(4001, "unauthorized");
    return;
  }
  clients.add(ws);
  ws.isAlive = true;
  ws.subscription = { symbols: null };
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", raw => {
    try {
      const msg=JSON.parse(raw.toString());
      if(msg?.type!=="subscribe") return;
      const symbols=Array.isArray(msg.symbols)?msg.symbols.map(String).filter(Boolean).slice(0,200):null;
      ws.subscription={symbols};
      ws.send(JSON.stringify({type:"subscription",symbols,count:symbols?symbols.length:0,serverTime:Date.now()}));
    } catch {}
  });
  ws.send(JSON.stringify({ type:"market", rows:state.rows, meta:{
    source:state.source, updatedAt:state.updatedAt, latencyMs:state.latencyMs,
    error:state.error, cycle:state.cycle, serverTime:Date.now()
  }}));
  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => clients.delete(ws));
});

const wsHeartbeat = setInterval(() => {
  for (const ws of clients) {
    if (ws.isAlive === false) { clients.delete(ws); try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30000);
wsHeartbeat.unref();

server.on("error", err=>console.error("server error",err));

async function loop(){let failures=0;while(true){const started=Date.now();try{const ok=await fetchMarket();failures=ok?0:Math.min(failures+1,6)}catch(e){state.error=e.message;broadcast();failures=Math.min(failures+1,6)}const base=failures===0?POLL_MS:Math.min(30000,POLL_MS*(1+failures*.8));await sleep(Math.max(250,base-(Date.now()-started)));}}

function shutdown(signal) {
  console.log(`Shutting down on ${signal}...`);
  clearInterval(wsHeartbeat);
  for (const ws of clients) try { ws.close(1001, "server shutdown"); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("unhandledRejection", e => console.error("unhandledRejection:", e));
process.on("uncaughtException", e => console.error("uncaughtException:", e));

server.listen(PORT, () => {
  console.log(`Iran Market Live Pro: http://localhost:${PORT}`);
  loop().catch(err => console.error("market loop:", err));
});
