/* Camada de dados: guarda tudo numa tabela "docs" do Supabase (col, id, data)
   e oferece a mesma interface que o site já usava (collection/doc/set/update/delete/get/onSnapshot). */
(function(){
"use strict";
const cfg=window.APP_CONFIG;
const sb=window.supabase.createClient(cfg.SUPABASE_URL,cfg.SUPABASE_KEY,{auth:{persistSession:true,autoRefreshToken:true}});
window.SB=sb;
const isObj=v=>v&&typeof v==="object"&&!Array.isArray(v);
function merge(a,b){const o=isObj(a)?{...a}:{};for(const k of Object.keys(b||{})){const v=b[k];
  if(isObj(v)&&v.__delete__===true){delete o[k];continue}
  o[k]=isObj(v)&&isObj(o[k])?merge(o[k],v):(isObj(v)?merge({},v):v)}return o}
function strip(v){if(!isObj(v))return v;const o={};for(const k of Object.keys(v)){const x=v[k];if(isObj(x)&&x.__delete__===true)continue;o[k]=isObj(x)?strip(x):x}return o}
function err(e){const x=new Error((e&&e.message)||"erro");x.code=e&&(e.code==="42501"||/row-level security/i.test(e.message||""))?"invalid_argument":(e&&e.code)||"unknown";return x}
const snapDoc=(id,data)=>({id,exists:data!=null,data:()=>data==null?undefined:data});
const snapCol=rows=>({docs:rows.map(r=>snapDoc(r.id,r.data)),size:rows.length,empty:!rows.length});
async function selectAll(q){const out=[];const step=500;for(let from=0;;from+=step){const {data,error}=await q().range(from,from+step-1);if(error)throw err(error);out.push(...data);if(data.length<step)break}return out}

/* tempo real: um canal por coleção, com quem estiver ouvindo */
const listeners={};const channels={};
function listen(col,fn){(listeners[col]=listeners[col]||new Set()).add(fn);
  if(!channels[col]){channels[col]=sb.channel("docs-"+col)
    .on("postgres_changes",{event:"INSERT",schema:"public",table:"docs",filter:"col=eq."+col},p=>emit(col,p.new.id,p.new.data))
    .on("postgres_changes",{event:"UPDATE",schema:"public",table:"docs",filter:"col=eq."+col},p=>emit(col,p.new.id,p.new.data))
    .on("postgres_changes",{event:"DELETE",schema:"public",table:"docs"},p=>{if(p.old&&p.old.col===col)emit(col,p.old.id,null)})
    .subscribe()}
  return()=>listeners[col].delete(fn)}
function emit(col,id,data){(listeners[col]||[]).forEach(f=>{try{f(id,data)}catch(e){console.error(e)}})}

function docRef(col,id){return{
  id,
  async get(){const {data,error}=await sb.from("docs").select("data").eq("col",col).eq("id",id).maybeSingle();if(error)throw err(error);return snapDoc(id,data?data.data:null)},
  async set(d){const {error}=await sb.from("docs").upsert({col,id,data:strip(d)});if(error)throw err(error);emit(col,id,strip(d))},
  async update(d){const {data,error}=await sb.from("docs").select("data").eq("col",col).eq("id",id).maybeSingle();if(error)throw err(error);
    if(!data){const x=new Error("not found");x.code="not_found";throw x}
    const nd=merge(data.data,d);const r=await sb.from("docs").update({data:nd}).eq("col",col).eq("id",id);if(r.error)throw err(r.error);emit(col,id,nd)},
  async delete(){const {error}=await sb.from("docs").delete().eq("col",col).eq("id",id);if(error)throw err(error);emit(col,id,null)},
  onSnapshot(cb,onErr){let alive=true;
    this.get().then(s=>alive&&cb(s)).catch(e=>onErr&&onErr(e));
    const off=listen(col,(i,d)=>{if(alive&&i===id)cb(snapDoc(id,d))});
    return()=>{alive=false;off()}}
}}
function query(col,filters){return{
  where(f,op,v){return query(col,[...filters,[f,op,v]])},
  async get(){const rows=await selectAll(()=>{let q=sb.from("docs").select("id,data").eq("col",col).order("id");
      for(const [f,op,v] of filters){const p="data->>"+f;const s=v==null?v:String(v);
        if(op==="=="||op==="eq")q=q.eq(p,s);else if(op==="!="||op==="ne")q=q.neq(p,s);else throw new Error("filtro não suportado: "+op)}
      return q});
    return snapCol(rows)},
  doc(id){return docRef(col,id)},
  async add(d){const id=Date.now().toString(36)+Math.random().toString(36).slice(2,8);await docRef(col,id).set(d);return docRef(col,id)},
  onSnapshot(cb,onErr){let alive=true;const cache=new Map();
    this.get().then(s=>{if(!alive)return;s.docs.forEach(d=>cache.set(d.id,d.data()));cb(snapCol([...cache].map(([id,data])=>({id,data}))))}).catch(e=>onErr&&onErr(e));
    const off=listen(col,(id,d)=>{if(!alive)return;if(d==null)cache.delete(id);else cache.set(id,d);cb(snapCol([...cache].map(([i,data])=>({id:i,data}))))});
    return()=>{alive=false;off()}}
}}
window.AppDB={
  collection:c=>query(c,[]),
  doc:p=>{const [c,...r]=p.split("/");return docRef(c,r.join("/"))}
};
})();
