/* Permite instalar o site como aplicativo. Sempre busca a versão mais nova na internet;
   só usa a cópia guardada se estiver sem conexão. Os dados (Supabase) nunca ficam guardados aqui. */
const CACHE="descontos-v3";
const ARQ=["./","./index.html","./config.js","./db.js","./manifest.webmanifest","./icon-192.png","./icon-512.png","./logo-uniggel.png","./logo-uniggel-branco.png","./fundo-login.jpg"];
self.addEventListener("install",e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ARQ)).then(()=>self.skipWaiting()))});
self.addEventListener("activate",e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim()))});
self.addEventListener("fetch",e=>{
  const u=new URL(e.request.url);
  if(e.request.method!=="GET"||u.origin!==location.origin)return;
  e.respondWith(fetch(e.request).then(r=>{const c=r.clone();caches.open(CACHE).then(x=>x.put(e.request,c));return r}).catch(()=>caches.match(e.request)));
});
