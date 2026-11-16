/* Dustline: a small Realtime Database client over plain HTTPS.
   The Firebase SDK talks to the database over a WebSocket on a second server ("s-gke-…firebaseio.com"), and some
   networks and browsers block that, which left multiplayer stuck offline. This client uses only the database's REST
   API: normal requests to write and read, and one streaming request (Server-Sent Events) per listened place to hear
   changes. It offers just the part of the SDK's ref() API that claude-shim.js uses. */
window.DustlineRestDB=function(dbURL,getToken){
  'use strict';
  var BASE=String(dbURL).replace(/\/+$/,'')+'/';
  var lastTok='',skewCbs=[],skew=0,streams={};
  function parts(p){return String(p||'').split('/').filter(Boolean);}
  function clone(v){return v==null?null:JSON.parse(JSON.stringify(v));}
  function tok(){return Promise.resolve(getToken()).then(function(t){lastTok=t||'';return lastTok;});}
  function url(path,t,q){return BASE+parts(path).map(encodeURIComponent).join('/')+'.json?auth='+encodeURIComponent(t)+(q||'');}
  function req(method,path,body){
    var sent=Date.now();
    return tok().then(function(t){
      return fetch(url(path,t),{method:method,body:body===undefined?undefined:JSON.stringify(body),cache:'no-store'});
    }).then(function(r){
      if(!r.ok)return r.text().then(function(x){var e=new Error('database '+r.status+' '+x.slice(0,120));e.status=r.status;throw e;});
      return r.json().then(function(j){
        // learn the server clock from any write that stored a server timestamp
        if(body&&typeof body==='object'&&j&&typeof j==='object'&&typeof j.t==='number'&&body.t&&body.t['.sv']){
          var mid=(sent+Date.now())/2;skew=j.t-mid;skewCbs.forEach(function(f){try{f(skew);}catch(e){}});
        }
        return j;
      });
    });
  }
  function getAt(tree,ps){var n=tree;for(var i=0;i<ps.length;i++){if(!n||typeof n!=='object')return null;n=n[ps[i]];}return n===undefined?null:n;}
  function setIn(holder,ps,v){ // holder = {root:...}
    if(!ps.length){holder.root=v;return;}
    if(!holder.root||typeof holder.root!=='object')holder.root={};
    var n=holder.root;for(var i=0;i<ps.length-1;i++){if(!n[ps[i]]||typeof n[ps[i]]!=='object')n[ps[i]]={};n=n[ps[i]];}
    if(v===null||v===undefined)delete n[ps[ps.length-1]];else n[ps[ps.length-1]]=v;
  }
  function Snap(key,val,path){this.key=key;this._v=val;this.ref=new Ref(path);}
  Snap.prototype.val=function(){return clone(this._v);};
  Snap.prototype.exists=function(){return this._v!==null&&this._v!==undefined;};
  Snap.prototype.forEach=function(cb){var v=this._v,p=this.ref.path;if(v&&typeof v==='object')Object.keys(v).sort().forEach(function(k){cb(new Snap(k,v[k],p+'/'+k));});};

  // one stream per place (path + query), shared by every listener on it
  function Stream(path,q){
    this.path=path;this.q=q||'';this.holder={root:null};this.got=false;this.ls=[];this.es=null;this.dead=false;this.retry=0;
    this.open();
  }
  Stream.prototype.open=function(){
    var self=this;if(self.dead)return;
    tok().then(function(t){
      if(self.dead)return;
      var es=self.es=new EventSource(url(self.path,t,self.q));
      es.addEventListener('put',function(e){self.retry=0;var m=JSON.parse(e.data);setIn(self.holder,parts(m.path),clone(m.data));
        if(!parts(m.path).length)self.got=true;self.fire();});
      es.addEventListener('patch',function(e){var m=JSON.parse(e.data),ps=parts(m.path);
        Object.keys(m.data||{}).forEach(function(k){setIn(self.holder,ps.concat(parts(k)),clone(m.data[k]));});self.fire();});
      es.addEventListener('auth_revoked',function(){self.reopen(0);});
      es.addEventListener('cancel',function(){self.reopen(5000);});
      es.onerror=function(){if(es.readyState===2)self.reopen(Math.min(15000,1000*Math.pow(2,self.retry++)));};
    }).catch(function(){self.reopen(3000);});
  };
  Stream.prototype.reopen=function(ms){var self=this;if(self.es){try{self.es.close();}catch(e){}self.es=null;}if(self.dead)return;setTimeout(function(){self.open();},ms);};
  Stream.prototype.close=function(){this.dead=true;if(this.es){try{this.es.close();}catch(e){}}this.es=null;};
  Stream.prototype.fire=function(){
    if(!this.got)return;
    var cur=this.holder.root,self=this;
    this.ls.slice().forEach(function(L){self.deliver(L,cur);});
  };
  Stream.prototype.deliver=function(L,cur){
    var key=parts(this.path).pop()||null;
    if(L.type==='value'){var j=JSON.stringify(cur);if(j===L.last)return;L.last=j;try{L.cb(new Snap(key,cur,this.path));}catch(e){}return;}
    var kids=cur&&typeof cur==='object'?cur:{},prev=L.prev||{},now={},p=this.path;
    Object.keys(kids).forEach(function(k){now[k]=JSON.stringify(kids[k]);});
    if(L.type==='child_added')Object.keys(now).sort().forEach(function(k){if(!(k in prev)){try{L.cb(new Snap(k,kids[k],p+'/'+k));}catch(e){}}});
    if(L.type==='child_changed')Object.keys(now).forEach(function(k){if(k in prev&&prev[k]!==now[k]){try{L.cb(new Snap(k,kids[k],p+'/'+k));}catch(e){}}});
    if(L.type==='child_removed')Object.keys(prev).forEach(function(k){if(!(k in now)){try{L.cb(new Snap(k,null,p+'/'+k));}catch(e){}}});
    L.prev=now;
  };

  function Ref(path,q){this.path=parts(path).join('/');this.key=parts(path).pop()||null;this.q=q||null;}
  Ref.prototype.qs=function(){return this.q?'&orderBy='+encodeURIComponent('"'+this.q.child+'"')+'&startAt='+encodeURIComponent(JSON.stringify(this.q.start)):'';};
  Ref.prototype.child=function(k){return new Ref(this.path+'/'+k);};
  var pushN=0;
  Ref.prototype.push=function(){return new Ref(this.path+'/'+Date.now().toString(36)+(pushN++%1296).toString(36)+Math.random().toString(36).slice(2,6));};
  Ref.prototype.set=function(v){return req('PUT',this.path,v==null?null:v).then(function(){});};
  Ref.prototype.update=function(o){return req('PATCH',this.path,o).then(function(){});};
  Ref.prototype.remove=function(){return req('DELETE',this.path).then(function(){});};
  Ref.prototype.once=function(type,cb){
    var self=this;
    return tok().then(function(t){return fetch(url(self.path,t,self.qs()),{cache:'no-store'});})
      .then(function(r){if(!r.ok)throw new Error('database '+r.status);return r.json();})
      .then(function(v){var s=new Snap(self.key,v,self.path);if(cb)cb(s);return s;});
  };
  Ref.prototype.on=function(type,cb){
    if(this.path==='.info/serverTimeOffset'){skewCbs.push(function(s){cb(new Snap('serverTimeOffset',s,'.info'));});cb(new Snap('serverTimeOffset',skew,'.info'));return cb;}
    var id=this.path+'?'+this.qs(),st=streams[id]||(streams[id]=new Stream(this.path,this.qs()));
    var L={type:type,cb:cb,last:undefined,prev:undefined};
    st.ls.push(L);if(st.got)st.deliver(L,st.holder.root);
    return cb;
  };
  Ref.prototype.off=function(type,cb){
    var self=this;
    Object.keys(streams).forEach(function(id){var st=streams[id];if(st.path!==self.path)return;
      st.ls=st.ls.filter(function(L){return (type&&L.type!==type)||(cb&&L.cb!==cb);});
      if(!st.ls.length){st.close();delete streams[id];}});
  };
  Ref.prototype.orderByChild=function(c){return new Ref(this.path,{child:c,start:null});};
  Ref.prototype.startAt=function(v){this.q.start=v;return this;};
  var onLeave=[];
  Ref.prototype.onDisconnect=function(){var p=this.path;return {remove:function(){if(onLeave.indexOf(p)<0)onLeave.push(p);return Promise.resolve();}};};
  // the REST API has no "remove when I disconnect": do it as the page closes (others also drop silent players after 90s)
  addEventListener('pagehide',function(){
    if(!lastTok)return;
    onLeave.forEach(function(p){try{fetch(url(p,lastTok),{method:'DELETE',keepalive:true});}catch(e){}});
  });
  return {ref:function(p){return new Ref(p);}};
};
