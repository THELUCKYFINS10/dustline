/* Dustline website shim.
   The game was written for claude.ai, where the page asks the host for three tools with claude.use(name):
   'user' (who is playing), 'db' (saved data) and 'room' (live multiplayer). On the website there is no claude.ai
   host, so this file provides the same three tools, backed by Firebase:
   - user: Firebase Authentication (an anonymous account per browser, optionally linked to Google)
   - db:   documents stored as JSON in the Realtime Database under /docs/<path>
   - room: live rooms in the Realtime Database under /rooms/<name> (presence per open tab, plus short-lived events)
   The game code itself runs unchanged; the build script loads this file before the game. */
(function(){
  'use strict';
  var CFG=window.DUSTLINE_FIREBASE;
  if(!CFG||!window.firebase){console.warn('Dustline: Firebase config missing, multiplayer and cloud saves are off');return;}
  var app=firebase.initializeApp(CFG);
  var auth=firebase.auth(),rtdb=firebase.database();
  var TS=firebase.database.ServerValue.TIMESTAMP;
  var PEER=Math.random().toString(36).slice(2,10)+Date.now().toString(36).slice(-4);   // this tab
  var clockSkew=0;rtdb.ref('.info/serverTimeOffset').on('value',function(s){clockSkew=s.val()||0;});
  function now(){return Date.now()+clockSkew;}

  // ---------- sign-in: every visitor gets an account straight away (anonymous), which they can link to Google ----------
  var ready=new Promise(function(res){
    var done=false;
    auth.onAuthStateChanged(function(u){
      if(u){if(!done){done=true;res(u);}return;}
      auth.signInAnonymously().catch(function(e){console.warn('Dustline: sign-in failed',e);if(!done){done=true;res(null);}});
    });
    setTimeout(function(){if(!done){done=true;res(auth.currentUser||null);}},12000);
  });
  function guestName(uid){return 'Guest-'+String(uid||'').slice(0,4).toUpperCase();}
  function displayName(u){return u?(u.displayName||guestName(u.uid)):'Guest';}

  // ---------- user ----------
  var userNs={
    me:function(){return ready.then(function(u){if(!u)return null;
      rtdb.ref('profiles/'+u.uid).update({name:displayName(u),t:TS}).catch(function(){});
      return {id:u.uid,name:displayName(u),avatarUrl:u.photoURL||'',anonymous:u.isAnonymous};});},
    id:function(){return ready.then(function(u){return u?u.uid:null;});},
    profiles:function(ids){
      return ready.then(function(){
        var out={};
        return Promise.all((ids||[]).slice(0,50).map(function(id){
          if(typeof id!=='string'||!id)return null;
          return rtdb.ref('profiles/'+id).once('value').then(function(s){var v=s.val();if(v&&v.name)out[id]={name:String(v.name).slice(0,40)};}).catch(function(){});
        })).then(function(){return out;});
      });
    }
  };

  // ---------- db: documents at /docs/<path>, stored as a JSON string so any key works ----------
  function cleanPath(p){return String(p||'').split('/').filter(Boolean).map(function(s){return s.replace(/[.#$\[\]]/g,'_');}).join('/');}
  function snap(id,v){var data=null;if(v&&typeof v.j==='string'){try{data=JSON.parse(v.j);}catch(e){}}
    return {id:id,exists:data!==null,data:function(){return data;}};}
  function docRef(path){
    var p=cleanPath(path),ref=rtdb.ref('docs/'+p),id=p.split('/').pop();
    return {
      id:id,path:p,
      get:function(){return ready.then(function(){return ref.once('value');}).then(function(s){return snap(id,s.val());});},
      set:function(obj){return ready.then(function(u){return ref.set({j:JSON.stringify(obj==null?null:obj),uid:u?u.uid:'',t:TS});});},
      update:function(obj){var self=this;return self.get().then(function(s){var cur=s.exists&&s.data()&&typeof s.data()==='object'?s.data():{};return self.set(Object.assign({},cur,obj));});},
      delete:function(){return ready.then(function(){return ref.remove();});},
      onSnapshot:function(fn){var h=function(s){try{fn(snap(id,s.val()));}catch(e){}};ready.then(function(){ref.on('value',h);});return function(){ref.off('value',h);};}
    };
  }
  function collRef(path){
    var p=cleanPath(path),ref=rtdb.ref('docs/'+p);
    function list(s){var docs=[];s.forEach(function(c){docs.push(snap(c.key,c.val()));});docs.forEach(function(d){d.ref=docRef(p+'/'+d.id);});return {docs:docs,size:docs.length,empty:!docs.length,forEach:function(f){docs.forEach(f);}};}
    return {
      doc:function(k){return docRef(p+'/'+k);},
      get:function(){return ready.then(function(){return ref.once('value');}).then(list);},
      onSnapshot:function(fn){var h=function(s){try{fn(list(s));}catch(e){}};ready.then(function(){ref.on('value',h);});return function(){ref.off('value',h);};}
    };
  }
  var dbNs={doc:docRef,collection:collRef};

  // ---------- room: live presence and events ----------
  // presence(patch) merges into this tab's state (a null value clears that field); each field travels as its own
  // JSON string, and only fields that changed are sent, at most about 12 times a second
  function makeRoom(name,lobby){
    var key=String(name).replace(/[.#$\[\]\/]/g,'_').slice(0,64)||'lobby';
    var base=rtdb.ref('rooms/'+key),peersRef=base.child('peers'),evRef=base.child('ev'),meRef=peersRef.child(PEER);
    var mine={},sentStr={},pending=null,flushT=0,lastFlush=0,joined=false,left=false;
    var peerMap={},peerList=[],listDirty=true,peerCbs=[],topicCbs={},joinedAt=0;
    var myUid='',isGuest=true;
    function rebuild(){
      if(!listDirty)return peerList;
      var cut=now()-90000;peerList=Object.keys(peerMap).filter(function(k){var e=peerMap[k];return k===PEER||!e.t||e.t>cut;}).map(function(k){var e=peerMap[k];return {peer:k,by:e.by,guest:e.guest,isMe:e.by===myUid,sameTab:k===PEER,presence:e.presence};});
      listDirty=false;return peerList;
    }
    var notifyQ=false;
    function notify(){if(notifyQ)return;notifyQ=true;Promise.resolve().then(function(){notifyQ=false;var l=rebuild();peerCbs.slice().forEach(function(f){try{f(l);}catch(e){}});});}
    function readPeer(s){
      var v=s.val()||{},k=s.key,e=peerMap[k]||{raw:{},presence:{}},changed=!peerMap[k];
      var f=v.f||{},ns={};if(v.t&&v.t!==e.t){e.t=v.t;listDirty=true;}
      Object.keys(f).forEach(function(fk){if(e.raw[fk]===f[fk]){ns[fk]=e.presence[fk];return;}changed=true;try{ns[fk]=JSON.parse(f[fk]);}catch(err){}});
      Object.keys(e.raw).forEach(function(fk){if(!(fk in f))changed=true;});
      if(!changed){if(listDirty)notify();return;}
      e.raw=Object.assign({},f);e.presence=ns;e.by=v.by||'';e.guest=!!v.g;
      peerMap[k]=e;listDirty=true;notify();
    }
    function flush(){
      flushT=0;if(!pending||left)return;
      var patch=pending;pending=null;lastFlush=performance.now();
      var up={},n=0;
      Object.keys(patch).forEach(function(fk){
        var v=patch[fk],safe=fk.replace(/[.#$\[\]\/]/g,'_');
        if(v===null||v===undefined){if(safe in sentStr){up['f/'+safe]=null;delete sentStr[safe];n++;}return;}
        var str=JSON.stringify(v);if(str.length>12000)return;
        if(sentStr[safe]===str)return;sentStr[safe]=str;up['f/'+safe]=str;n++;
      });
      if(!n)return;
      up.t=TS;
      meRef.update(up).catch(function(){});
    }
    function schedule(){
      if(flushT)return;
      var wait=Math.max(0,80-(performance.now()-lastFlush));
      flushT=setTimeout(flush,wait);
    }
    var room={
      presence:function(patch){
        if(!patch||typeof patch!=='object')return Promise.resolve();
        Object.keys(patch).forEach(function(k){mine[k]=patch[k];});
        pending=Object.assign(pending||{},patch);
        if(joined)schedule();
        return Promise.resolve();
      },
      peers:function(){return rebuild();},
      onPeers:function(fn){peerCbs.push(fn);if(joined)Promise.resolve().then(function(){try{fn(rebuild());}catch(e){}});return function(){peerCbs=peerCbs.filter(function(f){return f!==fn;});};},
      emit:function(topic,data){
        if(left)return Promise.resolve();
        var str;try{str=JSON.stringify(data===undefined?null:data);}catch(e){return Promise.reject(e);}
        if(str.length>7000)return Promise.reject(new Error('message too large'));
        var r=evRef.push();
        return r.set({topic:String(topic).slice(0,40),d:str,from:PEER,by:myUid,t:TS}).then(function(){setTimeout(function(){r.remove().catch(function(){});},8000);});
      },
      on:function(topic,fn){(topicCbs[topic]=topicCbs[topic]||[]).push(fn);return function(){topicCbs[topic]=(topicCbs[topic]||[]).filter(function(f){return f!==fn;});};},
      leave:function(){
        if(left)return Promise.resolve();left=true;
        peersRef.off();evRef.off();clearTimeout(flushT);
        return meRef.remove().catch(function(){});
      }
    };
    if(lobby)room.join=function(n){var r=makeRoom(n,false);return r._start().then(function(){return r;});};
    room._start=function(){
      return ready.then(function(u){
        if(!u)throw new Error('not signed in');
        myUid=u.uid;isGuest=u.isAnonymous;
        meRef.onDisconnect().remove();
        var init={by:myUid,g:isGuest?1:0,t:TS,f:{}};
        Object.keys(mine).forEach(function(k){var v=mine[k];if(v===null||v===undefined)return;var safe=k.replace(/[.#$\[\]\/]/g,'_'),s=JSON.stringify(v);if(s.length>12000)return;sentStr[safe]=s;init.f[safe]=s;});
        pending=null;
        return meRef.set(init).then(function(){
          joined=true;joinedAt=now();
          peersRef.on('child_added',readPeer);peersRef.on('child_changed',readPeer);
          peersRef.on('child_removed',function(s){if(peerMap[s.key]){delete peerMap[s.key];listDirty=true;notify();}});
          evRef.orderByChild('t').startAt(joinedAt-2000).on('child_added',function(s){
            var v=s.val();if(!v||typeof v.topic!=='string')return;
            var cbs=topicCbs[v.topic];if(!cbs||!cbs.length)return;
            var data=null;try{data=JSON.parse(v.d);}catch(e){}
            var msg={topic:v.topic,data:data,peer:v.from,by:v.by,isMe:v.by===myUid,sameTab:v.from===PEER,t:v.t};
            cbs.slice().forEach(function(f){try{f(msg);}catch(e){}});
          });
          // stay listed: refresh our timestamp now and then, and drop peers that stopped refreshing (closed laptops)
          setInterval(function(){if(!left)meRef.update({t:TS}).catch(function(){});},20000);
          setInterval(function(){var cut=now()-75000;peersRef.once('value',function(s){s.forEach(function(c){var v=c.val();if(v&&v.t&&v.t<cut&&c.key!==PEER&&v.by===myUid)c.ref.remove().catch(function(){});});});},60000);
          if(pending)schedule();
          notify();
        });
      });
    };
    return room;
  }
  var lobby=null,lobbyP=null;
  function roomNs(){
    if(lobbyP)return lobbyP;
    lobby=makeRoom('lobby',true);
    lobbyP=lobby._start().then(function(){return lobby;},function(e){console.warn('Dustline: multiplayer unavailable',e);return null;});
    return lobbyP;
  }

  // ---------- the claude.use() the game expects ----------
  var site={
    isSite:true,
    google:function(){
      var p=new firebase.auth.GoogleAuthProvider(),u=auth.currentUser;
      // a guest who signs in with Google keeps their progress: the guest account is upgraded in place
      var go=u&&u.isAnonymous?u.linkWithPopup(p).catch(function(e){if(e&&e.code==='auth/credential-already-in-use'&&e.credential)return auth.signInWithCredential(e.credential);throw e;}):auth.signInWithPopup(p);
      return go.then(function(){return true;});
    },
    signOut:function(){return auth.signOut().then(function(){try{localStorage.removeItem('dustline-auto');}catch(e){}});},
    user:function(){return ready.then(function(u){return u?{id:u.uid,name:displayName(u),anonymous:u.isAnonymous}:null;});}
  };
  window.claude={
    use:function(name){
      if(name==='user')return ready.then(function(u){return u?userNs:null;});
      if(name==='db')return ready.then(function(u){return u?dbNs:null;});
      if(name==='room')return roomNs();
      return Promise.resolve(null);
    },
    site:site
  };
})();
