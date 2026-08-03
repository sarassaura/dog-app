var ZE=Object.defineProperty,QE=Object.defineProperties;var XE=Object.getOwnPropertyDescriptors;var ov=Object.getOwnPropertySymbols;var JE=Object.prototype.hasOwnProperty,e0=Object.prototype.propertyIsEnumerable;var sv=(e,n,t)=>n in e?ZE(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,v=(e,n)=>{for(var t in n||={})JE.call(n,t)&&sv(e,t,n[t]);if(ov)for(var t of ov(n))e0.call(n,t)&&sv(e,t,n[t]);return e},V=(e,n)=>QE(e,XE(n));var ft=null,Qa=!1,Pi=1,t0=null,Oe=Symbol("SIGNAL");function T(e){let n=ft;return ft=e,n}function Xa(){return ft}var ni={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ii(e){if(Qa)throw new Error("");if(ft===null)return;ft.consumerOnSignalRead(e);let n=ft.producersTail;if(n!==void 0&&n.producer===e)return;let t,i=ft.recomputing;if(i&&(t=n!==void 0?n.nextProducer:ft.producers,t!==void 0&&t.producer===e)){ft.producersTail=t,t.lastReadVersion=e.version,t.knownValidAtEpoch=Pi;return}let r=e.consumersTail;if(r!==void 0&&r.consumer===ft&&(!i||r.knownValidAtEpoch===Pi))return;let o=Br(ft),s={producer:e,consumer:ft,nextProducer:t,prevConsumer:void 0,knownValidAtEpoch:Pi,lastReadVersion:e.version,nextConsumer:void 0};ft.producersTail=s,n!==void 0?n.nextProducer=s:ft.producers=s,o&&dv(e,s)}function av(){Pi++}function ji(e){if(!(Br(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===Pi)){if(!e.producerMustRecompute(e)&&!jr(e)){Vr(e);return}e.producerRecomputeValue(e),Vr(e)}}function $u(e){if(e.consumers===void 0)return;let n=Qa;Qa=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||n0(i)}}finally{Qa=n}}function Gu(){return ft?.consumerAllowSignalWrites!==!1}function n0(e){e.dirty=!0,$u(e),e.consumerMarkedDirty?.(e)}function Vr(e){e.dirty=!1,e.lastCleanEpoch=Pi}function kn(e){return e&&cv(e),T(e)}function cv(e){if(e.producersTail?.knownValidAtEpoch===Pi){let n=e.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}e.producersTail=void 0,e.recomputing=!0}function ri(e,n){T(n),e&&lv(e)}function lv(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(Br(e))do t=Wu(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function jr(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,i=n.lastReadVersion;if(i!==t.version||(ji(t),i!==t.version))return!0}return!1}function oi(e){if(Br(e)){let n=e.producers;for(;n!==void 0;)n=Wu(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function dv(e,n){let t=e.consumersTail,i=Br(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!i)for(let r=e.producers;r!==void 0;r=r.nextProducer)dv(r.producer,r)}function Wu(e){let n=e.producer,t=e.nextProducer,i=e.nextConsumer,r=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Br(n)){let o=n.producers;for(;o!==void 0;)o=Wu(o)}return t}function Br(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function Qo(e){t0?.(e)}function Xo(e,n){return Object.is(e,n)}function Jo(e,n){let t=Object.create(i0);t.computation=e,n!==void 0&&(t.equal=n);let i=()=>{if(ji(t),ii(t),t.value===un)throw t.error;return t.value};return i[Oe]=t,Qo(t),i}var Li=Symbol("UNSET"),Vi=Symbol("COMPUTING"),un=Symbol("ERRORED"),i0=V(v({},ni),{value:Li,dirty:!0,error:null,equal:Xo,kind:"computed",producerMustRecompute(e){return e.value===Li||e.value===Vi},producerRecomputeValue(e){if(e.value===Vi)throw new Error("");let n=e.value;e.value=Vi;let t=kn(e),i,r=!1;try{i=e.computation(),T(null),r=n!==Li&&n!==un&&i!==un&&e.equal(n,i)}catch(o){i=un,e.error=o}finally{ri(e,t)}if(r){e.value=n;return}e.value=i,e.version++}});function r0(){throw new Error}var uv=r0;function fv(e){uv(e)}function qu(e){uv=e}var o0=null;function Yu(e,n){let t=Object.create(es);t.value=e,n!==void 0&&(t.equal=n);let i=()=>hv(t);return i[Oe]=t,Qo(t),[i,s=>Bi(t,s),s=>Ja(t,s)]}function hv(e){return ii(e),e.value}function Bi(e,n){Gu()||fv(e),e.equal(e.value,n)||(e.value=n,s0(e))}function Ja(e,n){Gu()||fv(e),Bi(e,n(e.value))}var es=V(v({},ni),{equal:Xo,value:void 0,kind:"signal"});function s0(e){e.version++,av(),$u(e),o0?.(e)}var Ku=V(v({},ni),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Zu(e){if(e.dirty=!1,e.version>0&&!jr(e))return;e.version++;let n=kn(e);try{e.cleanup(),e.fn()}finally{ri(e,n)}}var Qu;function ec(){return Qu}function fn(e){let n=Qu;return Qu=e,n}var pv=Symbol("NotFound");function Ur(e){return e===pv||e?.name==="\u0275NotFound"}function Xu(e,n,t){let i=Object.create(a0);i.source=e,i.computation=n,t!=null&&(i.equal=t);let o=()=>{if(ji(i),ii(i),i.value===un)throw i.error;return i.value};return o[Oe]=i,Qo(i),o}function Ju(e,n){ji(e),Bi(e,n),Vr(e)}function mv(e,n){if(ji(e),e.value===un)throw e.error;Ja(e,n),Vr(e)}var a0=V(v({},ni),{value:Li,dirty:!0,error:null,equal:Xo,kind:"linkedSignal",producerMustRecompute(e){return e.value===Li||e.value===Vi},producerRecomputeValue(e){if(e.value===Vi)throw new Error("");let n=e.value;e.value=Vi;let t=kn(e),i,r=!1;try{let o=e.source(),s=n!==Li&&n!==un,a=s?{source:e.sourceValue,value:n}:void 0;i=e.computation(o,a),e.sourceValue=o,T(null),r=s&&i!==un&&e.equal(n,i)}catch(o){i=un,e.error=o}finally{ri(e,t)}if(r){e.value=n;return}e.value=i,e.version++}});function gv(e){let n=T(null);try{return e()}finally{T(n)}}function q(e){return typeof e=="function"}function Hr(e){let t=e(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var tc=Hr(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Ui(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var se=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(q(i))try{i()}catch(o){n=o instanceof tc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{vv(o)}catch(s){n=n??[],s instanceof tc?n=[...n,...s.errors]:n.push(s)}}if(n)throw new tc(n)}}add(n){var t;if(n&&n!==this)if(this.closed)vv(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&Ui(t,n)}remove(n){let{_finalizers:t}=this;t&&Ui(t,n),n instanceof e&&n._removeParent(this)}};se.EMPTY=(()=>{let e=new se;return e.closed=!0,e})();var ef=se.EMPTY;function nc(e){return e instanceof se||e&&"closed"in e&&q(e.remove)&&q(e.add)&&q(e.unsubscribe)}function vv(e){q(e)?e():e.unsubscribe()}var Qt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var zr={setTimeout(e,n,...t){let{delegate:i}=zr;return i?.setTimeout?i.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=zr;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function ic(e){zr.setTimeout(()=>{let{onUnhandledError:n}=Qt;if(n)n(e);else throw e})}function Hi(){}var yv=tf("C",void 0,void 0);function _v(e){return tf("E",void 0,e)}function bv(e){return tf("N",e,void 0)}function tf(e,n,t){return{kind:e,value:n,error:t}}var zi=null;function $r(e){if(Qt.useDeprecatedSynchronousErrorHandling){let n=!zi;if(n&&(zi={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:i}=zi;if(zi=null,t)throw i}}else e()}function Sv(e){Qt.useDeprecatedSynchronousErrorHandling&&zi&&(zi.errorThrown=!0,zi.error=e)}var $i=class extends se{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,nc(n)&&n.add(this)):this.destination=d0}static create(n,t,i){return new On(n,t,i)}next(n){this.isStopped?rf(bv(n),this):this._next(n)}error(n){this.isStopped?rf(_v(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?rf(yv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},c0=Function.prototype.bind;function nf(e,n){return c0.call(e,n)}var of=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(i){rc(i)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(i){rc(i)}else rc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){rc(t)}}},On=class extends $i{constructor(n,t,i){super();let r;if(q(n)||!n)r={next:n??void 0,error:t??void 0,complete:i??void 0};else{let o;this&&Qt.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&nf(n.next,o),error:n.error&&nf(n.error,o),complete:n.complete&&nf(n.complete,o)}):r=n}this.destination=new of(r)}};function rc(e){Qt.useDeprecatedSynchronousErrorHandling?Sv(e):ic(e)}function l0(e){throw e}function rf(e,n){let{onStoppedNotification:t}=Qt;t&&zr.setTimeout(()=>t(e,n))}var d0={closed:!0,next:Hi,error:l0,complete:Hi};var Gr=typeof Symbol=="function"&&Symbol.observable||"@@observable";function xt(e){return e}function sf(...e){return af(e)}function af(e){return e.length===0?xt:e.length===1?e[0]:function(t){return e.reduce((i,r)=>r(i),t)}}var L=class e{constructor(n){n&&(this._subscribe=n)}lift(n){let t=new e;return t.source=this,t.operator=n,t}subscribe(n,t,i){let r=f0(n)?n:new On(n,t,i);return $r(()=>{let{operator:o,source:s}=this;r.add(o?o.call(r,s):s?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(t){n.error(t)}}forEach(n,t){return t=Dv(t),new t((i,r)=>{let o=new On({next:s=>{try{n(s)}catch(a){r(a),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var t;return(t=this.source)===null||t===void 0?void 0:t.subscribe(n)}[Gr](){return this}pipe(...n){return af(n)(this)}toPromise(n){return n=Dv(n),new n((t,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>t(r))})}};L.create=e=>new L(e);function Dv(e){var n;return(n=e??Qt.Promise)!==null&&n!==void 0?n:Promise}function u0(e){return e&&q(e.next)&&q(e.error)&&q(e.complete)}function f0(e){return e&&e instanceof $i||u0(e)&&nc(e)}function h0(e){return q(e?.lift)}function Y(e){return n=>{if(h0(n))return n.lift(function(t){try{return e(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function X(e,n,t,i,r){return new cf(e,n,t,i,r)}var cf=class extends $i{constructor(n,t,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var wv=Hr(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var b=class extends L{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let t=new oc(this,this);return t.operator=n,t}_throwIfClosed(){if(this.closed)throw new wv}next(n){$r(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let t of this.currentObservers)t.next(n)}})}error(n){$r(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:t}=this;for(;t.length;)t.shift().error(n)}})}complete(){$r(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:t,isStopped:i,observers:r}=this;return t||i?ef:(this.currentObservers=null,r.push(n),new se(()=>{this.currentObservers=null,Ui(r,n)}))}_checkFinalizedStatuses(n){let{hasError:t,thrownError:i,isStopped:r}=this;t?n.error(i):r&&n.complete()}asObservable(){let n=new L;return n.source=this,n}};b.create=(e,n)=>new oc(e,n);var oc=class extends b{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,n)}error(n){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&i!==void 0?i:ef}};var $e=class extends b{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:i}=this;if(n)throw t;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var ts={now(){return(ts.delegate||Date).now()},delegate:void 0};var sc=class extends b{constructor(n=1/0,t=1/0,i=ts){super(),this._bufferSize=n,this._windowTime=t,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=t===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,t)}next(n){let{isStopped:t,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;t||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let t=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),t}_trimBuffer(){let{_bufferSize:n,_timestampProvider:t,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=t.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var ac=class extends se{constructor(n,t){super()}schedule(n,t=0){return this}};var ns={setInterval(e,n,...t){let{delegate:i}=ns;return i?.setInterval?i.setInterval(e,n,...t):setInterval(e,n,...t)},clearInterval(e){let{delegate:n}=ns;return(n?.clearInterval||clearInterval)(e)},delegate:void 0};var cc=class extends ac{constructor(n,t){super(n,t),this.scheduler=n,this.work=t,this.pending=!1}schedule(n,t=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,t)),this.pending=!0,this.delay=t,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,t),this}requestAsyncId(n,t,i=0){return ns.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,t,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return t;t!=null&&ns.clearInterval(t)}execute(n,t){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,t);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,t){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:t}=this,{actions:i}=t;this.work=this.state=this.scheduler=null,this.pending=!1,Ui(i,this),n!=null&&(this.id=this.recycleAsyncId(t,n,null)),this.delay=null,super.unsubscribe()}}};var lf=(()=>{class e{constructor(t,i=e.now){this.schedulerActionCtor=t,this.now=i}schedule(t,i=0,r){return new this.schedulerActionCtor(this,t).schedule(r,i)}}return e.now=ts.now,e})();var lc=class extends lf{constructor(n,t=lf.now){super(n,t),this.actions=[],this._active=!1}flush(n){let{actions:t}=this;if(this._active){t.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=t.shift());if(this._active=!1,i){for(;n=t.shift();)n.unsubscribe();throw i}}};var Gi=new lc(cc),Cv=Gi;var Fe=new L(e=>e.complete());function dc(e){return e&&q(e.schedule)}function df(e){return e[e.length-1]}function uc(e){return q(df(e))?e.pop():void 0}function hn(e){return dc(df(e))?e.pop():void 0}function Ev(e,n){return typeof df(e)=="number"?e.pop():n}function Iv(e,n,t,i){function r(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(f){s(f)}}function c(d){try{l(i.throw(d))}catch(f){s(f)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(e,n||[])).next())})}function xv(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],i=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&i>=e.length&&(e=void 0),{value:e&&e[i++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Wi(e){return this instanceof Wi?(this.v=e,this):new Wi(e)}function Nv(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(e,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(p){return function(m){return Promise.resolve(m).then(p,f)}}function a(p,m){i[p]&&(r[p]=function(y){return new Promise(function(w,G){o.push([p,y,w,G])>1||c(p,y)})},m&&(r[p]=m(r[p])))}function c(p,m){try{l(i[p](m))}catch(y){h(o[0][3],y)}}function l(p){p.value instanceof Wi?Promise.resolve(p.value.v).then(d,f):h(o[0][2],p)}function d(p){c("next",p)}function f(p){c("throw",p)}function h(p,m){p(m),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Mv(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof xv=="function"?xv(e):e[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(o){t[o]=e[o]&&function(s){return new Promise(function(a,c){s=e[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var fc=(e=>e&&typeof e.length=="number"&&typeof e!="function");function hc(e){return q(e?.then)}function pc(e){return q(e[Gr])}function mc(e){return Symbol.asyncIterator&&q(e?.[Symbol.asyncIterator])}function gc(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function p0(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var vc=p0();function yc(e){return q(e?.[vc])}function _c(e){return Nv(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:i,done:r}=yield Wi(t.read());if(r)return yield Wi(void 0);yield yield Wi(i)}}finally{t.releaseLock()}})}function bc(e){return q(e?.getReader)}function ye(e){if(e instanceof L)return e;if(e!=null){if(pc(e))return m0(e);if(fc(e))return g0(e);if(hc(e))return v0(e);if(mc(e))return Tv(e);if(yc(e))return y0(e);if(bc(e))return _0(e)}throw gc(e)}function m0(e){return new L(n=>{let t=e[Gr]();if(q(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function g0(e){return new L(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function v0(e){return new L(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,ic)})}function y0(e){return new L(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function Tv(e){return new L(n=>{b0(e,n).catch(t=>n.error(t))})}function _0(e){return Tv(_c(e))}function b0(e,n){var t,i,r,o;return Iv(this,void 0,void 0,function*(){try{for(t=Mv(e);i=yield t.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=t.return)&&(yield o.call(t))}finally{if(r)throw r.error}}n.complete()})}function _t(e,n,t,i=0,r=!1){let o=n.schedule(function(){t(),r?e.add(this.schedule(null,i)):this.unsubscribe()},i);if(e.add(o),!r)return o}function Sc(e,n=0){return Y((t,i)=>{t.subscribe(X(i,r=>_t(i,e,()=>i.next(r),n),()=>_t(i,e,()=>i.complete(),n),r=>_t(i,e,()=>i.error(r),n)))})}function Dc(e,n=0){return Y((t,i)=>{i.add(e.schedule(()=>t.subscribe(i),n))})}function Av(e,n){return ye(e).pipe(Dc(n),Sc(n))}function Rv(e,n){return ye(e).pipe(Dc(n),Sc(n))}function kv(e,n){return new L(t=>{let i=0;return n.schedule(function(){i===e.length?t.complete():(t.next(e[i++]),t.closed||this.schedule())})})}function Ov(e,n){return new L(t=>{let i;return _t(t,n,()=>{i=e[vc](),_t(t,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){t.error(s);return}o?t.complete():t.next(r)},0,!0)}),()=>q(i?.return)&&i.return()})}function wc(e,n){if(!e)throw new Error("Iterable cannot be null");return new L(t=>{_t(t,n,()=>{let i=e[Symbol.asyncIterator]();_t(t,n,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function Fv(e,n){return wc(_c(e),n)}function Pv(e,n){if(e!=null){if(pc(e))return Av(e,n);if(fc(e))return kv(e,n);if(hc(e))return Rv(e,n);if(mc(e))return wc(e,n);if(yc(e))return Ov(e,n);if(bc(e))return Fv(e,n)}throw gc(e)}function Se(e,n){return n?Pv(e,n):ye(e)}function x(...e){let n=hn(e);return Se(e,n)}function is(e,n){let t=q(e)?e:()=>e,i=r=>r.error(t());return new L(n?r=>n.schedule(i,0,r):i)}function rs(e){return!!e&&(e instanceof L||q(e.lift)&&q(e.subscribe))}var qi=Hr(e=>function(){e(this),this.name="EmptyError",this.message="no elements in sequence"});function Lv(e){return e instanceof Date&&!isNaN(e)}function U(e,n){return Y((t,i)=>{let r=0;t.subscribe(X(i,o=>{i.next(e.call(n,o,r++))}))})}var{isArray:S0}=Array;function D0(e,n){return S0(n)?e(...n):e(n)}function Cc(e){return U(n=>D0(e,n))}var{isArray:w0}=Array,{getPrototypeOf:C0,prototype:E0,keys:x0}=Object;function Ec(e){if(e.length===1){let n=e[0];if(w0(n))return{args:n,keys:null};if(I0(n)){let t=x0(n);return{args:t.map(i=>n[i]),keys:t}}}return{args:e,keys:null}}function I0(e){return e&&typeof e=="object"&&C0(e)===E0}function xc(e,n){return e.reduce((t,i,r)=>(t[i]=n[r],t),{})}function Wr(...e){let n=hn(e),t=uc(e),{args:i,keys:r}=Ec(e);if(i.length===0)return Se([],n);let o=new L(N0(i,n,r?s=>xc(r,s):xt));return t?o.pipe(Cc(t)):o}function N0(e,n,t=xt){return i=>{Vv(n,()=>{let{length:r}=e,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)Vv(n,()=>{let l=Se(e[c],n),d=!1;l.subscribe(X(i,f=>{o[c]=f,d||(d=!0,a--),a||i.next(t(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Vv(e,n,t){e?_t(t,e,n):n()}function jv(e,n,t,i,r,o,s,a){let c=[],l=0,d=0,f=!1,h=()=>{f&&!c.length&&!l&&n.complete()},p=y=>l<i?m(y):c.push(y),m=y=>{o&&n.next(y),l++;let w=!1;ye(t(y,d++)).subscribe(X(n,G=>{r?.(G),o?p(G):n.next(G)},()=>{w=!0},void 0,()=>{if(w)try{for(l--;c.length&&l<i;){let G=c.shift();s?_t(n,s,()=>m(G)):m(G)}h()}catch(G){n.error(G)}}))};return e.subscribe(X(n,p,()=>{f=!0,h()})),()=>{a?.()}}function Ge(e,n,t=1/0){return q(n)?Ge((i,r)=>U((o,s)=>n(i,o,r,s))(ye(e(i,r))),t):(typeof n=="number"&&(t=n),Y((i,r)=>jv(i,r,e,t)))}function si(e=1/0){return Ge(xt,e)}function Bv(){return si(1)}function pn(...e){return Bv()(Se(e,hn(e)))}function Yi(e){return new L(n=>{ye(e()).subscribe(n)})}function os(...e){let n=uc(e),{args:t,keys:i}=Ec(e),r=new L(o=>{let{length:s}=t;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let f=!1;ye(t[d]).subscribe(X(o,h=>{f||(f=!0,l--),a[d]=h},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?xc(i,a):a),o.complete())}))}});return n?r.pipe(Cc(n)):r}function Ic(e=0,n,t=Cv){let i=-1;return n!=null&&(dc(n)?t=n:i=n),new L(r=>{let o=Lv(e)?+e-t.now():e;o<0&&(o=0);let s=0;return t.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function mn(...e){let n=hn(e),t=Ev(e,1/0),i=e;return i.length?i.length===1?ye(i[0]):si(t)(Se(i,n)):Fe}function _e(e,n){return Y((t,i)=>{let r=0;t.subscribe(X(i,o=>e.call(n,o,r++)&&i.next(o)))})}function Uv(e){return Y((n,t)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,t.next(l)}s&&t.complete()},c=()=>{o=null,s&&t.complete()};n.subscribe(X(t,l=>{i=!0,r=l,o||ye(e(l)).subscribe(o=X(t,a,c))},()=>{s=!0,(!i||!o||o.closed)&&t.complete()}))})}function Nc(e,n=Gi){return Uv(()=>Ic(e,n))}function ai(e){return Y((n,t)=>{let i=null,r=!1,o;i=n.subscribe(X(t,void 0,void 0,s=>{o=ye(e(s,ai(e)(n))),i?(i.unsubscribe(),i=null,o.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(t))})}function ci(e,n){return q(n)?Ge(e,n,1):Ge(e,1)}function ss(e,n=Gi){return Y((t,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+e,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}t.subscribe(X(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,e),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Hv(e){return Y((n,t)=>{let i=!1;n.subscribe(X(t,r=>{i=!0,t.next(r)},()=>{i||t.next(e),t.complete()}))})}function Xe(e){return e<=0?()=>Fe:Y((n,t)=>{let i=0;n.subscribe(X(t,r=>{++i<=e&&(t.next(r),e<=i&&t.complete())}))})}function zv(){return Y((e,n)=>{e.subscribe(X(n,Hi))})}function $v(e){return U(()=>e)}function uf(e,n){return n?t=>pn(n.pipe(Xe(1),zv()),t.pipe(uf(e))):Ge((t,i)=>ye(e(t,i)).pipe(Xe(1),$v(t)))}function ff(e,n=Gi){let t=Ic(e,n);return uf(()=>t)}function Mc(e,n=xt){return e=e??M0,Y((t,i)=>{let r,o=!0;t.subscribe(X(i,s=>{let a=n(s);(o||!e(r,a))&&(o=!1,r=a,i.next(s))}))})}function M0(e,n){return e===n}function Gv(e=T0){return Y((n,t)=>{let i=!1;n.subscribe(X(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(e())))})}function T0(){return new qi}function Ki(e){return Y((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function Fn(e,n){let t=arguments.length>=2;return i=>i.pipe(e?_e((r,o)=>e(r,o,i)):xt,Xe(1),t?Hv(n):Gv(()=>new qi))}function Tc(e){return e<=0?()=>Fe:Y((n,t)=>{let i=[];n.subscribe(X(t,r=>{i.push(r),e<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Ac(){return Y((e,n)=>{let t,i=!1;e.subscribe(X(n,r=>{let o=t;t=r,i&&n.next([o,r]),i=!0}))})}function as(e={}){let{connector:n=()=>new b,resetOnError:t=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=e;return o=>{let s,a,c,l=0,d=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},p=()=>{h(),s=c=void 0,d=f=!1},m=()=>{let y=s;p(),y?.unsubscribe()};return Y((y,w)=>{l++,!f&&!d&&h();let G=c=c??n();w.add(()=>{l--,l===0&&!f&&!d&&(a=hf(m,r))}),G.subscribe(w),!s&&l>0&&(s=new On({next:xe=>G.next(xe),error:xe=>{f=!0,h(),a=hf(p,t,xe),G.error(xe)},complete:()=>{d=!0,h(),a=hf(p,i),G.complete()}}),ye(y).subscribe(s))})(o)}}function hf(e,n,...t){if(n===!0){e();return}if(n===!1)return;let i=new On({next:()=>{i.unsubscribe(),e()}});return ye(n(...t)).subscribe(i)}function Rc(e,n,t){let i,r=!1;return e&&typeof e=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:t}=e:i=e??1/0,as({connector:()=>new sc(i,n,t),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function cs(e){return _e((n,t)=>e<=t)}function rt(...e){let n=hn(e);return Y((t,i)=>{(n?pn(e,t,n):pn(e,t)).subscribe(i)})}function Pe(e,n){return Y((t,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();t.subscribe(X(i,c=>{r?.unsubscribe();let l=0,d=o++;ye(e(c,d)).subscribe(r=X(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function We(e){return Y((n,t)=>{ye(e).subscribe(X(t,()=>t.complete(),Hi)),!t.closed&&n.subscribe(t)})}function Le(e,n,t){let i=q(e)||n||t?{next:e,error:n,complete:t}:e;return i?Y((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(X(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):xt}var jc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",_=class extends Error{code;constructor(n,t){super(Xt(n,t)),this.code=n}};function A0(e){return`NG0${Math.abs(e)}`}function Xt(e,n){return`${A0(e)}${n?": "+n:""}`}function he(e){for(let n in e)if(e[n]===he)return n;throw Error("")}function Qv(e,n){for(let t in n)n.hasOwnProperty(t)&&!e.hasOwnProperty(t)&&(e[t]=n[t])}function ms(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(ms).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Bc(e,n){return e?n?`${e} ${n}`:e:n||""}var R0=he({__forward_ref__:he});function bt(e){return e.__forward_ref__=bt,e}function et(e){return If(e)?e():e}function If(e){return typeof e=="function"&&e.hasOwnProperty(R0)&&e.__forward_ref__===bt}function H(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function M(e){return{providers:e.providers||[],imports:e.imports||[]}}function gs(e){return k0(e,Uc)}function Nf(e){return gs(e)!==null}function k0(e,n){return e.hasOwnProperty(n)&&e[n]||null}function O0(e){let n=e?.[Uc]??null;return n||null}function mf(e){return e&&e.hasOwnProperty(Oc)?e[Oc]:null}var Uc=he({\u0275prov:he}),Oc=he({\u0275inj:he}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=H({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Mf(e){return e&&!!e.\u0275providers}var vs=he({\u0275cmp:he}),ys=he({\u0275dir:he}),Tf=he({\u0275pipe:he}),Af=he({\u0275mod:he}),us=he({\u0275fac:he}),er=he({__NG_ELEMENT_ID__:he}),Wv=he({__NG_ENV_ID__:he});function Xv(e){return Hc(e,"@NgModule"),e[Af]||null}function Vn(e){return Hc(e,"@Component"),e[vs]||null}function Rf(e){return Hc(e,"@Directive"),e[ys]||null}function Jv(e){return Hc(e,"@Pipe"),e[Tf]||null}function Hc(e,n){if(e==null)throw new _(-919,!1)}function _s(e){return typeof e=="string"?e:e==null?"":String(e)}var ey=he({ngErrorCode:he}),F0=he({ngErrorMessage:he}),P0=he({ngTokenPath:he});function kf(e,n){return ty("",-200,n)}function zc(e,n){throw new _(-201,!1)}function ty(e,n,t){let i=new _(n,e);return i[ey]=n,i[F0]=e,t&&(i[P0]=t),i}function L0(e){return e[ey]}var gf;function ny(){return gf}function ht(e){let n=gf;return gf=e,n}function Of(e,n,t){let i=gs(e);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(n!==void 0)return n;zc(e,"")}var fi=globalThis;var V0={},Zi=V0,j0="__NG_DI_FLAG__",vf=class{injector;constructor(n){this.injector=n}retrieve(n,t){let i=Qi(t)||0;try{return this.injector.get(n,i&8?null:Zi,i)}catch(r){if(Ur(r))return r;throw r}}};function B0(e,n=0){let t=ec();if(t===void 0)throw new _(-203,!1);if(t===null)return Of(e,void 0,n);{let i=U0(n),r=t.retrieve(e,i);if(Ur(r)){if(i.optional)return null;throw r}return r}}function C(e,n=0){return(ny()||B0)(et(e),n)}function u(e,n){return C(e,Qi(n))}function Qi(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function U0(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function yf(e){let n=[];for(let t=0;t<e.length;t++){let i=et(e[t]);if(Array.isArray(i)){if(i.length===0)throw new _(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=H0(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(C(r,o))}else n.push(C(i))}return n}function H0(e){return e[j0]}function di(e,n){let t=e.hasOwnProperty(us);return t?e[us]:null}function iy(e,n,t){if(e.length!==n.length)return!1;for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(t&&(r=t(r),o=t(o)),o!==r)return!1}return!0}function ry(e){return e.flat(Number.POSITIVE_INFINITY)}function $c(e,n){e.forEach(t=>Array.isArray(t)?$c(t,n):n(t))}function Ff(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function bs(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function oy(e,n){let t=[];for(let i=0;i<e;i++)t.push(n);return t}function sy(e,n,t,i){let r=e.length;if(r==n)e.push(t,i);else if(r===1)e.push(i,e[0]),e[0]=t;else{for(r--,e.push(e[r-1],e[r]);r>n;){let o=r-2;e[r]=e[o],r--}e[n]=t,e[n+1]=i}}function Gc(e,n,t){let i=Zr(e,n);return i>=0?e[i|1]=t:(i=~i,sy(e,i,n,t)),i}function Wc(e,n){let t=Zr(e,n);if(t>=0)return e[t|1]}function Zr(e,n){return z0(e,n,1)}function z0(e,n,t){let i=0,r=e.length>>t;for(;r!==i;){let o=i+(r-i>>1),s=e[o<<t];if(n===s)return o<<t;s>n?r=o:i=o+1}return~(r<<t)}var hi={},ot=[],tr=new g(""),Ss=new g("",-1),Pf=new g(""),Yr=class{get(n,t=Zi){if(t===Zi){let r=ty("",-201);throw r.name="\u0275NotFound",r}return t}};function Vt(e){return{\u0275providers:e}}function ay(e){return Vt([{provide:tr,multi:!0,useValue:e}])}function cy(...e){return{\u0275providers:Lf(!0,e),\u0275fromNgModule:!0}}function Lf(e,...n){let t=[],i=new Set,r,o=s=>{t.push(s)};return $c(n,s=>{let a=s;Fc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&ly(r,o),t}function ly(e,n){for(let t=0;t<e.length;t++){let{ngModule:i,providers:r}=e[t];Vf(r,o=>{n(o,i)})}}function Fc(e,n,t,i){if(e=et(e),!e)return!1;let r=null,o=mf(e),s=!o&&Vn(e);if(!o&&!s){let c=e.ngModule;if(o=mf(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=e}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)Fc(l,n,t,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;$c(o.imports,d=>{Fc(d,n,t,i)&&(l||=[],l.push(d))}),l!==void 0&&ly(l,n)}if(!a){let l=di(r)||(()=>new r);n({provide:r,useFactory:l,deps:ot},r),n({provide:Pf,useValue:r,multi:!0},r),n({provide:tr,useValue:()=>C(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=e;Vf(c,d=>{n(d,l)})}}else return!1;return r!==e&&e.providers!==void 0}function Vf(e,n){for(let t of e)Mf(t)&&(t=t.\u0275providers),Array.isArray(t)?Vf(t,n):n(t)}var $0=he({provide:String,useValue:he});function dy(e){return e!==null&&typeof e=="object"&&$0 in e}function G0(e){return!!(e&&e.useExisting)}function W0(e){return!!(e&&e.useFactory)}function Xi(e){return typeof e=="function"}function uy(e){return!!e.useClass}var Ds=new g(""),kc={},qv={},pf;function Qr(){return pf===void 0&&(pf=new Yr),pf}var de=class{},Ji=class extends de{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,bf(n,s=>this.processProvider(s)),this.records.set(Ss,qr(void 0,this)),r.has("environment")&&this.records.set(de,qr(void 0,this));let o=this.records.get(Ds);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Pf,ot,{self:!0}))}retrieve(n,t){let i=Qi(t)||0;try{return this.get(n,Zi,i)}catch(r){if(Ur(r))return r;throw r}}destroy(){ls(this),this._destroyed=!0;let n=T(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),T(n)}}onDestroy(n){return ls(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ls(this);let t=fn(this),i=ht(void 0),r;try{return n()}finally{fn(t),ht(i)}}get(n,t=Zi,i){if(ls(this),n.hasOwnProperty(Wv))return n[Wv](this);let r=Qi(i),o,s=fn(this),a=ht(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=Q0(n)&&gs(n);d&&this.injectableDefInScope(d)?l=qr(_f(n),kc):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Qr():this.parent;return t=r&8&&t===Zi?null:t,c.get(n,t)}catch(c){let l=L0(c);throw l===-200||l===-201?new _(l,null):c}finally{ht(a),fn(s)}}resolveInjectorInitializers(){let n=T(null),t=fn(this),i=ht(void 0),r;try{let o=this.get(tr,ot,{self:!0});for(let s of o)s()}finally{fn(t),ht(i),T(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=et(n);let t=Xi(n)?n:et(n&&n.provide),i=Y0(n);if(!Xi(n)&&n.multi===!0){let r=this.records.get(t);r||(r=qr(void 0,kc,!0),r.factory=()=>yf(r.multi),this.records.set(t,r)),t=n,r.multi.push(n)}this.records.set(t,i)}hydrate(n,t,i){let r=T(null);try{if(t.value===qv)throw kf("");return t.value===kc&&(t.value=qv,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Z0(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{T(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=et(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function _f(e){let n=gs(e),t=n!==null?n.factory:di(e);if(t!==null)return t;if(e instanceof g)throw new _(-204,!1);if(e instanceof Function)return q0(e);throw new _(-204,!1)}function q0(e){if(e.length>0)throw new _(-204,!1);let t=O0(e);return t!==null?()=>t.factory(e):()=>new e}function Y0(e){if(dy(e))return qr(void 0,e.useValue);{let n=jf(e);return qr(n,kc)}}function jf(e,n,t){let i;if(Xi(e)){let r=et(e);return di(r)||_f(r)}else if(dy(e))i=()=>et(e.useValue);else if(W0(e))i=()=>e.useFactory(...yf(e.deps||[]));else if(G0(e))i=(r,o)=>C(et(e.useExisting),o!==void 0&&o&8?8:void 0);else{let r=et(e&&(e.useClass||e.provide));if(K0(e))i=()=>new r(...yf(e.deps));else return di(r)||_f(r)}return i}function ls(e){if(e.destroyed)throw new _(-205,!1)}function qr(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function K0(e){return!!e.deps}function Z0(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function Q0(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function bf(e,n){for(let t of e)Array.isArray(t)?bf(t,n):t&&Mf(t)?bf(t.\u0275providers,n):n(t)}function Ye(e,n){let t;e instanceof Ji?(ls(e),t=e):t=new vf(e);let i,r=fn(t),o=ht(void 0);try{return n()}finally{fn(r),ht(o)}}function fy(){return ny()!==void 0||ec()!=null}var Jt=0,N=1,j=2,qe=3,jt=4,st=5,nr=6,Xr=7,He=8,vn=9,en=10,De=11,Jr=12,Bf=13,pi=14,mt=15,mi=16,ir=17,yn=18,_n=19,Uf=20,Pn=21,qc=22,ui=23,It=24,rr=25,bn=26,Ne=27,hy=1,Hf=6,or=7,ws=8,sr=9,Me=10;function jn(e){return Array.isArray(e)&&typeof e[hy]=="object"}function Bt(e){return Array.isArray(e)&&e[hy]===!0}function zf(e){return(e.flags&4)!==0}function Sn(e){return e.componentOffset>-1}function eo(e){return(e.flags&1)===1}function Dn(e){return!!e.template}function to(e){return(e[j]&512)!==0}function ar(e){return(e[j]&256)===256}var fe=(function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e[e.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",e})(fe||{}),ds,Kr="svg",Yc="math",py="",Yv="*",Sf=()=>Object.create(null);function X0(){return ds||(ds=Sf(),li(fe.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),li(fe.STYLE,void 0,[["*",["style"]]]),li(fe.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]],["img",["src"]],["video",["src"]]]),li(fe.URL,Yc,[["*",["href","xlink:href"]]]),li(fe.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),li(fe.URL,Kr,[["a",["href","xlink:href"]]]),li(fe.ATTRIBUTE_NO_BINDING,Kr,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),li(fe.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]]]),ds)}function li(e,n,t){let i=n??py;for(let[r,o]of t){let s=r.toLowerCase();for(let a of o){let c=a.toLowerCase(),l=ds[c]??=Sf(),d=l[i]??=Sf();d[s]=e}}}function my(e,n,t){let r=X0()[n.toLowerCase()];if(!r)return fe.NONE;let o=e.toLowerCase(),s;if(t){let a=r[t];a&&(s=a[o]??a[Yv])}if(s===void 0){let a=r[py];a&&(s=a[o]??a[Yv])}return s??fe.NONE}function tt(e){for(;Array.isArray(e);)e=e[Jt];return e}function $f(e,n){return tt(n[e])}function St(e,n){return tt(n[e.index])}function Kc(e,n){return e.data[n]}function Gf(e,n){return e[n]}function Wf(e,n,t,i){t>=e.data.length&&(e.data[t]=null,e.blueprint[t]=null),n[t]=i}function Ut(e,n){let t=n[e];return jn(t)?t:t[Jt]}function gy(e){return(e[j]&4)===4}function Zc(e){return(e[j]&128)===128}function vy(e){return Bt(e[qe])}function Nt(e,n){return n==null?null:e[n]}function qf(e){e[ir]=0}function Yf(e){e[j]&1024||(e[j]|=1024,Zc(e)&&cr(e))}function yy(e,n){for(;e>0;)n=n[pi],e--;return n}function Cs(e){return!!(e[j]&9216||e[It]?.dirty)}function Qc(e){e[en].changeDetectionScheduler?.notify(8),e[j]&64&&(e[j]|=1024),Cs(e)&&cr(e)}function cr(e){e[en].changeDetectionScheduler?.notify(0);let n=Ln(e);for(;n!==null&&!(n[j]&8192||(n[j]|=8192,!Zc(n)));)n=Ln(n)}function Xc(e,n){if(ar(e))throw new _(911,!1);e[Pn]===null&&(e[Pn]=[]),e[Pn].push(n)}function _y(e,n){if(e[Pn]===null)return;let t=e[Pn].indexOf(n);t!==-1&&e[Pn].splice(t,1)}function Ln(e){let n=e[qe];return Bt(n)?n[qe]:n}function Kf(e){return e[Xr]??=[]}function Zf(e){return e.cleanup??=[]}function by(e,n,t,i){let r=Kf(n);r.push(t),e.firstCreatePass&&Zf(e).push(i,r.length-1)}var te={lFrame:Ay(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Df=!1;function Sy(){return te.lFrame.elementDepthCount}function Dy(){te.lFrame.elementDepthCount++}function Qf(){te.lFrame.elementDepthCount--}function Jc(){return te.bindingsEnabled}function Xf(){return te.skipHydrationRootTNode!==null}function Jf(e){return te.skipHydrationRootTNode===e}function eh(){te.skipHydrationRootTNode=null}function O(){return te.lFrame.lView}function Ce(){return te.lFrame.tView}function Ke(){let e=th();for(;e!==null&&e.type===64;)e=e.parent;return e}function th(){return te.lFrame.currentTNode}function wy(){let e=te.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function no(e,n){let t=te.lFrame;t.currentTNode=e,t.isParent=n}function nh(){return te.lFrame.isParent}function ih(){te.lFrame.isParent=!1}function Cy(){return te.lFrame.contextLView}function rh(){return Df}function fs(e){let n=Df;return Df=e,n}function Es(){let e=te.lFrame,n=e.bindingRootIndex;return n===-1&&(n=e.bindingRootIndex=e.tView.bindingStartIndex),n}function Ey(e){return te.lFrame.bindingIndex=e}function Bn(){return te.lFrame.bindingIndex++}function oh(e){let n=te.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function xy(){return te.lFrame.inI18n}function Iy(e,n){let t=te.lFrame;t.bindingIndex=t.bindingRootIndex=e,el(n)}function Ny(){return te.lFrame.currentDirectiveIndex}function el(e){te.lFrame.currentDirectiveIndex=e}function My(e){let n=te.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function tl(){return te.lFrame.currentQueryIndex}function xs(e){te.lFrame.currentQueryIndex=e}function J0(e){let n=e[N];return n.type===2?n.declTNode:n.type===1?e[st]:null}function sh(e,n,t){if(t&4){let r=n,o=e;for(;r=r.parent,r===null&&!(t&1);)if(r=J0(o),r===null||(o=o[pi],r.type&10))break;if(r===null)return!1;n=r,e=o}let i=te.lFrame=Ty();return i.currentTNode=n,i.lView=e,!0}function nl(e){let n=Ty(),t=e[N];te.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function Ty(){let e=te.lFrame,n=e===null?null:e.child;return n===null?Ay(e):n}function Ay(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function Ry(){let e=te.lFrame;return te.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var ah=Ry;function il(){let e=Ry();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function ky(e){return(te.lFrame.contextLView=yy(e,te.lFrame.contextLView))[He]}function wn(){return te.lFrame.selectedIndex}function gi(e){te.lFrame.selectedIndex=e}function lr(){let e=te.lFrame;return Kc(e.tView,e.selectedIndex)}function dr(){te.lFrame.currentNamespace=Kr}function rl(){ex()}function ex(){te.lFrame.currentNamespace=null}function ch(){return te.lFrame.currentNamespace}var Oy=!0;function ol(){return Oy}function Is(e){Oy=e}function wf(e,n=null,t=null,i){let r=lh(e,n,t,i);return r.resolveInjectorInitializers(),r}function lh(e,n=null,t=null,i,r=new Set){let o=[t||ot,cy(e)],s;return new Ji(o,n||Qr(),s||null,r)}var ie=class e{static THROW_IF_NOT_FOUND=Zi;static NULL=new Yr;static create(n,t){if(Array.isArray(n))return wf({name:""},t,n,"");{let i=n.name??"";return wf({name:i},n.parent,n.providers,i)}}static \u0275prov=H({token:e,providedIn:"any",factory:()=>C(Ss)});static __NG_ELEMENT_ID__=-1},P=new g(""),Ve=class{static __NG_ELEMENT_ID__=tx;static __NG_ENV_ID__=n=>n},Pc=class extends Ve{_lView;constructor(n){super(),this._lView=n}get destroyed(){return ar(this._lView)}onDestroy(n){let t=this._lView;return Xc(t,n),()=>_y(t,n)}};function tx(){return new Pc(O())}var Fy=!1,Py=new g(""),Un=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new $e(!1);debugTaskTracker=u(Py,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new L(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=H({token:e,providedIn:"root",factory:()=>new e})}return e})(),Cf=class extends b{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,fy()&&(this.destroyRef=u(Ve,{optional:!0})??void 0,this.pendingTasks=u(Un,{optional:!0})??void 0)}emit(n){let t=T(null);try{super.next(n)}finally{T(t)}}subscribe(n,t,i){let r=n,o=t||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof se&&n.add(a),a}wrapInTimeout(n){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},ne=Cf;function Lc(...e){}function dh(e){let n,t;function i(){e=Lc;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{e(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),i()})),()=>i()}function Ly(e){return queueMicrotask(()=>e()),()=>{e=Lc}}var uh="isAngularZone",hs=uh+"_ID",nx=0,A=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ne(!1);onMicrotaskEmpty=new ne(!1);onStable=new ne(!1);onError=new ne(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Fy}=n;if(typeof Zone>"u")throw new _(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,ox(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(uh)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new _(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new _(909,!1)}run(n,t,i){return this._inner.run(n,t,i)}runTask(n,t,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,ix,Lc,Lc);try{return o.runTask(s,t,i)}finally{o.cancelTask(s)}}runGuarded(n,t,i){return this._inner.runGuarded(n,t,i)}runOutsideAngular(n){return this._outer.run(n)}},ix={};function fh(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function rx(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){dh(()=>{e.callbackScheduled=!1,Ef(e),e.isCheckStableRunning=!0,fh(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),Ef(e)}function ox(e){let n=()=>{rx(e)},t=nx++;e._inner=e._inner.fork({name:"angular",properties:{[uh]:!0,[hs]:t,[hs+t]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(sx(c))return i.invokeTask(o,s,a,c);try{return Kv(e),i.invokeTask(o,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),Zv(e)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return Kv(e),i.invoke(o,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!ax(c)&&n(),Zv(e)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,Ef(e),fh(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function Ef(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function Kv(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function Zv(e){e._nesting--,fh(e)}var ps=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ne;onMicrotaskEmpty=new ne;onStable=new ne;onError=new ne;run(n,t,i){return n.apply(t,i)}runGuarded(n,t,i){return n.apply(t,i)}runOutsideAngular(n){return n()}runTask(n,t,i,r){return n.apply(t,i)}};function sx(e){return Vy(e,"__ignore_ng_zone__")}function ax(e){return Vy(e,"__scheduler_tick__")}function Vy(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var pt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Dt=new g("",{factory:()=>{let e=u(A),n=u(de),t;return i=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw i}):(t??=n.get(pt),t.handleError(i))})}}}),jy={provide:tr,useValue:()=>{let e=u(pt,{optional:!0})},multi:!0},cx=new g("",{factory:()=>{let e=u(P).defaultView;if(!e)return;let n=u(Dt),t=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{e.addEventListener("unhandledrejection",t),e.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(Ve).onDestroy(()=>{e.removeEventListener("error",i),e.removeEventListener("unhandledrejection",t)})}});function hh(){return Vt([ay(()=>{u(cx)})])}function z(e,n){let[t,i,r]=Yu(e,n?.equal),o=t,s=o[Oe];return o.set=i,o.update=r,o.asReadonly=sl.bind(o),o}function sl(){let e=this[Oe];if(e.readonlyFn===void 0){let n=()=>this();n[Oe]=e,e.readonlyFn=n}return e.readonlyFn}var ur=new g("",{factory:()=>lx}),lx="ng";var al=new g(""),fr=new g("",{providedIn:"platform",factory:()=>"unknown"}),Ns=new g(""),vi=new g("",{factory:()=>u(P).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var io=(()=>{class e{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=dx}return e})();function dx(){return new io(O(),Ke())}var gn=class{},Ms=new g("",{factory:()=>!0});var ph=new g(""),cl=(()=>{class e{static \u0275prov=H({token:e,providedIn:"root",factory:()=>new xf})}return e})(),xf=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,i=this.queues.get(t);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,i]of this.queues)t===null?n||=this.flushQueue(i):n||=t.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Vc=class{[Oe];constructor(n){this[Oe]=n}destroy(){this[Oe].destroy()}};function Ht(e,n){let t=n?.injector??u(ie),i=n?.manualCleanup!==!0?t.get(Ve):null,r,o=t.get(io,null,{optional:!0}),s=t.get(gn);return o!==null?(r=hx(o.view,s,e),i instanceof Pc&&i._lView===o.view&&(i=null)):r=px(e,t.get(cl),s),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Vc(r)}var By=V(v({},Ku),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=fs(!1);try{Zu(this)}finally{fs(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=T(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],T(e)}}}),ux=V(v({},By),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(oi(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),fx=V(v({},By),{consumerMarkedDirty(){this.view[j]|=8192,cr(this.view),this.notifier.notify(13)},destroy(){if(oi(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[ui]?.delete(this)}});function hx(e,n,t){let i=Object.create(fx);return i.view=e,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=Uy(i,t),e[ui]??=new Set,e[ui].add(i),i.consumerMarkedDirty(i),i}function px(e,n,t){let i=Object.create(ux);return i.fn=Uy(i,e),i.scheduler=n,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Uy(e,n){return()=>{n(t=>(e.cleanupFns??=[]).push(t))}}function Mt(e){return typeof e=="function"&&e[Oe]!==void 0}var Ts=(()=>{class e{internalPendingTasks=u(Un);scheduler=u(gn);errorHandler=u(Dt);add(){let t=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(t)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(t))}}run(t){let i=this.add();try{t().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=H({token:e,providedIn:"root",factory:()=>new e})}return e})();function Bs(e){return{toString:e}.toString()}var ce=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(ce||{}),vl=class{previousValue;currentValue;firstChange;constructor(n,t,i){this.previousValue=n,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function x_(e,n,t,i){n!==null?n.applyValueToInputSignal(n,i):e[t]=i}var I_=null,Je=(()=>{I_=Hy;let e=()=>Hy;return e.ngInherit=!0,e})();function xx(){return I_}function Hy(e){return e.type.prototype.ngOnChanges&&(e.setInput=Nx),Ix}function Ix(){let e=N_(this),n=e?.current;if(n){let t=e.previous;if(t===hi)e.previous=n;else for(let i in n)t[i]=n[i];e.current=null,this.ngOnChanges(n)}}function Nx(e,n,t,i,r){let o=this.declaredInputs[i],s=N_(e)||Mx(e,{previous:hi,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new vl(l&&l.currentValue,t,c===hi),x_(e,n,r,t)}var Eh="__ngSimpleChanges__";function N_(e){return Object.hasOwn(e,Eh)&&e[Eh]||null}function Mx(e,n){return e[Eh]=n}var zy=[];var pe=function(e,n=null,t){for(let i=0;i<zy.length;i++){let r=zy[i];r(e,n,t)}};function Tx(e,n,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=xx()(n);(t.preOrderHooks??=[]).push(e,s),(t.preOrderCheckHooks??=[]).push(e,s)}r&&(t.preOrderHooks??=[]).push(0-e,r),o&&((t.preOrderHooks??=[]).push(e,o),(t.preOrderCheckHooks??=[]).push(e,o))}function M_(e,n){for(let t=n.directiveStart,i=n.directiveEnd;t<i;t++){let o=e.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(e.contentHooks??=[]).push(-t,s),a&&((e.contentHooks??=[]).push(t,a),(e.contentCheckHooks??=[]).push(t,a)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function hl(e,n,t){T_(e,n,3,t)}function pl(e,n,t,i){(e[j]&3)===t&&T_(e,n,t,i)}function mh(e,n){let t=e[j];(t&3)===n&&(t&=16383,t+=1,e[j]=t)}function T_(e,n,t,i){let r=i!==void 0?e[ir]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(e[ir]+=65536),(a<o||o==-1)&&(Ax(e,t,n,c),e[ir]=(e[ir]&4294901760)+c+2),c++}function $y(e,n){pe(ce.LifecycleHookStart,e,n);let t=T(null);try{n.call(e)}finally{T(t),pe(ce.LifecycleHookEnd,e,n)}}function Ax(e,n,t,i){let r=t[i]<0,o=t[i+1],s=r?-t[i]:t[i],a=e[s];r?e[j]>>14<e[ir]>>16&&(e[j]&3)===n&&(e[j]+=16384,$y(a,o)):$y(a,o)}var oo=-1,pr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function Rx(e){return(e.flags&8)!==0}function kx(e){return(e.flags&16)!==0}function Ox(e,n,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let o=t[i++],s=t[i++],a=t[i++];e.setAttribute(n,s,a,o)}else{let o=r,s=t[++i];Fx(o)?e.setProperty(n,o,s):e.setAttribute(n,o,s),i++}}return i}function A_(e){return e===3||e===4||e===6}function Fx(e){return e.charCodeAt(0)===64}function ao(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?Gy(e,t,r,null,n[++i]):Gy(e,t,r,null,null))}}return e}function Gy(e,n,t,i,r){let o=0,s=e.length;if(n===-1)s=-1;else for(;o<e.length;){let a=e[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<e.length;){let a=e[o];if(typeof a=="number")break;if(a===t){r!==null&&(e[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(e.splice(s,0,n),o=s+1),e.splice(o++,0,t),r!==null&&e.splice(o++,0,r)}function R_(e){return e!==oo}function yl(e){return e&32767}function Px(e){return e>>16}function _l(e,n){let t=Px(e),i=n;for(;t>0;)i=i[pi],t--;return i}var xh=!0;function bl(e){let n=xh;return xh=e,n}var Lx=256,k_=Lx-1,O_=5,Vx=0,Cn={};function jx(e,n,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(er)&&(i=t[er]),i==null&&(i=t[er]=Vx++);let r=i&k_,o=1<<r;n.data[e+(r>>O_)]|=o}function Sl(e,n){let t=F_(e,n);if(t!==-1)return t;let i=n[N];i.firstCreatePass&&(e.injectorIndex=n.length,gh(i.data,e),gh(n,null),gh(i.blueprint,null));let r=sp(e,n),o=e.injectorIndex;if(R_(r)){let s=yl(r),a=_l(r,n),c=a[N].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function gh(e,n){e.push(0,0,0,0,0,0,0,0,n)}function F_(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function sp(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,i=null,r=n;for(;r!==null;){if(i=B_(r),i===null)return oo;if(t++,r=r[pi],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return oo}function Ih(e,n,t){jx(e,n,t)}function Bx(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let i=t.length,r=0;for(;r<i;){let o=t[r];if(A_(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof t[r]=="string";)r++;else{if(o===n)return t[r+1];r=r+2}}}return null}function P_(e,n,t){if(t&8||e!==void 0)return e;zc(n,"NodeInjector")}function L_(e,n,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=e[vn],o=ht(void 0);try{return r?r.get(n,i,t&8):Of(n,i,t&8)}finally{ht(o)}}return P_(i,n,t)}function V_(e,n,t,i=0,r){if(e!==null){if(n[j]&2048&&!(i&2)){let s=$x(e,n,t,i,Cn);if(s!==Cn)return s}let o=j_(e,n,t,i,Cn);if(o!==Cn)return o}return L_(n,t,i,r)}function j_(e,n,t,i,r){let o=Hx(t);if(typeof o=="function"){if(!sh(n,e,i))return i&1?P_(r,t,i):L_(n,t,i,r);try{let s;if(s=o(i),s==null&&!(i&8))zc(t);else return s}finally{ah()}}else if(typeof o=="number"){let s=null,a=F_(e,n),c=oo,l=i&1?n[mt][st]:null;for((a===-1||i&4)&&(c=a===-1?sp(e,n):n[a+8],c===oo||!qy(i,!1)?a=-1:(s=n[N],a=yl(c),n=_l(c,n)));a!==-1;){let d=n[N];if(Wy(o,a,d.data)){let f=Ux(a,n,t,s,i,l);if(f!==Cn)return f}c=n[a+8],c!==oo&&qy(i,n[N].data[a+8]===l)&&Wy(o,a,n)?(s=d,a=yl(c),n=_l(c,n)):a=-1}}return r}function Ux(e,n,t,i,r,o){let s=n[N],a=s.data[e+8],c=i==null?Sn(a)&&xh:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=ml(a,s,t,c,l);return d!==null?Os(n,s,d,a,r):Cn}function ml(e,n,t,i,r){let o=e.providerIndexes,s=n.data,a=o&1048575,c=e.directiveStart,l=e.directiveEnd,d=o>>20,f=i?a:a+d,h=r?a+d:l;for(let p=f;p<h;p++){let m=s[p];if(p<c&&t===m||p>=c&&m.type===t)return p}if(r){let p=s[c];if(p&&Dn(p)&&p.type===t)return c}return null}function Os(e,n,t,i,r){let o=e[t],s=n.data;if(o instanceof pr){let a=o;if(a.resolving)throw kf("");let c=bl(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],d,f=a.injectImpl?ht(a.injectImpl):null,h=sh(e,i,0);try{o=e[t]=a.factory(void 0,r,s,e,i),n.firstCreatePass&&t>=i.directiveStart&&Tx(t,s[t],n)}finally{f!==null&&ht(f),bl(c),a.resolving=!1,ah()}}return o}function Hx(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=e.hasOwnProperty(er)?e[er]:void 0;return typeof n=="number"?n>=0?n&k_:zx:n}function Wy(e,n,t){let i=1<<e;return!!(t[n+(e>>O_)]&i)}function qy(e,n){return!(e&2)&&!(e&1&&n)}var yi=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,i){return V_(this._tNode,this._lView,n,Qi(i),t)}};function zx(){return new yi(Ke(),O())}function At(e){return Bs(()=>{let n=e.prototype.constructor,t=n[us]||Nh(n),i=Object.prototype,r=Object.getPrototypeOf(e.prototype).constructor;for(;r&&r!==i;){let o=r[us]||Nh(r);if(o&&o!==t)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Nh(e){return If(e)?()=>{let n=Nh(et(e));return n&&n()}:di(e)}function $x(e,n,t,i,r){let o=e,s=n;for(;o!==null&&s!==null&&s[j]&2048&&!to(s);){let a=j_(o,s,t,i|2,Cn);if(a!==Cn)return a;let c=o.parent;if(!c){let l=s[Uf];if(l){let d=l.get(t,Cn,i&-5);if(d!==Cn)return d}c=B_(s),s=s[pi]}o=c}return r}function B_(e){let n=e[N],t=n.type;return t===2?n.declTNode:t===1?e[st]:null}function Us(e){return Bx(Ke(),e)}function D(e){return{token:e.token,providedIn:e.autoProvided===!1?null:"root",factory:e.factory,value:void 0}}function Gx(){return ho(Ke(),O())}function ho(e,n){return new B(St(e,n))}var B=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=Gx}return e})();function U_(e){return e instanceof B?e.nativeElement:e}function Wx(){return this._results[Symbol.iterator]()}var Hn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new b}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let i=ry(n);(this._changesDetected=!iy(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Wx};function H_(e){return(e.flags&128)===128}var ap=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(ap||{}),z_=new Map,qx=0;function Yx(){return qx++}function Kx(e){z_.set(e[_n],e)}function Mh(e){z_.delete(e[_n])}var Yy="__ngContext__";function co(e,n){jn(n)?(e[Yy]=n[_n],Kx(n)):e[Yy]=n}function $_(e){return W_(e[Jr])}function G_(e){return W_(e[jt])}function W_(e){for(;e!==null&&!Bt(e);)e=e[jt];return e}var Th;function cp(e){Th=e}function q_(){if(Th!==void 0)return Th;if(typeof document<"u")return document;throw new _(210,!1)}var Y_="r";var K_="di";var lp=new g(""),Z_=!1,Q_=new g("",{factory:()=>Z_});var Ol=new g("");var Ky=new WeakMap;function Zx(e,n){if(e==null||typeof e!="object")return;let t=Ky.get(e);t||(t=new WeakSet,Ky.set(e,t)),t.add(n)}var Qx=(e,n,t,i)=>{};function Xx(e,n,t,i){Qx(e,n,t,i)}function Fl(e){return(e.flags&32)===32}var Jx=()=>null;function X_(e,n,t=!1){return Jx(e,n,t)}function J_(e,n){let t=e.contentQueries;if(t!==null){let i=T(null);try{for(let r=0;r<t.length;r+=2){let o=t[r],s=t[r+1];if(s!==-1){let a=e.data[s];xs(o),a.contentQueries(2,n[s],s)}}}finally{T(i)}}}function Ah(e,n,t){xs(0);let i=T(null);try{n(e,t)}finally{T(i)}}function dp(e,n,t){if(zf(n)){let i=T(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=e.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{T(i)}}}var rn=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(rn||{});var eI={"http://www.w3.org/2000/svg":Kr,"http://www.w3.org/1998/Math/MathML":Yc},ll;function tI(){if(ll===void 0&&(ll=null,fi.trustedTypes))try{ll=fi.trustedTypes.createPolicy("angular",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return ll}function Pl(e){return tI()?.createHTML(e)||e}var dl;function nI(){if(dl===void 0&&(dl=null,fi.trustedTypes))try{dl=fi.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return dl}function Zy(e){return nI()?.createScriptURL(e)||e}var zn=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${jc})`}},Rh=class extends zn{getTypeName(){return"HTML"}},kh=class extends zn{getTypeName(){return"Style"}},Oh=class extends zn{getTypeName(){return"Script"}},Fh=class extends zn{getTypeName(){return"URL"}},Ph=class extends zn{getTypeName(){return"ResourceURL"}};function zt(e){return e instanceof zn?e.changingThisBreaksApplicationSecurity:e}function Gn(e,n){let t=eb(e);if(t!=null&&t!==n){if(t==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${t} (see ${jc})`)}return t===n}function eb(e){return e instanceof zn&&e.getTypeName()||null}function up(e){return new Rh(e)}function fp(e){return new kh(e)}function hp(e){return new Oh(e)}function pp(e){return new Fh(e)}function mp(e){return new Ph(e)}function iI(e){let n=new Vh(e);return rI()?new Lh(n):n}var Lh=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let t=new window.DOMParser().parseFromString(Pl(n),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(n):(t.firstChild?.remove(),t)}catch{return null}}},Vh=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let t=this.inertDocument.createElement("template");return t.innerHTML=Pl(n),t}};function rI(){try{return!!new window.DOMParser().parseFromString(Pl(""),"text/html")}catch{return!1}}var oI=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Hs(e){return e=String(e),e.match(oI)?e:"unsafe:"+e}function Wn(e){let n={};for(let t of e.split(","))n[t]=!0;return n}function zs(...e){let n={};for(let t of e)for(let i in t)t.hasOwnProperty(i)&&(n[i]=!0);return n}var tb=Wn("area,br,col,hr,img,wbr"),nb=Wn("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),ib=Wn("rp,rt"),sI=zs(ib,nb),aI=zs(nb,Wn("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),cI=zs(ib,Wn("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Qy=zs(tb,aI,cI,sI),rb=Wn("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),lI=Wn("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),dI=Wn("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),uI=zs(rb,lI,dI),fI=Wn("script,style,template"),jh=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let t=n.firstChild,i=!0,r=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?i=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,i&&t.firstChild){r.push(t),t=mI(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let o=pI(t);if(o){t=o;break}t=r.pop()}}return this.buf.join("")}startElement(n){let t=Xy(n).toLowerCase();if(!Qy.hasOwnProperty(t))return this.sanitizedSomething=!0,!fI.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!uI.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;rb[a]&&(c=Hs(c)),this.buf.push(" ",s,'="',Jy(c),'"')}return this.buf.push(">"),!0}endElement(n){let t=Xy(n).toLowerCase();Qy.hasOwnProperty(t)&&!tb.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(n){this.buf.push(Jy(n))}};function hI(e,n){return(e.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function pI(e){let n=e.nextSibling;if(n&&e!==n.previousSibling)throw ob(n);return n}function mI(e){let n=e.firstChild;if(n&&hI(e,n))throw ob(n);return n}function Xy(e){let n=e.nodeName;return typeof n=="string"?n:"FORM"}function ob(e){return new Error(`Failed to sanitize html because the element is clobbered: ${e.outerHTML}`)}var gI=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,vI=/([^\#-~ |!])/g;function Jy(e){return e.replace(/&/g,"&amp;").replace(gI,function(n){let t=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((t-55296)*1024+(i-56320)+65536)+";"}).replace(vI,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var ul;function gp(e,n){let t=null;try{ul=ul||iI(e);let i=n?String(n):"";t=ul.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=t.innerHTML,t=ul.getInertBodyElement(i)}while(i!==o);let a=new jh().sanitizeChildren(e_(t)||t);return Pl(a)}finally{if(t){let i=e_(t)||t;for(;i.firstChild;)i.firstChild.remove()}}}function e_(e){return"content"in e&&yI(e)?e.content:null}function yI(e){return e.nodeType===Node.ELEMENT_NODE&&e.nodeName==="TEMPLATE"}var _I=/^>|^->|<!--|-->|--!>|<!-$/g,bI=/(<|>)/g,SI="\u200B$1\u200B";function DI(e){return e.replace(_I,n=>n.replace(bI,SI))}function wI(e,n){return e.createText(n)}function CI(e,n,t){e.setValue(n,t)}function EI(e,n){return e.createComment(DI(n))}function sb(e,n,t){return e.createElement(n,t)}function hr(e,n,t,i,r){e.insertBefore(n,t,i,r)}function ab(e,n,t){e.appendChild(n,t)}function t_(e,n,t,i,r){i!==null?hr(e,n,t,i,r):ab(e,n,t)}function cb(e,n,t,i){e.removeChild(null,n,t,i)}function xI(e,n,t){e.setAttribute(n,"style",t)}function II(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function lb(e,n,t){let{mergedAttrs:i,classes:r,styles:o}=t;i!==null&&Ox(e,n,i),r!==null&&II(e,n,r),o!==null&&xI(e,n,o)}function NI(e,n=!0){if(e[0]!=":")return[null,e];let t=e.indexOf(":",1);if(t===-1){if(n)throw new Error(`Unsupported format "${e}" expecting ":namespace:name"`);return[null,e]}return[e.slice(1,t),e.slice(t+1)]}function Ll(e){let n=ub();return n?n.sanitize(fe.URL,e)||"":Gn(e,"URL")?zt(e):Hs(_s(e))}function db(e){let n=ub();if(n)return Zy(n.sanitize(fe.RESOURCE_URL,e)||"");if(Gn(e,"ResourceURL"))return Zy(zt(e));throw new _(904,!1)}function MI(e,n){switch(TI(e,n)){case fe.RESOURCE_URL:return db;case fe.URL:return Ll;default:return null}}function vp(e,n,t){return MI(n,t)?.(e)??e}function ub(){let e=O();return e&&e[en].sanitizer}function TI(e,n){let[t,i]=AI(e);return my(i,n,t)}function AI(e){e=e.toLowerCase();let n=NI(e,!1);if(n[0])return n;let i=wn()===-1?null:lr(),r=i?.namespace;if(e==="#host"&&i?.type===2){let o=St(i,O());if(o.tagName&&(e=o.tagName.toLowerCase()),r==null){let s=o.namespaceURI;r=s&&eI[s]}}return[r,e]}function RI(e){return e instanceof Function?e():e}function kI(e,n,t){let i=e.length;for(;;){let r=e.indexOf(n,t);if(r===-1)return r;if(r===0||e.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||e.charCodeAt(r+o)<=32)return r}t=r+1}}var fb="ng-template";function OI(e,n,t,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&kI(n[r+1].toLowerCase(),t,0)!==-1)return!0}else if(yp(e))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===t)return!0}return!1}function yp(e){return e.type===4&&e.value!==fb}function FI(e,n,t){let i=e.type===4&&!t?fb:e.value;return n===i}function PI(e,n,t){let i=4,r=e.attrs,o=r!==null?jI(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!tn(i)&&!tn(c))return!1;if(s&&tn(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!FI(e,c,t)||c===""&&n.length===1){if(tn(i))return!1;s=!0}}else if(i&8){if(r===null||!OI(e,r,c,t)){if(tn(i))return!1;s=!0}}else{let l=n[++a],d=LI(c,r,yp(e),t);if(d===-1){if(tn(i))return!1;s=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(tn(i))return!1;s=!0}}}}return tn(i)||s}function tn(e){return(e&1)===0}function LI(e,n,t,i){if(n===null)return-1;let r=0;if(i||!t){let o=!1;for(;r<n.length;){let s=n[r];if(s===e)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return BI(n,e)}function hb(e,n,t=!1){for(let i=0;i<n.length;i++)if(PI(e,n[i],t))return!0;return!1}function VI(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function jI(e){for(let n=0;n<e.length;n++){let t=e[n];if(A_(t))return n}return e.length}function BI(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let i=e[t];if(typeof i=="number")return-1;if(i===n)return t;t++}return-1}function UI(e,n){e:for(let t=0;t<n.length;t++){let i=n[t];if(e.length===i.length){for(let r=0;r<e.length;r++)if(e[r]!==i[r])continue e;return!0}}return!1}function n_(e,n){return e?":not("+n.trim()+")":n}function HI(e){let n=e[0],t=1,i=2,r="",o=!1;for(;t<e.length;){let s=e[t];if(typeof s=="string")if(i&2){let a=e[++t];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!tn(s)&&(n+=n_(o,r),r=""),i=s,o=o||!tn(i);t++}return r!==""&&(n+=n_(o,r)),n}function zI(e){return e.map(HI).join(",")}function $I(e){let n=[],t=[],i=1,r=2;for(;i<e.length;){let o=e[i];if(typeof o=="string")r===2?o!==""&&n.push(o,e[++i]):r===8&&t.push(o);else{if(!tn(r))break;r=o}i++}return t.length&&n.push(1,...t),n}var wt={},En=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(En||{}),GI;function _p(e,n){return GI(e,n)}var dz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Bh=new WeakMap;function pb(e){return e?e[pi]??e:null}var As=new WeakSet;function WI(e,n,t){let i=Bh.get(e);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=pb(t);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],d=c.parentNode;c===n?(i.splice(a,1),As.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function qI(e,n,t){let i=pb(t),r=Bh.get(e);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Bh.set(e,[{el:n,declarationView:i}])}var _i=new Set,Vl=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(Vl||{}),xn=new g(""),i_=new Set;function sn(e){i_.has(e)||(i_.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var jl=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=H({token:e,providedIn:"root",factory:()=>new e})}return e})(),bp=[0,1,2,3],Sp=(()=>{class e{ngZone=u(A);scheduler=u(gn);errorHandler=u(pt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(xn,{optional:!0})}execute(){let t=this.sequences.size>0;t&&pe(ce.AfterRenderHooksStart),this.executing=!0;for(let i of bp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&pe(ce.AfterRenderHooksEnd)}register(t){let{view:i}=t;i!==void 0?((i[rr]??=[]).push(t),cr(i),i[j]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,i){return i?i.run(Vl.AFTER_NEXT_RENDER,t):t()}static \u0275prov=H({token:e,providedIn:"root",factory:()=>new e})}return e})(),Fs=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,i,r,o,s=null){this.impl=n,this.hooks=t,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[rr];n&&(this.view[rr]=n.filter(t=>t!==this))}};function $t(e,n){let t=n?.injector??u(ie);return sn("NgAfterNextRender"),KI(e,t,n,!0)}function YI(e){return e instanceof Function?[void 0,void 0,e,void 0]:[e.earlyRead,e.write,e.mixedReadWrite,e.read]}function KI(e,n,t,i){let r=n.get(jl);r.impl??=n.get(Sp);let o=n.get(xn,null,{optional:!0}),s=t?.manualCleanup!==!0?n.get(Ve):null,a=n.get(io,null,{optional:!0}),c=new Fs(r.impl,YI(e),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var Dp=new g("",{factory:()=>{let e=u(de),n=new Set;return e.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:e}}});function mb(e,n,t){let i=e.get(Dp);if(Array.isArray(n))for(let r of n)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),t?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(e)}function ZI(e,n){let t=e.get(Dp);if(Array.isArray(n))for(let i of n)t.queue.delete(i);else t.queue.delete(n)}function QI(e,n){let t=e.get(Dp);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)t.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function XI(e,n){for(let[t,i]of n)mb(e,i.animateFns)}function r_(e,n,t,i){let r=e?.[bn]?.enter;n!==null&&r&&r.has(t.index)&&XI(i,r)}function o_(e,n,t,i){try{t.get(Ss)}catch{return i(!1)}let r=e?.[bn];r?.enter?.has(n.index)&&ZI(t,r.enter.get(n.index).animateFns);let o=JI(e,n,r);if(o.size===0){let s=!1;if(e){let a=[];Bl(e,n,a),s=a.length>0}if(!s)return i(!1)}e&&_i.add(e[_n]),mb(t,()=>eN(e,n,r||void 0,o,i),r||void 0)}function JI(e,n,t){let i=new Map,r=t?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),e&&r)for(let[o,s]of r){if(i.has(o))continue;let c=e[N].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function eN(e,n,t,i,r){let o=[];if(t&&t.leave)for(let[s]of i){if(!t.leave.has(s))continue;let a=t.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}t.detachedLeaveAnimationFns=void 0}if(e&&Bl(e,n,o),o.length>0){let s=t||e?.[bn];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),nN(e,s.running,r)}else Promise.allSettled(o).then(()=>{e&&_i.delete(e[_n]),r(!0)})}else e&&_i.delete(e[_n]),r(!1)}function Bl(e,n,t){if(n.type&12){let r=e[n.index];if(Bt(r))for(let o=Me;o<r.length;o++){let s=r[o];s[N].type===2&&tN(s,t)}}let i=n.child;for(;i;)Bl(e,i,t),i=i.next}function tN(e,n){let t=e[bn];if(t&&t.leave)for(let r of t.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=e[N].firstChild;for(;i;)Bl(e,i,n),i=i.next}function nN(e,n,t){n.then(()=>{e[bn]?.running===n&&(e[bn].running=void 0,_i.delete(e[_n])),t(!0)})}function ro(e,n,t,i,r,o,s,a){if(r!=null){let c,l=!1;Bt(r)?c=r:jn(r)&&(l=!0,r=r[Jt]);let d=tt(r);e===0&&i!==null?(r_(a,i,o,t),s==null?ab(n,i,d):hr(n,i,d,s||null,!0)):e===1&&i!==null?(r_(a,i,o,t),hr(n,i,d,s||null,!0),WI(o,d,a)):e===2?(a?.[bn]?.leave?.has(o.index)&&qI(o,d,a),As.delete(d),o_(a,o,t,f=>{if(As.has(d)){As.delete(d);return}cb(n,d,l,f)})):e===3&&(As.delete(d),o_(a,o,t,()=>{n.destroyNode(d)})),c!=null&&hN(n,e,t,c,o,i,s)}}function iN(e,n){gb(e,n),n[Jt]=null,n[st]=null}function rN(e,n,t,i,r,o){i[Jt]=r,i[st]=n,Hl(e,i,t,1,r,o)}function gb(e,n){n[en].changeDetectionScheduler?.notify(9),Hl(e,n,n[De],2,null,null)}function oN(e){let n=e[Jr];if(!n)return vh(e[N],e);for(;n;){let t=null;if(jn(n))t=n[Jr];else{let i=n[Me];i&&(t=i)}if(!t){for(;n&&!n[jt]&&n!==e;)jn(n)&&vh(n[N],n),n=n[qe];n===null&&(n=e),jn(n)&&vh(n[N],n),t=n&&n[jt]}n=t}}function wp(e,n){let t=e[sr],i=t.indexOf(n);t.splice(i,1)}function Ul(e,n){if(ar(n))return;let t=n[De];t.destroyNode&&Hl(e,n,t,3,null,null),oN(n)}function vh(e,n){if(ar(n))return;let t=T(null);try{n[j]&=-129,n[j]|=256,n[It]&&oi(n[It]),aN(e,n),sN(e,n),n[N].type===1&&n[De].destroy();let i=n[mi];if(i!==null&&Bt(n[qe])){i!==n[qe]&&wp(i,n);let r=n[yn];r!==null&&r.detachView(e)}Mh(n)}finally{T(t)}}function sN(e,n){let t=e.cleanup,i=n[Xr];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[t[s+1]];t[s].call(a)}i!==null&&(n[Xr]=null);let r=n[Pn];if(r!==null){n[Pn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[ui];if(o!==null){n[ui]=null;for(let s of o)s.destroy()}}function aN(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=n[t[i]];if(!(r instanceof pr)){let o=t[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];pe(ce.LifecycleHookStart,a,c);try{c.call(a)}finally{pe(ce.LifecycleHookEnd,a,c)}}else{pe(ce.LifecycleHookStart,r,o);try{o.call(r)}finally{pe(ce.LifecycleHookEnd,r,o)}}}}}function vb(e,n,t){return cN(e,n.parent,t)}function cN(e,n,t){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return t[Jt];if(Sn(i)){let{encapsulation:r}=e.data[i.directiveStart+i.componentOffset];if(r===rn.None||r===rn.Emulated)return null}return St(i,t)}function yb(e,n,t){return dN(e,n,t)}function lN(e,n,t){return e.type&40?St(e,t):null}var dN=lN,s_;function Cp(e,n,t,i){let r=vb(e,i,n),o=n[De],s=i.parent||n[st],a=yb(s,i,n);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)t_(o,r,t[c],a,!1);else t_(o,r,t,a,!1);s_!==void 0&&s_(o,i,n,t,r)}function Rs(e,n){if(n!==null){let t=n.type;if(t&3)return St(n,e);if(t&4)return Uh(-1,e[n.index]);if(t&8){let i=n.child;if(i!==null)return Rs(e,i);{let r=e[n.index];return Bt(r)?Uh(-1,r):tt(r)}}else{if(t&128)return Rs(e,n.next);if(t&32)return _p(n,e)()||tt(e[n.index]);{let i=_b(e,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Ln(e[mt]);return Rs(r,i)}else return Rs(e,n.next)}}}return null}function _b(e,n){if(n!==null){let i=e[mt][st],r=n.projection;return i.projection[r]}return null}function Uh(e,n){let t=Me+e+1;if(t<n.length){let i=n[t],r=i[N].firstChild;if(r!==null)return Rs(i,r)}return n[or]}function Ep(e,n,t,i,r,o,s){for(;t!=null;){let a=i[vn];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(s&&n===0&&(c&&co(tt(c),i),t.flags|=2),!Fl(t))if(l&8)Ep(e,n,t.child,i,r,o,!1),ro(n,e,a,r,c,t,o,i);else if(l&32){let d=_p(t,i),f;for(;f=d();)ro(n,e,a,r,f,t,o,i);ro(n,e,a,r,c,t,o,i)}else l&16?bb(e,n,i,t,r,o):ro(n,e,a,r,c,t,o,i);t=s?t.projectionNext:t.next}}function Hl(e,n,t,i,r,o){e.type===3?uN(t,i,n,r,o):Ep(t,i,e.firstChild,n,r,o,!1)}function uN(e,n,t,i,r){let s=t[N].firstChild,a=s.next,c=tt(t[s.index]),l=tt(t[a.index]),d=a.index+1,f=t[d];if(n===1||n===0)i!==null&&(f&&f.hasChildNodes()?hr(e,i,f,r,!0):(hr(e,i,c,r,!0),hr(e,i,l,r,!0)));else if(n===2){if(f||(f=document.createDocumentFragment(),t[d]=f),c&&c.parentNode===f)return;let h=c;for(;h!==null;){let p=h.nextSibling;if(f.appendChild(h),h===l)break;h=p}}}function fN(e,n,t){let i=n[De],r=vb(e,t,n),o=t.parent||n[st],s=yb(o,t,n);bb(i,0,n,t,r,s)}function bb(e,n,t,i,r,o){let s=t[mt],c=s[st].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];ro(n,e,t[vn],r,d,i,o,t)}else{let l=c,d=s[qe];H_(i)&&(l.flags|=128),Ep(e,n,l,d,r,o,!0)}}function hN(e,n,t,i,r,o,s){let a=i[or],c=tt(i);if(a!==c&&ro(n,e,t,o,a,r,s),(i[j]&4)===0)for(let l=Me;l<i.length;l++){let d=i[l];Hl(d[N],d,e,n,o,a)}}function pN(e,n,t,i,r){if(n)r?e.addClass(t,i):e.removeClass(t,i);else{let o=i.indexOf("-")===-1?void 0:En.DashCase;r==null?e.removeStyle(t,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=En.Important),e.setStyle(t,i,r,o))}}function xp(e,n,t,i,r,o,s,a,c,l,d){let f=Ne+i,h=f+r,p=mN(f,h),m=typeof l=="function"?l():l;return p[N]={type:e,blueprint:p,template:t,queries:null,viewQuery:a,declTNode:n,data:p.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:m,incompleteFirstPass:!1,ssrId:d}}function mN(e,n){let t=[];for(let i=0;i<n;i++)t.push(i<e?null:wt);return t}function gN(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=xp(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function Ip(e,n,t,i,r,o,s,a,c,l,d){let f=n.blueprint.slice();return f[Jt]=r,f[j]=i|4|128|8|64|1024,(l!==null||e&&e[j]&2048)&&(f[j]|=2048),qf(f),f[qe]=f[pi]=e,f[He]=t,f[en]=s||e&&e[en],f[De]=a||e&&e[De],f[vn]=c||e&&e[vn]||null,f[st]=o,f[_n]=Yx(),f[nr]=d,f[Uf]=l,f[mt]=n.type==2?e[mt]:f,f}function vN(e,n,t){let i=St(n,e),r=gN(t),o=e[en].rendererFactory,s=Np(e,Ip(e,r,null,Sb(t),i,n,null,o.createRenderer(i,t),null,null,null));return e[n.index]=s}function Sb(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function Db(e,n,t,i){if(t===0)return-1;let r=n.length;for(let o=0;o<t;o++)n.push(i),e.blueprint.push(i),e.data.push(null);return r}function Np(e,n){return e[Jr]?e[Bf][jt]=n:e[Jr]=n,e[Bf]=n,n}function E(e=1){wb(Ce(),O(),wn()+e,!1)}function wb(e,n,t,i){if(!i)if((n[j]&3)===3){let o=e.preOrderCheckHooks;o!==null&&hl(n,o,t)}else{let o=e.preOrderHooks;o!==null&&pl(n,o,0,t)}gi(t)}var zl=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(zl||{});function mr(e,n,t,i){let r=T(null);try{let[o,s,a]=e.inputs[t],c=null;(s&zl.SignalBased)!==0&&(c=n[o][Oe]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),e.setInput!==null?e.setInput(n,c,i,t,o):x_(n,c,o,i)}finally{T(r)}}function Cb(e,n,t,i,r){let o=wn(),s=i&2;try{gi(-1),s&&n.length>Ne&&wb(e,n,Ne,!1);let a=s?ce.TemplateUpdateStart:ce.TemplateCreateStart;pe(a,r,t),t(i,r)}finally{gi(o);let a=s?ce.TemplateUpdateEnd:ce.TemplateCreateEnd;pe(a,r,t)}}function $l(e,n,t){wN(e,n,t),(t.flags&64)===64&&CN(e,n,t)}function $s(e,n,t=St){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?t(n,e):e[s];e[r++]=a}}}function yN(e,n,t,i){let o=i.get(Q_,Z_)||t===rn.ShadowDom||t===rn.ExperimentalIsolatedShadowDom,s=e.selectRootElement(n,o);return _N(s),s}function _N(e){bN(e)}var bN=()=>null;function SN(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function DN(e,n,t,i,r,o){let s=n[N];if(Gl(e,s,n,t,i)){Sn(e)&&xb(n,e.index);return}e.type&3&&(t=SN(t)),Eb(e,n,t,i,r,o)}function Eb(e,n,t,i,r,o){if(e.type&3){let s=St(e,n);i=o!=null?o(i,e.value||"",t):i,r.setProperty(s,t,i)}else e.type&12}function xb(e,n){let t=Ut(n,e);t[j]&16||(t[j]|=64)}function wN(e,n,t){let i=t.directiveStart,r=t.directiveEnd;Sn(t)&&vN(n,t,e.data[i+t.componentOffset]),e.firstCreatePass||Sl(t,n);let o=t.initialInputs;for(let s=i;s<r;s++){let a=e.data[s],c=Os(n,e,s,t);if(co(c,n),o!==null&&IN(n,s-i,c,a,t,o),Dn(a)){let l=Ut(t.index,n);l[He]=Os(n,e,s,t)}}}function CN(e,n,t){let i=t.directiveStart,r=t.directiveEnd,o=t.index,s=Ny();try{gi(o);for(let a=i;a<r;a++){let c=e.data[a],l=n[a];el(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&EN(c,l)}}finally{gi(-1),el(s)}}function EN(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function Mp(e,n){let t=e.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let o=t[r];hb(n,o.selectors,!1)&&(i??=[],Dn(o)?i.unshift(o):i.push(o))}return i}function xN(e,n,t,i,r,o){let s=St(e,n);Ib(n[De],s,o,e.value,t,i,r)}function Ib(e,n,t,i,r,o,s){if(o==null)s?.(o,i||"",r),e.removeAttribute(n,r,t);else{let a=s==null?_s(o):s(o,i||"",r);e.setAttribute(n,r,a,t)}}function IN(e,n,t,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];mr(i,t,c,l)}}function Tp(e,n,t,i,r){let o=Ne+t,s=n[N],a=r(s,n,e,i,t);n[o]=a,no(e,!0);let c=e.type===2;return c?(lb(n[De],a,e),(Sy()===0||eo(e))&&co(a,n),Dy()):co(a,n),ol()&&(!c||!Fl(e))&&Cp(s,n,a,e),e}function Ap(e){let n=e;return nh()?ih():(n=n.parent,no(n,!1)),n}function NN(e,n){let t=e[vn];if(!t)return;let i;try{i=t.get(Dt,null)}catch{i=null}i?.(n)}function Gl(e,n,t,i,r){let o=e.inputs?.[i],s=e.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],f=n.data[l];mr(f,t[l],d,r),a=!0}if(o)for(let c of o){let l=t[c],d=n.data[c];mr(d,l,i,r),a=!0}return a}function MN(e,n,t,i,r,o){let s=null,a=null,c=null,l=!1,d=e.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&e.hostDirectiveInputs?.hasOwnProperty(r)){let f=e.hostDirectiveInputs[r];for(let h=0;h<f.length;h+=2){let p=f[h];if(p>=a&&p<=c){let m=n.data[p],y=f[h+1];mr(m,t[p],y,o),l=!0}else if(p>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(mr(i,t[s],r,o),l=!0),l}function TN(e,n){let t=Ut(n,e),i=t[N];AN(i,t);let r=t[Jt];r!==null&&t[nr]===null&&(t[nr]=X_(r,t[vn])),pe(ce.ComponentStart);try{Rp(i,t,t[He])}finally{pe(ce.ComponentEnd,t[He])}}function AN(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function Rp(e,n,t){nl(n);try{let i=e.viewQuery;i!==null&&Ah(1,i,t);let r=e.template;r!==null&&Cb(e,n,r,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[yn]?.finishViewCreation(e),e.staticContentQueries&&J_(e,n),e.staticViewQueries&&Ah(2,e.viewQuery,t);let o=e.components;o!==null&&RN(n,o)}catch(i){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),i}finally{n[j]&=-5,il()}}function RN(e,n){for(let t=0;t<n.length;t++)TN(e,n[t])}function Gs(e,n,t,i){let r=T(null);try{let o=n.tView,a=e[j]&4096?4096:16,c=Ip(e,o,t,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=e[n.index];c[mi]=l;let d=e[yn];return d!==null&&(c[yn]=d.createEmbeddedView(o)),Rp(o,c,t),c}finally{T(r)}}function lo(e,n){return!n||n.firstChild===null||H_(e)}function Ps(e,n,t,i,r=!1){if(e.type===3){let o=e.firstChild,s=o.next,a=tt(n[o.index]),c=tt(n[s.index]),l=a;for(;l!==null&&(i.push(l),l!==c);)l=l.nextSibling;return i}for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let o=n[t.index];if(o!==null)if(Bt(o)){let a=o[or];a!==o[Jt]&&i.push(tt(o)),o[j]&4||Nb(o,i),i.push(a)}else i.push(tt(o));let s=t.type;if(s&8)Ps(e,n,t.child,i);else if(s&32){let a=_p(t,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=_b(n,t);if(Array.isArray(a))i.push(...a);else{let c=Ln(n[mt]);Ps(c[N],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function Nb(e,n){for(let t=Me;t<e.length;t++){let i=e[t],r=i[N].firstChild;r!==null&&Ps(i[N],i,r,n)}}function Mb(e){if(e[rr]!==null){for(let n of e[rr])n.impl.addSequence(n);e[rr].length=0}}var Tb=[];function kN(e){return e[It]??ON(e)}function ON(e){let n=Tb.pop()??Object.create(PN);return n.lView=e,n}function FN(e){e.lView[It]!==e&&(e.lView=null,Tb.push(e))}var PN=V(v({},ni),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{cr(e.lView)},consumerOnSignalRead(){this.lView[It]=this}});function LN(e){let n=e[It]??Object.create(VN);return n.lView=e,n}var VN=V(v({},ni),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=Ln(e.lView);for(;n&&!Ab(n[N]);)n=Ln(n);n&&Yf(n)},consumerOnSignalRead(){this.lView[It]=this}});function Ab(e){return e.type!==2}function Rb(e){if(e[ui]===null)return;let n=!0;for(;n;){let t=!1;for(let i of e[ui])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=t&&!!(e[j]&8192)}}var jN=100;function kb(e,n=0){let i=e[en].rendererFactory,r=!1;r||i.begin?.();try{BN(e,n)}finally{r||i.end?.()}}function BN(e,n){let t=rh();try{fs(!0),Hh(e,n);let i=0;for(;Cs(e);){if(i===jN)throw new _(103,!1);i++,Hh(e,1)}}finally{fs(t)}}function UN(e,n,t,i){if(ar(n))return;let r=n[j],o=!1,s=!1;nl(n);let a=!0,c=null,l=null;o||(Ab(e)?(l=kN(n),c=kn(l)):Xa()===null?(a=!1,l=LN(n),c=kn(l)):n[It]&&(oi(n[It]),n[It]=null));try{qf(n),Ey(e.bindingStartIndex),t!==null&&Cb(e,n,t,2,i);let d=(r&3)===3;if(!o)if(d){let p=e.preOrderCheckHooks;p!==null&&hl(n,p,null)}else{let p=e.preOrderHooks;p!==null&&pl(n,p,0,null),mh(n,0)}if(s||HN(n),Rb(n),Ob(n,0),e.contentQueries!==null&&J_(e,n),!o)if(d){let p=e.contentCheckHooks;p!==null&&hl(n,p)}else{let p=e.contentHooks;p!==null&&pl(n,p,1),mh(n,1)}$N(e,n);let f=e.components;f!==null&&Pb(n,f,0);let h=e.viewQuery;if(h!==null&&Ah(2,h,i),!o)if(d){let p=e.viewCheckHooks;p!==null&&hl(n,p)}else{let p=e.viewHooks;p!==null&&pl(n,p,2),mh(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[qc]){for(let p of n[qc])p();n[qc]=null}o||(Mb(n),n[j]&=-73)}catch(d){throw o||cr(n),d}finally{l!==null&&(ri(l,c),a&&FN(l)),il()}}function Ob(e,n){for(let t=$_(e);t!==null;t=G_(t))for(let i=Me;i<t.length;i++){let r=t[i];Fb(r,n)}}function HN(e){for(let n=$_(e);n!==null;n=G_(n)){if(!(n[j]&2))continue;let t=n[sr];for(let i=0;i<t.length;i++){let r=t[i];Yf(r)}}}function zN(e,n,t){pe(ce.ComponentStart);let i=Ut(n,e);try{Fb(i,t)}finally{pe(ce.ComponentEnd,i[He])}}function Fb(e,n){Zc(e)&&Hh(e,n)}function Hh(e,n){let i=e[N],r=e[j],o=e[It],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&jr(o)),s||=!1,o&&(o.dirty=!1),e[j]&=-9217,s)UN(i,e,i.template,e[He]);else if(r&8192){let a=T(null);try{Rb(e),Ob(e,1);let c=i.components;c!==null&&Pb(e,c,1),Mb(e)}finally{T(a)}}}function Pb(e,n,t){for(let i=0;i<n.length;i++)zN(e,n[i],t)}function $N(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)gi(~r);else{let o=r,s=t[++i],a=t[++i];Iy(s,o);let c=n[o];pe(ce.HostBindingsUpdateStart,c);try{a(2,c)}finally{pe(ce.HostBindingsUpdateEnd,c)}}}}finally{gi(-1)}}function kp(e,n){let t=rh()?64:1088;for(e[en].changeDetectionScheduler?.notify(n);e;){e[j]|=t;let i=Ln(e);if(to(e)&&!i)return e;e=i}return null}function Lb(e,n,t,i){return[e,!0,0,n,null,i,null,t,null,null]}function Vb(e,n){let t=Me+n;if(t<e.length)return e[t]}function Ws(e,n,t,i=!0){let r=n[N];if(GN(r,n,e,t),i){let s=Uh(t,e),a=n[De],c=a.parentNode(e[or]);c!==null&&rN(r,e[st],a,n,c,s)}let o=n[nr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function jb(e,n){let t=Ls(e,n);return t!==void 0&&Ul(t[N],t),t}function Ls(e,n){if(e.length<=Me)return;let t=Me+n,i=e[t];if(i){let r=i[mi];r!==null&&r!==e&&wp(r,i),n>0&&(e[t-1][jt]=i[jt]);let o=bs(e,Me+n);iN(i[N],i);let s=o[yn];s!==null&&s.detachView(o[N]),i[qe]=null,i[jt]=null,i[j]&=-129}return i}function GN(e,n,t,i){let r=Me+i,o=t.length;i>0&&(t[r-1][jt]=n),i<o-Me?(n[jt]=t[r],Ff(t,Me+i,n)):(t.push(n),n[jt]=null),n[qe]=t;let s=n[mi];s!==null&&t!==s&&Bb(s,n);let a=n[yn];a!==null&&a.insertView(e),Qc(n),n[j]|=128}function Bb(e,n){let t=e[sr],i=n[qe];if(jn(i))e[j]|=2;else{let r=i[qe][mt];n[mt]!==r&&(e[j]|=2)}t===null?e[sr]=[n]:t.push(n)}var bi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[N];return Ps(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[He]}set context(n){this._lView[He]=n}get destroyed(){return ar(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[qe];if(Bt(n)){let t=n[ws],i=t?t.indexOf(this):-1;i>-1&&(Ls(n,i),bs(t,i))}this._attachedToViewContainer=!1}Ul(this._lView[N],this._lView)}onDestroy(n){Xc(this._lView,n)}markForCheck(){kp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[j]&=-129}reattach(){Qc(this._lView),this._lView[j]|=128}detectChanges(){this._lView[j]|=1024,kb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new _(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=to(this._lView),t=this._lView[mi];t!==null&&!n&&wp(t,this._lView),gb(this._lView[N],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new _(902,!1);this._appRef=n;let t=to(this._lView),i=this._lView[mi];i!==null&&!t&&Bb(i,this._lView),Qc(this._lView)}};var on=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=WN;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let o=Gs(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new bi(o)}}return e})();function WN(){return Wl(Ke(),O())}function Wl(e,n){return e.type&4?new on(n,e,ho(e,n)):null}function po(e,n,t,i,r){let o=e.data[n];if(o===null)o=qN(e,n,t,i,r),xy()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=i,o.attrs=r;let s=wy();o.injectorIndex=s===null?-1:s.injectorIndex}return no(o,!0),o}function qN(e,n,t,i,r){let o=th(),s=nh(),a=s?o:o&&o.parent,c=e.data[n]=KN(e,a,t,n,i,r);return YN(e,c,o,s),c}function YN(e,n,t,i){e.firstChild===null&&(e.firstChild=n),t!==null&&(i?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function KN(e,n,t,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Xf()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:ch(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function ZN(e){let n=e[Hf]??[],i=e[qe][De],r=[];for(let o of n)o.data[K_]!==void 0?r.push(o):QN(o,i);e[Hf]=r}function QN(e,n){let t=0,i=e.firstChild;if(i){let r=e.data[Y_];for(;t<r;){let o=i.nextSibling;cb(n,i,!1),i=o,t++}}}var XN=()=>null,JN=()=>null;function Dl(e,n){return XN(e,n)}function Ub(e,n,t){return JN(e,n,t)}var Hb=class{},ze=class{},Ie=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>eM()};function eM(){let e=O(),n=Ke(),t=Ut(n.index,e);return(jn(t)?t:e)[De]}var zb=(()=>{class e{static \u0275prov=H({token:e,providedIn:"root",factory:()=>null})}return e})();function $b(e){return e.debugInfo?.className||e.type.name||null}var gl={},wl=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,i){let r=this.injector.get(n,gl,i);return r!==gl||t===gl?r:this.parentInjector.get(n,t,i)}};function Op(e,n,t){return e[n]=t}function tM(e,n){return e[n]}function Tt(e,n,t){if(t===wt)return!1;let i=e[n];return Object.is(i,t)?!1:(e[n]=t,!0)}function nM(e,n,t,i){let r=Tt(e,n,t);return Tt(e,n+1,i)||r}function so(e,n,t){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&Zx(r,o);let s=Sn(e)?Ut(e.index,n):n;kp(s,5);let a=n[He],c=a_(n,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=a_(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function a_(e,n,t,i){let r=T(null);try{return pe(ce.OutputStart,n,t),t(i)!==!1}catch(o){return NN(e,o),!1}finally{pe(ce.OutputEnd,n,t),T(r)}}function Gb(e,n,t,i,r,o,s,a){let c=eo(e),l=!1,d=null;if(!i&&c&&(d=rM(n,t,o,e.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let f=St(e,t),h=i?i(f):f;Xx(t,h,o,a),i||(a.__ngNativeEl__=f);let p=r.listen(h,o,a);if(!iM(o)){let m=i?y=>i(tt(y[e.index])):e.index;Wb(m,n,t,o,a,p,!1)}}return l}function iM(e){return e.startsWith("animation")||e.startsWith("transition")}function rM(e,n,t,i){let r=e.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===t&&r[o+1]===i){let a=n[Xr],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function Wb(e,n,t,i,r,o,s){let a=n.firstCreatePass?Zf(n):null,c=Kf(t),l=c.length;c.push(r,o),a&&a.push(i,e,l,(l+1)*(s?-1:1))}function c_(e,n,t,i,r){let o=null,s=null,a=null,c=!1,l=e.directiveToIndex.get(t.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&e.hostDirectiveOutputs?.hasOwnProperty(i)){let d=e.hostDirectiveOutputs[i];for(let f=0;f<d.length;f+=2){let h=d[f];if(h>=s&&h<=a)c=!0,Cl(e,n,h,d[f+1],i,r);else if(h>a)break}}return t.outputs.hasOwnProperty(i)&&(c=!0,Cl(e,n,o,i,i,r)),c}function Cl(e,n,t,i,r,o){let s=n[t],a=n[N],l=a.data[t].outputs[i],f=s[l].subscribe(o);Wb(e.index,a,n,r,o,f,!0)}function Fp(){oM()}function oM(){let e=O(),n=Ce(),t=Ke();if(n.firstCreatePass&&aM(n,t),t.controlDirectiveIndex===-1)return;sn("NgSignalForms");let i=e[t.controlDirectiveIndex];n.data[t.controlDirectiveIndex].controlDef.create(i,new El(e,n,t))}function Pp(){sM()}function sM(){let e=O(),n=Ce(),t=lr();if(t.controlDirectiveIndex===-1)return;let i=n.data[t.controlDirectiveIndex].controlDef,r=e[t.controlDirectiveIndex];i.update(r,new El(e,n,t))}var El=class{lView;tView;tNode;hasPassThrough;constructor(n,t,i){this.lView=n,this.tView=t,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return St(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,t){let i=this.tView.data[this.tNode.customControlIndex];c_(this.tNode,this.lView,i,n,so(this.tNode,this.lView,t))}listenToCustomControlModel(n){let t=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];c_(this.tNode,this.lView,i,t,so(this.tNode,this.lView,n))}listenToDom(n,t){Gb(this.tNode,this.tView,this.lView,void 0,this.lView[De],n,t,so(this.tNode,this.lView,t))}setInputOnDirectives(n,t){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];mr(a,c,n,t),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];mr(l,d,c,t),o=!0}return o}setCustomControlModelInput(n){let t=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";MN(this.tNode,this.tView,this.lView,t,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let t=this.tView.data[this.tNode.customControlIndex];return(t.signalFormsInputPresence??=this._buildCustomControlInputCache(t))[n]===!0}_buildCustomControlInputCache(n){let t={};for(let i in n.inputs)t[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)t[r.inputs[s]]=!0;let o=l_(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];t[c]=!0}let s=l_(o.directive);s!==null&&i.push(...s)}}}return t}};function l_(e){return typeof e=="function"&&"\u0275dir"in e?e.\u0275dir.hostDirectives??null:null}function aM(e,n,t){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(e.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=e.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}cM(e,n)}function cM(e,n){for(let t=n.directiveStart;t<n.directiveEnd;t++){let i=e.data[t];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(d_(i,"value")){n.flags|=1024,n.customControlIndex=t;return}if(d_(i,"checked")){n.flags|=2048,n.customControlIndex=t;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let t=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[h,p,m]=f;if(c>=p&&c<=m)return n.flags|=r,n.customControlIndex=h,!0}}}return!1};if(t("value",1024)||t("checked",2048))return}}function d_(e,n){return lM(e,n)&&dM(e,n+"Change")}function lM(e,n){return n in e.inputs}function dM(e,n){return n in e.outputs}var zh=Symbol("BINDING");var yr=new g("");function xl(e,n,t){let i=t?e.styles:null,r=t?e.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Bc(r,a);else if(o==2){let c=a,l=n[++s];i=Bc(i,c+": "+l+";")}}t?e.styles=i:e.stylesWithoutHost=i,t?e.classes=r:e.classesWithoutHost=r}function ae(e,n=0){let t=O();if(t===null)return C(e,n);let i=Ke();return V_(i,t,et(e),n)}function qs(){let e="invalid";throw new Error(e)}function qb(e,n,t,i,r){let o=i===null?null:{"":-1},s=r(e,t);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}hM(e,n,t,a,o,c,l)}o!==null&&i!==null&&uM(t,i,o)}function uM(e,n,t){let i=e.localNames=[];for(let r=0;r<n.length;r+=2){let o=t[n[r+1]];if(o==null)throw new _(-301,!1);i.push(n[r],o)}}function fM(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function hM(e,n,t,i,r,o,s){let a=i.length,c=null;for(let h=0;h<a;h++){let p=i[h];c===null&&Dn(p)&&(c=p,fM(e,t,h)),Ih(Sl(t,n),e,p.type)}_M(t,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let p=i[h];p.providersResolver&&p.providersResolver(p)}let l=!1,d=!1,f=Db(e,n,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let p=i[h];if(t.mergedAttrs=ao(t.mergedAttrs,p.hostAttrs),mM(e,t,n,f,p),yM(f,p,r),s!==null&&s.has(p)){let[y,w]=s.get(p);t.directiveToIndex.set(p.type,[f,y+t.directiveStart,w+t.directiveStart])}else(o===null||!o.has(p))&&t.directiveToIndex.set(p.type,f);p.contentQueries!==null&&(t.flags|=4),(p.hostBindings!==null||p.hostAttrs!==null||p.hostVars!==0)&&(t.flags|=64);let m=p.type.prototype;!l&&(m.ngOnChanges||m.ngOnInit||m.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(m.ngOnChanges||m.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),f++}pM(e,t,o)}function pM(e,n,t){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=e.data[i];if(t===null||!t.has(r))u_(0,n,r,i),u_(1,n,r,i),h_(n,i,!1);else{let o=t.get(r);f_(0,n,o,i),f_(1,n,o,i),h_(n,i,!0)}}}function u_(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;e===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),Yb(n,o)}}function f_(e,n,t,i){let r=e===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;e===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),Yb(n,s)}}function Yb(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function h_(e,n,t){let{attrs:i,inputs:r,hostDirectiveInputs:o}=e;if(i===null||!t&&r===null||t&&o===null||yp(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function mM(e,n,t,i,r){e.data[i]=r;let o=r.factory||(r.factory=di(r.type,!0)),s=new pr(o,Dn(r),ae,null);e.blueprint[i]=s,t[i]=s,gM(e,n,i,Db(e,t,r.hostVars,wt),r)}function gM(e,n,t,i,r){let o=r.hostBindings;if(o){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~n.index;vM(s)!=a&&s.push(a),s.push(t,i,o)}}function vM(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function yM(e,n,t){if(t){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)t[n.exportAs[i]]=e;Dn(n)&&(t[""]=e)}}function _M(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function Lp(e,n,t,i,r,o,s,a){let c=n[N],l=c.consts,d=Nt(l,s),f=po(c,e,t,i,d);return o&&qb(c,n,f,Nt(l,a),r),f.mergedAttrs=ao(f.mergedAttrs,f.attrs),f.attrs!==null&&xl(f,f.attrs,!1),f.mergedAttrs!==null&&xl(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function Vp(e,n){M_(e,n),zf(n)&&e.queries.elementEnd(n)}function bM(e,n,t,i,r,o){let s=n.consts,a=Nt(s,r),c=po(n,e,t,i,a);if(c.mergedAttrs=ao(c.mergedAttrs,c.attrs),o!=null){let l=Nt(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&xl(c,c.attrs,!1),c.mergedAttrs!==null&&xl(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var Kb=typeof ShadowRoot<"u",SM=typeof Document<"u";function DM(e){return Object.keys(e).map(n=>{let[t,i,r]=e[n],o={propName:t,templateName:n,isSignal:(i&zl.SignalBased)!==0};return r&&(o.transform=r),o})}function wM(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function CM(e,n,t){let i=n instanceof de?n:n?.injector;return i&&e.getStandaloneInjector!==null&&(i=e.getStandaloneInjector(i)||i),i?new wl(t,i):t}function EM(e){let n=e.get(ze,null);if(n===null)throw new _(407,!1);let t=e.get(zb,null),i=e.get(gn,null),r=e.get(xn,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function xM(e,n){let t=Zb(e);return sb(n,t,t==="svg"?Kr:t==="math"?Yc:null)}function IM(e){if(e?.toLowerCase()==="script")throw new _(905,!1)}function Zb(e){return(e.selectors[0][0]||"div").toLowerCase()}var gr=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=DM(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=wM(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=zI(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,i,r,o,s){pe(ce.DynamicComponentStart);let a=T(null);try{let c=this.componentDef,l=CM(c,r||this.ngModule,n),d=EM(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate($b(c),()=>this.createComponentRef(d,l,t,i,o,s)):this.createComponentRef(d,l,t,i,o,s)}finally{T(a)}}createComponentRef(n,t,i,r,o,s){let a=this.componentDef,c=NM(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?yN(l,r,a.encapsulation,t):xM(a,l);IM(d?.tagName);let f=t.get(yr,null),h=MM(d,()=>t.get(P,null)??q_());f&&f.addHost(h);let p=s?.some(p_)||o?.some(w=>typeof w!="function"&&w.bindings.some(p_)),m=Ip(null,c,null,512|Sb(a),null,null,n,l,t,null,X_(d,t,!0));f&&Kb&&h instanceof ShadowRoot&&Xc(m,()=>{f.removeHost(h)}),m[Ne]=d,nl(m);let y=null;try{let w=Lp(Ne,m,2,"#host",()=>c.directiveRegistry,!0,0);lb(l,d,w),co(d,m),$l(c,m,w),dp(c,w,m),Vp(c,w),i!==void 0&&AM(w,this.ngContentSelectors,i),y=Ut(w.index,m),m[He]=y[He],Rp(c,m,null)}catch(w){throw y!==null&&Mh(y),Mh(m),w}finally{pe(ce.DynamicComponentEnd),il()}return new Il(this.componentType,m,!!p)}};function NM(e,n,t,i){let r=e?["ng-version","22.1.0"]:$I(n.selectors[0]),o=null,s=null,a=0;if(t)for(let d of t)a+=d[zh].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let h of f.bindings){a+=h[zh].requiredVars;let p=d+1;h.create&&(h.targetIdx=p,(o??=[]).push(h)),h.update&&(h.targetIdx=p,(s??=[]).push(h))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,h=Rf(f);c.push(h)}return xp(0,null,TM(o,s),1,a,c,null,null,null,[r],null)}function MM(e,n){let t=e.getRootNode?.();return SM&&t instanceof Document?t.head:t&&Kb&&t instanceof ShadowRoot?t:n().head}function TM(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let i of e)i.create();if(t&2&&n)for(let i of n)i.update()}}function p_(e){let n=e[zh].kind;return n==="input"||n==="twoWay"}var Il=class extends Hb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Kc(t[N],Ne),this.location=ho(this._tNode,t),this.instance=Ut(this._tNode.index,t)[He],this.hostView=this.changeDetectorRef=new bi(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let r=this._rootLView,o=Gl(i,r[N],r,n,t);this.previousInputValues.set(n,t);let s=Ut(i.index,r);kp(s,1)}get injector(){return new yi(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function AM(e,n,t){let i=e.projection=[];for(let r=0;r<n.length;r++){let o=t[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Rt=(()=>{class e{static __NG_ELEMENT_ID__=RM}return e})();function RM(){let e=Ke();return Qb(e,O())}var $h=class e extends Rt{_lContainer;_hostTNode;_hostLView;constructor(n,t,i){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=i}get element(){return ho(this._hostTNode,this._hostLView)}get injector(){return new yi(this._hostTNode,this._hostLView)}get parentInjector(){let n=sp(this._hostTNode,this._hostLView);if(R_(n)){let t=_l(n,this._hostLView),i=yl(n),r=t[N].data[i+8];return new yi(r,t)}else return new yi(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=m_(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-Me}createEmbeddedView(n,t,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Dl(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,r,lo(this._hostTNode,s)),a}createComponent(n,t,i,r,o,s,a){let c,l=t||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new gr(Vn(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let G=this.parentInjector.get(de,null);G&&(o=G)}let h=Vn(d.componentType??{}),p=Dl(this._lContainer,h?.id??null),m=p?.firstChild??null,y=d.create(f,r,m,o,s,a);return this.insertImpl(y.hostView,c,lo(this._hostTNode,p)),y}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,i){let r=n._lView;if(vy(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[qe],l=new e(c,c[st],c[qe]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(t),s=this._lContainer;return Ws(s,r,o,i),n.attachToViewContainerRef(),Ff(yh(s),o,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=m_(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),i=Ls(this._lContainer,t);i&&(bs(yh(this._lContainer),t),Ul(i[N],i))}detach(n){let t=this._adjustIndex(n,-1),i=Ls(this._lContainer,t);return i&&bs(yh(this._lContainer),t)!=null?new bi(i):null}_adjustIndex(n,t=0){return n??this.length+t}};function m_(e){return e[ws]}function yh(e){return e[ws]||(e[ws]=[])}function Qb(e,n){let t,i=n[e.index];return Bt(i)?t=i:(t=Lb(i,n,null,e),n[e.index]=t,Np(n,t)),OM(t,n,e,i),new $h(t,e,n)}function kM(e,n){let t=e[De],i=t.createComment(""),r=St(n,e),o=t.parentNode(r);return hr(t,o,i,t.nextSibling(r),!1),i}var OM=LM,FM=()=>!1;function PM(e,n,t){return FM(e,n,t)}function LM(e,n,t,i){if(e[or])return;let r;t.type&8?r=tt(i):r=kM(n,t),e[or]=r}var Gh=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},Wh=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let i=n.contentQueries!==null?n.contentQueries[0]:t.length,r=[];for(let o=0;o<i;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new e(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)Bp(n,t).matches!==null&&this.queries[t].setDirty()}},Nl=class{flags;read;predicate;constructor(n,t,i=null){this.flags=t,this.read=i,typeof n=="string"?this.predicate=HM(n):this.predicate=n}},qh=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,t!==null?t.push(o):t=[o])}return t!==null?new e(t):null}template(n,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Yh=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,t,VM(t,o)),this.matchTNodeWithReadOption(n,t,ml(t,n,o,!1,!1))}else i===on?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,ml(t,n,i,!1,!1))}matchTNodeWithReadOption(n,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===B||r===Rt||r===on&&t.type&4)this.addMatch(t.index,-2);else{let o=ml(t,n,r,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,i)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function VM(e,n){let t=e.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===n)return t[i+1]}return null}function jM(e,n){return e.type&11?ho(e,n):e.type&4?Wl(e,n):null}function BM(e,n,t,i){return t===-1?jM(n,e):t===-2?UM(e,n,i):Os(e,e[N],t,n)}function UM(e,n,t){if(t===B)return ho(n,e);if(t===on)return Wl(n,e);if(t===Rt)return Qb(n,e)}function Xb(e,n,t,i){let r=n[yn].queries[i];if(r.matches===null){let o=e.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(BM(n,d,s[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Kh(e,n,t,i){let r=e.queries.getByIndex(t),o=r.matches;if(o!==null){let s=Xb(e,n,r,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let f=Me;f<d.length;f++){let h=d[f];h[mi]===h[qe]&&Kh(h[N],h,l,i)}if(d[sr]!==null){let f=d[sr];for(let h=0;h<f.length;h++){let p=f[h];Kh(p[N],p,l,i)}}}}}return i}function jp(e,n){return e[yn].queries[n].queryList}function Jb(e,n,t){let i=new Hn((t&4)===4);return by(e,n,i,i.destroy),(n[yn]??=new Wh).queries.push(new Gh(i))-1}function eS(e,n,t){let i=Ce();return i.firstCreatePass&&(nS(i,new Nl(e,n,t),-1),(n&2)===2&&(i.staticViewQueries=!0)),Jb(i,O(),n)}function tS(e,n,t,i){let r=Ce();if(r.firstCreatePass){let o=Ke();nS(r,new Nl(n,t,i),o.index),zM(r,e),(t&2)===2&&(r.staticContentQueries=!0)}return Jb(r,O(),t)}function HM(e){return e.split(",").map(n=>n.trim())}function nS(e,n,t){e.queries===null&&(e.queries=new qh),e.queries.track(new Yh(n,t))}function zM(e,n){let t=e.contentQueries||(e.contentQueries=[]),i=t.length?t[t.length-1]:-1;n!==i&&t.push(e.queries.length-1,n)}function Bp(e,n){return e.queries.getByIndex(n)}function iS(e,n){let t=e[N],i=Bp(t,n);return i.crossesNgTemplate?Kh(t,e,n,[]):Xb(t,e,i,n)}function rS(e,n,t){let i,r=Jo(()=>{i._dirtyCounter();let o=$M(i,e);if(n&&o===void 0)throw new _(-951,!1);return o});return i=r[Oe],i._dirtyCounter=z(0),i._flatValue=void 0,r}function Up(e){return rS(!0,!1,e)}function Hp(e){return rS(!0,!0,e)}function oS(e,n){let t=e[Oe];t._lView=O(),t._queryIndex=n,t._queryList=jp(t._lView,n),t._queryList.onDirty(()=>t._dirtyCounter.update(i=>i+1))}function $M(e,n){let t=e._lView,i=e._queryIndex;if(t===void 0||i===void 0||t[j]&4)return n?void 0:ot;let r=jp(t,i),o=iS(t,i);return r.reset(o,U_),n?r.first:r._changesDetected||e._flatValue===void 0?e._flatValue=r.toArray():e._flatValue}function In(e){return!!e&&typeof e.then=="function"}function ql(e){return!!e&&typeof e.subscribe=="function"}var $n=class{},Yl=class{};var Ml=class extends $n{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,t,i,r=!0){super(),this.ngModuleType=n,this._parent=t;let o=Xv(n);this._bootstrapComponents=RI(o.bootstrap),this._r3Injector=lh(n,t,[{provide:$n,useValue:this},...i],ms(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Tl=class extends Yl{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Ml(this.moduleType,n,[])}};var Vs=class extends $n{injector;instance=null;constructor(n){super();let t=new Ji([...n.providers,{provide:$n,useValue:this}],n.parent||Qr(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function mo(e,n,t=null){return new Vs({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var GM=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Lf(!1,t.type),r=i.length>0?mo([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=H({token:e,providedIn:"environment",factory:()=>new e(C(de))})}return e})();function $(e){return Bs(()=>{let n=sS(e),t=V(v({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection!==ap.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(GM).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||rn.Emulated,styles:e.styles||ot,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&sn("NgStandalone"),aS(t);let i=e.dependencies;return t.directiveDefs=g_(i,WM),t.pipeDefs=g_(i,Jv),t.id=KM(t),t})}function WM(e){return Vn(e)||Rf(e)}function R(e){return Bs(()=>({type:e.type,bootstrap:e.bootstrap||ot,declarations:e.declarations||ot,imports:e.imports||ot,exports:e.exports||ot,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function qM(e,n){if(e==null)return hi;let t={};for(let i in e)if(e.hasOwnProperty(i)){let r=e[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=zl.None,c=null),t[o]=[i,a,c],n[o]=s}return t}function YM(e){if(e==null)return hi;let n={};for(let t in e)e.hasOwnProperty(t)&&(n[e[t]]=t);return n}function k(e){return Bs(()=>{let n=sS(e);return aS(n),n})}function zp(e){return{type:e.type,name:e.name,factory:null,pure:e.pure!==!1,standalone:e.standalone??!0,onDestroy:e.type.prototype.ngOnDestroy||null}}function sS(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||hi,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||ot,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:qM(e.inputs,n),outputs:YM(e.outputs),debugInfo:null}}function aS(e){e.features?.forEach(n=>n(e))}function g_(e,n){return e?()=>{let t=typeof e=="function"?e():e,i=[];for(let r of t){let o=n(r);o!==null&&i.push(o)}return i}:null}function KM(e){let n=0,t=typeof e.consts=="function"?"":e.consts,i=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var $p=new g("");function Kl(e){return Vt([{provide:$p,multi:!0,useValue:e}])}var Gp=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=u($p,{optional:!0})??[];injector=u(ie);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let o=Ye(this.injector,r);if(In(o))t.push(o);else if(ql(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function Wp(e){return n=>{n.controlDef={create:(t,i)=>{t?.\u0275ngControlCreate(i)},update:(t,i)=>{t?.\u0275ngControlUpdate?.(i)},passThroughInput:e}}}function ZM(e){return Object.getPrototypeOf(e.prototype).constructor}function ke(e){let n=ZM(e.type),t=!0,i=[e];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,vs)?n[vs]:void 0,s=Object.hasOwn(n,ys)?n[ys]:void 0;if(Dn(e))r=o??s;else{if(o)throw new _(903,!1);r=s}if(r){if(t){i.push(r);let c=e;c.inputs=_h(e.inputs),c.declaredInputs=_h(e.declaredInputs),c.outputs=_h(e.outputs);let l=r.hostBindings;l&&tT(e,l);let d=r.viewQuery,f=r.contentQueries;if(d&&JM(e,d),f&&eT(e,f),QM(e,r),Qv(e.outputs,r.outputs),Dn(r)&&r.data.animation){let h=e.data;h.animation=(h.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let c=0;c<a.length;c++){let l=a[c];l&&l.ngInherit&&l(e),l===ke&&(t=!1)}}n=Object.getPrototypeOf(n)}XM(i)}function QM(e,n){for(let t in n.inputs){if(!n.inputs.hasOwnProperty(t)||e.inputs.hasOwnProperty(t))continue;let i=n.inputs[t];i!==void 0&&(e.inputs[t]=i,e.declaredInputs[t]=n.declaredInputs[t])}}function XM(e){let n=0,t=null;for(let i=e.length-1;i>=0;i--){let r=e[i];r.hostVars=n+=r.hostVars,r.hostAttrs=ao(r.hostAttrs,t=ao(t,r.hostAttrs))}}function _h(e){return e===hi?{}:e===ot?[]:e}function JM(e,n){let t=e.viewQuery;t?e.viewQuery=(i,r)=>{n(i,r),t(i,r)}:e.viewQuery=n}function eT(e,n){let t=e.contentQueries;t?e.contentQueries=(i,r,o)=>{n(i,r,o),t(i,r,o)}:e.contentQueries=n}function tT(e,n){let t=e.hostBindings;t?e.hostBindings=(i,r)=>{n(i,r),t(i,r)}:e.hostBindings=n}function cS(e,n,t,i,r,o,s,a){if(t.firstCreatePass){e.mergedAttrs=ao(e.mergedAttrs,e.attrs);let d=e.tView=xp(2,e,r,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}a&&(e.flags|=a),no(e,!1);let c=iT(t,n,e,i);ol()&&Cp(t,n,c,e),co(c,n);let l=Lb(c,n,c,e);n[i+Ne]=l,Np(n,l),PM(l,e,n)}function nT(e,n,t,i,r,o,s,a,c,l,d){let f=t+Ne,h;return n.firstCreatePass?(h=po(n,f,4,s||null,a||null),Jc()&&qb(n,e,h,Nt(n.consts,l),Mp),M_(n,h)):h=n.data[f],cS(h,e,n,t,i,r,o,c),eo(h)&&$l(n,e,h),l!=null&&$s(e,h,d),h}function uo(e,n,t,i,r,o,s,a,c,l,d){let f=t+Ne,h;if(n.firstCreatePass){if(h=po(n,f,4,s||null,a||null),l!=null){let p=Nt(n.consts,l);h.localNames=[];for(let m=0;m<p.length;m+=2)h.localNames.push(p[m],-1)}}else h=n.data[f];return cS(h,e,n,t,i,r,o,c),l!=null&&$s(e,h,d),h}function Si(e,n,t,i,r,o,s,a){let c=O(),l=Ce(),d=Nt(l.consts,o);return nT(c,l,e,n,t,i,r,d,void 0,s,a),Si}function Zl(e,n,t,i,r,o,s,a){let c=O(),l=Ce(),d=Nt(l.consts,o);return uo(c,l,e,n,t,i,r,d,void 0,s,a),Zl}var iT=rT;function rT(e,n,t,i){return Is(!0),n[De].createComment("")}var Ql=(()=>{class e{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"platform"})}return e})();var qp=new g("");var go=new g("");function lS(){qu(()=>{let e="";throw new _(600,e)})}var oT=10;var Ct=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(Dt);afterRenderManager=u(jl);zonelessEnabled=u(Ms);rootEffectScheduler=u(cl);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new b;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(Un);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(U(t=>!t))}constructor(){u(xn,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=u(de);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=ie.NULL){return this._injector.get(A).run(()=>{if(pe(ce.BootstrapComponentStart),!this._injector.get(Gp).done){let G="";throw new _(405,G)}let a=Vn(t),c=this._injector.get($n),l=new gr(a,c);this.componentTypes.push(t);let{hostElement:d,directives:f,bindings:h}=sT(i),p=d||l.selector,m=l.create(r,[],p,c.injector,f,h),y=m.location.nativeElement,w=m.injector.get(qp,null);return w?.registerApplication(y),m.onDestroy(()=>{this.detachView(m.hostView),ks(this.components,m),w?.unregisterApplication(y)}),this._loadComponent(m),pe(ce.BootstrapComponentEnd,m),m})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){pe(ce.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Vl.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw pe(ce.ChangeDetectionEnd),new _(101,!1);let t=T(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,T(t),this.afterTick.next(),pe(ce.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ze,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<oT;){pe(ce.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{pe(ce.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Cs(r))continue;let o=i&&!this.zonelessEnabled?0:1;kb(r,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Cs(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;ks(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(go,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>ks(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new _(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function sT(e){return e===void 0||typeof e=="string"||e instanceof Element?{hostElement:e}:e}function ks(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function Xl(e,n){let t=O(),i=Bn();if(Tt(t,i,n)){let r=Ce(),o=lr();if(Gl(o,r,t,e,n))Sn(o)&&xb(t,o.index);else{let a=St(o,t);Ib(t[De],a,null,o.value,e,n,null)}}return Xl}function oe(e,n,t,i){let r=O(),o=Bn();if(Tt(r,o,n)){let s=Ce(),a=lr();xN(a,r,e,n,t,i)}return oe}var Zh=class{destroy(n){}updateValue(n,t){}swap(n,t){let i=Math.min(n,t),r=Math.max(n,t),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,t){this.attach(t,this.detach(n))}};function bh(e,n,t,i,r){return e===t&&Object.is(n,i)?1:Object.is(r(e,n),r(t,i))?-1:0}function aT(e,n,t,i){let r,o,s=0,a=e.length-1,c=void 0;if(Array.isArray(n)){T(i);let l=n.length-1;for(T(null);s<=a&&s<=l;){let d=e.at(s),f=n[s],h=bh(s,d,s,f,t);if(h!==0){h<0&&e.updateValue(s,f),s++;continue}let p=e.at(a),m=n[l],y=bh(a,p,l,m,t);if(y!==0){y<0&&e.updateValue(a,m),a--,l--;continue}let w=t(s,d),G=t(a,p),xe=t(s,f);if(Object.is(xe,G)){let Ue=t(l,m);Object.is(Ue,w)?(e.swap(s,a),e.updateValue(a,m),l--,a--):e.move(a,s),e.updateValue(s,f),s++;continue}if(r??=new Al,o??=y_(e,s,a,t),Qh(e,r,s,xe))e.updateValue(s,f),s++,a++;else if(o.has(xe))r.set(w,e.detach(s)),a--;else{let Ue=e.create(s,n[s]);e.attach(s,Ue),s++,a++}}for(;s<=l;)v_(e,r,t,s,n[s]),s++}else if(n!=null){T(i);let l=n[Symbol.iterator]();T(null);let d=l.next();for(;!d.done&&s<=a;){let f=e.at(s),h=d.value,p=bh(s,f,s,h,t);if(p!==0)p<0&&e.updateValue(s,h),s++,d=l.next();else{r??=new Al,o??=y_(e,s,a,t);let m=t(s,h);if(Qh(e,r,s,m))e.updateValue(s,h),s++,a++,d=l.next();else if(!o.has(m))e.attach(s,e.create(s,h)),s++,a++,d=l.next();else{let y=t(s,f);r.set(y,e.detach(s)),a--}}}for(;!d.done;)v_(e,r,t,e.length,d.value),d=l.next()}for(;s<=a;)e.destroy(e.detach(a--));r?.forEach(l=>{e.destroy(l)})}function Qh(e,n,t,i){return n!==void 0&&n.has(i)?(e.attach(t,n.get(i)),n.delete(i),!0):!1}function v_(e,n,t,i,r){if(Qh(e,n,i,t(i,r)))e.updateValue(i,r);else{let o=e.create(i,r);e.attach(i,o)}}function y_(e,n,t,i){let r=new Set;for(let o=n;o<=t;o++)r.add(i(o,e.at(o)));return r}var Al=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let t=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(n,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,t){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(n,t)}forEach(n){for(let[t,i]of this.kvMap)if(n(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,t)}}};function me(e,n,t,i,r,o,s,a){sn("NgControlFlow");let c=O(),l=Ce(),d=Nt(l.consts,o);return uo(c,l,e,n,t,i,r,d,256,s,a),Yp}function Yp(e,n,t,i,r,o,s,a){sn("NgControlFlow");let c=O(),l=Ce(),d=Nt(l.consts,o);return uo(c,l,e,n,t,i,r,d,512,s,a),Yp}function ge(e,n){sn("NgControlFlow");let t=O(),i=Bn(),r=t[i]!==wt?t[i]:-1,o=r!==-1?Rl(t,Ne+r):void 0,s=0;if(Tt(t,i,e)){let a=T(null);try{if(o!==void 0&&jb(o,s),e!==-1){let c=Ne+e,l=Rl(t,c),d=tp(t[N],c),f=Ub(l,d,t),h=Gs(t,d,n,{dehydratedView:f});Ws(l,h,s,lo(d,f))}}finally{T(a)}}else if(o!==void 0){let a=Vb(o,s);a!==void 0&&(a[He]=n)}}var Xh=class{lContainer;$implicit;$index;constructor(n,t,i){this.lContainer=n,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Me}};function Ys(e){return e}function Kp(e,n){return n}var Jh=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,t,i){this.hasEmptyBlock=n,this.trackByFn=t,this.liveCollection=i}};function _r(e,n,t,i,r,o,s,a,c,l,d,f,h){sn("NgControlFlow");let p=O(),m=Ce(),y=c!==void 0,w=O(),G=a?s.bind(w[mt][He]):s,xe=new Jh(y,G);w[Ne+e]=xe,uo(p,m,e+1,n,t,i,r,Nt(m.consts,o),256),y&&uo(p,m,e+2,c,l,d,f,Nt(m.consts,h),512)}var ep=class extends Zh{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,t,i){super(),this.lContainer=n,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Me}at(n){return this.getLView(n)[He].$implicit}attach(n,t){let i=t[nr];this.needsIndexUpdate||=n!==this.length,Ws(this.lContainer,t,n,lo(this.templateTNode,i)),cT(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,lT(this.lContainer,n),dT(this.lContainer,n)}create(n,t){let i=Dl(this.lContainer,this.templateTNode.tView.ssrId);return Gs(this.hostLView,this.templateTNode,new Xh(this.lContainer,t,n),{dehydratedView:i})}destroy(n){Ul(n[N],n)}updateValue(n,t){this.getLView(n)[He].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[He].$index=n}getLView(n){return uT(this.lContainer,n)}};function br(e){let n=T(null),t=wn();try{let i=O(),r=i[N],o=i[t],s=t+1,a=Rl(i,s);if(o.liveCollection===void 0){let l=tp(r,s);o.liveCollection=new ep(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(aT(c,e,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=Bn(),d=c.length===0;if(Tt(i,l,d)){let f=t+2,h=Rl(i,f);if(d){let p=tp(r,f),m=Ub(h,p,i),y=Gs(i,p,void 0,{dehydratedView:m});Ws(h,y,0,lo(p,m))}else r.firstUpdatePass&&ZN(h),jb(h,0)}}}finally{T(n)}}function Rl(e,n){return e[n]}function cT(e,n){if(e.length<=Me)return;let t=Me+n,i=e[t],r=i?i[bn]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[vn];QI(o,r),_i.delete(i[_n]),r.detachedLeaveAnimationFns=void 0}}function lT(e,n){if(e.length<=Me)return;let t=Me+n,i=e[t],r=i?i[bn]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function dT(e,n){return Ls(e,n)}function uT(e,n){return Vb(e,n)}function tp(e,n){return Kc(e,n)}function be(e,n,t){let i=O(),r=Bn();if(Tt(i,r,n)){let o=Ce(),s=lr();DN(s,i,e,n,i[De],t)}return be}function np(e,n,t,i,r){Gl(n,e,t,r?"class":"style",i)}function S(e,n,t,i){let r=O(),o=r[N],s=e+Ne,a=o.firstCreatePass?Lp(s,r,2,n,Mp,Jc(),t,i):o.data[s];if(Sn(a)){let c=r[en].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate($b(l),()=>(__(e,n,r,a,i),S))}}return __(e,n,r,a,i),S}function __(e,n,t,i,r){if(Tp(i,t,e,n,dS),eo(i)){let o=t[N];$l(o,t,i),dp(o,i,t)}r!=null&&$s(t,i)}function I(){let e=Ce(),n=Ke(),t=Ap(n);return e.firstCreatePass&&Vp(e,t),Jf(t)&&eh(),Qf(),t.classesWithoutHost!=null&&Rx(t)&&np(e,t,O(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&kx(t)&&np(e,t,O(),t.stylesWithoutHost,!1),I}function ve(e,n,t,i){return S(e,n,t,i),I(),ve}function nt(e,n,t,i){let r=O(),o=r[N],s=e+Ne,a=o.firstCreatePass?bM(s,o,2,n,t,i):o.data[s];return Tp(a,r,e,n,dS),i!=null&&$s(r,a),nt}function it(){let e=Ke(),n=Ap(e);return Jf(n)&&eh(),Qf(),it}function an(e,n,t,i){return nt(e,n,t,i),it(),an}var dS=(e,n,t,i,r)=>(Is(!0),sb(n[De],i,ch()));function Zp(e,n,t){let i=O(),r=i[N],o=e+Ne,s=r.firstCreatePass?Lp(o,i,8,"ng-container",Mp,Jc(),n,t):r.data[o];if(Tp(s,i,e,"ng-container",fT),eo(s)){let a=i[N];$l(a,i,s),dp(a,s,i)}return t!=null&&$s(i,s),Zp}function Qp(){let e=Ce(),n=Ke(),t=Ap(n);return e.firstCreatePass&&Vp(e,t),Qp}function Di(e,n,t){return Zp(e,n,t),Qp(),Di}var fT=(e,n,t,i,r)=>(Is(!0),EI(n[De],""));function gt(e,n,t){let i=O(),r=Bn();if(Tt(i,r,n)){let o=Ce(),s=lr();Eb(s,i,e,n,i[De],t)}return gt}var Ks="en-US";var hT=Ks;function uS(e){typeof e=="string"&&(hT=e.toLowerCase().replace(/_/g,"-"))}function Ee(e,n,t){let i=O(),r=Ce(),o=Ke();return pT(r,i,i[De],o,e,n,t),Ee}function pT(e,n,t,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=so(i,n,o),Gb(i,e,n,s,t,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let h=d[f],p=d[f+1];c??=so(i,n,o),Cl(i,n,h,p,r,c)}if(l&&l.length)for(let f of l)c??=so(i,n,o),Cl(i,n,f,r,r,c)}}function Te(e=1){return ky(e)}function mT(e,n){let t=null,i=VI(e);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){t=r;continue}if(i===null?hb(e,o,!0):UI(i,o))return r}return t}function Ae(e){let n=O()[mt][st];if(!n.projection){let t=e?e.length:1,i=n.projection=oy(t,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=e?mT(o,e):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function J(e,n=0,t,i,r,o){let s=O(),a=Ce(),c=i?e+1:null;c!==null&&uo(s,a,c,i,r,o,null,t);let l=po(a,Ne+e,16,null,t||null);l.projection===null&&(l.projection=n),ih();let f=!s[nr]||Xf();s[mt][st].projection[l.projection]===null&&c!==null?gT(s,a,c):f&&!Fl(l)&&fN(a,s,l)}function gT(e,n,t){let i=Ne+t,r=n.data[i],o=e[i],s=Dl(o,r.tView.ssrId),a=Gs(e,r,void 0,{dehydratedView:s});Ws(o,a,0,lo(r,s))}function kt(e,n,t,i){return tS(e,n,t,i),kt}function at(e,n,t){return eS(e,n,t),at}function K(e){let n=O(),t=Ce(),i=tl();xs(i+1);let r=Bp(t,i);if(e.dirty&&gy(n)===((r.metadata.flags&2)===2)){if(r.matches===null)e.reset([]);else{let o=iS(n,i);e.reset(o,U_),e.notifyOnChanges()}return!0}return!1}function Z(){return jp(O(),tl())}function Jl(e,n,t,i,r){return oS(n,tS(e,t,i,r)),Jl}function ed(e,n,t,i){return oS(e,eS(n,t,i)),ed}function td(e=1){xs(tl()+e)}function wi(e){let n=Cy();return Gf(n,Ne+e)}function fl(e,n){return e<<17|n<<2}function vr(e){return e>>17&32767}function vT(e){return(e&2)==2}function yT(e,n){return e&131071|n<<17}function ip(e){return e|2}function fo(e){return(e&131068)>>2}function Sh(e,n){return e&-131069|n<<2}function _T(e){return(e&1)===1}function rp(e){return e|1}function bT(e,n,t,i,r,o){let s=o?n.classBindings:n.styleBindings,a=vr(s),c=fo(s);e[i]=t;let l=!1,d;if(Array.isArray(t)){let f=t;d=f[1],(d===null||Zr(f,d)>0)&&(l=!0)}else d=t;if(r)if(c!==0){let h=vr(e[a+1]);e[i+1]=fl(h,a),h!==0&&(e[h+1]=Sh(e[h+1],i)),e[a+1]=yT(e[a+1],i)}else e[i+1]=fl(a,0),a!==0&&(e[a+1]=Sh(e[a+1],i)),a=i;else e[i+1]=fl(c,0),a===0?a=i:e[c+1]=Sh(e[c+1],i),c=i;l&&(e[i+1]=ip(e[i+1])),b_(e,d,i,!0),b_(e,d,i,!1),ST(n,d,e,i,o),s=fl(a,c),o?n.classBindings=s:n.styleBindings=s}function ST(e,n,t,i,r){let o=r?e.residualClasses:e.residualStyles;o!=null&&typeof n=="string"&&Zr(o,n)>=0&&(t[i+1]=rp(t[i+1]))}function b_(e,n,t,i){let r=e[t+1],o=n===null,s=i?vr(r):fo(r),a=!1;for(;s!==0&&(a===!1||o);){let c=e[s],l=e[s+1];DT(c,n)&&(a=!0,e[s+1]=i?rp(l):ip(l)),s=i?vr(l):fo(l)}a&&(e[t+1]=i?ip(r):rp(r))}function DT(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?Zr(e,n)>=0:!1}var nn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function wT(e){return e.substring(nn.key,nn.keyEnd)}function CT(e){return ET(e),fS(e,hS(e,0,nn.textEnd))}function fS(e,n){let t=nn.textEnd;return t===n?-1:(n=nn.keyEnd=xT(e,nn.key=n,t),hS(e,n,t))}function ET(e){nn.key=0,nn.keyEnd=0,nn.value=0,nn.valueEnd=0,nn.textEnd=e.length}function hS(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function xT(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function Ci(e,n,t){return pS(e,n,t,!1),Ci}function ee(e,n){return pS(e,n,null,!0),ee}function Et(e){NT(OT,IT,e,!0)}function IT(e,n){for(let t=CT(n);t>=0;t=fS(n,t))Gc(e,wT(n),!0)}function pS(e,n,t,i){let r=O(),o=Ce(),s=oh(2);if(o.firstUpdatePass&&gS(o,e,s,i),n!==wt&&Tt(r,s,n)){let a=o.data[wn()];vS(o,a,r,r[De],e,r[s+1]=PT(n,t),i,s)}}function NT(e,n,t,i){let r=Ce(),o=oh(2);r.firstUpdatePass&&gS(r,null,o,i);let s=O();if(t!==wt&&Tt(s,o,t)){let a=r.data[wn()];if(yS(a,i)&&!mS(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=Bc(c,t||"")),np(r,a,s,t,i)}else FT(r,a,s,s[De],s[o+1],s[o+1]=kT(e,n,t),i,o)}}function mS(e,n){return n>=e.expandoStartIndex}function gS(e,n,t,i){let r=e.data;if(r[t+1]===null){let o=r[wn()],s=mS(e,t);yS(o,i)&&n===null&&!s&&(n=!1),n=MT(r,o,n,i),bT(r,o,n,t,s,i)}}function MT(e,n,t,i){let r=My(e),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(t=Dh(null,e,n,t,i),t=js(t,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||e[s]!==r)if(t=Dh(r,e,n,t,i),o===null){let c=TT(e,n,i);c!==void 0&&Array.isArray(c)&&(c=Dh(null,e,n,c[1],i),c=js(c,n.attrs,i),AT(e,n,i,c))}else o=RT(e,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),t}function TT(e,n,t){let i=t?n.classBindings:n.styleBindings;if(fo(i)!==0)return e[vr(i)]}function AT(e,n,t,i){let r=t?n.classBindings:n.styleBindings;e[vr(r)]=i}function RT(e,n,t){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=e[o].hostAttrs;i=js(i,s,t)}return js(i,n.attrs,t)}function Dh(e,n,t,i,r){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=n[a],i=js(i,o.hostAttrs,r),o!==e);)a++;return e!==null&&(t.directiveStylingLast=a),i}function js(e,n,t){let i=t?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),Gc(e,s,t?!0:n[++o]))}return e===void 0?null:e}function kT(e,n,t){if(t==null||t==="")return ot;let i=[],r=zt(t);if(Array.isArray(r))for(let o=0;o<r.length;o++)e(i,r[o],!0);else if(r instanceof Set)for(let o of r)e(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&e(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function OT(e,n,t){let i=String(n);i!==""&&!i.includes(" ")&&Gc(e,i,t)}function FT(e,n,t,i,r,o,s,a){r===wt&&(r=ot);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let h=c<r.length?r[c+1]:void 0,p=l<o.length?o[l+1]:void 0,m=null,y;d===f?(c+=2,l+=2,h!==p&&(m=f,y=p)):f===null||d!==null&&d<f?(c+=2,m=d):(l+=2,m=f,y=p),m!==null&&vS(e,n,t,i,m,y,s,a),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function vS(e,n,t,i,r,o,s,a){if(!(n.type&3))return;let c=e.data,l=c[a+1],d=_T(l)?S_(c,n,t,r,fo(l),s):void 0;if(!kl(d)){kl(o)||vT(l)&&(o=S_(c,null,t,r,a,s));let f=$f(wn(),t);pN(i,s,f,r,o)}}function S_(e,n,t,i,r,o){let s=n===null,a;for(;r>0;){let c=e[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,h=t[r+1];h===wt&&(h=f?ot:void 0);let p=f?Wc(h,i):d===i?h:void 0;if(l&&!kl(p)&&(p=Wc(c,i)),kl(p)&&(a=p,s))return a;let m=e[r+1];r=s?vr(m):fo(m)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=Wc(c,i))}return a}function kl(e){return e!==void 0}function PT(e,n){return e==null||e===""||(typeof n=="string"?e=e+n:typeof e=="object"&&(e=ms(zt(e)))),e}function yS(e,n){return(e.flags&(n?8:16))!==0}function je(e,n=""){let t=O(),i=Ce(),r=e+Ne,o=i.firstCreatePass?po(i,r,1,n,null):i.data[r],s=LT(i,t,o,n);t[r]=s,ol()&&Cp(i,t,s,o),no(o,!1)}var LT=(e,n,t,i)=>(Is(!0),wI(n[De],i));function VT(e,n,t,i=""){return Tt(e,Bn(),t)?n+_s(t)+i:wt}function Nn(e){return qn("",e),Nn}function qn(e,n,t){let i=O(),r=VT(i,e,n,t);return r!==wt&&jT(i,wn(),r),qn}function jT(e,n,t){let i=$f(n,e);CI(e[De],i,t)}function vo(e){return Tt(O(),Bn(),e)?_s(e):wt}function D_(e,n,t){let i=Ce();i.firstCreatePass&&_S(n,i.data,i.blueprint,Dn(e),t)}function _S(e,n,t,i,r){if(e=et(e),Array.isArray(e))for(let o=0;o<e.length;o++)_S(e[o],n,t,i,r);else{let o=Ce(),s=O(),a=Ke(),c=Xi(e)?e:et(e.provide),l=jf(e),d=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(Xi(e)||!e.multi){let p=new pr(l,r,ae,null),m=Ch(c,n,r?d:d+h,f);m===-1?(Ih(Sl(a,s),o,c),wh(o,e,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),s.push(p)):(t[m]=p,s[m]=p)}else{let p=Ch(c,n,d+h,f),m=Ch(c,n,d,d+h),y=p>=0&&t[p],w=m>=0&&t[m];if(r&&!w||!r&&!y){Ih(Sl(a,s),o,c);let G=HT(r?UT:BT,t.length,r,i,l,e);!r&&w&&(t[m].providerFactory=G),wh(o,e,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(G),s.push(G)}else{let G=bS(t[r?m:p],l,!r&&i);wh(o,e,p>-1?p:m,G)}!r&&i&&w&&t[m].componentProviders++}}}function wh(e,n,t,i){let r=Xi(n),o=uy(n);if(r||o){let c=(o?et(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[i,c]):l[d+1].push(i,c)}else l.push(t,c)}}}function bS(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function Ch(e,n,t,i){for(let r=t;r<i;r++)if(n[r]===e)return r;return-1}function BT(e,n,t,i,r){return op(this.multi,[])}function UT(e,n,t,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Os(i,i[N],this.providerFactory.index,r);s=c.slice(0,a),op(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],op(o,s);return s}function op(e,n){for(let t=0;t<e.length;t++){let i=e[t];n.push(i())}return n}function HT(e,n,t,i,r,o){let s=new pr(e,t,ae,null);return s.multi=[],s.index=n,s.componentProviders=0,bS(s,r,i&&!t),s}function Re(e,n){return t=>{t.providersResolver=(i,r)=>D_(i,r?r(e):e,!1),n&&(t.viewProvidersResolver=(i,r)=>D_(i,r?r(n):n,!0))}}function Xp(e,n){let t=Es()+e,i=O();return i[t]===wt?Op(i,t,n()):tM(i,t)}function Jp(e,n,t){return DS(O(),Es(),e,n,t)}function Zs(e,n,t,i){return zT(O(),Es(),e,n,t,i)}function SS(e,n){let t=e[n];return t===wt?void 0:t}function DS(e,n,t,i,r,o){let s=n+t;return Tt(e,s,r)?Op(e,s+1,o?i.call(o,r):i(r)):SS(e,s+1)}function zT(e,n,t,i,r,o,s){let a=n+t;return nM(e,a,r,o)?Op(e,a+2,s?i.call(s,r,o):i(r,o)):SS(e,a+2)}function em(e,n){let t=Ce(),i,r=e+Ne;t.firstCreatePass?(i=$T(n,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let o=i.factory||(i.factory=di(i.type,!0)),s,a=ht(ae);try{let c=bl(!1),l=o();return bl(c),Wf(t,O(),r,l),l}finally{ht(a)}}function $T(e,n){if(n)for(let t=n.length-1;t>=0;t--){let i=n[t];if(e===i.name)return i}}function tm(e,n,t){let i=e+Ne,r=O(),o=Gf(r,i);return GT(r,i)?DS(r,Es(),n,o.transform,t,o):o.transform(t)}function GT(e,n){return e[N].data[n].pure}function Qs(e,n){return Wl(e,n)}var wS=(()=>{class e{applicationErrorHandler=u(Dt);appRef=u(Ct);taskService=u(Un);ngZone=u(A);zonelessEnabled=u(Ms);tracing=u(xn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new se;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(hs):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(ph,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Ly:dh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(hs+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function CS(){return[{provide:gn,useExisting:wS},{provide:A,useClass:ps},{provide:Ms,useValue:!0}]}var nm=(()=>{class e{compileModuleSync(t){return new Tl(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function WT(){return typeof $localize<"u"&&$localize.locale||Ks}var nd=new g("",{factory:()=>u(nd,{optional:!0,skipSelf:!0})||WT()});function ct(e,n){return Jo(e,n?.equal)}function Q(e){return gv(e)}var ES=class e extends Error{_brand;constructor(n){super(n)}static IDLE=new e("IDLE");static LOADING=new e("LOADING")},qT=e=>e;function id(e,n){if(typeof e=="function"){let t=Xu(e,qT,n?.equal);return xS(t,n?.debugName,n?.set)}else{let t=Xu(e.source,e.computation,e.equal);return xS(t,e.debugName,e.set)}}function xS(e,n,t){let i=e[Oe],r=e;if(t!==void 0){let o=s=>Ju(i,s);r.set=s=>t(s,o),r.update=s=>t(s(Q(e)),o)}else r.set=o=>Ju(i,o),r.update=o=>mv(i,o);return r.asReadonly=sl.bind(e),r}var AS=Symbol("InputSignalNode#UNSET"),cA=V(v({},es),{transformFn:void 0,applyValueToInputSignal(e,n){Bi(e,n)}});function RS(e,n){let t=Object.create(cA);t.value=e,t.transformFn=n?.transform;function i(){if(ii(t),t.value===AS){let r=null;throw new _(-950,r)}return t.value}return i[Oe]=t,i}var yo=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Us(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},kS=(()=>{let e=new g("");return e.__NG_ELEMENT_ID__=n=>{let t=Ke();if(t===null)throw new _(-204,!1);if(t.type&2)return t.value;if(n&8)return null;throw new _(-204,!1)},e})();function cm(e){return lA(e)?e.default:e}function lA(e){return e&&typeof e=="object"&&"default"in e}function IS(e,n){return RS(e,n)}function dA(e){return RS(AS,e)}var Sr=(IS.required=dA,IS);function NS(e,n){return Up(n)}function uA(e,n){return Hp(n)}var Js=(NS.required=uA,NS);function MS(e,n){return Up(n)}function fA(e,n){return Hp(n)}var OS=(MS.required=fA,MS);var hA=1e4;var c5=hA-1e3;var Ze=(()=>{class e{static __NG_ELEMENT_ID__=pA}return e})();function pA(e){return mA(Ke(),O(),(e&16)===16)}function mA(e,n,t){if(Sn(e)&&!t){let i=Ut(e.index,n);return new bi(i,i)}else if(e.type&175){let i=n[mt];return new bi(i,n)}return null}var rm=new g(""),gA=new g("");function Xs(e){return!e.moduleRef}function vA(e){let n=Xs(e)?e.r3Injector:e.moduleRef.injector,t=n.get(A);return t.run(()=>{Xs(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let i=n.get(Dt),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Xs(e)){let o=()=>n.destroy(),s=e.platformInjector.get(rm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>e.moduleRef.destroy(),s=e.platformInjector.get(rm);s.add(o),e.moduleRef.onDestroy(()=>{ks(e.allPlatformModules,e.moduleRef),r.unsubscribe(),s.delete(o)})}return _A(i,t,()=>{let o=n.get(Un),s=o.add(),a=n.get(Gp);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(nd,Ks);if(uS(c||Ks),!n.get(gA,!0))return Xs(e)?n.get(Ct):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(Xs(e)){let d=n.get(Ct);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return yA?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{o.remove(s)})})})}var yA;function _A(e,n,t){try{let i=t();return In(i)?i.catch(r=>{throw n.runOutsideAngular(()=>e(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>e(i)),i}}var rd=null;function bA(e=[],n){return ie.create({name:n,providers:[{provide:Ds,useValue:"platform"},{provide:rm,useValue:new Set([()=>rd=null])},...e]})}function SA(e=[]){if(rd)return rd;let n=bA(e);return rd=n,lS(),DA(n),n}function DA(e){let n=e.get(al,null);Ye(e,()=>{n?.forEach(t=>t())})}function FS(e){let{rootComponent:n,appProviders:t,platformProviders:i,platformRef:r}=e;pe(ce.BootstrapApplicationStart);try{let o=r?.injector??SA(i),s=[CS(),jy,...t||[]],a=new Vs({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return vA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{pe(ce.BootstrapApplicationEnd)}}function re(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Mn(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}var im=Symbol("NOT_SET"),PS=new Set,wA=V(v({},es),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:im,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==im&&!jr(this))return this.signal;try{for(let r of this.cleanup??PS)r()}finally{this.cleanup?.clear()}let n=[];e!==void 0&&n.push(e),n.push(this.registerCleanupFn);let t=kn(this),i;try{i=this.userFn.apply(null,n)}finally{ri(this,t)}return(this.value===im||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),om=class extends Fs{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,t,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Ve),s),this.scheduler=r;for(let a of bp){let c=t[a];if(c===void 0)continue;let l=Object.create(wA);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(ii(l),l.value),l.signal[Oe]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let t of n.cleanup??PS)t()}finally{oi(n)}}};function lm(e,n){let t=n?.injector??u(ie),i=t.get(gn),r=t.get(jl),o=t.get(xn,null,{optional:!0});r.impl??=t.get(Sp);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=t.get(io,null,{optional:!0}),c=new om(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,t,o?.snapshot(null));return r.impl.register(c),c}function od(e,n){let t=Vn(e),i=n.elementInjector||Qr();return new gr(t).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}function LS(e){let n=Vn(e);if(!n)return null;let t=new gr(n);return{get selector(){return t.selector},get type(){return t.componentType},get inputs(){return t.inputs},get outputs(){return t.outputs},get ngContentSelectors(){return t.ngContentSelectors},get isStandalone(){return n.standalone},get isSignal(){return n.signals}}}var VS=null;function Gt(){return VS}function dm(e){VS??=e}var ea=class{},Yn=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:()=>u(jS),providedIn:"platform"})}return e})(),um=new g(""),jS=(()=>{class e extends Yn{_location;_history;_doc=u(P);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Gt().getBaseHref(this._doc)}onPopState(t){let i=Gt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=Gt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:()=>new e,providedIn:"platform"})}return e})();function sd(e,n){return e?n?e.endsWith("/")?n.startsWith("/")?e+n.slice(1):e+n:n.startsWith("/")?e+n:`${e}/${n}`:e:n}function BS(e){let n=e.search(/#|\?|$/);return e[n-1]==="/"?e.slice(0,n-1)+e.slice(n):e}function cn(e){return e&&e[0]!=="?"?`?${e}`:e}var ln=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:()=>u(cd),providedIn:"root"})}return e})(),ad=new g(""),cd=(()=>{class e extends ln{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(P).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return sd(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+cn(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+cn(o));this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+cn(o));this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||e)(C(Yn),C(ad,8))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Tn=(()=>{class e{_subject=new b;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=xA(BS(US(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+cn(i))}normalize(t){return e.stripTrailingSlash(EA(this._basePath,US(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+cn(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+cn(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=cn;static joinWithSlash=sd;static stripTrailingSlash=BS;static \u0275fac=function(i){return new(i||e)(C(ln))};static \u0275prov=H({token:e,factory:()=>CA(),providedIn:"root"})}return e})();function CA(){return new Tn(C(ln))}function EA(e,n){if(!e||!n.startsWith(e))return n;let t=n.substring(e.length);return t===""||["/",";","?","#"].includes(t[0])?t:n}function US(e){return e.replace(/\/index\.html$/,"")}function xA(e){if(new RegExp("^(https?:)?//").test(e)){let[,t]=e.split(/\/\/[^\/]+/);return t}return e}var pm=(()=>{class e extends ln{_platformLocation;_baseHref="";_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,i!=null&&(this._baseHref=i)}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}path(t=!1){let i=this._platformLocation.hash??"#";return i.length>0?i.substring(1):i}prepareExternalUrl(t){let i=sd(this._baseHref,t);return i.length>0?"#"+i:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+cn(o))||this._platformLocation.pathname;this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+cn(o))||this._platformLocation.pathname;this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||e)(C(Yn),C(ad,8))};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})();var ta=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(ie);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(t,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||e)(ae(Rt))};static \u0275dir=k({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Je]})}return e})();function NA(e,n){return new _(2100,!1)}var fm=class{createSubscription(n,t,i){return Q(()=>n.subscribe({next:t,error:i}))}dispose(n){Q(()=>n.unsubscribe())}},hm=class{createSubscription(n,t,i){return n.then(r=>t?.(r),r=>i?.(r)),{unsubscribe:()=>{t=null,i=null}}}dispose(n){n.unsubscribe()}},MA=new hm,TA=new fm,mm=(()=>{class e{_ref;_latestValue=null;markForCheckOnValueUpdate=!0;_subscription=null;_obj=null;_strategy=null;applicationErrorHandler=u(Dt);constructor(t){this._ref=t}ngOnDestroy(){this._subscription&&this._dispose(),this._ref=null}transform(t){if(!this._obj){if(t)try{this.markForCheckOnValueUpdate=!1,this._subscribe(t)}finally{this.markForCheckOnValueUpdate=!0}return this._latestValue}return t!==this._obj?(this._dispose(),this.transform(t)):this._latestValue}_subscribe(t){this._obj=t,this._strategy=this._selectStrategy(t),this._subscription=this._strategy.createSubscription(t,i=>this._updateLatestValue(t,i),i=>this.applicationErrorHandler(i))}_selectStrategy(t){if(In(t))return MA;if(ql(t))return TA;throw NA(e,t)}_dispose(){this._strategy.dispose(this._subscription),this._latestValue=null,this._subscription=null,this._obj=null}_updateLatestValue(t,i){t===this._obj&&(this._latestValue=i,this.markForCheckOnValueUpdate&&this._ref?.markForCheck())}static \u0275fac=function(i){return new(i||e)(ae(Ze,16))};static \u0275pipe=zp({name:"async",type:e,pure:!1})}return e})();function na(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let i=t.indexOf("="),[r,o]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var vm="browser";function HS(e){return e===vm}var ym=(()=>{class e{static \u0275prov=H({token:e,providedIn:"root",factory:()=>new gm(u(P),window)})}return e})(),gm=class{document;window;offset=()=>[0,0];constructor(n,t){this.document=n,this.window=t}setOffset(n){Array.isArray(n)?this.offset=()=>n:this.offset=n}getScrollPosition(){return[this.window.scrollX,this.window.scrollY]}scrollToPosition(n,t){this.window.scrollTo(V(v({},t),{left:n[0],top:n[1]}))}scrollToAnchor(n,t){let i=OA(this.document,n);i&&(this.scrollToElement(i,t),i.focus({preventScroll:!0}))}setHistoryScrollRestoration(n){try{this.window.history.scrollRestoration=n}catch{console.warn(Xt(2400,!1))}}scrollToElement(n,t){let i=n.getBoundingClientRect(),r=i.left+this.window.pageXOffset,o=i.top+this.window.pageYOffset,s=this.offset();this.window.scrollTo(V(v({},t),{left:r-s[0],top:o-s[1]}))}};function OA(e,n){let t=e.getElementById(n)||e.getElementsByName(n)[0];if(t)return t;if(typeof e.createTreeWalker=="function"&&e.body&&typeof e.body.attachShadow=="function"){let i=e.createTreeWalker(e.body,NodeFilter.SHOW_ELEMENT),r=i.currentNode;for(;r;){let o=r.shadowRoot;if(o){let s=o.getElementById(n)||o.querySelector(`[name="${CSS.escape(n)}"]`);if(s)return s}r=i.nextNode()}}return null}var ia=class{_doc;constructor(n){this._doc=n}manager},ld=(()=>{class e extends ia{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,o){return t.addEventListener(i,r,o),()=>this.removeEventListener(t,i,r,o)}removeEventListener(t,i,r,o){return t.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||e)(C(P))};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})(),fd=new g(""),Dm=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(s=>{s.manager=this});let r=t.filter(s=>!(s instanceof ld));this._plugins=r.slice().reverse();let o=t.find(s=>s instanceof ld);o&&this._plugins.push(o)}addEventListener(t,i,r,o){return this._findPluginFor(i).addEventListener(t,i,r,o)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(o=>o.supports(t)),!i)throw new _(-5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||e)(C(fd),C(A))};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})(),_m="ng-app-id";function zS(e){for(let n of e)n.remove()}function $S(e,n){let t=n.createElement("style");return t.textContent=e,t}function PA(e,n,t,i){let r=e.head?.querySelectorAll(`style[${_m}="${n}"],link[${_m}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(_m),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]});return!0}function Sm(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var wm=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,o={}){this.doc=t,this.appId=i,this.nonce=r,PA(t,i,this.inline,this.external)&&this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,$S);i?.forEach(r=>this.addUsage(r,this.external,Sm))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let o=i.get(t);o?o.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(zS(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])zS(t);this.hosts.clear()}addHost(t){if(!this.hosts.has(t)){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,$S(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,Sm(i,this.doc)))}}removeHost(t){this.hosts.delete(t);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===t?o.remove():r.push(o);i.elements=r}}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||e)(C(P),C(ur),C(vi,8),C(fr))};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})(),bm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Cm=/%COMP%/g;var WS="%COMP%",LA=`_nghost-${WS}`,VA=`_ngcontent-${WS}`,jA=!0,BA=new g("",{factory:()=>jA}),UA=new g("");function HA(e){return VA.replace(Cm,e)}function zA(e){return LA.replace(Cm,e)}function qS(e,n){return n.map(t=>t.replace(Cm,e))}var Em=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(t,i,r,o,s,a,c=null,l=null,d=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.cssVarNamespace=d??"",this.defaultRenderer=new ra(t,s,a,this.tracingService,this.cssVarNamespace)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof ud?r.applyToHost(t):r instanceof oa&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case rn.Emulated:o=new ud(c,l,i,this.appId,d,s,a,f,this.cssVarNamespace);break;case rn.ShadowDom:return new dd(c,t,i,s,a,this.nonce,f,this.cssVarNamespace,l);case rn.ExperimentalIsolatedShadowDom:return new dd(c,t,i,s,a,this.nonce,f,this.cssVarNamespace);default:o=new oa(c,l,i,d,s,a,f,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||e)(C(Dm),C(yr),C(ur),C(BA),C(P),C(A),C(vi),C(xn,8),C(UA,8))};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})(),ra=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,i,r,o=""){this.eventManager=n,this.doc=t,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(bm[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){(GS(n)?n.content:n).appendChild(t)}insertBefore(n,t,i){n&&(GS(n)?n.content:n).insertBefore(t,i)}removeChild(n,t){t.remove()}selectRootElement(n,t){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new _(-5104,!1);return t||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,i,r){if(r){t=r+":"+t;let o=bm[r];o?n.setAttributeNS(o,t,i):n.setAttribute(t,i)}else n.setAttribute(t,i)}removeAttribute(n,t,i){if(i){let r=bm[i];r?n.removeAttributeNS(r,t):n.removeAttribute(`${i}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,i,r){let o=t.startsWith("--");o&&(t=t.replace("%NS%",this.cssVarNamespace)),o||r&(En.DashCase|En.Important)?n.style.setProperty(t,i,r&En.Important?"important":""):n.style[t]=i}removeStyle(n,t,i){let r=t.startsWith("--");r&&(t=t.replace("%NS%",this.cssVarNamespace)),r||i&En.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,i){n!=null&&(n[t]=i)}setValue(n,t){n.nodeValue=t}listen(n,t,i,r){if(typeof n=="string"&&(n=Gt().getGlobalEventTarget(this.doc,n),!n))throw new _(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,t,o)),this.eventManager.addEventListener(n,t,o,r)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function GS(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var dd=class extends ra{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,i,r,o,s,a,c,l){super(n,r,o,a,c),this.hostEl=t,this.sharedStylesHost=l,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=i.styles;d=qS(i.id,d).map(h=>h.replace(/%NS%/g,c));for(let h of d){let p=document.createElement("style");s&&p.setAttribute("nonce",s),p.textContent=h,this.shadowRoot.appendChild(p)}let f=i.getExternalStyles?.();if(f)for(let h of f){let p=Sm(h,r);s&&p.setAttribute("nonce",s),this.shadowRoot.appendChild(p)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,i){return super.insertBefore(this.nodeOrShadowRoot(n),t,i)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},oa=class extends ra{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,i,r,o,s,a,c,l){super(n,o,s,a,c),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let d=i.styles,f=l?qS(l,d):d;this.styles=f.map(h=>h.replace(/%NS%/g,c)),this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&_i.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},ud=class extends oa{contentAttr;hostAttr;constructor(n,t,i,r,o,s,a,c,l){let d=r+"-"+i.id;super(n,t,i,o,s,a,c,l,d),this.contentAttr=HA(d),this.hostAttr=zA(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let i=super.createElement(n,t);return super.setAttribute(i,this.contentAttr,""),i}};var hd=class e extends ea{supportsDOMEvents=!0;static makeCurrent(){dm(new e)}onAndCancel(n,t,i,r){return n.addEventListener(t,i,r),()=>{n.removeEventListener(t,i,r)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=$A();return t==null?null:GA(t)}resetBaseElement(){sa=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return na(document.cookie,n)}},sa=null;function $A(){return sa=sa||document.head.querySelector("base"),sa?sa.getAttribute("href"):null}function GA(e){return new URL(e,document.baseURI).pathname}var YS=["alt","control","meta","shift"],WA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},qA={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},KS=(()=>{class e extends ia{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,i,r,o){let s=e.parseEventName(i),a=e.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Gt().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=e._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),YS.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(t,i){let r=WA[t.key]||t.key,o="";return i.indexOf("code.")>-1&&(r=t.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),YS.forEach(s=>{if(s!==r){let a=qA[s];a(t)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(t,i,r){return o=>{e.matchEventFullKeyCode(o,t)&&r.runGuarded(()=>i(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||e)(C(P))};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})();async function xm(e,n,t){let i=v({rootComponent:e},YA(n,t));return FS(i)}function YA(e,n){return{platformRef:n?.platformRef,appProviders:[...JA,...e?.providers??[]],platformProviders:XA}}function KA(){hd.makeCurrent()}function ZA(){return new pt}function QA(){return cp(document),document}var XA=[{provide:fr,useValue:vm},{provide:al,useValue:KA,multi:!0},{provide:P,useFactory:QA}];var JA=[{provide:Ds,useValue:"root"},{provide:pt,useFactory:ZA},{provide:fd,useClass:ld,multi:!0},{provide:fd,useClass:KS,multi:!0},Em,{provide:yr,useClass:wm},{provide:wm,useExisting:yr},Dm,{provide:ze,useExisting:Em},[]];var Zn=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),o=t.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let t=this.headers.get(n.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,t){return this.clone({name:n,value:t,op:"a"})}set(n,t){return this.clone({name:n,value:t,op:"s"})}delete(n,t){return this.clone({name:n,value:t,op:"d"})}maybeSetNormalizedName(n,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,n)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[t,i]of n.headers.entries())this.headers.set(t,i),this.normalizedNames.set(t,n.normalizedNames.get(t))}clone(n){let t=new e;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([n]),t}applyUpdate(n){let t=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,t);let r=n.op==="a"?(this.headers.get(t)||[]).slice():[];r.push(...i),this.headers.set(t,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(t),this.normalizedNames.delete(t);else{let s=Array.isArray(o)?o:[o],a=this.headers.get(t);if(!a)return;a=a.filter(c=>s.indexOf(c)===-1),a.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,a)}break}}addHeaderEntry(n,t){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(n,t){let i=(Array.isArray(t)?t:[t]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>n(this.normalizedNames.get(t),this.headers.get(t)))}};var md=class{map=new Map;set(n,t){return this.map.set(n,t),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},gd=class{encodeKey(n){return ZS(n)}encodeValue(n){return ZS(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function eR(e,n){let t=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=t.get(s)||[];c.push(a),t.set(s,c)}),t}var tR=/%(\d[a-f0-9])/gi,nR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function ZS(e){return encodeURIComponent(e).replace(tR,(n,t)=>nR[t]??n)}function pd(e){return`${e}`}var Kn=class e{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new gd,n.fromString){if(n.fromObject)throw new _(2805,!1);this.map=eR(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(t=>{let i=n.fromObject[t],r=Array.isArray(i)?i.map(pd):[pd(i)];this.map.set(t,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let t=this.map.get(n);return t?t[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,t){return this.clone({param:n,value:t,op:"a"})}appendAll(n){let t=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{t.push({param:i,value:o,op:"a"})}):t.push({param:i,value:r,op:"a"})}),this.clone(t)}set(n,t){return this.clone({param:n,value:t,op:"s"})}delete(n,t){return this.clone({param:n,value:t,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let t=this.encoder.encodeKey(n);return this.map.get(n).map(i=>t+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let t=new e({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(n),t}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,t]of this.cloneFrom.map.entries())this.map.set(n,t);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let t=n.op==="a"?(this.map.get(n.param)||[]).slice():[];t.push(pd(n.value)),this.map.set(n.param,t);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(pd(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function iR(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function QS(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function XS(e){return typeof Blob<"u"&&e instanceof Blob}function JS(e){return typeof FormData<"u"&&e instanceof FormData}function rR(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var Im="Content-Type",eD="Accept",nD="text/plain",iD="application/json",oR=`${iD}, ${nD}, */*`,_o=class e{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,t,i,r){this.url=t,this.method=n.toUpperCase();let o;if(iR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new _(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Zn,this.context??=new md,!this.params)this.params=new Kn,this.urlWithParams=t;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=t;else{let a=t,c="",l=t.indexOf("#");l!==-1&&(c=t.substring(l),a=t.substring(0,l));let d=a.indexOf("?"),f=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+f+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||QS(this.body)||XS(this.body)||JS(this.body)||rR(this.body)?this.body:this.body instanceof Kn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||JS(this.body)?null:XS(this.body)?this.body.type||null:QS(this.body)?null:typeof this.body=="string"?nD:this.body instanceof Kn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?iD:null}clone(n={}){let t=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,h=n.integrity||this.integrity,p=n.referrerPolicy||this.referrerPolicy,m=n.transferCache??this.transferCache,y=n.timeout??this.timeout,w=n.body!==void 0?n.body:this.body,G=n.withCredentials??this.withCredentials,xe=n.reportProgress??this.reportProgress,Ue=n.reportUploadProgress??this.reportUploadProgress,Pr=n.reportDownloadProgress??this.reportDownloadProgress,Ko=n.headers||this.headers,Oi=n.params||this.params,Za=n.context??this.context;return n.setHeaders!==void 0&&(Ko=Object.keys(n.setHeaders).reduce((Lr,Fi)=>Lr.set(Fi,n.setHeaders[Fi]),Ko)),n.setParams&&(Oi=Object.keys(n.setParams).reduce((Lr,Fi)=>Lr.set(Fi,n.setParams[Fi]),Oi)),new e(t,i,w,{params:Oi,headers:Ko,context:Za,reportProgress:xe,reportUploadProgress:Ue,reportDownloadProgress:Pr,responseType:r,withCredentials:G,transferCache:m,keepalive:o,cache:a,priority:s,timeout:y,mode:c,redirect:l,credentials:d,referrer:f,integrity:h,referrerPolicy:p})}},bo=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(bo||{}),So=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,t=200,i="OK"){this.headers=n.headers||new Zn,this.status=n.status!==void 0?n.status:t,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},vd=class e extends So{constructor(n={}){super(n)}type=bo.ResponseHeader;clone(n={}){return new e({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},aa=class e extends So{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=bo.Response;clone(n={}){return new e({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Dr=class extends So{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},sR=200;var aR=/^\)\]\}',?\n/,Pq=1024*1024,rD=new g("",{factory:()=>null}),yd=(()=>{class e{fetchImpl=u(Mm,{optional:!0})?.fetch??((...t)=>globalThis.fetch(...t));ngZone=u(A);destroyRef=u(Ve);maxResponseSize=u(rD);handle(t){return new L(i=>{let r=new AbortController;this.doRequest(t,r.signal,i).then(Tm,s=>i.error(new Dr({error:s})));let o;return t.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},t.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}async doRequest(t,i,r){let o=this.createRequestInit(t),s;try{let w=this.ngZone.runOutsideAngular(()=>this.fetchImpl(t.urlWithParams,v({signal:i},o)));cR(w),r.next({type:bo.Sent}),s=await w}catch(w){r.error(new Dr({error:w,status:w.status??0,statusText:w.statusText,url:t.urlWithParams,headers:w.headers}));return}let a=new Zn(s.headers),c=s.statusText,l=s.url||t.urlWithParams,d=s.status,f=null,h=t.reportProgress||t.reportDownloadProgress;if(h&&r.next(new vd({headers:a,status:d,statusText:c,url:l})),s.body){let w=s.headers.get("content-length"),G=w!==null?Number(w):NaN;this.maxResponseSize!==null&&Number.isFinite(G)&&G>this.maxResponseSize&&tD(this.maxResponseSize);let xe=[],Ue=s.body.getReader(),Pr=0,Ko,Oi,Za=typeof Zone<"u"&&Zone.current,Lr=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await Ue.cancel(),Lr=!0;break}let{done:Zo,value:zu}=await Ue.read();if(Zo)break;if(xe.push(zu),Pr+=zu.length,this.maxResponseSize!==null&&Pr>this.maxResponseSize&&(await Ue.cancel(),tD(this.maxResponseSize)),h){Oi=t.responseType==="text"?(Oi??"")+(Ko??=new TextDecoder).decode(zu,{stream:!0}):void 0;let rv=()=>r.next({type:bo.DownloadProgress,total:Number.isFinite(G)?G:void 0,loaded:Pr,partialText:Oi});Za?Za.run(rv):rv()}}}),Lr){r.complete();return}let Fi=this.concatChunks(xe,Pr);try{let Zo=s.headers.get(Im)??"";f=this.parseBody(t,Fi,Zo,d)}catch(Zo){r.error(new Dr({error:Zo,headers:new Zn(s.headers),status:s.status,statusText:s.statusText,url:s.url||t.urlWithParams}));return}}d===0&&(d=f?sR:0);let p=d>=200&&d<300,m=s.redirected,y=s.type;p?(r.next(new aa({body:f,headers:a,status:d,statusText:c,url:l,redirected:m,responseType:y})),r.complete()):r.error(new Dr({error:f,headers:a,status:d,statusText:c,url:l,redirected:m,responseType:y}))}parseBody(t,i,r,o){switch(t.responseType){case"json":let s=new TextDecoder().decode(i).replace(aR,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(t){if(t.reportUploadProgress)throw new _(2824,!1);let i={},r;if(r=t.credentials,t.withCredentials&&(r="include"),t.headers.forEach((o,s)=>i[o]=s.join(",")),t.headers.has(eD)||(i[eD]=oR),!t.headers.has(Im)){let o=t.detectContentTypeHeader();o!==null&&(i[Im]=o)}return{body:t.serializeBody(),method:t.method,headers:i,credentials:r,keepalive:t.keepalive,cache:t.cache,priority:t.priority,mode:t.mode,redirect:t.redirect,referrer:t.referrer,integrity:t.integrity,referrerPolicy:t.referrerPolicy}}concatChunks(t,i){let r=new Uint8Array(i),o=0;for(let s of t)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),Mm=class{};function Tm(){}function cR(e){e.then(Tm,Tm)}function tD(e){throw new _(-2825,!1)}var lR=new g("",{factory:()=>!0}),dR="XSRF-TOKEN",uR=new g("",{factory:()=>dR}),fR="X-XSRF-TOKEN",hR=new g("",{factory:()=>fR}),pR=(()=>{class e{cookieName=u(uR);doc=u(P);lastCookieString="";lastToken=null;parseCount=0;getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=na(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),oD=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=C(pR),r},providedIn:"root"})}return e})();function sD(e,n){if(!u(lR)||e.method==="GET"||e.method==="HEAD")return n(e);try{let r=u(Yn).href,{origin:o}=new URL(r),{origin:s}=new URL(e.url,o);if(o!==s)return n(e)}catch{return n(e)}let t=u(oD).getToken(),i=u(hR);return t!=null&&!e.headers.has(i)&&(e=e.clone({headers:e.headers.set(i,t)})),n(e)}function mR(e,n){return n(e)}function gR(e,n,t){return(i,r)=>Ye(t,()=>n(i,o=>e(o,r)))}var aD=new g("",{factory:()=>[sD]}),cD=new g(""),lD=new g("",{factory:()=>!0});var Am=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=C(yd),r},providedIn:"root"})}return e})();var _d=(()=>{class e{backend;injector;chain=null;pendingTasks=u(Ts);contributeToStability=u(lD);constructor(t,i){this.backend=t,this.injector=i}handle(t){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(aD),...this.injector.get(cD,[])]));this.chain=r.reduceRight((o,s)=>gR(o,s,this.injector),mR)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return Q(()=>i(t,o=>this.backend.handle(o))).pipe(Ki(r))}else return Q(()=>i(t,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||e)(C(Am),C(de))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Rm=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=C(_d),r},providedIn:"root"})}return e})();function Nm(e,n){return v({body:n},e)}var bd=(()=>{class e{handler;constructor(t){this.handler=t}request(t,i,r={}){let o;if(t instanceof _o)o=t;else{let c;r.headers instanceof Zn?c=r.headers:c=new Zn(r.headers);let l;r.params&&(r.params instanceof Kn?l=r.params:l=new Kn({fromObject:r.params})),o=new _o(t,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=x(o).pipe(ci(c=>this.handler.handle(c)));if(t instanceof _o||r.observe==="events")return s;let a=s.pipe(_e(c=>c instanceof aa));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(U(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new _(2806,!1);return c.body}));case"blob":return a.pipe(U(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new _(2807,!1);return c.body}));case"text":return a.pipe(U(c=>{if(c.body!==null&&typeof c.body!="string")throw new _(2808,!1);return c.body}));default:return a.pipe(U(c=>c.body))}case"response":return a;default:throw new _(2809,!1)}}delete(t,i={}){return this.request("DELETE",t,i)}get(t,i={}){return this.request("GET",t,i)}head(t,i={}){return this.request("HEAD",t,i)}jsonp(t,i){return this.request("JSONP",t,{params:new Kn().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,i={}){return this.request("OPTIONS",t,i)}patch(t,i,r={}){return this.request("PATCH",t,Nm(r,i))}post(t,i,r={}){return this.request("POST",t,Nm(r,i))}put(t,i,r={}){return this.request("PUT",t,Nm(r,i))}static \u0275fac=function(i){return new(i||e)(C(Rm))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function km(...e){let n=[bd,yd,_d,{provide:Rm,useExisting:_d},{provide:Am,useFactory:()=>u(yd)},{provide:aD,useValue:sD,multi:!0}];for(let t of e)n.push(...t.\u0275providers);return Vt(n)}var Do=(()=>{class e{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||e)(C(P))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Om=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=H({token:e,factory:function(i){let r=null;return i?r=new(i||e):r=C(yR),r},providedIn:"root"})}return e})(),yR=(()=>{class e extends Om{_doc=u(P);sanitize(t,i){if(i==null)return null;switch(t){case fe.NONE:return i;case fe.HTML:return Gn(i,"HTML")?zt(i):gp(this._doc,String(i)).toString();case fe.STYLE:return Gn(i,"Style")?zt(i):i;case fe.SCRIPT:if(Gn(i,"Script"))return zt(i);throw new _(5200,!1);case fe.URL:return Gn(i,"URL")?zt(i):Hs(String(i));case fe.RESOURCE_URL:if(Gn(i,"ResourceURL"))return zt(i);throw new _(-5201,!1);default:throw new _(5202,!1)}}bypassSecurityTrustHtml(t){return up(t)}bypassSecurityTrustStyle(t){return fp(t)}bypassSecurityTrustScript(t){return hp(t)}bypassSecurityTrustUrl(t){return pp(t)}bypassSecurityTrustResourceUrl(t){return mp(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var W="primary",_a=Symbol("RouteTitle"),jm=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t[0]:t}return null}getAll(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function Cr(e){return new jm(e)}function Fm(e,n,t){for(let i=0;i<e.length;i++){let r=e[i],o=n[i];if(r[0]===":")t[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function _D(e,n,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>e.length||t.pathMatch==="full"&&(n.hasChildren()||i.length<e.length))return null;let c={},l=e.slice(0,i.length);return Fm(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>e.length||t.pathMatch==="full"&&n.hasChildren()&&t.path!=="**")return null;let a={};return!Fm(o,e.slice(0,o.length),a)||!Fm(s,e.slice(e.length-s.length),a)?null:{consumed:e,posParams:a}}function xd(e){return new Promise((n,t)=>{e.pipe(Fn()).subscribe({next:i=>n(i),error:i=>t(i)})})}function _R(e,n){if(e.length!==n.length)return!1;for(let t=0;t<e.length;++t)if(!An(e[t],n[t]))return!1;return!0}function An(e,n){let t=e?Bm(e):void 0,i=n?Bm(n):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let o=0;o<t.length;o++)if(r=t[o],!bD(e[r],n[r]))return!1;return!0}function Bm(e){return[...Object.keys(e),...Object.getOwnPropertySymbols(e)]}function bD(e,n){if(Array.isArray(e)&&Array.isArray(n)){if(e.length!==n.length)return!1;let t=[...e].sort(),i=[...n].sort();return t.every((r,o)=>i[o]===r)}else return e===n}function bR(e){return e.length>0?e[e.length-1]:null}function xr(e){return rs(e)?e:In(e)?Se(Promise.resolve(e)):x(e)}function SD(e){return rs(e)?xd(e):Promise.resolve(e)}var SR={exact:CD,subset:ED},DD={exact:DR,subset:wR,ignored:()=>!0},wD={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Um={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function uD(e,n,t){return SR[t.paths](e.root,n.root,t.matrixParams)&&DD[t.queryParams](e.queryParams,n.queryParams)&&!(t.fragment==="exact"&&e.fragment!==n.fragment)}function DR(e,n){return An(e,n)}function CD(e,n,t){if(!wr(e.segments,n.segments)||!wd(e.segments,n.segments,t)||e.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!e.children[i]||!CD(e.children[i],n.children[i],t))return!1;return!0}function wR(e,n){return Object.keys(n).length<=Object.keys(e).length&&Object.keys(n).every(t=>bD(e[t],n[t]))}function ED(e,n,t){return xD(e,n,n.segments,t)}function xD(e,n,t,i){if(e.segments.length>t.length){let r=e.segments.slice(0,t.length);return!(!wr(r,t)||n.hasChildren()||!wd(r,t,i))}else if(e.segments.length===t.length){if(!wr(e.segments,t)||!wd(e.segments,t,i))return!1;for(let r in n.children)if(!e.children[r]||!ED(e.children[r],n.children[r],i))return!1;return!0}else{let r=t.slice(0,e.segments.length),o=t.slice(e.segments.length);return!wr(e.segments,r)||!wd(e.segments,r,i)||!e.children[W]?!1:xD(e.children[W],n,o,i)}}function wd(e,n,t){return n.every((i,r)=>DD[t](e[r].parameters,i.parameters))}var Ft=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ue([],{}),t={},i=null){this.root=n,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Cr(this.queryParams),this._queryParamMap}toString(){return xR.serialize(this)}},ue=class{segments;children;parent=null;constructor(n,t){this.segments=n,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Cd(this)}},Ei=class{path;parameters;_parameterMap;constructor(n,t){this.path=n,this.parameters=t}get parameterMap(){return this._parameterMap??=Cr(this.parameters),this._parameterMap}toString(){return ND(this)}};function CR(e,n){return wr(e,n)&&e.every((t,i)=>An(t.parameters,n[i].parameters))}function wr(e,n){return e.length!==n.length?!1:e.every((t,i)=>t.path===n[i].path)}function ER(e,n){let t=[];return Object.entries(e.children).forEach(([i,r])=>{i===W&&(t=t.concat(n(r,i)))}),Object.entries(e.children).forEach(([i,r])=>{i!==W&&(t=t.concat(n(r,i)))}),t}var Ni=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:()=>new Xn})}return e})(),Xn=class{parse(n){let t=new zm(n);return new Ft(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(n){let t=`/${ca(n.root,!0)}`,i=MR(n.queryParams),r=typeof n.fragment=="string"?`#${IR(n.fragment)}`:"";return`${t}${i}${r}`}},xR=new Xn;function Cd(e){return e.segments.map(n=>ND(n)).join("/")}function ca(e,n){if(!e.hasChildren())return Cd(e);if(n){let t=e.children[W]?ca(e.children[W],!1):"",i=[];return Object.entries(e.children).forEach(([r,o])=>{r!==W&&i.push(`${r}:${ca(o,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=ER(e,(i,r)=>r===W?[ca(e.children[W],!1)]:[`${r}:${ca(i,!1)}`]);return Object.keys(e.children).length===1&&e.children[W]!=null?`${Cd(e)}/${t[0]}`:`${Cd(e)}/(${t.join("//")})`}}function ID(e){return encodeURIComponent(e).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Sd(e){return ID(e).replace(/%3B/gi,";")}function IR(e){return encodeURI(e)}function Hm(e){return ID(e).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Ed(e){return decodeURIComponent(e)}function fD(e){return Ed(e.replace(/\+/g,"%20"))}function ND(e){return`${Hm(e.path)}${NR(e.parameters)}`}function NR(e){return Object.entries(e).map(([n,t])=>`;${Hm(n)}=${Hm(t)}`).join("")}function MR(e){let n=Object.entries(e).map(([t,i])=>Array.isArray(i)?i.map(r=>`${Sd(t)}=${Sd(r)}`).join("&"):`${Sd(t)}=${Sd(i)}`).filter(t=>t);return n.length?`?${n.join("&")}`:""}var TR=/^[^\/()?;#]+/;function Pm(e){let n=e.match(TR);return n?n[0]:""}var AR=/^[^\/()?;=#]+/;function RR(e){let n=e.match(AR);return n?n[0]:""}var kR=/^[^=?&#]+/;function OR(e){let n=e.match(kR);return n?n[0]:""}var FR=/^[^&#]+/;function PR(e){let n=e.match(FR);return n?n[0]:""}var zm=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ue([],{}):new ue([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new _(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(t.length>0||Object.keys(i).length>0)&&(r[W]=new ue(t,i)),r}parseSegment(){let n=Pm(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new _(4009,!1);return this.capture(n),new Ei(Ed(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let t=RR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=Pm(this.remaining);r&&(i=r,this.capture(i))}n[Ed(t)]=Ed(i)}parseQueryParam(n){let t=OR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let s=PR(this.remaining);s&&(i=s,this.capture(i))}let r=fD(t),o=fD(i);if(Object.hasOwn(n,r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,t){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Pm(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new _(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=W);let a=this.parseChildren(t+1);i[s??W]=Object.keys(a).length===1&&a[W]?a[W]:new ue([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new _(4011,!1)}};function MD(e){return e.segments.length>0?new ue([],{[W]:e}):e}function TD(e){let n=Object.create(null);for(let[i,r]of Object.entries(e.children)){let o=TD(r);if(i===W&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let t=new ue(e.segments,n);return LR(t)}function LR(e){if(e.numberOfChildren===1&&e.children[W]){let n=e.children[W];return new ue(e.segments.concat(n.segments),n.children)}return e}function xi(e){return e instanceof Ft}function AD(e,n,t=null,i=null,r=new Xn){let o=RD(e);return kD(o,n,t,i,r)}function RD(e){let n;function t(o){let s={};for(let c of o.children){let l=t(c);s[c.outlet]=l}let a=new ue(o.url,s);return o===e&&(n=a),a}let i=t(e.root),r=MD(i);return n??r}function kD(e,n,t,i,r){let o=e;for(;o.parent;)o=o.parent;if(n.length===0)return Lm(o,o,o,t,i,r);let s=VR(n);if(s.toRoot())return Lm(o,o,new ue([],{}),t,i,r);let a=jR(s,o,e),c=a.processChildren?da(a.segmentGroup,a.index,s.commands):FD(a.segmentGroup,a.index,s.commands);return Lm(o,a.segmentGroup,c,t,i,r)}function Id(e){return typeof e=="object"&&e!=null&&!e.outlets&&!e.segmentPath}function fa(e){return typeof e=="object"&&e!=null&&e.outlets}function hD(e,n,t){e||="\u0275";let i=new Ft;return i.queryParams={[e]:n},t.parse(t.serialize(i)).queryParams[e]}function Lm(e,n,t,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(f=>hD(l,f,o)):hD(l,d,o);let a;e===n?a=t:a=OD(e,n,t);let c=MD(TD(a));return new Ft(c,s,r)}function OD(e,n,t){let i=Object.create(null);return Object.entries(e.children).forEach(([r,o])=>{o===n?i[r]=t:i[r]=OD(o,n,t)}),new ue(e.segments,i)}var Nd=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,t,i){if(this.isAbsolute=n,this.numberOfDoubleDots=t,this.commands=i,n&&i.length>0&&Id(i[0]))throw new _(4003,!1);let r=i.find(fa);if(r&&r!==bR(i))throw new _(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function VR(e){if(typeof e[0]=="string"&&e.length===1&&e[0]==="/")return new Nd(!0,0,e);let n=0,t=!1,i=e.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Nd(t,n,i)}var Co=class{segmentGroup;processChildren;index;constructor(n,t,i){this.segmentGroup=n,this.processChildren=t,this.index=i}};function jR(e,n,t){if(e.isAbsolute)return new Co(n,!0,0);if(!t)return new Co(n,!1,NaN);if(t.parent===null)return new Co(t,!0,0);let i=Id(e.commands[0])?0:1,r=t.segments.length-1+i;return BR(t,r,e.numberOfDoubleDots)}function BR(e,n,t){let i=e,r=n,o=t;for(;o>r;){if(o-=r,i=i.parent,!i)throw new _(4005,!1);r=i.segments.length}return new Co(i,!1,r-o)}function UR(e){return fa(e[0])?e[0].outlets:{[W]:e}}function FD(e,n,t){if(e??=new ue([],{}),e.segments.length===0&&e.hasChildren())return da(e,n,t);let i=HR(e,n,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<e.segments.length){let o=new ue(e.segments.slice(0,i.pathIndex),{});return o.children[W]=new ue(e.segments.slice(i.pathIndex),e.children),da(o,0,r)}else return i.match&&r.length===0?new ue(e.segments,{}):i.match&&!e.hasChildren()?$m(e,n,t):i.match?da(e,0,r):$m(e,n,t)}function da(e,n,t){if(t.length===0)return new ue(e.segments,{});{let i=UR(t),r=Object.create(null);if(Object.keys(i).some(o=>o!==W)&&e.children[W]&&e.numberOfChildren===1&&e.children[W].segments.length===0){let o=da(e.children[W],n,t);return new ue(e.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=FD(e.children[o],n,s))}),Object.entries(e.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new ue(e.segments,r)}}function HR(e,n,t){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<e.segments.length;){if(i>=t.length)return o;let s=e.segments[r],a=t[i];if(fa(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!mD(c,l,s))return o;i+=2}else{if(!mD(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function $m(e,n,t){let i=e.segments.slice(0,n),r=0;for(;r<t.length;){let o=t[r];if(fa(o)){let c=zR(o.outlets);return new ue(i,c)}if(r===0&&Id(t[0])){let c=e.segments[n];i.push(new Ei(c.path,pD(t[0]))),r++;continue}let s=fa(o)?o.outlets[W]:`${o}`,a=r<t.length-1?t[r+1]:null;s&&a&&Id(a)?(i.push(new Ei(s,pD(a))),r+=2):(i.push(new Ei(s,{})),r++)}return new ue(i,{})}function zR(e){let n={};return Object.entries(e).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[t]=$m(new ue([],{}),0,i))}),n}function pD(e){let n={};return Object.entries(e).forEach(([t,i])=>n[t]=`${i}`),n}function mD(e,n,t){return e==t.path&&An(n,t.parameters)}var Eo="imperative",Qe=(function(e){return e[e.NavigationStart=0]="NavigationStart",e[e.NavigationEnd=1]="NavigationEnd",e[e.NavigationCancel=2]="NavigationCancel",e[e.NavigationError=3]="NavigationError",e[e.RoutesRecognized=4]="RoutesRecognized",e[e.ResolveStart=5]="ResolveStart",e[e.ResolveEnd=6]="ResolveEnd",e[e.GuardsCheckStart=7]="GuardsCheckStart",e[e.GuardsCheckEnd=8]="GuardsCheckEnd",e[e.RouteConfigLoadStart=9]="RouteConfigLoadStart",e[e.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",e[e.ChildActivationStart=11]="ChildActivationStart",e[e.ChildActivationEnd=12]="ChildActivationEnd",e[e.ActivationStart=13]="ActivationStart",e[e.ActivationEnd=14]="ActivationEnd",e[e.Scroll=15]="Scroll",e[e.NavigationSkipped=16]="NavigationSkipped",e})(Qe||{}),Pt=class{id;url;constructor(n,t){this.id=n,this.url=t}},Ii=class extends Pt{type=Qe.NavigationStart;navigationTrigger;restoredState;constructor(n,t,i="imperative",r=null){super(n,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},qt=class extends Pt{urlAfterRedirects;type=Qe.NavigationEnd;constructor(n,t,i){super(n,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},lt=(function(e){return e[e.Redirect=0]="Redirect",e[e.SupersededByNewNavigation=1]="SupersededByNewNavigation",e[e.NoDataFromResolver=2]="NoDataFromResolver",e[e.GuardRejected=3]="GuardRejected",e[e.Aborted=4]="Aborted",e})(lt||{}),Io=(function(e){return e[e.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",e[e.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",e})(Io||{}),Wt=class extends Pt{reason;code;type=Qe.NavigationCancel;constructor(n,t,i,r){super(n,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function PD(e){return e instanceof Wt&&(e.code===lt.Redirect||e.code===lt.SupersededByNewNavigation)}var Rn=class extends Pt{reason;code;type=Qe.NavigationSkipped;constructor(n,t,i,r){super(n,t),this.reason=i,this.code=r}},Er=class extends Pt{error;target;type=Qe.NavigationError;constructor(n,t,i,r){super(n,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},ha=class extends Pt{urlAfterRedirects;state;type=Qe.RoutesRecognized;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Md=class extends Pt{urlAfterRedirects;state;type=Qe.GuardsCheckStart;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Td=class extends Pt{urlAfterRedirects;state;shouldActivate;type=Qe.GuardsCheckEnd;constructor(n,t,i,r,o){super(n,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Ad=class extends Pt{urlAfterRedirects;state;type=Qe.ResolveStart;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Rd=class extends Pt{urlAfterRedirects;state;type=Qe.ResolveEnd;constructor(n,t,i,r){super(n,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},kd=class{route;type=Qe.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Od=class{route;type=Qe.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Fd=class{snapshot;type=Qe.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Pd=class{snapshot;type=Qe.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Ld=class{snapshot;type=Qe.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Vd=class{snapshot;type=Qe.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},No=class{routerEvent;position;anchor;scrollBehavior;type=Qe.Scroll;constructor(n,t,i,r){this.routerEvent=n,this.position=t,this.anchor=i,this.scrollBehavior=r}toString(){let n=this.position?`${this.position[0]}, ${this.position[1]}`:null;return`Scroll(anchor: '${this.anchor}', position: '${n}')`}},Mo=class{},pa=class{},To=class{url;navigationBehaviorOptions;constructor(n,t){this.url=n,this.navigationBehaviorOptions=t}};function $R(e){return!(e instanceof Mo)&&!(e instanceof To)&&!(e instanceof pa)}var jd=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Ir(this.rootInjector)}},Ir=(()=>{class e{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new jd(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||e)(C(de))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Bd=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let t=this.pathFromRoot(n);return t.length>1?t[t.length-2]:null}children(n){let t=Gm(n,this._root);return t?t.children.map(i=>i.value):[]}firstChild(n){let t=Gm(n,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(n){let t=Wm(n,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Wm(n,this._root).map(t=>t.value)}};function Gm(e,n){if(e===n.value)return n;for(let t of n.children){let i=Gm(e,t);if(i)return i}return null}function Wm(e,n){if(e===n.value)return[n];for(let t of n.children){let i=Wm(e,t);if(i.length)return i.unshift(n),i}return[]}var Ot=class{value;children;constructor(n,t){this.value=n,this.children=t}toString(){return`TreeNode(${this.value})`}};function wo(e){let n={};return e&&e.children.forEach(t=>n[t.value.outlet]=t),n}var ma=class extends Bd{snapshot;constructor(n,t){super(n),this.snapshot=t,tg(this,n)}toString(){return this.snapshot.toString()}};function LD(e,n){let t=GR(e,n),i=new $e([new Ei("",{})]),r=new $e({}),o=new $e({}),s=new $e({}),a=new $e(""),c=new Yt(i,r,s,a,o,W,e,t.root);return c.snapshot=t.root,new ma(new Ot(c,[]),t)}function GR(e,n){let t={},i={},r={},s=new Ao([],t,r,"",i,W,e,null,{},n);return new ga("",new Ot(s,[]))}var Yt=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,t,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(U(l=>l[_a]))??x(void 0),this.url=n,this.params=t,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(U(n=>Cr(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(U(n=>Cr(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},WR="always";function eg(e,n,t){let i,{routeConfig:r}=e;return n!==null&&(t==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:v(v({},n.params),e.params),data:v(v({},n.data),e.data),resolve:v(v(v(v({},e.data),n.data),r?.data),e._resolvedData)}:i={params:v({},e.params),data:v({},e.data),resolve:v(v({},e.data),e._resolvedData??{})},r&&jD(r)&&(i.resolve[_a]=r.title),i}var Ao=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[_a]}constructor(n,t,i,r,o,s,a,c,l,d){this.url=n,this.params=t,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Cr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Cr(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${t}')`}},ga=class extends Bd{url;constructor(n,t){super(t),this.url=n,tg(this,t)}toString(){return VD(this._root)}};function tg(e,n){n.value._routerState=e,n.children.forEach(t=>tg(e,t))}function VD(e){let n=e.children.length>0?` { ${e.children.map(VD).join(", ")} } `:"";return`${e.value}${n}`}function Vm(e){if(e.snapshot){let n=e.snapshot,t=e._futureSnapshot;e.snapshot=t,An(n.queryParams,t.queryParams)||e.queryParamsSubject.next(t.queryParams),n.fragment!==t.fragment&&e.fragmentSubject.next(t.fragment),An(n.params,t.params)||e.paramsSubject.next(t.params),_R(n.url,t.url)||e.urlSubject.next(t.url),An(n.data,t.data)||e.dataSubject.next(t.data)}else e.snapshot=e._futureSnapshot,e.dataSubject.next(e._futureSnapshot.data)}function qm(e,n){let t=An(e.params,n.params)&&CR(e.url,n.url),i=!e.parent!=!n.parent;return t&&!i&&(!e.parent||qm(e.parent,n.parent))}function jD(e){return typeof e.title=="string"||e.title===null}var BD=new g(""),ba=(()=>{class e{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=W;activateEvents=new ne;deactivateEvents=new ne;attachEvents=new ne;detachEvents=new ne;routerOutletData=Sr();parentContexts=u(Ir);location=u(Rt);changeDetector=u(Ze);inputBinder=u(Sa,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new _(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new _(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new _(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new _(4013,!1);this._activatedRoute=t;let r=this.location,s=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Ym(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Je]})}return e})(),Ym=class{route;childContexts;parent;outletData;constructor(n,t,i,r){this.route=n,this.childContexts=t,this.parent=i,this.outletData=r}get(n,t){return n===Yt?this.route:n===Ir?this.childContexts:n===BD?this.outletData:this.parent.get(n,t)}},Sa=new g(""),UD=(()=>{class e{options;outletDataSubscriptions=new Map;outletSeenKeys=new Map;constructor(t){this.options=t,this.options.queryParams??=!0}bindActivatedRouteToOutletComponent(t){this.unsubscribeFromRouteData(t),this.subscribeToRouteData(t)}unsubscribeFromRouteData(t){this.outletDataSubscriptions.get(t)?.unsubscribe(),this.outletDataSubscriptions.delete(t),this.outletSeenKeys.delete(t)}subscribeToRouteData(t){let{activatedRoute:i}=t,r=Wr([this.options.queryParams?i.queryParams:x({}),i.params,i.data]).pipe(Pe(([o,s,a],c)=>(a=v(v(v({},o),s),a),c===0?x(a):Promise.resolve(a)))).subscribe(o=>{if(!t.isActivated||!t.activatedComponentRef||t.activatedRoute!==i||i.component===null){this.unsubscribeFromRouteData(t);return}let s=LS(i.component);if(!s){this.unsubscribeFromRouteData(t);return}let a=this.outletSeenKeys.get(t);a||(a=new Set,this.outletSeenKeys.set(t,a));for(let l of Object.keys(o))a.add(l);let c=this.options.unmatchedInputBehavior??"alwaysUndefined";for(let{templateName:l}of s.inputs){let d=o[l];(d!==void 0||c==="alwaysUndefined"||a.has(l))&&t.activatedComponentRef.setInput(l,d)}});this.outletDataSubscriptions.set(t,r)}static \u0275fac=function(i){qs()};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})(),ng=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&ve(0,"router-outlet")},dependencies:[ba],encapsulation:2,changeDetection:1})}return e})();function ig(e){let n=e.children&&e.children.map(ig),t=n?V(v({},e),{children:n}):v({},e);return!t.component&&!t.loadComponent&&(n||t.loadChildren)&&t.outlet&&t.outlet!==W&&(t.component=ng),t}function qR(e,n,t){let i=new Set,r=va(e,n._root,t?t._root:void 0,i);return{newlyCreatedRoutes:i,state:new ma(r,n)}}function va(e,n,t,i){if(t&&e.shouldReuseRoute(n.value,t.value.snapshot)){let r=t.value;r._futureSnapshot=n.value;let o=YR(e,n,t,i);return new Ot(r,o)}else{if(e.shouldAttach(n.value)){let s=e.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(c=>va(e,c,void 0,i)),a}}let r=KR(n.value);i.add(r);let o=n.children.map(s=>va(e,s,void 0,i));return new Ot(r,o)}}function YR(e,n,t,i){return n.children.map(r=>{for(let o of t.children)if(e.shouldReuseRoute(r.value,o.value.snapshot))return va(e,r,o,i);return va(e,r,void 0,i)})}function KR(e){return new Yt(new $e(e.url),new $e(e.params),new $e(e.queryParams),new $e(e.fragment),new $e(e.data),e.outlet,e.component,e)}var Ro=class{redirectTo;navigationBehaviorOptions;constructor(n,t){this.redirectTo=n,this.navigationBehaviorOptions=t}},HD="ngNavigationCancelingError";function Ud(e,n){let{redirectTo:t,navigationBehaviorOptions:i}=xi(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=zD(!1,lt.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function zD(e,n){let t=new Error(`NavigationCancelingError: ${e||""}`);return t[HD]=!0,t.cancellationCode=n,t}function ZR(e){return $D(e)&&xi(e.url)}function $D(e){return!!e&&e[HD]}var Km=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,t,i,r,o){this.routeReuseStrategy=n,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,n),Vm(this.futureState.root),this.activateChildRoutes(t,i,n)}deactivateChildRoutes(n,t,i){let r=wo(t);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,t,i){let r=n.value,o=t?t.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,t,s.children)}else this.deactivateChildRoutes(n,t,i);else o&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(n,t){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,t):this.deactivateRouteAndOutlet(n,t)}detachAndStoreRouteSubtree(n,t){let i=t.getContext(n.value.outlet),r=i&&n.value.component?i.children:t,o=wo(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,t){let i=t.getContext(n.value.outlet),r=i&&n.value.component?i.children:t,o=wo(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,t,i){let r=wo(t);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Vd(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Pd(n.value.snapshot))}activateRoutes(n,t,i){let r=n.value,o=t?t.value:null;if(Vm(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,t,s.children)}else this.activateChildRoutes(n,t,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Vm(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Hd=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},xo=class{component;route;constructor(n,t){this.component=n,this.route=t}};function QR(e,n,t){let i=e._root,r=n?n._root:null;return la(i,r,t,[i.value])}function XR(e){let n=e.routeConfig?e.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:e,guards:n}}function Oo(e,n){let t=Symbol(),i=n.get(e,t);return i===t?typeof e=="function"&&!Nf(e)?e:n.get(e):i}function la(e,n,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=wo(n);return e.children.forEach(s=>{JR(s,o[s.value.outlet],t,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>ua(a,t.getContext(s),r)),r}function JR(e,n,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=e.value,s=n?n.value:null,a=t?t.getContext(e.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=ek(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Hd(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?la(e,n,a?a.children:null,i,r):la(e,n,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new xo(a.outlet.component,s))}else s&&ua(n,a,r),r.canActivateChecks.push(new Hd(i)),o.component?la(e,null,a?a.children:null,i,r):la(e,null,t,i,r);return r}function ek(e,n,t){if(typeof t=="function")return Ye(n._environmentInjector,()=>t(e,n));switch(t){case"pathParamsChange":return!wr(e.url,n.url);case"pathParamsOrQueryParamsChange":return!wr(e.url,n.url)||!An(e.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!qm(e,n)||!An(e.queryParams,n.queryParams);default:return!qm(e,n)}}function ua(e,n,t){let i=wo(e),r=e.value;Object.entries(i).forEach(([o,s])=>{r.component?n?ua(s,n.children.getContext(o),t):ua(s,null,t):ua(s,n,t)}),r.component?n&&n.outlet&&n.outlet.isActivated?t.canDeactivateChecks.push(new xo(n.outlet.component,r)):t.canDeactivateChecks.push(new xo(null,r)):t.canDeactivateChecks.push(new xo(null,r))}function Da(e){return typeof e=="function"}function tk(e){return typeof e=="boolean"}function nk(e){return e&&Da(e.canLoad)}function ik(e){return e&&Da(e.canActivate)}function rk(e){return e&&Da(e.canActivateChild)}function ok(e){return e&&Da(e.canDeactivate)}function sk(e){return e&&Da(e.canMatch)}function GD(e){return e instanceof qi||e?.name==="EmptyError"}var Dd=Symbol("INITIAL_VALUE");function ko(){return Pe(e=>Wr(e.map(n=>n.pipe(Xe(1),rt(Dd)))).pipe(U(n=>{for(let t of n)if(t!==!0){if(t===Dd)return Dd;if(t===!1||ak(t))return t}return!0}),_e(n=>n!==Dd),Xe(1)))}function ak(e){return xi(e)||e instanceof Ro}function WD(e){return e.aborted?x(void 0).pipe(Xe(1)):new L(n=>{let t=()=>{n.next(),n.complete()};return e.addEventListener("abort",t),()=>e.removeEventListener("abort",t)})}function qD(e){return We(WD(e))}function ck(e){return Ge(n=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?x(V(v({},n),{guardsResult:!0})):lk(o,t,i).pipe(Ge(s=>s&&tk(s)?dk(t,r,e):x(s)),U(s=>V(v({},n),{guardsResult:s})))})}function lk(e,n,t){return Se(e).pipe(Ge(i=>mk(i.component,i.route,t,n)),Fn(i=>i!==!0,!0))}function dk(e,n,t){return Se(n).pipe(ci(i=>pn(fk(i.route.parent,t),uk(i.route,t),pk(e,i.path),hk(e,i.route))),Fn(i=>i!==!0,!0))}function uk(e,n){return e!==null&&n&&n(new Ld(e)),x(!0)}function fk(e,n){return e!==null&&n&&n(new Fd(e)),x(!0)}function hk(e,n){let t=n.routeConfig?n.routeConfig.canActivate:null;if(!t||t.length===0)return x(!0);let i=t.map(r=>Yi(()=>{let o=n._environmentInjector,s=Oo(r,o),a=ik(s)?s.canActivate(n,e):Ye(o,()=>s(n,e));return xr(a).pipe(Fn())}));return x(i).pipe(ko())}function pk(e,n){let t=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>XR(o)).filter(o=>o!==null).map(o=>Yi(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Oo(a,c),d=rk(l)?l.canActivateChild(t,e):Ye(c,()=>l(t,e));return xr(d).pipe(Fn())});return x(s).pipe(ko())}));return x(r).pipe(ko())}function mk(e,n,t,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return x(!0);let o=r.map(s=>{let a=n._environmentInjector,c=Oo(s,a),l=ok(c)?c.canDeactivate(e,n,t,i):Ye(a,()=>c(e,n,t,i));return xr(l).pipe(Fn())});return x(o).pipe(ko())}function gk(e,n,t,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return x(!0);let s=o.map(a=>{let c=Oo(a,e),l=nk(c)?c.canLoad(n,t):Ye(e,()=>c(n,t)),d=xr(l);return r?d.pipe(qD(r)):d});return x(s).pipe(ko(),YD(i))}function YD(e){return sf(Le(n=>{if(typeof n!="boolean")throw Ud(e,n)}),U(n=>n===!0))}function vk(e,n,t,i,r,o){let s=n.canMatch;if(!s||s.length===0)return x(!0);let a=s.map(c=>{let l=Oo(c,e),d=sk(l)?l.canMatch(n,t,r):Ye(e,()=>l(n,t,r));return xr(d).pipe(qD(o))});return x(a).pipe(ko(),YD(i))}var Qn=class e extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,e.prototype)}},ya=class e extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,e.prototype)}};function yk(e){throw new _(4e3,!1)}function _k(e){throw zD(!1,lt.GuardRejected)}var Zm=class{urlSerializer;urlTree;constructor(n,t){this.urlSerializer=n,this.urlTree=t}async lineralizeSegments(n,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[W])throw yk(`${n.redirectTo}`);r=r.children[W]}}async applyRedirectCommands(n,t,i,r,o){let s=await bk(t,r,o);if(s instanceof Ft)throw new ya(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new ya(a);return a}applyRedirectCreateUrlTree(n,t,i,r){let o=this.createSegmentGroup(n,t.root,i,r);return new Ft(o,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(n,t){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=t[a]}else i[r]=o}),i}createSegmentGroup(n,t,i,r){let o=this.createSegments(n,t.segments,i,r),s=Object.create(null);return Object.entries(t.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new ue(o,s)}createSegments(n,t,i,r){return t.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,t,i){let r=i[t.path.substring(1)];if(!r)throw new _(4001,!1);return r}findOrReturn(n,t){let i=0;for(let r of t){if(r.path===n.path)return t.splice(i),r;i++}return n}};function bk(e,n,t){if(typeof e=="string")return Promise.resolve(e);let i=e;return xd(xr(Ye(t,()=>i(n))))}function Sk(e,n){return e.providers&&!e._injector&&(e._injector=mo(e.providers,n,`Route: ${e.path}`)),e._injector??n}function dn(e){return e.outlet||W}function Dk(e,n){let t=e.filter(i=>dn(i)===n);return t.push(...e.filter(i=>dn(i)!==n)),t}var Qm={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function KD(e){return{routeConfig:e.routeConfig,url:e.url,params:e.params,queryParams:e.queryParams,fragment:e.fragment,data:e.data,outlet:e.outlet,title:e.title,paramMap:e.paramMap,queryParamMap:e.queryParamMap}}function wk(e,n,t,i,r,o,s){let a=ZD(e,n,t);if(!a.matched)return x(a);let c=KD(o(a));return i=Sk(n,i),vk(i,n,t,r,c,s).pipe(U(l=>l===!0?a:v({},Qm)))}function ZD(e,n,t){if(n.path==="")return n.pathMatch==="full"&&(e.hasChildren()||t.length>0)?v({},Qm):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(n.matcher||_D)(t,e,n);if(!r)return v({},Qm);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?v(v({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function gD(e,n,t,i,r){return t.length>0&&xk(e,t,i,r)?{segmentGroup:new ue(n,Ek(i,new ue(t,e.children))),slicedSegments:[]}:t.length===0&&Ik(e,t,i)?{segmentGroup:new ue(e.segments,Ck(e,t,i,e.children)),slicedSegments:t}:{segmentGroup:new ue(e.segments,e.children),slicedSegments:t}}function Ck(e,n,t,i){let r={};for(let o of t)if($d(e,n,o)&&!i[dn(o)]){let s=new ue([],{});r[dn(o)]=s}return v(v({},i),r)}function Ek(e,n){let t={};t[W]=n;for(let i of e)if(i.path===""&&dn(i)!==W){let r=new ue([],{});t[dn(i)]=r}return t}function xk(e,n,t,i){return t.some(r=>!$d(e,n,r)||!(dn(r)!==W)?!1:!(i!==void 0&&dn(r)===i))}function Ik(e,n,t){return t.some(i=>$d(e,n,i))}function $d(e,n,t){return(e.hasChildren()||n.length>0)&&t.pathMatch==="full"?!1:t.path===""}function Nk(e,n,t){return n.length===0&&!e.children[t]}var Xm=class{};async function Mk(e,n,t,i,r,o,s,a){return new Jm(e,n,t,i,r,s,o,a).recognize()}var Tk=31,Jm=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,t,i,r,o,s,a,c){this.injector=n,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Zm(this.urlSerializer,this.urlTree)}noMatchError(n){return new _(4002,`'${n.segmentGroup}'`)}async recognize(){let n=gD(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=await this.match(n),r=new Ot(i,t),o=new ga("",r),s=AD(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let t=new Ao([],Object.freeze({}),Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),W,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,W,t),rootSnapshot:t}}catch(i){if(i instanceof ya)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Qn?this.noMatchError(i):i}}async processSegmentGroup(n,t,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,t,i,o);let s=await this.processSegment(n,t,i,i.segments,r,!0,o);return s instanceof Ot?[s]:[]}async processChildren(n,t,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=Dk(t,c),f=await this.processSegmentGroup(n,d,l,c,r);s.push(...f)}let a=QD(s);return Ak(a),a}async processSegment(n,t,i,r,o,s,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??n,t,c,i,r,o,s,a)}catch(l){if(l instanceof Qn||GD(l))continue;throw l}if(Nk(i,r,o))return new Xm;throw new Qn(i)}async processSegmentAgainstRoute(n,t,i,r,o,s,a,c){if(dn(i)!==s&&(s===W||!$d(r,o,i)))throw new Qn(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,t,i,o,s,c);throw new Qn(r)}async expandSegmentAgainstRouteUsingRedirect(n,t,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:h}=ZD(t,r,o);if(!c)throw new Qn(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>Tk&&(this.allowRedirects=!1));let p=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let m=await this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,KD(p),n),y=await this.applyRedirects.lineralizeSegments(r,m);return this.processSegment(n,i,t,y.concat(h),s,!1,a)}createSnapshot(n,t,i,r,o){let s=new Ao(i,r,Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,kk(t),dn(t),t.component??t._loadedComponent??null,t,Ok(t),n),a=eg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,t,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Ue=>this.createSnapshot(n,i,Ue.consumedSegments,Ue.parameters,s),c=await xd(wk(t,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new Qn(t);n=i._injector??n;let{routes:l}=await this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:p}=c,m=this.createSnapshot(n,i,h,f,s),{segmentGroup:y,slicedSegments:w}=gD(t,h,p,l,o);if(w.length===0&&y.hasChildren()){let Ue=await this.processChildren(d,l,y,m);return new Ot(m,Ue)}if(l.length===0&&w.length===0)return new Ot(m,[]);let G=dn(i)===o,xe=await this.processSegment(d,l,y,w,G?W:o,!0,m);return new Ot(m,xe instanceof Ot?[xe]:[])}async getChildConfig(n,t,i){if(t.children)return{routes:t.children,injector:n};if(t.loadChildren){if(t._loadedRoutes!==void 0){let o=t._loadedNgModuleFactory;return o&&!t._loadedInjector&&(t._loadedInjector=o.create(n).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await xd(gk(n,t,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,t);return t._loadedRoutes=o.routes,t._loadedInjector=o.injector,t._loadedNgModuleFactory=o.factory,o}throw _k(t)}return{routes:[],injector:n}}};function Ak(e){e.sort((n,t)=>n.value.outlet===W?-1:t.value.outlet===W?1:n.value.outlet.localeCompare(t.value.outlet))}function Rk(e){let n=e.value.routeConfig;return n&&n.path===""}function QD(e){let n=[],t=new Set;for(let i of e){if(!Rk(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):n.push(i)}for(let i of t){let r=QD(i.children);n.push(new Ot(i.value,r))}return n.filter(i=>!t.has(i))}function kk(e){return e.data||{}}function Ok(e){return e.resolve||{}}function Fk(e,n,t,i,r,o,s){return Ge(async a=>{let{state:c,tree:l}=await Mk(e,n,t,i,a.extractedUrl,r,o,s);return V(v({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function Pk(e){return Ge(n=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=n;if(!i.length)return x(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of XD(a))o.add(c);let s=0;return Se(o).pipe(ci(a=>r.has(a)?Lk(a,t,e):(a.data=eg(a,a.parent,e).resolve,x(void 0))),Le(()=>s++),Tc(1),Ge(a=>s===o.size?x(n):Fe))})}function XD(e){let n=e.children.map(t=>XD(t)).flat();return[e,...n]}function Lk(e,n,t){let i=e.routeConfig,r=e._resolve;return i?.title!==void 0&&!jD(i)&&(r[_a]=i.title),Yi(()=>(e.data=eg(e,e.parent,t).resolve,Vk(r,e,n).pipe(U(o=>(e._resolvedData=o,e.data=v(v({},e.data),o),null)))))}function Vk(e,n,t){let i=Bm(e);if(i.length===0)return x({});let r={};return Se(i).pipe(Ge(o=>jk(e[o],n,t).pipe(Fn(),Le(s=>{if(s instanceof Ro)throw Ud(new Xn,s);r[o]=s}))),Tc(1),U(()=>r),ai(o=>GD(o)?Fe:is(o)))}function jk(e,n,t){let i=n._environmentInjector,r=Oo(e,i),o=r.resolve?r.resolve(n,t):Ye(i,()=>r(n,t));return xr(o)}function vD(e){return Pe(n=>{let t=e(n);return t?Se(t).pipe(U(()=>n)):x(n)})}var rg=(()=>{class e{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===W);return i}getResolvedTitleForRoute(t){return t.data[_a]}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:()=>u(JD)})}return e})(),JD=(()=>{class e extends rg{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||e)(C(Do))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Mi=new g("",{factory:()=>({})}),Nr=new g(""),Gd=(()=>{class e{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(nm);async loadComponent(t,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await SD(Ye(t,()=>i.loadComponent())),s=await tw(cm(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await ew(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();async function ew(e,n,t,i){let r=await SD(Ye(t,()=>e.loadChildren())),o=await tw(cm(r)),s;o instanceof Yl||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(e);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(t).injector,d=s,c=a.get(Nr,[],{optional:!0,self:!0}).flat()),{routes:c.map(ig),injector:a,factory:d}}async function tw(e){return e}var Wd=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:()=>u(Bk)})}return e})(),Bk=(()=>{class e{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),og=new g(""),sg=new g("");function nw(e,n,t){let i=e.get(sg),r=e.get(P);if(!r.startViewTransition||i.skipNextTransition)return i.skipNextTransition=!1,new Promise(l=>setTimeout(l));let o,s=new Promise(l=>{o=l}),a=r.startViewTransition(()=>(o(),Uk(e)));a.updateCallbackDone.catch(l=>{}),a.ready.catch(l=>{}),a.finished.catch(l=>{});let{onViewTransitionCreated:c}=i;return c&&Ye(e,()=>c({transition:a,from:n,to:t})),s}function Uk(e){return new Promise(n=>{$t({read:()=>setTimeout(n)},{injector:e})})}var iw=new g(""),Hk=()=>{},ag=new g(""),qd=(()=>{class e{currentNavigation=z(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=z(null);events=new b;transitionAbortWithErrorSubject=new b;configLoader=u(Gd);environmentInjector=u(de);destroyRef=u(Ve);urlSerializer=u(Ni);rootContexts=u(Ir);location=u(Tn);inputBindingEnabled=u(Sa,{optional:!0})!==null;titleStrategy=u(rg);options=u(Mi,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||WR;urlHandlingStrategy=u(Wd);createViewTransition=u(og,{optional:!0});navigationErrorHandler=u(ag,{optional:!0});activatedRouteInjectorFeature=u(iw,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>x(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new kd(r)),i=r=>this.events.next(new Od(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;Q(()=>{this.transitions?.next(V(v({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new $e(null),this.transitions.pipe(_e(i=>i!==null),Pe(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return x(i).pipe(Pe(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",lt.SupersededByNewNavigation),Fe;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?V(v({},l),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=c.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new Rn(c.id,this.urlSerializer.serialize(c.rawUrl),"",Io.IgnoredSameUrlNavigation)),c.resolve(!1),Fe;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return x(c).pipe(Pe(h=>(this.events.next(new Ii(h.id,this.urlSerializer.serialize(h.extractedUrl),h.source,h.restoredState)),h.id!==this.navigationId?Fe:Promise.resolve(h))),Fk(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),Le(h=>{i.targetSnapshot=h.targetSnapshot,i.urlAfterRedirects=h.urlAfterRedirects,this.currentNavigation.update(p=>(p.finalUrl=h.urlAfterRedirects,p)),this.events.next(new pa)}),Pe(h=>Se(i.routesRecognizeHandler.deferredHandle??x(void 0)).pipe(U(()=>h))),Le(()=>{let h=new ha(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(h)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:h,extractedUrl:p,source:m,restoredState:y,extras:w}=c,G=new Ii(h,this.urlSerializer.serialize(p),m,y);this.events.next(G);let xe=LD(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=V(v({},c),{targetSnapshot:xe,urlAfterRedirects:p,extras:V(v({},w),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Ue=>(Ue.finalUrl=p,Ue)),x(i)}else return this.events.next(new Rn(c.id,this.urlSerializer.serialize(c.extractedUrl),"",Io.IgnoredByUrlHandlingStrategy)),c.resolve(!1),Fe}),U(c=>{let l=new Md(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=V(v({},c),{guards:QR(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),ck(c=>this.events.next(c)),Pe(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw Ud(this.urlSerializer,c.guardsResult);let l=new Td(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!a())return Fe;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",lt.GuardRejected),Fe;if(c.guards.canActivateChecks.length===0)return x(c);let d=new Ad(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!a())return Fe;let f=!1;return x(c).pipe(Pk(this.paramsInheritanceStrategy),Le({next:()=>{f=!0;let h=new Rd(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(h)},complete:()=>{f||this.cancelNavigationTransition(c,"",lt.NoDataFromResolver)}}))}),vD(c=>{let l=f=>{let h=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let p=f._environmentInjector;h.push(this.configLoader.loadComponent(p,f.routeConfig).then(m=>{f.component=m}))}for(let p of f.children)h.push(...l(p));return h},d=l(c.targetSnapshot.root);return d.length===0?x(c):Se(Promise.all(d).then(()=>c))}),Pe(c=>{let{newlyCreatedRoutes:l,state:d}=qR(t.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=V(v({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),x(c)}),this.activatedRouteInjectorFeature?.operator()??(c=>c),vD(()=>this.afterPreactivation()),Pe(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return d?Se(d).pipe(U(()=>i)):x(i)}),Xe(1),Pe(c=>{r=!1,this.events.next(new Mo);let l=i.beforeActivateHandler.deferredHandle;return l?Se(l.then(()=>c)):x(c)}),Le(c=>{new Km(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(l=>(l.abort=Hk,l)),this.lastSuccessfulNavigation.set(Q(this.currentNavigation)),this.events.next(new qt(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),We(WD(s.signal).pipe(_e(()=>!o&&r),Le(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",lt.Aborted)}))),Le({complete:()=>{o=!0}}),We(this.transitionAbortWithErrorSubject.pipe(Le(c=>{throw c}))),Ki(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",lt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),ai(c=>{if(o=!0,yD(i),this.destroyed)return i.resolve(!1),Fe;if($D(c))this.events.next(new Wt(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),ZR(c)?this.events.next(new To(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new Er(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let d=Ye(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof Ro){let{message:f,cancellationCode:h}=Ud(this.urlSerializer,d);this.events.next(new Wt(i.id,this.urlSerializer.serialize(i.extractedUrl),f,h)),this.events.next(new To(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return Fe}))}))}cancelNavigationTransition(t,i,r){yD(t);let o=new Wt(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(o),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Q(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function zk(e){return e!==Eo}function yD(e){if(e.newlyCreatedRoutes)for(let n of e.newlyCreatedRoutes)n._localInjector?.destroy()}var rw=new g("");var ow=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:()=>u($k)})}return e})(),zd=class{shouldDetach(n){return!1}store(n,t){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,t){return n.routeConfig===t.routeConfig}shouldDestroyInjector(n){return!0}},$k=(()=>{class e extends zd{static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),Yd=(()=>{class e{urlSerializer=u(Ni);options=u(Mi,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(Tn);urlHandlingStrategy=u(Wd);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Ft;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let o=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,s=r??o;return s instanceof Ft?this.urlSerializer.serialize(s):s}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=LD(null,u(de));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:()=>u(Gk)})}return e})(),Gk=(()=>{class e extends Yd{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof Ii?this.updateStateMemento():t instanceof Rn?this.commitTransition(i):t instanceof ha?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Mo?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Wt&&!PD(t)?this.restoreHistory(i):t instanceof Er?this.restoreHistory(i,!0):t instanceof qt&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(t)||s){let c=this.browserPageId,l=v(v({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(t,"",l)}else{let c=v(v({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(t,"",c)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===t.finalUrl&&o===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i,r){return this.canceledNavigationResolution==="computed"?v({navigationId:t,\u0275routerPageId:i},this.routerUrlState(r)):v({navigationId:t},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function Kd(e,n){e.events.pipe(_e(t=>t instanceof qt||t instanceof Wt||t instanceof Er||t instanceof Rn),U(t=>t instanceof qt||t instanceof Rn?0:(t instanceof Wt?t.code===lt.Redirect||t.code===lt.SupersededByNewNavigation:!1)?2:1),_e(t=>t!==2),Xe(1)).subscribe(()=>{n()})}var Kt=(()=>{class e{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(Ql);stateManager=u(Yd);options=u(Mi,{optional:!0})||{};pendingTasks=u(Un);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(qd);urlSerializer=u(Ni);location=u(Tn);urlHandlingStrategy=u(Wd);injector=u(de);_events=new b;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(ow);injectorCleanup=u(rw,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(Nr,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(Sa,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new se;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Q(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Wt&&i.code!==lt.Redirect&&i.code!==lt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof qt)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof To){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=v({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||zk(r.source)},s);this.scheduleNavigation(a,Eo,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}$R(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Eo,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,o)=>{this.navigateToSyncWithBrowser(t,r,i,o)})}navigateToSyncWithBrowser(t,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??t;if(r?.\u0275routerUrl&&(o=V(v({},o),{browserUrl:t})),r){let l=v({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Dt)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Q(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(ig),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=v(v({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=RD(h)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),f=this.currentUrlTree.root}return kD(f,t,d,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=xi(t)?t:this.parseUrl(t),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Eo,null,i)}navigate(t,i={skipLocationChange:!1}){return Wk(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(Xt(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=v({},wD):i===!1?r=v({},Um):r=v(v({},Um),i),xi(t))return uD(this.currentUrlTree,t,r);let o=this.parseUrl(t);return uD(this.currentUrlTree,o,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(t,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((f,h)=>{a=f,c=h});let d=this.pendingTasks.add();return Kd(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function Wk(e){for(let n=0;n<e.length;n++)if(e[n]==null)throw new _(4008,!1)}var Yk=(()=>{class e{router=u(Kt);stateManager=u(Yd);fragment=z("");queryParams=z({});path=z("");serializer=u(Ni);constructor(){this.updateState(),this.router.events?.subscribe(t=>{t instanceof qt&&this.updateState()})}updateState(){let{fragment:t,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(t),this.queryParams.set(r),this.path.set(this.serializer.serialize(new Ft(i)))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),Ti=(()=>{class e{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=u(new yo("href"),{optional:!0});reactiveHref=id(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Q(this.reactiveHref)}set href(t){this.reactiveHref.set(t)}set target(t){this._target.set(t)}get target(){return Q(this._target)}_target=z(void 0);set queryParams(t){this._queryParams.set(t)}get queryParams(){return Q(this._queryParams)}_queryParams=z(void 0,{equal:()=>!1});set fragment(t){this._fragment.set(t)}get fragment(){return Q(this._fragment)}_fragment=z(void 0);set queryParamsHandling(t){this._queryParamsHandling.set(t)}get queryParamsHandling(){return Q(this._queryParamsHandling)}_queryParamsHandling=z(void 0);set state(t){this._state.set(t)}get state(){return Q(this._state)}_state=z(void 0,{equal:()=>!1});set info(t){this._info.set(t)}get info(){return Q(this._info)}_info=z(void 0,{equal:()=>!1});set relativeTo(t){this._relativeTo.set(t)}get relativeTo(){return Q(this._relativeTo)}_relativeTo=z(void 0);set preserveFragment(t){this._preserveFragment.set(t)}get preserveFragment(){return Q(this._preserveFragment)}_preserveFragment=z(!1);set skipLocationChange(t){this._skipLocationChange.set(t)}get skipLocationChange(){return Q(this._skipLocationChange)}_skipLocationChange=z(!1);set replaceUrl(t){this._replaceUrl.set(t)}get replaceUrl(){return Q(this._replaceUrl)}_replaceUrl=z(!1);browserUrl=Sr(void 0);isAnchorElement;onChanges=new b;applicationErrorHandler=u(Dt);options=u(Mi,{optional:!0});reactiveRouterState=u(Yk);constructor(t,i,r,o,s,a){this.router=t,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(t){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",t)}ngOnChanges(t){this.onChanges.next(this)}routerLinkInput=z(null);set routerLink(t){t==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(xi(t)?this.routerLinkInput.set(t):this.routerLinkInput.set(Array.isArray(t)?t:[t]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(t,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(t!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=v({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(a,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(t,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,t,i):r.removeAttribute(o,t)}_urlTree=ct(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let t=r=>r==="preserve"||r==="merge";(t(this._queryParamsHandling())||t(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:xi(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(t,i)=>this.computeHref(t)===this.computeHref(i)});get urlTree(){return Q(this._urlTree)}computeHref(t){return t!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(t))??"":null}static \u0275fac=function(i){return new(i||e)(ae(Kt),ae(Yt),Us("tabindex"),ae(Ie),ae(B),ae(ln))};static \u0275dir=k({type:e,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&Ee("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&oe("href",r.reactiveHref(),vp)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",re],skipLocationChange:[2,"skipLocationChange","skipLocationChange",re],replaceUrl:[2,"replaceUrl","replaceUrl",re],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[Je]})}return e})();var wa=class{};var sw=(()=>{class e{router;injector;preloadingStrategy;loader;subscription;constructor(t,i,r,o){this.router=t,this.injector=i,this.preloadingStrategy=r,this.loader=o}setUpPreloading(){this.subscription=this.router.events.pipe(_e(t=>t instanceof qt),ci(()=>this.preload())).subscribe(()=>{})}preload(){return this.processRoutes(this.injector,this.router.config)}ngOnDestroy(){this.subscription?.unsubscribe()}processRoutes(t,i){let r=[];for(let o of i){o.providers&&!o._injector&&(o._injector=mo(o.providers,t,""));let s=o._injector??t;o._loadedNgModuleFactory&&!o._loadedInjector&&(o._loadedInjector=o._loadedNgModuleFactory.create(s).injector);let a=o._loadedInjector??s;(o.loadChildren&&!o._loadedRoutes&&o.canLoad===void 0||o.loadComponent&&!o._loadedComponent)&&r.push(this.preloadConfig(s,o)),(o.children||o._loadedRoutes)&&r.push(this.processRoutes(a,o.children??o._loadedRoutes))}return Se(r).pipe(si())}preloadConfig(t,i){return this.preloadingStrategy.preload(i,()=>{if(t.destroyed)return x(null);let r;i.loadChildren&&i.canLoad===void 0?r=Se(this.loader.loadChildren(t,i)):r=x(null);let o=r.pipe(Ge(s=>s===null?x(void 0):(i._loadedRoutes=s.routes,i._loadedInjector=s.injector,i._loadedNgModuleFactory=s.factory,this.processRoutes(s.injector??t,s.routes))));if(i.loadComponent&&!i._loadedComponent){let s=this.loader.loadComponent(t,i);return Se([o,s]).pipe(si())}else return o})}static \u0275fac=function(i){return new(i||e)(C(Kt),C(de),C(wa),C(Gd))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),aw=new g(""),Kk=(()=>{class e{options;routerEventsSubscription;scrollEventsSubscription;lastId=0;lastSource=Eo;restoredId=0;store={};isHydrating=u(lp,{optional:!0})??!1;urlSerializer=u(Ni);zone=u(A);viewportScroller=u(ym);transitions=u(qd);constructor(t){this.options=t,this.options.scrollPositionRestoration||="disabled",this.options.anchorScrolling||="disabled",this.isHydrating&&u(Ct).whenStable().then(()=>{this.isHydrating=!1})}init(){this.options.scrollPositionRestoration!=="disabled"&&this.viewportScroller.setHistoryScrollRestoration("manual"),this.routerEventsSubscription=this.createScrollEvents(),this.scrollEventsSubscription=this.consumeScrollEvents()}createScrollEvents(){return this.transitions.events.subscribe(t=>{t instanceof Ii?(this.store[this.lastId]=this.viewportScroller.getScrollPosition(),this.lastSource=t.navigationTrigger,this.restoredId=t.restoredState?t.restoredState.navigationId:0):t instanceof qt?(this.lastId=t.id,this.scheduleScrollEvent(t,this.urlSerializer.parse(t.urlAfterRedirects).fragment)):t instanceof Rn&&t.code===Io.IgnoredSameUrlNavigation&&(this.lastSource=void 0,this.restoredId=0,this.scheduleScrollEvent(t,this.urlSerializer.parse(t.url).fragment))})}consumeScrollEvents(){return this.transitions.events.subscribe(t=>{if(!(t instanceof No)||t.scrollBehavior==="manual")return;let i={behavior:"instant"};t.position?this.options.scrollPositionRestoration==="top"?this.viewportScroller.scrollToPosition([0,0],i):this.options.scrollPositionRestoration==="enabled"&&this.viewportScroller.scrollToPosition(t.position,i):t.anchor&&this.options.anchorScrolling==="enabled"?this.viewportScroller.scrollToAnchor(t.anchor):this.options.scrollPositionRestoration!=="disabled"&&this.viewportScroller.scrollToPosition([0,0])})}scheduleScrollEvent(t,i){if(this.isHydrating)return;let r=Q(this.transitions.currentNavigation)?.extras.scroll;this.zone.runOutsideAngular(async()=>{await new Promise(o=>{setTimeout(o),typeof requestAnimationFrame<"u"&&requestAnimationFrame(o)}),this.zone.run(()=>{this.transitions.events.next(new No(t,this.lastSource==="popstate"?this.store[this.restoredId]:null,i,r))})})}ngOnDestroy(){this.routerEventsSubscription?.unsubscribe(),this.scrollEventsSubscription?.unsubscribe()}static \u0275fac=function(i){qs()};static \u0275prov=H({token:e,factory:e.\u0275fac})}return e})();function lg(e,...n){return Vt([{provide:Nr,multi:!0,useValue:e},{provide:Yt,useFactory:cw},{provide:go,multi:!0,useFactory:lw},n.map(t=>t.\u0275providers)])}function cw(){return u(Kt).routerState.root}function Ca(e,n){return{\u0275kind:e,\u0275providers:n}}function lw(){let e=u(ie);return n=>{let t=e.get(Ct);if(n!==t.components[0])return;let i=e.get(Kt),r=e.get(dw);e.get(dg)===1&&i.initialNavigation(),e.get(hw,null,{optional:!0})?.setUpPreloading(),e.get(aw,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var dw=new g("",{factory:()=>new b}),dg=new g("",{factory:()=>1});function uw(){let e=[{provide:Ol,useValue:!0},{provide:dg,useValue:0},Kl(()=>{let n=u(ie);return n.get(um,Promise.resolve()).then(()=>new Promise(i=>{let r=n.get(Kt),o=n.get(dw);Kd(r,()=>{i(!0)}),n.get(qd).afterPreactivation=()=>(i(!0),o.closed?x(void 0):o),r.initialNavigation()}))})];return Ca(2,e)}function fw(){let e=[Kl(()=>{u(Kt).setUpLocationChangeListener()}),{provide:dg,useValue:2}];return Ca(3,e)}var hw=new g("");function pw(e){return Ca(0,[{provide:hw,useExisting:sw},{provide:wa,useExisting:e}])}function mw(e={}){return Ca(8,[{provide:Sa,useFactory:()=>new UD(e)}])}function gw(e){sn("NgRouterViewTransitions");let n=[{provide:og,useValue:nw},{provide:sg,useValue:v({skipNextTransition:!!e?.skipInitialTransition},e)}];return Ca(9,n)}var vw=[Tn,{provide:Ni,useClass:Xn},Kt,Ir,{provide:Yt,useFactory:cw},Gd],Mr=(()=>{class e{constructor(){}static forRoot(t,i){return{ngModule:e,providers:[vw,[],{provide:Nr,multi:!0,useValue:t},[],i?.errorHandler?{provide:ag,useValue:i.errorHandler}:[],{provide:Mi,useValue:i||{}},i?.useHash?Qk():Xk(),Zk(),i?.preloadingStrategy?pw(i.preloadingStrategy).\u0275providers:[],i?.initialNavigation?Jk(i):[],i?.bindToComponentInputs?mw(typeof i.bindToComponentInputs=="object"?i.bindToComponentInputs:{}).\u0275providers:[],i?.enableViewTransitions?gw().\u0275providers:[],eO()]}}static forChild(t){return{ngModule:e,providers:[{provide:Nr,multi:!0,useValue:t}]}}static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})();function Zk(){return{provide:aw,useFactory:()=>{let e=u(ym),n=u(Mi);return n.scrollOffset&&e.setOffset(n.scrollOffset),new Kk(n)}}}function Qk(){return{provide:ln,useClass:pm}}function Xk(){return{provide:ln,useClass:cd}}function Jk(e){return[e.initialNavigation==="disabled"?fw().\u0275providers:[],e.initialNavigation==="enabledBlocking"?uw().\u0275providers:[]]}var cg=new g("");function eO(){return[{provide:cg,useFactory:lw},{provide:go,multi:!0,useExisting:cg}]}var Fo=class e{url="https://dog.ceo/api";async getAllBreeds(){let n=await fetch(`${this.url}/breeds/list/all`);if(!n.ok)throw new Error("Erro ao buscar ra\xE7as");return n.json()}async getBreed(n,t){let i=`${this.url}/breed/${n}/images/random`;n&&t&&(i=`${this.url}/breed/${n}/${t}/images/random`);let r=await fetch(i);if(!r.ok)throw new Error(`Erro ao buscar ra\xE7a ${n} ${t}`);return r.json()}async getSubBreeds(n){let t=await fetch(`${this.url}/breed/${n}/list`);if(!t.ok)throw new Error("Erro ao buscar sub-ra\xE7as");return t.json()}static \u0275fac=function(t){return new(t||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})};var tO=new g("cdk-dir-doc",{providedIn:"root",factory:()=>u(P)}),nO=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function yw(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?nO.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Jn=(()=>{class e{get value(){return this.valueSignal()}valueSignal=z("ltr");change=new ne;constructor(){let t=u(tO,{optional:!0});if(t){let i=t.body?t.body.dir:null,r=t.documentElement?t.documentElement.dir:null;this.valueSignal.set(yw(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var le=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})();var iO=["*"];var rO=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],oO=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],sO=new g("MAT_CARD_CONFIG"),_w=(()=>{class e{appearance;constructor(){let t=u(sO,{optional:!0});this.appearance=t?.appearance||"raised"}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&ee("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:iO,decls:1,vars:0,template:function(i,r){i&1&&(Ae(),J(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-elevated-container-elevation, var(--%NS%mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--%NS%mat-card-outlined-container-color, var(--%NS%mat-sys-surface));
  border-radius: var(--%NS%mat-card-outlined-container-shape, var(--%NS%mat-sys-corner-medium));
  border-width: var(--%NS%mat-card-outlined-outline-width, 1px);
  border-color: var(--%NS%mat-card-outlined-outline-color, var(--%NS%mat-sys-outline-variant));
  box-shadow: var(--%NS%mat-card-outlined-container-elevation, var(--%NS%mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--%NS%mat-card-filled-container-color, var(--%NS%mat-sys-surface-container-highest));
  border-radius: var(--%NS%mat-card-filled-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-filled-container-elevation, var(--%NS%mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--%NS%mat-card-title-text-font, var(--%NS%mat-sys-title-large-font));
  line-height: var(--%NS%mat-card-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-size: var(--%NS%mat-card-title-text-size, var(--%NS%mat-sys-title-large-size));
  letter-spacing: var(--%NS%mat-card-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  font-weight: var(--%NS%mat-card-title-text-weight, var(--%NS%mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--%NS%mat-card-subtitle-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-card-subtitle-text-font, var(--%NS%mat-sys-title-medium-font));
  line-height: var(--%NS%mat-card-subtitle-text-line-height, var(--%NS%mat-sys-title-medium-line-height));
  font-size: var(--%NS%mat-card-subtitle-text-size, var(--%NS%mat-sys-title-medium-size));
  letter-spacing: var(--%NS%mat-card-subtitle-text-tracking, var(--%NS%mat-sys-title-medium-tracking));
  font-weight: var(--%NS%mat-card-subtitle-text-weight, var(--%NS%mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return e})(),bw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return e})();var Sw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return e})();var Dw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:oO,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(Ae(rO),J(0),nt(1,"div",0),J(2,1),it(),J(3,2))},encapsulation:2})}return e})(),ww=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["mat-card-footer"]],hostAttrs:[1,"mat-mdc-card-footer"]})}return e})();var Cw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();function Ai(e,...n){return n.length?n.some(t=>e[t]):e.altKey||e.shiftKey||e.ctrlKey||e.metaKey}var Tw=(()=>{class e{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||e)(ae(Ie),ae(B))};static \u0275dir=k({type:e})}return e})(),cO=(()=>{class e extends Tw{static \u0275fac=(()=>{let t;return function(r){return(t||(t=At(e)))(r||e)}})();static \u0275dir=k({type:e,features:[ke]})}return e})(),cu=new g("");var lO={provide:cu,useExisting:bt(()=>lu),multi:!0};function dO(){let e=Gt()?Gt().getUserAgent():"";return/android (\d+)/.test(e.toLowerCase())}var uO=new g(""),lu=(()=>{class e extends Tw{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!dO())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||e)(ae(Ie),ae(B),ae(uO,8))};static \u0275dir=k({type:e,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&Ee("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Re([lO]),ke]})}return e})();function pg(e){return e==null||mg(e)===0}function mg(e){return e==null?null:Array.isArray(e)||typeof e=="string"?e.length:e instanceof Set?e.size:null}var du=new g(""),gg=new g(""),fO=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Ta=class{static min(n){return hO(n)}static max(n){return pO(n)}static required(n){return Aw(n)}static requiredTrue(n){return mO(n)}static email(n){return gO(n)}static minLength(n){return vO(n)}static maxLength(n){return yO(n)}static pattern(n){return _O(n)}static nullValidator(n){return Qd()}static compose(n){return Lw(n)}static composeAsync(n){return Vw(n)}};function hO(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t<e?{min:{min:e,actual:n.value}}:null}}function pO(e){return n=>{if(n.value==null||e==null)return null;let t=parseFloat(n.value);return!isNaN(t)&&t>e?{max:{max:e,actual:n.value}}:null}}function Aw(e){return pg(e.value)?{required:!0}:null}function mO(e){return e.value===!0?null:{required:!0}}function gO(e){return pg(e.value)||fO.test(e.value)?null:{email:!0}}function vO(e){return n=>{let t=n.value?.length??mg(n.value);return t===null||t===0?null:t<e?{minlength:{requiredLength:e,actualLength:t}}:null}}function yO(e){return n=>{let t=n.value?.length??mg(n.value);return t!==null&&t>e?{maxlength:{requiredLength:e,actualLength:t}}:null}}function _O(e){if(!e)return Qd;let n,t;return typeof e=="string"?(t="",e.charAt(0)!=="^"&&(t+="^"),t+=e,e.charAt(e.length-1)!=="$"&&(t+="$"),n=new RegExp(t)):(t=e.toString(),n=e),i=>{if(pg(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:t,actualValue:r}}}}function Qd(e){return null}function Rw(e){return e!=null}function kw(e){return In(e)?Se(e):e}function Ow(e){let n={};return e.forEach(t=>{n=t!=null?v(v({},n),t):n}),Object.keys(n).length===0?null:n}function Fw(e,n){return n.map(t=>t(e))}function bO(e){return!e.validate}function Pw(e){return e.map(n=>bO(n)?n:t=>n.validate(t))}function Lw(e){if(!e)return null;let n=e.filter(Rw);return n.length==0?null:function(t){return Ow(Fw(t,n))}}function vg(e){return e!=null?Lw(Pw(e)):null}function Vw(e){if(!e)return null;let n=e.filter(Rw);return n.length==0?null:function(t){let i=Fw(t,n).map(kw);return os(i).pipe(U(Ow))}}function yg(e){return e!=null?Vw(Pw(e)):null}function Ew(e,n){return e===null?[n]:Array.isArray(e)?[...e,n]:[e,n]}function jw(e){return e._rawValidators}function Bw(e){return e._rawAsyncValidators}function fg(e){return e?Array.isArray(e)?e:[e]:[]}function Xd(e,n){return Array.isArray(e)?e.includes(n):e===n}function xw(e,n){let t=fg(n);return fg(e).forEach(r=>{Xd(t,r)||t.push(r)}),t}function Iw(e,n){return fg(n).filter(t=>!Xd(e,t))}var Jd=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=vg(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=yg(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,t){return this.control?this.control.hasError(n,t):!1}getError(n,t){return this.control?this.control.getError(n,t):null}},Tr=class extends Jd{name;get formDirective(){return null}get path(){return null}};var Ea="VALID",Zd="INVALID",Po="PENDING",xa="DISABLED",Ri=class{},eu=class extends Ri{value;source;constructor(n,t){super(),this.value=n,this.source=t}},Na=class extends Ri{pristine;source;constructor(n,t){super(),this.pristine=n,this.source=t}},Ma=class extends Ri{touched;source;constructor(n,t){super(),this.touched=n,this.source=t}},Lo=class extends Ri{status;source;constructor(n,t){super(),this.status=n,this.source=t}},tu=class extends Ri{source;constructor(n){super(),this.source=n}},Vo=class extends Ri{source;constructor(n){super(),this.source=n}};function Uw(e){return(uu(e)?e.validators:e)||null}function SO(e){return Array.isArray(e)?vg(e):e||null}function Hw(e,n){return(uu(n)?n.asyncValidators:e)||null}function DO(e){return Array.isArray(e)?yg(e):e||null}function uu(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}function wO(e,n,t){let i=e.controls;if(!(n?Object.keys(i):i).length)throw new _(1e3,"");if(!zw(i,t))throw new _(1001,"")}function CO(e,n,t){e._forEachChild((i,r)=>{if(t[r]===void 0)throw new _(-1002,"")})}var nu=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=z(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,t){this._assignValidators(n),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Q(this.statusReactive)}set status(n){Q(()=>this.statusReactive.set(n))}_status=ct(()=>this.statusReactive());statusReactive=z(void 0);get valid(){return this.status===Ea}get invalid(){return this.status===Zd}get pending(){return this.status===Po}get disabled(){return this.status===xa}get enabled(){return this.status!==xa}errors;get pristine(){return Q(this.pristineReactive)}set pristine(n){Q(()=>this.pristineReactive.set(n))}_pristine=ct(()=>this.pristineReactive());pristineReactive=z(!0);get dirty(){return!this.pristine}get touched(){return Q(this.touchedReactive)}set touched(n){Q(()=>this.touchedReactive.set(n))}_touched=ct(()=>this.touchedReactive());touchedReactive=z(!1);get untouched(){return!this.touched}_events=new b;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(xw(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(xw(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(Iw(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(Iw(n,this._rawAsyncValidators))}hasValidator(n){return Xd(this._rawValidators,n)}hasAsyncValidator(n){return Xd(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let t=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(V(v({},n),{sourceControl:i})),t&&n.emitEvent!==!1&&this._events.next(new Ma(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(n))}markAsUntouched(n={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),t&&n.emitEvent!==!1&&this._events.next(new Ma(!1,i))}markAsDirty(n={}){let t=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(V(v({},n),{sourceControl:i})),t&&n.emitEvent!==!1&&this._events.next(new Na(!1,i))}markAsPristine(n={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),t&&n.emitEvent!==!1&&this._events.next(new Na(!0,i))}markAsPending(n={}){this.status=Po;let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Lo(this.status,t)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(V(v({},n),{sourceControl:t}))}disable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=xa,this.errors=null,this._forEachChild(r=>{r.disable(V(v({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new eu(this.value,i)),this._events.next(new Lo(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(V(v({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=Ea,this._forEachChild(i=>{i.enable(V(v({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(V(v({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,t){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Ea||this.status===Po)&&this._runAsyncValidator(i,n.emitEvent)}let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new eu(this.value,t)),this._events.next(new Lo(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(V(v({},n),{sourceControl:t}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?xa:Ea}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,t){if(this.asyncValidator){this.status=Po,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:n!==!1};let i=kw(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,t={}){this.errors=n,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(n){let t=n;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(n,t){let i=t?this.get(t):this;return i?.errors?i.errors[n]:null}hasError(n,t){return!!this.getError(n,t)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,t,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Lo(this.status,t)),this._parent&&this._parent._updateControlsErrors(n,t,i)}_initObservables(){this.valueChanges=new ne,this.statusChanges=new ne}_calculateStatus(){return this._allControlsDisabled()?xa:this.errors?Zd:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Po)?Po:this._anyControlsHaveStatus(Zd)?Zd:Ea}_anyControlsHaveStatus(n){return this._anyControls(t=>t.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,t),r&&this._events.next(new Na(this.pristine,t))}_updateTouched(n={},t){this.touched=this._anyControlsTouched(),this._events.next(new Ma(this.touched,t)),n.onlySelf||this._parent?._updateTouched(n,t)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){uu(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=SO(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=DO(this._rawAsyncValidators)}_updateHasRequiredValidator(){Q(()=>this._hasRequired.set(this.hasValidator(Ta.required)))}};function zw(e,n){return Object.hasOwn(e,n)}function EO(e){return e.tagName==="INPUT"||e.tagName==="SELECT"||e.tagName==="TEXTAREA"}function xO(e,n,t,i){switch(t){case"name":e.setAttribute(n,t,i);break;case"disabled":case"readonly":case"required":i?e.setAttribute(n,t,""):e.removeAttribute(n,t);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?e.setAttribute(n,t,i.toString()):e.removeAttribute(n,t);break}}var hg=class{kind;context;control;message;constructor({kind:n,context:t,control:i}){this.kind=n,this.context=t,this.control=i}};var IO=(()=>{class e{_validator=Qd;_onChange;_enabled;ngOnChanges(t){if(this.inputName in t){let i=this.normalizeInput(t[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Qd,this._onChange?.()}}validate(t){return this._validator(t)}registerOnValidatorChange(t){this._onChange=t}enabled(t){return t!=null}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,features:[Je]})}return e})();var NO={provide:du,useExisting:bt(()=>$w),multi:!0};var $w=(()=>{class e extends IO{required;inputName="required";normalizeInput=re;createValidator=t=>Aw;enabled(t){return t}static \u0275fac=(()=>{let t;return function(r){return(t||(t=At(e)))(r||e)}})();static \u0275dir=k({type:e,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&oe("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Re([NO]),ke]})}return e})();var MO=new g(""),Aa=new g("",{factory:()=>fu}),fu="always";function TO(e,n,t=fu){_g(e,n),n.valueAccessor.writeValue(e.value),(e.disabled||t==="always")&&n.valueAccessor.setDisabledState?.(e.disabled),RO(e,n),OO(e,n),kO(e,n),AO(e,n)}function iu(e,n,t=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),ou(e,n),e&&(n._invokeOnDestroyCallbacks(),e._registerOnCollectionChange(()=>{}))}function ru(e,n){e.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(n)})}function AO(e,n){if(n.valueAccessor.setDisabledState){let t=i=>{n.valueAccessor.setDisabledState(i)};e.registerOnDisabledChange(t),n._registerOnDestroy(()=>{e._unregisterOnDisabledChange(t)})}}function _g(e,n){let t=jw(e);n.validator!==null?e.setValidators(Ew(t,n.validator)):typeof t=="function"&&e.setValidators([t]);let i=Bw(e);n.asyncValidator!==null?e.setAsyncValidators(Ew(i,n.asyncValidator)):typeof i=="function"&&e.setAsyncValidators([i]);let r=()=>e.updateValueAndValidity();ru(n._rawValidators,r),ru(n._rawAsyncValidators,r)}function ou(e,n){let t=!1;if(e!==null){if(n.validator!==null){let r=jw(e);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(t=!0,e.setValidators(o))}}if(n.asyncValidator!==null){let r=Bw(e);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(t=!0,e.setAsyncValidators(o))}}}let i=()=>{};return ru(n._rawValidators,i),ru(n._rawAsyncValidators,i),t}function RO(e,n){n.valueAccessor.registerOnChange(t=>{e._pendingValue=t,e._pendingChange=!0,e._pendingDirty=!0,e.updateOn==="change"&&Gw(e,n)})}function kO(e,n){n.valueAccessor.registerOnTouched(()=>{e._pendingTouched=!0,e.updateOn==="blur"&&e._pendingChange&&Gw(e,n),e.updateOn!=="submit"&&e.markAsTouched()})}function Gw(e,n){e._pendingDirty&&e.markAsDirty(),e.setValue(e._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(e._pendingValue),e._pendingChange=!1}function OO(e,n){let t=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};e.registerOnChange(t),n._registerOnDestroy(()=>{e._unregisterOnChange(t)})}function Ww(e,n){e==null,_g(e,n)}function FO(e,n){return ou(e,n)}function PO(e,n){if(!e.hasOwnProperty("model"))return!1;let t=e.model;return t.isFirstChange()?!0:!Object.is(n,t.currentValue)}function LO(e){return Object.getPrototypeOf(e.constructor)===cO}function qw(e,n){e._syncPendingControls(),n.forEach(t=>{let i=t.control;i.updateOn==="submit"&&i._pendingChange&&(t.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function VO(e,n){if(!n)return null;Array.isArray(n);let t,i,r;return n.forEach(o=>{o.constructor===lu?t=o:LO(o)?i=o:r=o}),r||i||t||null}function jO(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}var BO={provide:MO,useFactory:()=>{let e=u(ki,{self:!0});return{setParseErrors:n=>{e.setParseErrorSource(n)},set onReset(n){e.onReset=n}}}},ki=class extends Jd{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(t=>{t instanceof Vo&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=VO(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,t,i){super(),this.injector=n,this.renderer=t,this.rawValueAccessors=i,this.injector?.get(Ve)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(Ze);if(!this.control||!n)return;let t=n.markForCheck.bind(n);this.subscription=new se,this.subscription.add(this.control.valueChanges.subscribe(t)),this.subscription.add(this.control.statusChanges.subscribe(t)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Vo&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=EO(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof $w))}ngControlUpdate(n,t){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,t,i,r){if(t[i]===r)return;t[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&xO(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let t=this.control;return Object.entries(n).map(([i,r])=>new hg({context:r,kind:i,control:t}))}setParseErrorSource(n){if(n===void 0)return;let t=null,i=ct(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>t).bind(this),Ht(()=>{t=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},su=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Yw=(()=>{class e extends su{constructor(t){super(t)}static \u0275fac=function(i){return new(i||e)(ae(ki,2))};static \u0275dir=k({type:e,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&ee("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[ke]})}return e})(),Kw=(()=>{class e extends su{constructor(t){super(t)}static \u0275fac=function(i){return new(i||e)(ae(Tr,10))};static \u0275dir=k({type:e,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&ee("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[ke]})}return e})(),au=class extends nu{constructor(n,t,i){super(Uw(t),Hw(i,t)),this.controls=n,this._initObservables(),this._setUpdateStrategy(t),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,t){let i=this._find(n);return i||(this.controls[n]=t,t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange),t)}addControl(n,t,i={}){this.registerControl(n,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,t={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:t.emitEvent}),this._onCollectionChange()}setControl(n,t,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],t&&this.registerControl(n,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,t={}){Q(()=>{CO(this,!0,n),Object.keys(n).forEach(i=>{wO(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t)})}patchValue(n,t={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t))}reset(n={},t={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,V(v({},t),{onlySelf:!0}))}),this._updatePristine(t,this),this._updateTouched(t,this),this.updateValueAndValidity(t),t?.emitEvent!==!1&&this._events.next(new Vo(this))}getRawValue(){return this._reduceChildren({},(n,t,i)=>(n[i]=t.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(t,i)=>i._syncPendingControls()?!0:t);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(t=>{let i=this.controls[t];i&&n(i,t)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[t,i]of Object.entries(this.controls))if(this.contains(t)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(t,i,r)=>((i.enabled||this.disabled)&&(t[r]=i.value),t))}_reduceChildren(n,t){let i=n;return this._forEachChild((r,o)=>{i=t(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return zw(this.controls,n)?this.controls[n]:null}};var UO={provide:Tr,useExisting:bt(()=>Ra)},Ia=Promise.resolve(),Ra=(()=>{class e extends Tr{callSetDisabledState;get submitted(){return Q(this.submittedReactive)}_submitted=ct(()=>this.submittedReactive());submittedReactive=z(!1);_directives=new Set;form;ngSubmit=new ne;options;constructor(t,i,r){super(),this.callSetDisabledState=r,this.form=new au({},vg(t),yg(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(t){Ia.then(()=>{let i=this._findContainer(t.path);t.control=i.registerControl(t.name,t.control),t._setupWithForm(this.callSetDisabledState),t.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(t)})}getControl(t){return this.form.get(t.path)}removeControl(t){Ia.then(()=>{this._findContainer(t.path)?.removeControl(t.name),this._directives.delete(t)})}addFormGroup(t){Ia.then(()=>{let i=this._findContainer(t.path),r=new au({});Ww(r,t),i.registerControl(t.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(t){Ia.then(()=>{this._findContainer(t.path)?.removeControl?.(t.name)})}getFormGroup(t){return this.form.get(t.path)}updateModel(t,i){Ia.then(()=>{this.form.get(t.path).setValue(i)})}setValue(t){this.control.setValue(t)}onSubmit(t){return this.submittedReactive.set(!0),qw(this.form,this._directives),this.ngSubmit.emit(t),this.form._events.next(new tu(this.control)),t?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(t=void 0){this.form.reset(t),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(t){return t.pop(),t.length?this.form.get(t):this.form}static \u0275fac=function(i){return new(i||e)(ae(du,10),ae(gg,10),ae(Aa,8))};static \u0275dir=k({type:e,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&Ee("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([UO]),ke]})}return e})();function Nw(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function Mw(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var bg=class extends nu{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,t,i){super(Uw(t),Hw(i,t)),this._applyFormState(n),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),uu(t)&&(t.nonNullable||t.initialValueIsDefault)&&(Mw(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,t={}){Q(()=>{this.value=this._pendingValue=n,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)})}patchValue(n,t={}){this.setValue(n,t)}reset(n=this.defaultValue,t={}){this._applyFormState(n),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new Vo(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){Nw(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){Nw(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){Mw(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var HO=e=>e instanceof bg;var Zw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return e})();var zO=(()=>{class e extends Tr{callSetDisabledState;get submitted(){return Q(this._submittedReactive)}set submitted(t){this._submittedReactive.set(t)}_submitted=ct(()=>this._submittedReactive());_submittedReactive=z(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(t,i,r){super(),this.callSetDisabledState=r,this._setValidators(t),this._setAsyncValidators(i)}ngOnChanges(t){this.onChanges(t)}ngOnDestroy(){this.onDestroy()}onChanges(t){this._checkFormPresent(),t.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(ou(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(t){let i=this.form.get(t.path);return t._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(t),i}getControl(t){return this.form.get(t.path)}removeControl(t){iu(t.control||null,t,!1),jO(this.directives,t)}addFormGroup(t){this._setUpFormContainer(t)}removeFormGroup(t){this._cleanUpFormContainer(t)}getFormGroup(t){return this.form.get(t.path)}getFormArray(t){return this.form.get(t.path)}addFormArray(t){this._setUpFormContainer(t)}removeFormArray(t){this._cleanUpFormContainer(t)}updateModel(t,i){this.form.get(t.path).setValue(i)}onReset(){this.resetForm()}resetForm(t=void 0,i={}){this.form.reset(t,i),this._submittedReactive.set(!1)}onSubmit(t){return this.submitted=!0,qw(this.form,this.directives),this.ngSubmit.emit(t),this.form._events.next(new tu(this.control)),t?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(t=>{let i=t.control,r=this.form.get(t.path);i!==r&&(iu(i||null,t),HO(r)&&t._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(t){let i=this.form.get(t.path);Ww(i,t),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(t){let i=this.form?.get(t.path);i&&FO(i,t)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){_g(this.form,this),this._oldForm&&ou(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||e)(ae(du,10),ae(gg,10),ae(Aa,8))};static \u0275dir=k({type:e,features:[ke,Je]})}return e})();var Qw=new g(""),$O={provide:ki,useExisting:bt(()=>Sg)},Sg=(()=>{class e extends ki{_ngModelWarningConfig;callSetDisabledState;viewModel;form;set isDisabled(t){}model;update=new ne;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(t,i,r,o,s,a,c){super(c,a,r),this._ngModelWarningConfig=o,this.callSetDisabledState=s,this._setValidators(t),this._setAsyncValidators(i)}ngOnChanges(t){if(this._isControlChanged(t)){let i=t.form.previousValue;i&&(iu(i,this,!1),this.removeParseErrorsValidator(i)),this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,TO(this.form,this,this.callSetDisabledState)),this.form.updateValueAndValidity({emitEvent:!1})}PO(t,this.viewModel)&&(this.form.setValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.form&&iu(this.form,this,!1)}get path(){return[]}get control(){return this.form}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_isControlChanged(t){return t.hasOwnProperty("form")}\u0275ngControlCreate(t){super.ngControlCreate(t)}\u0275ngControlUpdate(t){super.ngControlUpdate(t,!0)}static \u0275fac=function(i){return new(i||e)(ae(du,10),ae(gg,10),ae(cu,10),ae(Qw,8),ae(Aa,8),ae(Ie,8),ae(ie,8))};static \u0275dir=k({type:e,selectors:[["","formControl",""]],inputs:{form:[0,"formControl","form"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},exportAs:["ngForm"],standalone:!1,features:[Re([$O,BO]),ke,Je,Wp(null)]})}return e})();var GO={provide:Tr,useExisting:bt(()=>Dg)},Dg=(()=>{class e extends zO{form=null;ngSubmit=new ne;get control(){return this.form}static \u0275fac=(()=>{let t;return function(r){return(t||(t=At(e)))(r||e)}})();static \u0275dir=k({type:e,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&Ee("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([GO]),ke]})}return e})();var Xw=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})();var Jw=(()=>{class e{static withConfig(t){return{ngModule:e,providers:[{provide:Aa,useValue:t.callSetDisabledState??fu}]}}static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[Xw]})}return e})(),eC=(()=>{class e{static withConfig(t){return{ngModule:e,providers:[{provide:Qw,useValue:t.warnOnNgModelWithFormControl??"always"},{provide:Aa,useValue:t.callSetDisabledState??fu}]}}static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[Xw]})}return e})();function ka(e){return e.buttons===0||e.detail===0}function Oa(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var wg;function tC(){if(wg==null){let e=typeof document<"u"?document.head:null;wg=!!(e&&(e.createShadowRoot||e.attachShadow))}return wg}function Cg(e){if(tC()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Eg(){let e=typeof document<"u"&&document?document.activeElement:null;for(;e&&e.shadowRoot;){let n=e.shadowRoot.activeElement;if(n===e)break;e=n}return e}function dt(e){if(e.composedPath)try{return e.composedPath()[0]}catch{}return e.target}var xg;try{xg=typeof Intl<"u"&&Intl.v8BreakIterator}catch{xg=!1}var we=(()=>{class e{_platformId=u(fr);isBrowser=this._platformId?HS(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||xg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var Fa;function nC(){if(Fa==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Fa=!0}))}finally{Fa=Fa||!1}return Fa}function jo(e){return nC()?e:!!e.capture}function Lt(e){return e instanceof B?e.nativeElement:e}var iC=new g("cdk-input-modality-detector-options"),rC={ignoreKeys:[18,17,224,91,16]},oC=650,Ig={passive:!0,capture:!0},sC=(()=>{class e{_platform=u(we);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new $e(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(i=>i===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=dt(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<oC||(this._modality.next(ka(t)?"keyboard":"mouse"),this._mostRecentTarget=dt(t))};_onTouchstart=t=>{if(Oa(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=dt(t)};constructor(){let t=u(A),i=u(P),r=u(iC,{optional:!0});if(this._options=v(v({},rC),r),this.modalityDetected=this._modality.pipe(cs(1)),this.modalityChanged=this.modalityDetected.pipe(Mc()),this._platform.isBrowser){let o=u(ze).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Ig),o.listen(i,"mousedown",this._onMousedown,Ig),o.listen(i,"touchstart",this._onTouchstart,Ig)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),Pa=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(Pa||{}),aC=new g("cdk-focus-monitor-default-options"),hu=jo({passive:!0,capture:!0}),La=(()=>{class e{_ngZone=u(A);_platform=u(we);_inputModalityDetector=u(sC);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(P);_stopInputModalityDetector=new b;constructor(){let t=u(aC,{optional:!0});this._detectionMode=t?.detectionMode||Pa.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let i=dt(t);for(let r=i;r;r=r.parentElement)t.type==="focus"?this._onFocus(t,r):this._onBlur(t,r)};monitor(t,i=!1){let r=Lt(t);if(!this._platform.isBrowser||r.nodeType!==1)return x();let o=Cg(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new b,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(t){let i=Lt(t),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(t,i,r){let o=Lt(t),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((t,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===Pa.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,i){t.classList.toggle("cdk-focused",!!i),t.classList.toggle("cdk-touch-focused",i==="touch"),t.classList.toggle("cdk-keyboard-focused",i==="keyboard"),t.classList.toggle("cdk-mouse-focused",i==="mouse"),t.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(t,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&i,this._detectionMode===Pa.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?oC:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(t,i){let r=this._elementInfo.get(i),o=dt(t);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(t,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&t.relatedTarget instanceof Node&&i.contains(t.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(t,i){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(i))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let i=t.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,hu),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,hu)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(We(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(t){let i=t.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,hu),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,hu),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,i,r){this._setClasses(t,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(t){let i=[];return this._elementInfo.forEach((r,o)=>{(o===t||r.checkChildren&&o.contains(t))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let o=t.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var pu=new WeakMap,vt=(()=>{class e{_appRef;_injector=u(ie);_environmentInjector=u(de);load(t){let i=this._appRef=this._appRef||this._injector.get(Ct),r=pu.get(i);r||(r={loaders:new Set,refs:[]},pu.set(i,r),i.onDestroy(()=>{pu.get(i)?.refs.forEach(o=>o.destroy()),pu.delete(i)})),r.loaders.has(t)||(r.loaders.add(t),r.refs.push(od(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var gu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return e})(),mu;function qO(){if(mu===void 0&&(mu=null,typeof window<"u")){let e=window;if(e.trustedTypes!==void 0)try{mu=e.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return mu}function Bo(e){return qO()?.createHTML(e)||e}function ei(e){return Array.isArray(e)?e:[e]}var cC=new Set,Ar,vu=(()=>{class e{_platform=u(we);_nonce=u(vi,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):KO}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&YO(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function YO(e,n){if(!cC.has(e))try{Ar||(Ar=document.createElement("style"),n&&Ar.setAttribute("nonce",n),Ar.setAttribute("type","text/css"),document.head.appendChild(Ar)),Ar.sheet&&(Ar.sheet.insertRule(`@media ${e.replace(/[{}]/g,"")} {body{ }}`,0),cC.add(e))}catch(t){console.error(t)}}function KO(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var Ng=(()=>{class e{_mediaMatcher=u(vu);_zone=u(A);_queries=new Map;_destroySubject=new b;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(t){return lC(ei(t)).some(r=>this._registerQuery(r).mql.matches)}observe(t){let r=lC(ei(t)).map(s=>this._registerQuery(s).observable),o=Wr(r);return o=pn(o.pipe(Xe(1)),o.pipe(cs(1),ss(0))),o.pipe(U(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(t){if(this._queries.has(t))return this._queries.get(t);let i=this._mediaMatcher.matchMedia(t),o={observable:new L(s=>{let a=c=>this._zone.run(()=>s.next(c));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(rt(i),U(({matches:s})=>({query:t,matches:s})),We(this._destroySubject)),mql:i};return this._queries.set(t,o),o}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function lC(e){return e.map(n=>n.split(",")).reduce((n,t)=>n.concat(t)).map(n=>n.trim())}var ZO=(()=>{class e{create(t){return typeof MutationObserver>"u"?null:new MutationObserver(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var dC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({providers:[ZO]})}return e})();var QO=200,yu=class{_letterKeyStream=new b;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new b;selectedItem=this._selectedItem;constructor(n,t){let i=typeof t?.debounceInterval=="number"?t.debounceInterval:QO;t?.skipPredicate&&(this._skipPredicateFn=t.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let t=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(t>=65&&t<=90||t>=48&&t<=57)&&this._letterKeyStream.next(String.fromCharCode(t))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Le(t=>this._pressedLetters.push(t)),ss(n),_e(()=>this._pressedLetters.length>0),U(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(t=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(t)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var Uo=class{_items;_activeItemIndex=z(-1);_activeItem=z(null);_wrap=!1;_typeaheadSubscription=se.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,t){this._items=n,n instanceof Hn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Mt(n)&&(this._effectRef=Ht(()=>this._itemsChanged(n()),{injector:t}))}tabOut=new b;change=new b;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let t=this._getItemsArray();return this._typeahead=new yu(t,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,t=10){return this._pageUpAndDown={enabled:n,delta:t},this}setActiveItem(n){let t=this._activeItem();this.updateActiveItem(n),this._activeItem()!==t&&this.change.next(this._activeItemIndex())}onKeydown(n){let t=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(t){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||Ai(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let t=this._getItemsArray(),i=typeof n=="number"?n:t.indexOf(n),r=t[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let t=this._getItemsArray();for(let i=1;i<=t.length;i++){let r=(this._activeItemIndex()+n*i+t.length)%t.length,o=t[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,t){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=t,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Mt(this._items)?this._items():this._items instanceof Hn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let t=this._activeItem();if(t){let i=n.indexOf(t);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Va=class extends Uo{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var ja=class extends Uo{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var hC=new Map,ut=class e{_appId=u(ur);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){this._appId!=="ng"&&(n+=this._appId);let i=hC.get(n);return i===void 0?i=0:i++,hC.set(n,i),`${n}${t?e._infix+"-":""}${i}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})};var Rr=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return e})();var kr;function pC(){if(kr==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return kr=!1,kr;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)kr=!0;else{let e=Element.prototype.scrollTo;e?kr=!/\{\s*\[native code\]\s*\}/.test(e.toString()):kr=!1}}return kr}function Tg(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Ho,mC=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Ag(){if(Ho)return Ho;if(typeof document!="object"||!document)return Ho=new Set(mC),Ho;let e=document.createElement("input");return Ho=new Set(mC.filter(n=>(e.setAttribute("type",n),e.type===n))),Ho}function Be(e){return e==null?"":typeof e=="string"?e:`${e}px`}function zo(e){return e!=null&&`${e}`!="false"}var vC={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var XO=new g("MATERIAL_ANIMATIONS"),yC=null;function Rg(){return u(XO,{optional:!0})?.animationsDisabled||u(Ns,{optional:!0})==="NoopAnimations"?"di-disabled":(yC??=u(vu).matchMedia("(prefers-reduced-motion)").matches,yC?"reduced-motion":"enabled")}function yt(){return Rg()!=="enabled"}var Zt=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(Zt||{}),kg=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Zt.HIDDEN;constructor(n,t,i,r=!1){this._renderer=n,this.element=t,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},_C=jo({passive:!0,capture:!0}),Og=class{_events=new Map;addHandler(n,t,i,r){let o=this._events.get(t);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(t,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,_C)})}removeHandler(n,t,i){let r=this._events.get(n);if(!r)return;let o=r.get(t);o&&(o.delete(i),o.size===0&&r.delete(t),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,_C)))}_delegateEventHandler=n=>{let t=dt(n);t&&this._events.get(n.type)?.forEach((i,r)=>{(r===t||r.contains(t))&&i.forEach(o=>o.handleEvent(n))})}},Ba={enterDuration:225,exitDuration:150},JO=800,bC=jo({passive:!0,capture:!0}),SC=["mousedown","touchstart"],DC=["mouseup","mouseleave","touchend","touchcancel"],eF=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return e})(),Ua=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Og;constructor(n,t,i,r,o){this._target=n,this._ngZone=t,this._platform=r,r.isBrowser&&(this._containerElement=Lt(i)),o&&o.get(vt).load(eF)}fadeInRipple(n,t,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=v(v({},Ba),i.animation);i.centered&&(n=r.left+r.width/2,t=r.top+r.height/2);let s=i.radius||tF(n,t,r),a=n-r.left,c=t-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),h=f.transitionProperty,p=f.transitionDuration,m=h==="none"||p==="0s"||p==="0s, 0s"||r.width===0&&r.height===0,y=new kg(this,d,i,m);d.style.transform="scale3d(1, 1, 1)",y.state=Zt.FADING_IN,i.persistent||(this._mostRecentTransientRipple=y);let w=null;return!m&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let G=()=>{w&&(w.fallbackTimer=null),clearTimeout(Ue),this._finishRippleTransition(y)},xe=()=>this._destroyRipple(y),Ue=setTimeout(xe,l+100);d.addEventListener("transitionend",G),d.addEventListener("transitioncancel",xe),w={onTransitionEnd:G,onTransitionCancel:xe,fallbackTimer:Ue}}),this._activeRipples.set(y,w),(m||!l)&&this._finishRippleTransition(y),y}fadeOutRipple(n){if(n.state===Zt.FADING_OUT||n.state===Zt.HIDDEN)return;let t=n.element,i=v(v({},Ba),n.config.animation);t.style.transitionDuration=`${i.exitDuration}ms`,t.style.opacity="0",n.state=Zt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=Lt(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,SC.forEach(i=>{e._eventManager.addHandler(this._ngZone,i,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{DC.forEach(t=>{this._triggerElement.addEventListener(t,this,bC)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Zt.FADING_IN?this._startFadeOutTransition(n):n.state===Zt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Zt.VISIBLE,!i&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Zt.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=ka(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+JO;!this._target.rippleDisabled&&!t&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Oa(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let i=0;i<t.length;i++)this.fadeInRipple(t[i].clientX,t[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===Zt.VISIBLE||n.config.terminateOnPointerUp&&n.state===Zt.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(SC.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(DC.forEach(t=>n.removeEventListener(t,this,bC)),this._pointerUpEventsRegistered=!1))}};function tF(e,n,t){let i=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),r=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(i*i+r*r)}var Ha=new g("mat-ripple-global-options"),wC=(()=>{class e{_elementRef=u(B);_animationsDisabled=yt();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(t){t&&this.fadeOutAllNonPersistent(),this._disabled=t,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(t){this._trigger=t,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let t=u(A),i=u(we),r=u(Ha,{optional:!0}),o=u(ie);this._globalOptions=r||{},this._rippleRenderer=new Ua(this,t,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:v(v(v({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(t,i=0,r){return typeof t=="number"?this._rippleRenderer.fadeInRipple(t,i,v(v({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,v(v({},this.rippleConfig),t))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return e})();var nF={capture:!0},iF=["focus","mousedown","mouseenter","touchstart"],Fg="mat-ripple-loader-uninitialized",Pg="mat-ripple-loader-class-name",CC="mat-ripple-loader-centered",_u="mat-ripple-loader-disabled",bu=(()=>{class e{_document=u(P);_animationsDisabled=yt();_globalRippleOptions=u(Ha,{optional:!0});_platform=u(we);_ngZone=u(A);_injector=u(ie);_eventCleanups;_hosts=new Map;constructor(){let t=u(ze).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>iF.map(i=>t.listen(this._document,i,this._onInteraction,nF)))}ngOnDestroy(){let t=this._hosts.keys();for(let i of t)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(t,i){t.setAttribute(Fg,this._globalRippleOptions?.namespace??""),(i.className||!t.hasAttribute(Pg))&&t.setAttribute(Pg,i.className||""),i.centered&&t.setAttribute(CC,""),i.disabled&&t.setAttribute(_u,"")}setDisabled(t,i){let r=this._hosts.get(t);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(t))):i?t.setAttribute(_u,""):t.removeAttribute(_u)}_onInteraction=t=>{let i=dt(t);if(i instanceof HTMLElement){let r=i.closest(`[${Fg}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",t.getAttribute(Pg)),t.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ba.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ba.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||t.hasAttribute(_u),rippleConfig:{centered:t.hasAttribute(CC),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new Ua(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:a,renderer:c,hasSetUpEvents:l}),t.removeAttribute(Fg)}destroyRipple(t){let i=this._hosts.get(t);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var EC=new g("");var Su=(()=>{class e{isErrorState(t,i){return!!(t&&t.invalid&&(t.touched||i&&i.submitted))}isSignalErrorState(t){if(!t)return!1;let i=t().invalid(),r=t().touched();return i&&r}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var Du=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,t,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,t?Mt(t.field)&&!t.updateValueAndValidity?(this.formField=t,this.ngControl=null):(this.formField=null,this.ngControl=t):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,t=this._getCurrentErrorState(this.matcher||this._defaultMatcher);t!==n&&(this.errorState=t,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let t=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,t)??!1}};var Lg=class{_box;_destroyed=new b;_resizeSubject=new b;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(t=>this._resizeSubject.next(t)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new L(t=>{let i=this._resizeSubject.subscribe(t);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(_e(t=>t.some(i=>i.target===n)),Rc({bufferSize:1,refCount:!0}),We(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},xC=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=u(A);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,t]of this._observers)t.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(t,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Lg(r)),this._observers.get(r).observe(t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var rF=["notch"],oF=["*"],IC=["iconPrefixContainer"],NC=["textPrefixContainer"],MC=["iconSuffixContainer"],TC=["textSuffixContainer"],sF=["textField"],aF=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],cF=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function lF(e,n){e&1&&ve(0,"span",21)}function dF(e,n){if(e&1&&(S(0,"label",20),J(1,1),me(2,lF,1,0,"span",21),I()),e&2){let t=Te(2);be("floating",t._shouldLabelFloat())("monitorResize",t._hasOutline())("id",t._labelId),oe("for",t._control.disableAutomaticLabeling?null:t._control.id),E(2),ge(!t.hideRequiredMarker&&t._control.required?2:-1)}}function uF(e,n){if(e&1&&me(0,dF,3,5,"label",20),e&2){let t=Te();ge(t._hasFloatingLabel()?0:-1)}}function fF(e,n){e&1&&ve(0,"div",7)}function hF(e,n){}function pF(e,n){if(e&1&&Si(0,hF,0,0,"ng-template",13),e&2){Te(2);let t=wi(1);be("ngTemplateOutlet",t)}}function mF(e,n){if(e&1&&(S(0,"div",9),me(1,pF,1,1,null,13),I()),e&2){let t=Te();be("matFormFieldNotchedOutlineOpen",t._shouldLabelFloat()),E(),ge(t._forceDisplayInfixLabel()?-1:1)}}function gF(e,n){e&1&&(S(0,"div",10,2),J(2,2),I())}function vF(e,n){e&1&&(S(0,"div",11,3),J(2,3),I())}function yF(e,n){}function _F(e,n){if(e&1&&Si(0,yF,0,0,"ng-template",13),e&2){Te();let t=wi(1);be("ngTemplateOutlet",t)}}function bF(e,n){e&1&&(S(0,"div",14,4),J(2,4),I())}function SF(e,n){e&1&&(S(0,"div",15,5),J(2,5),I())}function DF(e,n){e&1&&ve(0,"div",16)}function wF(e,n){e&1&&(S(0,"div",18),J(1,6),I())}function CF(e,n){if(e&1&&(S(0,"mat-hint",22),je(1),I()),e&2){let t=Te(2);be("id",t._hintLabelId),E(),Nn(t.hintLabel)}}function EF(e,n){if(e&1&&(S(0,"div",19),me(1,CF,2,2,"mat-hint",22),J(2,7),ve(3,"div",23),J(4,8),I()),e&2){let t=Te();E(),ge(t.hintLabel?1:-1)}}var za=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["mat-label"]]})}return e})(),xF=new g("MatError");var Vg=(()=>{class e{align="start";id=u(ut).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(gt("id",r.id),oe("align",null),ee("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),IF=new g("MatPrefix");var LC=new g("MatSuffix"),jg=(()=>{class e{set _isTextSelector(t){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[Re([{provide:LC,useExisting:e}])]})}return e})(),VC=new g("FloatingLabelParent"),AC=(()=>{class e{_elementRef=u(B);get floating(){return this._floating}set floating(t){this._floating=t,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(t){this._monitorResize=t,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(xC);_ngZone=u(A);_parent=u(VC);_resizeSubscription=new se;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return NF(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function NF(e){let n=e;if(n.offsetParent!==null)return n.scrollWidth;let t=n.cloneNode(!0);t.style.setProperty("position","absolute"),t.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(t);let i=t.scrollWidth;return t.remove(),i}var RC="mdc-line-ripple--active",wu="mdc-line-ripple--deactivating",kC=(()=>{class e{_elementRef=u(B);_cleanupTransitionEnd;constructor(){let t=u(A),i=u(Ie);t.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let t=this._elementRef.nativeElement.classList;t.remove(wu),t.add(RC)}deactivate(){this._elementRef.nativeElement.classList.add(wu)}_handleTransitionEnd=t=>{let i=this._elementRef.nativeElement.classList,r=i.contains(wu);t.propertyName==="opacity"&&r&&i.remove(RC,wu)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),OC=(()=>{class e{_elementRef=u(B);_ngZone=u(A);open=!1;_notch;ngAfterViewInit(){let t=this._elementRef.nativeElement,i=t.querySelector(".mdc-floating-label");i?(t.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):t.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(t){let i=this._notch.nativeElement;!this.open||!t?i.style.width="":i.style.width=`calc(${t}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(t){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${t}px)`)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&at(rF,5),i&2){let o;K(o=Z())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:oF,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Ae(),an(0,"div",1),nt(1,"div",2,0),J(3),it(),an(4,"div",3))},encapsulation:2})}return e})(),Bg=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e})}return e})();var $a=new g("MatFormField"),MF=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),FC="fill",TF="auto",PC="fixed",AF="translateY(-50%)",Cu=(()=>{class e{_elementRef=u(B);_changeDetectorRef=u(Ze);_platform=u(we);_idGenerator=u(ut);_ngZone=u(A);_defaults=u(MF,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Js("iconPrefixContainer");_textPrefixContainerSignal=Js("textPrefixContainer");_iconSuffixContainerSignal=Js("iconSuffixContainer");_textSuffixContainerSignal=Js("textSuffixContainer");_prefixSuffixContainers=ct(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(t=>t?.nativeElement).filter(t=>t!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=OS(za);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(t){this._hideRequiredMarker=zo(t)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||TF}set floatLabel(t){t!==this._floatLabel&&(this._floatLabel=t,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(t){let i=t||this._defaults?.appearance||FC;this._appearanceSignal.set(i)}_appearanceSignal=z(FC);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||PC}set subscriptSizing(t){this._subscriptSizing=t||this._defaults?.subscriptSizing||PC}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(t){this._hintLabel=t,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(t){this._explicitFormFieldControl=t}_destroyed=new b;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=yt();constructor(){let t=this._defaults,i=u(Jn);t&&(t.appearance&&(this.appearance=t.appearance),this._hideRequiredMarker=!!t?.hideRequiredMarker,t.color&&(this.color=t.color)),Ht(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=ct(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(t){let i=this._control,r="mat-mdc-form-field-type-";t&&this._elementRef.nativeElement.classList.remove(r+t.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(rt([void 0,void 0]),U(()=>[i.errorState,i.userAriaDescribedBy]),Ac(),_e(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(We(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(t=>!t._isText),this._hasTextPrefix=!!this._prefixChildren.find(t=>t._isText),this._hasIconSuffix=!!this._suffixChildren.find(t=>!t._isText),this._hasTextSuffix=!!this._suffixChildren.find(t=>t._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),mn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let t=this._control.focused;t&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!t&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",t),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",t)}_syncOutlineLabelOffset(){lm({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let t of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(t,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:t=>this._writeOutlinedLabelStyles(t())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=ct(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(t){let i=this._control?this._control.ngControl:null;return i&&i[t]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let t=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&t.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?t.push(o.id):this._hintLabel&&t.push(this._hintLabelId),s&&t.push(s.id)}else this._errorChildren&&t.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||t;r=t.concat(i.filter(s=>s&&!o.includes(s)))}else r=t;this._control.setDescribedByIds(r),this._describedByIds=t}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let t=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=t?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,p=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,m=`var(--mat-mdc-form-field-label-transform, ${AF} translateX(${p}))`,y=s+a+c+l;return[m,y]}_writeOutlinedLabelStyles(t){if(t!==null){let[i,r]=t;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let t=this._elementRef.nativeElement;if(t.getRootNode){let i=t.getRootNode();return i&&i!==t}return document.documentElement.contains(t)}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Jl(o,r._labelChild,za,5),kt(o,Bg,5)(o,IF,5)(o,LC,5)(o,xF,5)(o,Vg,5)),i&2){td();let s;K(s=Z())&&(r._formFieldControl=s.first),K(s=Z())&&(r._prefixChildren=s),K(s=Z())&&(r._suffixChildren=s),K(s=Z())&&(r._errorChildren=s),K(s=Z())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(ed(r._iconPrefixContainerSignal,IC,5)(r._textPrefixContainerSignal,NC,5)(r._iconSuffixContainerSignal,MC,5)(r._textSuffixContainerSignal,TC,5),at(sF,5)(IC,5)(NC,5)(MC,5)(TC,5)(AC,5)(OC,5)(kC,5)),i&2){td(4);let o;K(o=Z())&&(r._textField=o.first),K(o=Z())&&(r._iconPrefixContainer=o.first),K(o=Z())&&(r._textPrefixContainer=o.first),K(o=Z())&&(r._iconSuffixContainer=o.first),K(o=Z())&&(r._textSuffixContainer=o.first),K(o=Z())&&(r._floatingLabel=o.first),K(o=Z())&&(r._notchedOutline=o.first),K(o=Z())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&ee("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Re([{provide:$a,useExisting:e},{provide:VC,useExisting:e}])],ngContentSelectors:cF,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Ae(aF),Si(0,uF,1,1,"ng-template",null,0,Qs),S(2,"div",6,1),Ee("click",function(s){return r._control.onContainerClick(s)}),me(4,fF,1,0,"div",7),S(5,"div",8),me(6,mF,2,2,"div",9),me(7,gF,3,0,"div",10),me(8,vF,3,0,"div",11),S(9,"div",12),me(10,_F,1,1,null,13),J(11),I(),me(12,bF,3,0,"div",14),me(13,SF,3,0,"div",15),I(),me(14,DF,1,0,"div",16),I(),S(15,"div",17),me(16,wF,2,0,"div",18)(17,EF,5,1,"div",19),I()),i&2){let o;E(2),ee("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),E(2),ge(!r._hasOutline()&&!r._control.disabled?4:-1),E(2),ge(r._hasOutline()?6:-1),E(),ge(r._hasIconPrefix?7:-1),E(),ge(r._hasTextPrefix?8:-1),E(2),ge(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),E(2),ge(r._hasTextSuffix?12:-1),E(),ge(r._hasIconSuffix?13:-1),E(),ge(r._hasOutline()?-1:14),E(),ee("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();E(),ge((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[AC,OC,ta,kC,Vg],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return e})();var $o=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();var $C=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],GC=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function RF(e,n){e&1&&(S(0,"span",3),J(1,1),I())}function kF(e,n){e&1&&(S(0,"span",6),J(1,2),I())}function OF(e,n){e&1&&(S(0,"span",3),J(1,1),S(2,"span",7),dr(),S(3,"svg",8),ve(4,"path",9),I()()())}function FF(e,n){e&1&&(S(0,"span",6),J(1,2),I())}var PF=`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-outline-width, 1px);
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));
  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  height: var(--%NS%mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`;var LF=["*"];var WC=new g("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),jC=new g("MatChipAvatar"),BC=new g("MatChipTrailingIcon"),UC=new g("MatChipEdit"),HC=new g("MatChipRemove"),Hg=new g("MatChip"),qC=(()=>{class e{_elementRef=u(B);_parentChip=u(Hg);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(t){this._disabled=t}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){u(vt).load(Rr),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(oe("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),ee("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",re],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?-1:Mn(t)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return e})(),YC=(()=>{class e extends qC{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(t){!this.disabled&&this._isPrimary&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let t;return function(r){return(t||(t=At(e)))(r||e)}})();static \u0275dir=k({type:e,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&Ee("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(oe("tabindex",r._getTabindex()),ee("mdc-evolution-chip__action--presentational",!1))},features:[ke]})}return e})();var Ug=(()=>{class e{_changeDetectorRef=u(Ze);_elementRef=u(B);_tagName=u(kS);_ngZone=u(A);_focusMonitor=u(La);_globalRippleOptions=u(Ha,{optional:!0});_document=u(P);_onFocus=new b;_onBlur=new b;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=yt();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=u(ut).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(t){this._value=t}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(t){this._disabled=t}_disabled=!1;removed=new ne;destroyed=new ne;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=u(bu);_injector=u(ie);constructor(){let t=u(vt);t.load(Rr),t.load(gu),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=mn(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(t){(t.keyCode===8&&!t.repeat||t.keyCode===46)&&(t.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(t){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===t||r.contains(t)})}_getActions(){let t=[];return this.editIcon&&t.push(this.editIcon),this.primaryAction&&t.push(this.primaryAction),this.removeIcon&&t.push(this.removeIcon),t}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(t){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{let i=t!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&kt(o,jC,5)(o,UC,5)(o,BC,5)(o,HC,5)(o,jC,5)(o,BC,5)(o,UC,5)(o,HC,5),i&2){let s;K(s=Z())&&(r.leadingIcon=s.first),K(s=Z())&&(r.editIcon=s.first),K(s=Z())&&(r.trailingIcon=s.first),K(s=Z())&&(r.removeIcon=s.first),K(s=Z())&&(r._allLeadingIcons=s),K(s=Z())&&(r._allTrailingIcons=s),K(s=Z())&&(r._allEditIcons=s),K(s=Z())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&at(YC,5),i&2){let o;K(o=Z())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&Ee("keydown",function(s){return r._handleKeydown(s)}),i&2&&(gt("id",r.id),oe("role",r.role)("aria-label",r.ariaLabel),Et("mat-"+(r.color||"primary")),ee("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",re],highlighted:[2,"highlighted","highlighted",re],disableRipple:[2,"disableRipple","disableRipple",re],disabled:[2,"disabled","disabled",re]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[Re([{provide:Hg,useExisting:e}])],ngContentSelectors:GC,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(Ae($C),ve(0,"span",0),S(1,"span",1)(2,"span",2),me(3,RF,2,0,"span",3),S(4,"span",4),J(5),ve(6,"span",5),I()()(),me(7,kF,2,0,"span",6)),i&2&&(E(3),ge(r.leadingIcon?3:-1),E(4),ge(r._hasTrailingIcon()?7:-1))},dependencies:[qC],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-outline-width, 1px);
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));
  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  height: var(--%NS%mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return e})();var KC=(()=>{class e extends Ug{_defaultOptions=u(WC,{optional:!0});chipListSelectable=!0;_chipListMultiple=!1;_chipListHideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get selectable(){return this._selectable&&this.chipListSelectable}set selectable(t){this._selectable=t,this._changeDetectorRef.markForCheck()}_selectable=!0;get selected(){return this._selected}set selected(t){this._setSelectedState(t,!1,!0)}_selected=!1;get ariaSelected(){return this.selectable?this.selected.toString():null}basicChipAttrName="mat-basic-chip-option";selectionChange=new ne;ngOnInit(){super.ngOnInit(),this.role="presentation"}select(){this._setSelectedState(!0,!1,!0)}deselect(){this._setSelectedState(!1,!1,!0)}selectViaInteraction(){this._setSelectedState(!0,!0,!0)}toggleSelected(t=!1){return this._setSelectedState(!this.selected,t,!0),this.selected}_handlePrimaryActionInteraction(){this.disabled||(this.focus(),this.selectable&&this.toggleSelected(!0))}_hasLeadingGraphic(){return this.leadingIcon?!0:!this._chipListHideSingleSelectionIndicator||this._chipListMultiple}_setSelectedState(t,i,r){t!==this.selected&&(this._selected=t,r&&this.selectionChange.emit({source:this,isUserInput:i,selected:this.selected}),this._changeDetectorRef.markForCheck())}static \u0275fac=(()=>{let t;return function(r){return(t||(t=At(e)))(r||e)}})();static \u0275cmp=$({type:e,selectors:[["mat-basic-chip-option"],["","mat-basic-chip-option",""],["mat-chip-option"],["","mat-chip-option",""]],hostAttrs:[1,"mat-mdc-chip","mat-mdc-chip-option"],hostVars:37,hostBindings:function(i,r){i&2&&(gt("id",r.id),oe("tabindex",null)("aria-label",null)("aria-description",null)("role",r.role),ee("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--filter",!r._isBasicChip)("mdc-evolution-chip--selectable",!r._isBasicChip)("mat-mdc-chip-selected",r.selected)("mat-mdc-chip-multiple",r._chipListMultiple)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-chip-with-avatar",r.leadingIcon)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--selected",r.selected)("mdc-evolution-chip--selecting",!r._animationsDisabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-primary-graphic",r._hasLeadingGraphic())("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon()))},inputs:{selectable:[2,"selectable","selectable",re],selected:[2,"selected","selected",re]},outputs:{selectionChange:"selectionChange"},features:[Re([{provide:Ug,useExisting:e},{provide:Hg,useExisting:e}]),ke],ngContentSelectors:GC,decls:8,vars:6,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipAction","","role","option",3,"_allowFocusWhenDisabled"],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"],[1,"mdc-evolution-chip__checkmark"],["viewBox","-2 -3 30 30","focusable","false","aria-hidden","true",1,"mdc-evolution-chip__checkmark-svg"],["fill","none","stroke","currentColor","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-evolution-chip__checkmark-path"]],template:function(i,r){i&1&&(Ae($C),ve(0,"span",0),S(1,"span",1)(2,"button",2),me(3,OF,5,0,"span",3),S(4,"span",4),J(5),ve(6,"span",5),I()()(),me(7,FF,2,0,"span",6)),i&2&&(E(2),be("_allowFocusWhenDisabled",!0),oe("aria-description",r.ariaDescription)("aria-label",r.ariaLabel)("aria-selected",r.ariaSelected),E(),ge(r._hasLeadingGraphic()?3:-1),E(4),ge(r._hasTrailingIcon()?7:-1))},dependencies:[YC],styles:[PF],encapsulation:2})}return e})();var ZC=(()=>{class e{_elementRef=u(B);_changeDetectorRef=u(Ze);_dir=u(Jn,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new b;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(t=>t._onFocus)}get chipDestroyedChanges(){return this._getChipStream(t=>t.destroyed)}get chipRemovedChanges(){return this._getChipStream(t=>t.removed)}get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(t){this._explicitRole=t}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new Hn;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(t=>t._hasFocus())}_syncChipsState(){this._chips?.forEach(t=>{t._chipListDisabled=this._disabled,t._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(t){this._originatesFromChip(t)&&this._keyManager.onKeydown(t)}_isValidIndex(t){return t>=0&&t<this._chips.length}_allowFocusEscape(){let t=this._elementRef.nativeElement.tabIndex;t!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=t))}_getChipStream(t){return this._chips.changes.pipe(rt(null),Pe(()=>mn(...this._chips.map(t))))}_originatesFromChip(t){let i=t.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(rt(this._chips)).subscribe(t=>{let i=[];t.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new ja(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(t=>this._skipPredicate(t)),this.chipFocusChanges.pipe(We(this._destroyed)).subscribe(({chip:t})=>{let i=t._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(We(this._destroyed)).subscribe(t=>this._keyManager.withHorizontalOrientation(t))}_skipPredicate(t){return t.disabled}_trackChipSetChanges(){this._chips.changes.pipe(rt(null),We(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(We(this._destroyed)).subscribe(t=>{let r=this._chips.toArray().indexOf(t.chip),o=t.chip._hasFocus(),s=t.chip._hadFocusOnRemove&&this._keyManager.activeItem&&t.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let t=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[t];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&kt(o,Ug,5),i&2){let s;K(s=Z())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&Ee("keydown",function(s){return r._handleKeydown(s)}),i&2&&oe("role",r.role)},inputs:{disabled:[2,"disabled","disabled",re],role:"role",tabIndex:[2,"tabIndex","tabIndex",t=>t==null?0:Mn(t)]},ngContentSelectors:LF,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(Ae(),nt(0,"div",0),J(1),it())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})}return e})();var QC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({providers:[Su,{provide:WC,useValue:{separatorKeyCodes:[13]}}],imports:[$o,le]})}return e})();var XC=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();function JC(e){return Error(`Unable to find icon with the name "${e}"`)}function jF(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function eE(e){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${e}".`)}function tE(e){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${e}".`)}var ti=class{url;svgText;options;svgElement=null;constructor(n,t,i){this.url=n,this.svgText=t,this.options=i}},iE=(()=>{class e{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(t,i,r,o){this._httpClient=t,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(t,i,r){return this.addSvgIconInNamespace("",t,i,r)}addSvgIconLiteral(t,i,r){return this.addSvgIconLiteralInNamespace("",t,i,r)}addSvgIconInNamespace(t,i,r,o){return this._addSvgIconConfig(t,i,new ti(r,null,o))}addSvgIconResolver(t){return this._resolvers.push(t),this}addSvgIconLiteralInNamespace(t,i,r,o){let s=this._sanitizer.sanitize(fe.HTML,r);if(!s)throw tE(r);let a=Bo(s);return this._addSvgIconConfig(t,i,new ti("",a,o))}addSvgIconSet(t,i){return this.addSvgIconSetInNamespace("",t,i)}addSvgIconSetLiteral(t,i){return this.addSvgIconSetLiteralInNamespace("",t,i)}addSvgIconSetInNamespace(t,i,r){return this._addSvgIconSetConfig(t,new ti(i,null,r))}addSvgIconSetLiteralInNamespace(t,i,r){let o=this._sanitizer.sanitize(fe.HTML,i);if(!o)throw tE(i);let s=Bo(o);return this._addSvgIconSetConfig(t,new ti("",s,r))}registerFontClassAlias(t,i=t){return this._fontCssClassesByAlias.set(t,i),this}classNameForFontAlias(t){return this._fontCssClassesByAlias.get(t)||t}setDefaultFontSetClass(...t){return this._defaultFontSetClass=t,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(t){let i=this._sanitizer.sanitize(fe.RESOURCE_URL,t);if(!i)throw eE(t);let r=this._cachedIconsByUrl.get(i);return r?x(Eu(r)):this._loadSvgIconFromConfig(new ti(t,null)).pipe(Le(o=>this._cachedIconsByUrl.set(i,o)),U(o=>Eu(o)))}getNamedSvgIcon(t,i=""){let r=nE(i,t),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,t),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(t,s):is(JC(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(t){return t.svgText?x(Eu(this._svgElementFromConfig(t))):this._loadSvgIconFromConfig(t).pipe(U(i=>Eu(i)))}_getSvgFromIconSetConfigs(t,i){let r=this._extractIconWithNameFromAnySet(t,i);if(r)return x(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(ai(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(fe.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),x(null)})));return os(o).pipe(U(()=>{let s=this._extractIconWithNameFromAnySet(t,i);if(!s)throw JC(t);return s}))}_extractIconWithNameFromAnySet(t,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(t)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,t,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(t){return this._fetchIcon(t).pipe(Le(i=>t.svgText=i),U(()=>this._svgElementFromConfig(t)))}_loadSvgIconSetFromConfig(t){return t.svgText?x(null):this._fetchIcon(t).pipe(Le(i=>t.svgText=i))}_extractSvgIconFromSet(t,i,r){let o=t.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(Bo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(t){let i=this._document.createElement("DIV");i.innerHTML=t;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(t){let i=this._svgElementFromString(Bo("<svg></svg>")),r=t.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<t.childNodes.length;o++)t.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(t.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(t,i){return t.setAttribute("fit",""),t.setAttribute("height","100%"),t.setAttribute("width","100%"),t.setAttribute("preserveAspectRatio","xMidYMid meet"),t.setAttribute("focusable","false"),i&&i.viewBox&&t.setAttribute("viewBox",i.viewBox),t}_fetchIcon(t){let{url:i,options:r}=t,o=r?.withCredentials??!1;if(!this._httpClient)throw jF();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(fe.RESOURCE_URL,i);if(!s)throw eE(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(U(l=>Bo(l)),Ki(()=>this._inProgressUrlFetches.delete(s)),as());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(t,i,r){return this._svgIconConfigs.set(nE(t,i),r),this}_addSvgIconSetConfig(t,i){let r=this._iconSetConfigs.get(t);return r?r.push(i):this._iconSetConfigs.set(t,[i]),this}_svgElementFromConfig(t){if(!t.svgElement){let i=this._svgElementFromString(t.svgText);this._setSvgAttributes(i,t.options),t.svgElement=i}return t.svgElement}_getIconConfigFromResolvers(t,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,t);if(o)return BF(o)?new ti(o.url,null,o.options):new ti(o,null)}}static \u0275fac=function(i){return new(i||e)(C(bd,8),C(Om),C(P,8),C(pt))};static \u0275prov=H({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function Eu(e){return e.cloneNode(!0)}function nE(e,n){return e+":"+n}function BF(e){return!!(e.url&&e.options)}var UF=["*"],HF=new g("MAT_ICON_DEFAULT_OPTIONS"),zF=new g("mat-icon-location",{providedIn:"root",factory:()=>{let e=u(P),n=e?e.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),rE=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],$F=rE.map(e=>`[${e}]`).join(", "),GF=/^url\(['"]?#(.*?)['"]?\)$/,xu=(()=>{class e{_elementRef=u(B);_iconRegistry=u(iE);_location=u(zF);_errorHandler=u(pt);_defaultColor;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(t){t!==this._svgIcon&&(t?this._updateSvgIcon(t):this._svgIcon&&this._clearSvgElement(),this._svgIcon=t)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(t){let i=this._cleanupFontValue(t);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(t){let i=this._cleanupFontValue(t);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=se.EMPTY;constructor(){let t=u(new yo("aria-hidden"),{optional:!0}),i=u(HF,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),t||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(t){if(!t)return["",""];let i=t.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${t}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let t=this._elementsWithExternalReferences;if(t&&t.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(t){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(t),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(t)}_clearSvgElement(){let t=this._elementRef.nativeElement,i=t.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=t.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let t=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>t.classList.remove(r)),i.forEach(r=>t.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&t.classList.remove(this._previousFontIconClass),this.fontIcon&&t.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(t){return typeof t=="string"?t.trim().split(" ")[0]:t}_prependPathToReferences(t){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${t}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(t){let i=t.querySelectorAll($F),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)rE.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(GF):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(t){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),t){let[i,r]=this._splitIconName(t);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Xe(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(oe("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),Et(r.color?"mat-"+r.color:""),ee("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",re],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:UF,decls:1,vars:0,template:function(i,r){i&1&&(Ae(),J(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--%NS%mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return e})(),Iu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();var WF=["*",[["","progressIndicator",""]]],qF=["*","[progressIndicator]"];function YF(e,n){e&1&&(nt(0,"div",1),J(1,1),it())}var KF=new g("MAT_BUTTON_CONFIG");function sE(e){return e==null?void 0:Mn(e)}var aE=(()=>{class e{_elementRef=u(B);_ngZone=u(A);_animationsDisabled=yt();_config=u(KF,{optional:!0});_focusMonitor=u(La);_cleanupClick;_renderer=u(Ie);_rippleLoader=u(bu);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}showProgress=Sr(!1,{transform:re});constructor(){u(vt).load(Rr);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",i){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(oe("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Et(r.color?"mat-"+r.color:""),ee("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",re],disabled:[2,"disabled","disabled",re],ariaDisabled:[2,"aria-disabled","ariaDisabled",re],disabledInteractive:[2,"disabledInteractive","disabledInteractive",re],tabIndex:[2,"tabIndex","tabIndex",sE],_tabindex:[2,"tabindex","_tabindex",sE],showProgress:[1,"showProgress"]}})}return e})(),zg=(()=>{class e extends aE{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[ke],ngContentSelectors:qF,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ae(WF),an(0,"span",0),J(1),me(2,YF,2,0,"div",1),an(3,"span",2)(4,"span",3)),i&2&&(E(2),ge(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return e})();var Nu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[$o,le]})}return e})();var cE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})();var lE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[cE,le]})}return e})();var dE=(()=>{class e{_animationsDisabled=yt();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&ee("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return e})();var QF=["text"],XF=[[["mat-icon"]],"*"],JF=["mat-icon","*"];function eP(e,n){if(e&1&&ve(0,"mat-pseudo-checkbox",1),e&2){let t=Te();be("disabled",t.disabled)("state",t.selected?"checked":"unchecked")}}function tP(e,n){if(e&1&&ve(0,"mat-pseudo-checkbox",3),e&2){let t=Te();be("disabled",t.disabled)}}function nP(e,n){if(e&1&&(S(0,"span",4),je(1),I()),e&2){let t=Te();E(),qn("(",t.group.label,")")}}var $g=new g("MAT_OPTION_PARENT_COMPONENT"),Gg=new g("MatOptgroup");var Ga=class{source;isUserInput;constructor(n,t=!1){this.source=n,this.isUserInput=t}},Wo=(()=>{class e{_element=u(B);_changeDetectorRef=u(Ze);_parent=u($g,{optional:!0});group=u(Gg,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(ut).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(t){this._disabled.set(t)}_disabled=z(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new ne;_text;_stateChanges=new b;constructor(){let t=u(vt);t.load(Rr),t.load(gu),this._signalDisableRipple=!!this._parent&&Mt(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(t=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),t&&this._emitSelectionChangeEvent())}deselect(t=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),t&&this._emitSelectionChangeEvent())}focus(t,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!Ai(t)&&(this._selectViaInteraction(),t.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let t=this.viewValue;t!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=t)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(t=!1){this.onSelectionChange.emit(new Ga(this,t))}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&at(QF,7),i&2){let o;K(o=Z())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&Ee("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(gt("id",r.id),oe("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),ee("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",re]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:JF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Ae(XF),me(0,eP,1,2,"mat-pseudo-checkbox",1),J(1),S(2,"span",2,0),J(4,1),I(),me(5,tP,1,1,"mat-pseudo-checkbox",3),me(6,nP,2,1,"span",4),ve(7,"div",5)),i&2&&(ge(r.multiple?0:-1),E(5),ge(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),E(),ge(r.group&&r.group._inert?6:-1),E(),be("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[dE,wC],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--%NS%mat-option-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-option-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-option-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-option-label-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-option-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  font-weight: var(--%NS%mat-option-label-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--%NS%mat-option-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--%NS%mat-option-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--%NS%mat-option-selected-state-layer-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --%NS%mat-list-list-item-selected-container-color: var(--%NS%mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return e})();function uE(e,n,t){if(t.length){let i=n.toArray(),r=t.toArray(),o=0;for(let s=0;s<e+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function fE(e,n,t,i){return e<t?e:e+n>t+i?Math.max(0,e-i+n):t}var iP=20,Wg=(()=>{class e{_ngZone=u(A);_platform=u(we);_renderer=u(ze).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new b;_scrolledCount=0;scrollContainers=new Map;register(t){this.scrollContainers.has(t)||this.scrollContainers.set(t,t.elementScrolled().subscribe(()=>this._scrolled.next(t)))}deregister(t){let i=this.scrollContainers.get(t);i&&(i.unsubscribe(),this.scrollContainers.delete(t))}scrolled(t=iP){return this._platform.isBrowser?new L(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=t>0?this._scrolled.pipe(Nc(t)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):x()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((t,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(t,i){let r=this.getAncestorScrollContainers(t);return this.scrolled(i).pipe(_e(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(t){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,t)&&i.push(o)}),i}_targetContainsElement(t,i){let r=Lt(i),o=t.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var rP=20,Or=(()=>{class e{_platform=u(we);_listeners;_viewportSize=null;_change=new b;_document=u(P);constructor(){let t=u(A),i=u(ze).createRenderer(null,null);t.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(t=>t()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let t={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),t}getViewportRect(){let t=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:t.top,left:t.left,bottom:t.top+r,right:t.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let t=this._document,i=this._getWindow(),r=t.documentElement,o=r.getBoundingClientRect(),s=-o.top||t.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||t.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(t=rP){return t>0?this._change.pipe(Nc(t)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let t=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:t.innerWidth,height:t.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var Mu=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})(),qg=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le,Mu,le,Mu]})}return e})();var Wa=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Yg=class extends Wa{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,t,i,r,o,s){super(),this.component=n,this.viewContainerRef=t,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},qo=class extends Wa{templateRef;viewContainerRef;context;injector;constructor(n,t,i,r){super(),this.templateRef=n,this.viewContainerRef=t,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,t=this.context){return this.context=t,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Kg=class extends Wa{element;constructor(n){super(),this.element=n instanceof B?n.nativeElement:n}},Zg=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Yg)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof qo)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Kg)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Tu=class extends Zg{outletElement;_appRef;_defaultInjector;constructor(n,t,i){super(),this.outletElement=n,this._appRef=t,this._defaultInjector=i}attachComponentPortal(n){let t;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get($n,null,{optional:!0})||void 0;t=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>t.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||ie.NULL,o=r.get(de,i.injector);t=od(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(t.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(t.hostView),t.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(t)),this._attachedPortal=n,t}attachTemplatePortal(n){let t=n.viewContainerRef,i=t.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=t.indexOf(i);r!==-1&&t.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let t=n.element;t.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");t.parentNode.insertBefore(i,t),this.outletElement.appendChild(t),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(t,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var hE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})();var pE=pC();function SE(e){return new Au(e.get(Or),e.get(P))}var Au=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,t){this._viewportRuler=n,this._document=t}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Be(-this._previousScrollPosition.left),n.style.top=Be(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,t=this._document.body,i=n.style,r=t.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),pE&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),pE&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let t=this._document.documentElement,i=this._viewportRuler.getViewportSize();return t.scrollHeight>i.height||t.scrollWidth>i.width}};function DE(e,n){return new Ru(e.get(Wg),e.get(A),e.get(Or),n)}var Ru=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,t,i,r){this._scrollDispatcher=n,this._ngZone=t,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(_e(t=>!t||!this._overlayRef.overlayElement.contains(t.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let t=this._viewportRuler.getViewportScrollPosition().top;Math.abs(t-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var qa=class{enable(){}disable(){}attach(){}};function Qg(e,n){return n.some(t=>{let i=e.bottom<t.top,r=e.top>t.bottom,o=e.right<t.left,s=e.left>t.right;return i||r||o||s})}function mE(e,n){return n.some(t=>{let i=e.top<t.top,r=e.bottom>t.bottom,o=e.left<t.left,s=e.right>t.right;return i||r||o||s})}function Vu(e,n){return new ku(e.get(Wg),e.get(Or),e.get(A),n)}var ku=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,t,i,r){this._scrollDispatcher=n,this._viewportRuler=t,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let t=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Qg(t,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},wE=(()=>{class e{_injector=u(ie);noop=()=>new qa;close=t=>DE(this._injector,t);block=()=>SE(this._injector);reposition=t=>Vu(this._injector,t);static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),Yo=class{positionStrategy;scrollStrategy=new qa;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let t=Object.keys(n);for(let i of t)n[i]!==void 0&&(this[i]=n[i])}}};var Ou=class{connectionPair;scrollableViewProperties;constructor(n,t){this.connectionPair=n,this.scrollableViewProperties=t}};var CE=(()=>{class e{_attachedOverlays=[];_document=u(P);_isAttached=!1;ngOnDestroy(){this.detach()}add(t){this.remove(t),this._attachedOverlays.push(t)}remove(t){let i=this._attachedOverlays.indexOf(t);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(t,i,r){return r.observers.length<1?!1:t.eventPredicate?t.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),EE=(()=>{class e extends CE{_ngZone=u(A);_renderer=u(ze).createRenderer(null,null);_cleanupKeydown;add(t){super.add(t),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=t=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,t,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(t));break}}};static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),xE=(()=>{class e extends CE{_platform=u(we);_ngZone=u(A);_renderer=u(ze).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(t){if(super.add(t),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(t=>t()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=t=>{this._pointerDownEventTarget=dt(t)};_clickListener=t=>{let i=dt(t),r=t.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,t,c))){if(gE(a.overlayElement,i)||gE(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(t)):c.next(t)}}};static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();function gE(e,n){let t=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===e)return!0;i=t&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var IE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return e})(),NE=(()=>{class e{_platform=u(we);_containerElement;_document=u(P);_styleLoader=u(vt);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let t="cdk-overlay-container";if(this._platform.isBrowser||Tg()){let r=this._document.querySelectorAll(`.${t}[platform="server"], .${t}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(t),Tg()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(IE)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),Xg=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,t,i,r){this._renderer=t,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=t.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Jg(e){return e&&e.nodeType===1}var Fu=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new b;_attachments=new b;_detachments=new b;_positionStrategy;_scrollStrategy;_locationChanges=se.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new b;_outsidePointerEvents=new b;_afterNextRenderRef;constructor(n,t,i,r,o,s,a,c,l,d=!1,f,h){this._portalOutlet=n,this._host=t,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let t=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=$t(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof t?.onDestroy=="function"&&t.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),t}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=v(v({},this._config),n),this._updateElementSize()}setDirection(n){this._config=V(v({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Be(this._config.width),n.height=Be(this._config.height),n.minWidth=Be(this._config.minWidth),n.minHeight=Be(this._config.minHeight),n.maxWidth=Be(this._config.maxWidth),n.maxHeight=Be(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Jg(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Xg(this._document,this._renderer,this._ngZone,t=>{this._backdropClick.next(t)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,t,i){let r=ei(t||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=$t(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(t){if(n)throw t;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},vE="cdk-overlay-connected-position-bounding-box",oP=/([A-Za-z%]+)$/;function ju(e,n){return new Pu(n,e.get(Or),e.get(P),e.get(we),e.get(NE))}var Pu=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new b;_resizeSubscription=se.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,t,i,r,o){this._viewportRuler=t,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(vE),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,t=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,t,a),d=this._getOverlayFit(l,t,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:t,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:t})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Fr(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(vE),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof B?this._origin.nativeElement:Jg(this._origin)?this._origin:null}_getOriginPoint(n,t,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}t.left<0&&(r-=t.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,t.top<0&&(o-=t.top),{x:r,y:o}}_getOverlayPoint(n,t,i){let r;i.overlayX=="center"?r=-t.width/2:i.overlayX==="start"?r=this._isRtl()?-t.width:0:r=this._isRtl()?0:-t.width;let o;return i.overlayY=="center"?o=-t.height/2:o=i.overlayY=="top"?0:-t.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,t,i,r){let o=_E(t),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,f=s+o.width-i.width,h=0-a,p=a+o.height-i.height,m=this._subtractOverflows(o.width,d,f),y=this._subtractOverflows(o.height,h,p),w=m*y;return{visibleArea:w,isCompletelyWithinViewport:o.width*o.height===w,fitsInViewportVertically:y===o.height,fitsInViewportHorizontally:m==o.width}}_canFitWithFlexibleDimensions(n,t,i){if(this._hasFlexibleDimensions){let r=i.bottom-t.y,o=i.right-t.x,s=yE(this._overlayRef.getConfig().minHeight),a=yE(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,t,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=_E(t),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,t){if(this._setTransformOrigin(n),this._setOverlayElementStyles(t,n),this._setBoundingBoxStyles(t,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!sP(this._lastScrollVisibility,i)){let r=new Ou(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let t=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<t.length;o++)t[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,t){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(t.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(t.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let p=Math.min(i.bottom-n.y+i.top,n.y),m=this._lastBoundingBoxSize.height;o=p*2,s=n.y-p,o>m&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-m/2)}let c=t.overlayX==="start"&&!r||t.overlayX==="end"&&r,l=t.overlayX==="end"&&!r||t.overlayX==="start"&&r,d,f,h;if(l)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let p=Math.min(i.right-n.x+i.left,n.x),m=this._lastBoundingBoxSize.width;d=p*2,f=n.x-p,d>m&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-m/2)}return{top:s,left:f,bottom:a,right:h,width:d,height:o}}_setBoundingBoxStyles(n,t){let i=this._calculateBoundingBoxRect(n,t);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Be(i.width),r.height=Be(i.height),r.top=Be(i.top)||"auto",r.bottom=Be(i.bottom)||"auto",r.left=Be(i.left)||"auto",r.right=Be(i.right)||"auto",t.overlayX==="center"?r.alignItems="center":r.alignItems=t.overlayX==="end"?"flex-end":"flex-start",t.overlayY==="center"?r.justifyContent="center":r.justifyContent=t.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Be(o)),s&&(r.maxWidth=Be(s))}this._lastBoundingBoxSize=i,Fr(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Fr(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Fr(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,t){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();Fr(i,this._getExactOverlayY(t,n,d)),Fr(i,this._getExactOverlayX(t,n,d))}else i.position="static";let a="",c=this._getOffset(t,"x"),l=this._getOffset(t,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Be(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Be(s.maxWidth):o&&(i.maxWidth="")),Fr(this._pane.style,i)}_getExactOverlayY(n,t,i){let r={top:"",bottom:""},o=this._getOverlayPoint(t,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Be(o.y);return r}_getExactOverlayX(n,t,i){let r={left:"",right:""},o=this._getOverlayPoint(t,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Be(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),t=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:mE(n,i),isOriginOutsideView:Qg(n,i),isOverlayClipped:mE(t,i),isOverlayOutsideView:Qg(t,i)}}_subtractOverflows(n,...t){return t.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,t=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+t-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:t-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,t){return t==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&ei(n).forEach(t=>{t!==""&&this._appliedPanelClasses.indexOf(t)===-1&&(this._appliedPanelClasses.push(t),this._pane.classList.add(t))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof B)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let t=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+t,height:i,width:t}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",t=this._overlayContainer.getContainerElement();n&&(t.style.display="block");let i=t.getBoundingClientRect();return n&&(t.style.display=""),i}};function Fr(e,n){for(let t in n)n.hasOwnProperty(t)&&(e[t]=n[t]);return e}function yE(e){if(typeof e!="number"&&e!=null){let[n,t]=e.split(oP);return!t||t==="px"?parseFloat(n):null}return e||null}function _E(e){return{top:Math.floor(e.top),right:Math.floor(e.right),bottom:Math.floor(e.bottom),left:Math.floor(e.left),width:Math.floor(e.width),height:Math.floor(e.height)}}function sP(e,n){return e===n?!0:e.isOriginClipped===n.isOriginClipped&&e.isOriginOutsideView===n.isOriginOutsideView&&e.isOverlayClipped===n.isOverlayClipped&&e.isOverlayOutsideView===n.isOverlayOutsideView}var bE="cdk-global-overlay-wrapper";function ME(e){return new Lu}var Lu=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let t=n.getConfig();this._overlayRef=n,this._width&&!t.width&&n.updateSize({width:this._width}),this._height&&!t.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(bE),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",p="",m="",y="";c?y="flex-start":d==="center"?(y="center",h?m=f:p=f):h?d==="left"||d==="end"?(y="flex-end",p=f):(d==="right"||d==="start")&&(y="flex-start",m=f):d==="left"||d==="start"?(y="flex-start",p=f):(d==="right"||d==="end")&&(y="flex-end",m=f),n.position=this._cssPosition,n.marginLeft=c?"0":p,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":m,t.justifyContent=y,t.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,t=this._overlayRef.hostElement,i=t.style;t.classList.remove(bE),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},TE=(()=>{class e{_injector=u(ie);global(){return ME()}flexibleConnectedTo(t){return ju(this._injector,t)}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})(),AE=new g("OVERLAY_DEFAULT_CONFIG");function Bu(e,n){e.get(vt).load(IE);let t=e.get(NE),i=e.get(P),r=e.get(ut),o=e.get(Ct),s=e.get(Jn),a=e.get(Ie,null,{optional:!0})||e.get(ze).createRenderer(null,null),c=new Yo(n),l=e.get(AE,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,!i.body||!("showPopover"in i.body)?c.usePopover=!1:c.usePopover=n?.usePopover??l;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Jg(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):t.getContainerElement().appendChild(f),new Fu(new Tu(d,o,e),f,d,c,e.get(A),e.get(EE),i,e.get(Tn),e.get(xE),n?.disableAnimations??e.get(Ns,null,{optional:!0})==="NoopAnimations",e.get(de),a)}var RE=(()=>{class e{scrollStrategies=u(wE);_positionBuilder=u(TE);_injector=u(ie);create(t){return Bu(this._injector,t)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var ev=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({providers:[RE],imports:[le,hE,qg,qg]})}return e})();var kE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();var tv=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[$o,kE,Wo,le]})}return e})();var aP=["panel"],cP=["*"];function lP(e,n){if(e&1&&(nt(0,"div",1,0),J(2),it()),e&2){let t=n.id,i=Te();Et(i._classList),ee("mat-mdc-autocomplete-visible",i.showPanel)("mat-mdc-autocomplete-hidden",!i.showPanel)("mat-autocomplete-panel-animations-enabled",!i._animationsDisabled)("mat-primary",i._color==="primary")("mat-accent",i._color==="accent")("mat-warn",i._color==="warn"),gt("id",i.id),oe("aria-label",i.ariaLabel||null)("aria-labelledby",i._getPanelAriaLabelledby(t))}}var nv=class{source;option;constructor(n,t){this.source=n,this.option=t}},FE=new g("mat-autocomplete-default-options",{providedIn:"root",factory:()=>({autoActiveFirstOption:!1,autoSelectActiveOption:!1,hideSingleSelectionIndicator:!1,requireSelection:!1,hasBackdrop:!1})}),PE=(()=>{class e{_changeDetectorRef=u(Ze);_elementRef=u(B);_defaults=u(FE);_animationsDisabled=yt();_activeOptionChanges=se.EMPTY;_keyManager;showPanel=!1;get isOpen(){return this._isOpen&&this.showPanel}_isOpen=!1;_latestOpeningTrigger;_setColor(t){this._color=t,this._changeDetectorRef.markForCheck()}_color;template;panel;options;optionGroups;ariaLabel;ariaLabelledby;displayWith=null;autoActiveFirstOption;autoSelectActiveOption;requireSelection;panelWidth;disableRipple=!1;optionSelected=new ne;opened=new ne;closed=new ne;optionActivated=new ne;set classList(t){this._classList=t,this._elementRef.nativeElement.className=""}_classList;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(t){this._hideSingleSelectionIndicator=t,this._syncParentProperties()}_hideSingleSelectionIndicator;_syncParentProperties(){if(this.options)for(let t of this.options)t._changeDetectorRef.markForCheck()}id=u(ut).getId("mat-autocomplete-");inertGroups;constructor(){let t=u(we);this.inertGroups=t?.SAFARI||!1,this.autoActiveFirstOption=!!this._defaults.autoActiveFirstOption,this.autoSelectActiveOption=!!this._defaults.autoSelectActiveOption,this.requireSelection=!!this._defaults.requireSelection,this._hideSingleSelectionIndicator=this._defaults.hideSingleSelectionIndicator??!1}ngAfterContentInit(){this._keyManager=new Va(this.options).withWrap().skipPredicate(this._skipPredicate),this._activeOptionChanges=this._keyManager.change.subscribe(t=>{this.isOpen&&this.optionActivated.emit({source:this,option:this.options.toArray()[t]||null})}),this._setVisibility()}ngOnDestroy(){this._keyManager?.destroy(),this._activeOptionChanges.unsubscribe()}_setScrollTop(t){this.panel&&(this.panel.nativeElement.scrollTop=t)}_getScrollTop(){return this.panel?this.panel.nativeElement.scrollTop:0}_setVisibility(){this.showPanel=!!this.options?.length,this._changeDetectorRef.markForCheck()}_emitSelectEvent(t){let i=new nv(this,t);this.optionSelected.emit(i)}_getPanelAriaLabelledby(t){if(this.ariaLabel)return null;let i=t?t+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:t}_skipPredicate(){return!1}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-autocomplete"]],contentQueries:function(i,r,o){if(i&1&&kt(o,Wo,5)(o,Gg,5),i&2){let s;K(s=Z())&&(r.options=s),K(s=Z())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&at(on,7)(aP,5),i&2){let o;K(o=Z())&&(r.template=o.first),K(o=Z())&&(r.panel=o.first)}},hostAttrs:[1,"mat-mdc-autocomplete"],inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],displayWith:"displayWith",autoActiveFirstOption:[2,"autoActiveFirstOption","autoActiveFirstOption",re],autoSelectActiveOption:[2,"autoSelectActiveOption","autoSelectActiveOption",re],requireSelection:[2,"requireSelection","requireSelection",re],panelWidth:"panelWidth",disableRipple:[2,"disableRipple","disableRipple",re],classList:[0,"class","classList"],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",re]},outputs:{optionSelected:"optionSelected",opened:"opened",closed:"closed",optionActivated:"optionActivated"},exportAs:["matAutocomplete"],features:[Re([{provide:$g,useExisting:e}])],ngContentSelectors:cP,decls:1,vars:0,consts:[["panel",""],["role","listbox",1,"mat-mdc-autocomplete-panel","mdc-menu-surface","mdc-menu-surface--open",3,"id"]],template:function(i,r){i&1&&(Ae(),Zl(0,lP,3,17,"ng-template"))},styles:[`div.mat-mdc-autocomplete-panel {
  width: 100%;
  max-height: 256px;
  visibility: hidden;
  transform-origin: center top;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  position: relative;
  border-radius: var(--%NS%mat-autocomplete-container-shape, var(--%NS%mat-sys-corner-extra-small));
  box-shadow: var(--%NS%mat-autocomplete-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  background-color: var(--%NS%mat-autocomplete-background-color, var(--%NS%mat-sys-surface-container));
}
@media (forced-colors: active) {
  div.mat-mdc-autocomplete-panel {
    outline: solid 1px;
  }
}
.cdk-overlay-pane:not(.mat-mdc-autocomplete-panel-above) div.mat-mdc-autocomplete-panel {
  border-top-left-radius: 0;
  border-top-right-radius: 0;
}
.mat-mdc-autocomplete-panel-above div.mat-mdc-autocomplete-panel {
  border-bottom-left-radius: 0;
  border-bottom-right-radius: 0;
  transform-origin: center bottom;
}
div.mat-mdc-autocomplete-panel.mat-mdc-autocomplete-visible {
  visibility: visible;
}

div.mat-mdc-autocomplete-panel.mat-mdc-autocomplete-hidden,
.cdk-overlay-pane:has(> .mat-mdc-autocomplete-hidden) {
  visibility: hidden;
  pointer-events: none;
}

@keyframes _mat-autocomplete-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
.mat-autocomplete-panel-animations-enabled {
  animation: _mat-autocomplete-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}

mat-autocomplete {
  display: none;
}
`],encapsulation:2})}return e})();var dP={provide:cu,useExisting:bt(()=>iv),multi:!0};var uP=new g("mat-autocomplete-scroll-strategy",{providedIn:"root",factory:()=>{let e=u(ie);return()=>Vu(e)}}),iv=(()=>{class e{_environmentInjector=u(de);_element=u(B);_injector=u(ie);_viewContainerRef=u(Rt);_zone=u(A);_changeDetectorRef=u(Ze);_dir=u(Jn,{optional:!0});_formField=u($a,{optional:!0,host:!0});_viewportRuler=u(Or);_scrollStrategy=u(uP);_renderer=u(Ie);_animationsDisabled=yt();_defaults=u(FE,{optional:!0});_overlayRef=null;_portal;_componentDestroyed=!1;_initialized=new b;_keydownSubscription;_outsideClickSubscription;_cleanupWindowBlur;_previousValue=null;_valueOnAttach=null;_valueOnLastKeydown=null;_positionStrategy;_manuallyFloatingLabel=!1;_closingActionsSubscription;_viewportSubscription=se.EMPTY;_breakpointObserver=u(Ng);_handsetLandscapeSubscription=se.EMPTY;_canOpenOnNextFocus=!0;_valueBeforeAutoSelection;_pendingAutoselectedOption=null;_closeKeyEventStream=new b;_overlayPanelClass=ei(this._defaults?.overlayPanelClass||[]);_windowBlurHandler=()=>{this._canOpenOnNextFocus=this.panelOpen||!this._hasFocus()};_onChange=()=>{};_onTouched=()=>{};autocomplete;position="auto";connectedTo;autocompleteAttribute="off";autocompleteDisabled=!1;_aboveClass="mat-mdc-autocomplete-panel-above";ngAfterViewInit(){this._initialized.next(),this._initialized.complete(),this._cleanupWindowBlur=this._renderer.listen("window","blur",this._windowBlurHandler)}ngOnChanges(t){t.position&&this._positionStrategy&&(this._setStrategyPositions(this._positionStrategy),this.panelOpen&&this._overlayRef.updatePosition())}ngOnDestroy(){this._cleanupWindowBlur?.(),this._handsetLandscapeSubscription.unsubscribe(),this._viewportSubscription.unsubscribe(),this._componentDestroyed=!0,this._destroyPanel(),this._closeKeyEventStream.complete()}get panelOpen(){return this._overlayAttached&&this.autocomplete.showPanel}_overlayAttached=!1;openPanel(){this._openPanelInternal()}closePanel(){this._resetLabel(),this._overlayAttached&&(this.panelOpen&&this._zone.run(()=>{this.autocomplete.closed.emit()}),this.autocomplete._latestOpeningTrigger===this&&(this.autocomplete._isOpen=!1,this.autocomplete._latestOpeningTrigger=null),this._overlayAttached=!1,this._pendingAutoselectedOption=null,this._overlayRef&&this._overlayRef.hasAttached()&&(this._overlayRef.detach(),this._closingActionsSubscription.unsubscribe()),this._updatePanelState(),this._componentDestroyed||this._changeDetectorRef.detectChanges())}updatePosition(){this._overlayAttached&&this._overlayRef.updatePosition()}get panelClosingActions(){return mn(this.optionSelections,this.autocomplete._keyManager.tabOut.pipe(_e(()=>this._overlayAttached)),this._closeKeyEventStream,this._getOutsideClickStream(),this._overlayRef?this._overlayRef.detachments().pipe(_e(()=>this._overlayAttached)):x()).pipe(U(t=>t instanceof Ga?t:null))}optionSelections=Yi(()=>{let t=this.autocomplete?this.autocomplete.options:null;return t?t.changes.pipe(rt(t),Pe(()=>mn(...t.map(i=>i.onSelectionChange)))):this._initialized.pipe(Pe(()=>this.optionSelections))});get activeOption(){return this.autocomplete&&this.autocomplete._keyManager?this.autocomplete._keyManager.activeItem:null}_getOutsideClickStream(){return new L(t=>{let i=o=>{let s=dt(o),a=this._formField?this._formField.getConnectedOverlayOrigin().nativeElement:null,c=this.connectedTo?this.connectedTo.elementRef.nativeElement:null;this._overlayAttached&&s!==this._element.nativeElement&&!this._hasFocus()&&(!a||!a.contains(s))&&(!c||!c.contains(s))&&this._overlayRef&&!this._overlayRef.overlayElement.contains(s)&&t.next(o)},r=[this._renderer.listen("document","click",i),this._renderer.listen("document","auxclick",i),this._renderer.listen("document","touchend",i)];return()=>{r.forEach(o=>o())}})}writeValue(t){Promise.resolve(null).then(()=>this._assignOptionValue(t))}registerOnChange(t){this._onChange=t}registerOnTouched(t){this._onTouched=t}setDisabledState(t){this._element.nativeElement.disabled=t}_handleKeydown(t){let i=t,r=i.keyCode,o=Ai(i);if(r===27&&!o&&i.preventDefault(),this._valueOnLastKeydown=this._element.nativeElement.value,this.activeOption&&r===13&&this.panelOpen&&!o)this.activeOption._selectViaInteraction(),this._resetActiveItem(),i.preventDefault();else if(this.autocomplete){let s=this.autocomplete._keyManager.activeItem,a=r===38||r===40;r===9||a&&!o&&this.panelOpen?this.autocomplete._keyManager.onKeydown(i):a&&this._canOpen()&&this._openPanelInternal(this._valueOnLastKeydown),(a||this.autocomplete._keyManager.activeItem!==s)&&(this._scrollToOption(this.autocomplete._keyManager.activeItemIndex||0),this.autocomplete.autoSelectActiveOption&&this.activeOption&&(this._pendingAutoselectedOption||(this._valueBeforeAutoSelection=this._valueOnLastKeydown),this._pendingAutoselectedOption=this.activeOption,this._assignOptionValue(this.activeOption.value)))}}_handleInput(t){let i=t.target,r=i.value;if(i.type==="number"&&(r=r==""?null:parseFloat(r)),this._previousValue!==r){if(this._previousValue=r,this._pendingAutoselectedOption=null,(!this.autocomplete||!this.autocomplete.requireSelection)&&this._onChange(r),!r)this._clearPreviousSelectedOption(null,!1);else if(this.panelOpen&&!this.autocomplete.requireSelection){let o=this.autocomplete.options?.find(s=>s.selected);if(o){let s=this._getDisplayValue(o.value);r!==s&&o.deselect(!1)}}if(this._canOpen()&&this._hasFocus()){let o=this._valueOnLastKeydown??this._element.nativeElement.value;this._valueOnLastKeydown=null,this._openPanelInternal(o)}}}_handleFocus(){this._canOpenOnNextFocus?this._canOpen()&&(this._previousValue=this._element.nativeElement.value,this._attachOverlay(this._previousValue),this._floatLabel(!0)):this._canOpenOnNextFocus=!0}_handleClick(){this._canOpen()&&!this.panelOpen&&this._openPanelInternal()}_hasFocus(){return Eg()===this._element.nativeElement}_floatLabel(t=!1){this._formField&&this._formField.floatLabel==="auto"&&(t?this._formField._animateAndLockLabel():this._formField.floatLabel="always",this._manuallyFloatingLabel=!0)}_resetLabel(){this._manuallyFloatingLabel&&(this._formField&&(this._formField.floatLabel="auto"),this._manuallyFloatingLabel=!1)}_subscribeToClosingActions(){let t=new L(r=>{$t(()=>{r.next()},{injector:this._environmentInjector})}),i=this.autocomplete.options?.changes.pipe(Le(()=>this._positionStrategy.reapplyLastPosition()),ff(0))??x();return mn(t,i).pipe(Pe(()=>this._zone.run(()=>{let r=this.panelOpen;return this._resetActiveItem(),this._updatePanelState(),this._changeDetectorRef.detectChanges(),this.panelOpen&&this._overlayRef.updatePosition(),r!==this.panelOpen&&(this.panelOpen?this._emitOpened():this.autocomplete.closed.emit()),this.panelClosingActions})),Xe(1)).subscribe(r=>this._setValueAndClose(r))}_emitOpened(){this.autocomplete.opened.emit()}_destroyPanel(){this._overlayRef&&(this.closePanel(),this._overlayRef.dispose(),this._overlayRef=null)}_getDisplayValue(t){let i=this.autocomplete;return i&&i.displayWith?i.displayWith(t):t}_assignOptionValue(t){let i=this._getDisplayValue(t);t==null&&this._clearPreviousSelectedOption(null,!1),this._updateNativeInputValue(i??"")}_updateNativeInputValue(t){this._formField?this._formField._control.value=t:this._element.nativeElement.value=t,this._previousValue=t}_setValueAndClose(t){let i=this.autocomplete,r=t?t.source:this._pendingAutoselectedOption;r?(this._clearPreviousSelectedOption(r),this._assignOptionValue(r.value),this._onChange(r.value),i._emitSelectEvent(r),this._element.nativeElement.focus()):i.requireSelection&&this._element.nativeElement.value!==this._valueOnAttach&&(this._clearPreviousSelectedOption(null),this._assignOptionValue(null),this._onChange(null)),this.closePanel()}_clearPreviousSelectedOption(t,i){this.autocomplete?.options?.forEach(r=>{r!==t&&r.selected&&r.deselect(i)})}_openPanelInternal(t=this._element.nativeElement.value){this._attachOverlay(t),this._floatLabel()}_attachOverlay(t){if(!this.autocomplete)return;let i=this._overlayRef;i?(this._positionStrategy.setOrigin(this._getConnectedElement()),i.updateSize({width:this._getPanelWidth()})):(this._portal=new qo(this.autocomplete.template,this._viewContainerRef,{id:this._formField?.getLabelId()}),i=Bu(this._injector,this._getOverlayConfig()),this._overlayRef=i,this._viewportSubscription=this._viewportRuler.change().subscribe(()=>{this.panelOpen&&i&&i.updateSize({width:this._getPanelWidth()})}),this._handsetLandscapeSubscription=this._breakpointObserver.observe(vC.HandsetLandscape).subscribe(o=>{o.matches?this._positionStrategy.withFlexibleDimensions(!0).withGrowAfterOpen(!0).withViewportMargin(8):this._positionStrategy.withFlexibleDimensions(!1).withGrowAfterOpen(!1).withViewportMargin(0)})),i&&!i.hasAttached()&&(i.attach(this._portal),this._valueOnAttach=t,this._valueOnLastKeydown=null,this._closingActionsSubscription=this._subscribeToClosingActions());let r=this.panelOpen;this.autocomplete._isOpen=this._overlayAttached=!0,this.autocomplete._latestOpeningTrigger=this,this.autocomplete._setColor(this._formField?.color),this._updatePanelState(),this.panelOpen&&r!==this.panelOpen&&this._emitOpened()}_handlePanelKeydown=t=>{(t.keyCode===27&&!Ai(t)||t.keyCode===38&&Ai(t,"altKey"))&&(this._pendingAutoselectedOption&&(this._updateNativeInputValue(this._valueBeforeAutoSelection??""),this._pendingAutoselectedOption=null),this._closeKeyEventStream.next(),this._resetActiveItem(),t.stopPropagation(),t.preventDefault())};_updatePanelState(){if(this.autocomplete._setVisibility(),this.panelOpen){let t=this._overlayRef;this._keydownSubscription||(this._keydownSubscription=t.keydownEvents().subscribe(this._handlePanelKeydown)),this._outsideClickSubscription||(this._outsideClickSubscription=t.outsidePointerEvents().subscribe())}else this._keydownSubscription?.unsubscribe(),this._outsideClickSubscription?.unsubscribe(),this._keydownSubscription=this._outsideClickSubscription=void 0}_getOverlayConfig(){return new Yo({positionStrategy:this._getOverlayPosition(),scrollStrategy:this._scrollStrategy(),width:this._getPanelWidth(),direction:this._dir??void 0,hasBackdrop:this._defaults?.hasBackdrop,backdropClass:this._defaults?.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:this._overlayPanelClass,disableAnimations:this._animationsDisabled})}_getOverlayPosition(){let t=ju(this._injector,this._getConnectedElement()).withFlexibleDimensions(!1).withPush(!1).withPopoverLocation("inline");return this._setStrategyPositions(t),this._positionStrategy=t,t}_setStrategyPositions(t){let i=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],r=this._aboveClass,o=[{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:r},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:r}],s;this.position==="above"?s=o:this.position==="below"?s=i:s=[...i,...o],t.withPositions(s)}_getConnectedElement(){return this.connectedTo?this.connectedTo.elementRef:this._formField?this._formField.getConnectedOverlayOrigin():this._element}_getPanelWidth(){return this.autocomplete.panelWidth||this._getHostWidth()}_getHostWidth(){return this._getConnectedElement().nativeElement.getBoundingClientRect().width}_resetActiveItem(){let t=this.autocomplete;if(t.autoActiveFirstOption){let i=-1;for(let r=0;r<t.options.length;r++)if(!t.options.get(r).disabled){i=r;break}t._keyManager.setActiveItem(i)}else t._keyManager.setActiveItem(-1)}_canOpen(){let t=this._element.nativeElement;return!t.readOnly&&!t.disabled&&!this.autocompleteDisabled}_scrollToOption(t){let i=this.autocomplete,r=uE(t,i.options,i.optionGroups);if(t===0&&r===1)i._setScrollTop(0);else if(i.panel){let o=i.options.toArray()[t];if(o){let s=o._getHostElement(),a=fE(s.offsetTop,s.offsetHeight,i._getScrollTop(),i.panel.nativeElement.offsetHeight);i._setScrollTop(a)}}}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["input","matAutocomplete",""],["textarea","matAutocomplete",""]],hostAttrs:[1,"mat-mdc-autocomplete-trigger"],hostVars:7,hostBindings:function(i,r){i&1&&Ee("focusin",function(){return r._handleFocus()})("blur",function(){return r._onTouched()})("input",function(s){return r._handleInput(s)})("keydown",function(s){return r._handleKeydown(s)})("click",function(){return r._handleClick()}),i&2&&oe("autocomplete",r.autocompleteAttribute)("role",r.autocompleteDisabled?null:"combobox")("aria-autocomplete",r.autocompleteDisabled?null:"list")("aria-activedescendant",r.panelOpen&&r.activeOption?r.activeOption.id:null)("aria-expanded",r.autocompleteDisabled?null:r.panelOpen.toString())("aria-controls",r.autocompleteDisabled||!r.panelOpen?null:r.autocomplete?.id)("aria-haspopup",r.autocompleteDisabled?null:"listbox")},inputs:{autocomplete:[0,"matAutocomplete","autocomplete"],position:[0,"matAutocompletePosition","position"],connectedTo:[0,"matAutocompleteConnectedTo","connectedTo"],autocompleteAttribute:[0,"autocomplete","autocompleteAttribute"],autocompleteDisabled:[2,"matAutocompleteDisabled","autocompleteDisabled",re]},exportAs:["matAutocompleteTrigger"],features:[Re([dP]),Je]})}return e})(),LE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[ev,tv,Mu,tv,le]})}return e})();var hP=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return e})(),pP={passive:!0},VE=(()=>{class e{_platform=u(we);_ngZone=u(A);_renderer=u(ze).createRenderer(null,null);_styleLoader=u(vt);_monitoredElements=new Map;monitor(t){if(!this._platform.isBrowser)return Fe;this._styleLoader.load(hP);let i=Lt(t),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new b,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,pP)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(t){let i=Lt(t),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((t,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||e)};static \u0275prov=D({token:e,factory:e.\u0275fac})}return e})();var jE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({})}return e})();var BE=new g("MAT_INPUT_VALUE_ACCESSOR");var Ya=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[dC,Cu,le]})}return e})();var gP=["button","checkbox","file","hidden","image","radio","range","reset","submit"],vP=new g("MAT_INPUT_CONFIG"),UE=(()=>{class e{_elementRef=u(B);_platform=u(we);ngControl=u(ki,{optional:!0,self:!0});_autofillMonitor=u(VE);_ngZone=u(A);_formField=u($a,{optional:!0});_renderer=u(Ie);_uid=u(ut).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(vP,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new b;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=zo(t),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(t){this._id=t||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Ta.required)??!1}set required(t){this._required=zo(t)}_required;get type(){return this._type}set type(t){this._type=t||"text",this._validateType(),!this._isTextarea&&Ag().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(t){this._errorStateTracker.matcher=t}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(t){t!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(t):this._inputValueAccessor.value=t,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(t){this._readonly=zo(t)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(t){this._errorStateTracker.errorState=t}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(t=>Ag().has(t));constructor(){let t=u(Ra,{optional:!0}),i=u(Dg,{optional:!0}),r=u(Su),o=u(BE,{optional:!0,self:!0}),s=u(EC,{optional:!0,self:!0}),a=this._elementRef.nativeElement,c=a.nodeName.toLowerCase();o?Mt(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Du(r,s||this.ngControl,i,t,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=c==="select",this._isTextarea=c==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Ht(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(t=>{this.autofilled=t.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(t){this._elementRef.nativeElement.focus(t)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(t){if(t!==this.focused){if(!this._isNativeSelect&&t&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=t,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let t=this._elementRef.nativeElement.value;this._previousNativeValue!==t&&(this._previousNativeValue=t,this.stateChanges.next())}_dirtyCheckPlaceholder(){let t=this._getPlaceholder();if(t!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=t,t?i.setAttribute("placeholder",t):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){gP.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let t=this._elementRef.nativeElement.validity;return t&&t.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let t=this._elementRef.nativeElement,i=t.options[0];return this.focused||t.multiple||!this.empty||!!(t.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(t){let i=this._elementRef.nativeElement;t.length?i.setAttribute("aria-describedby",t.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let t=this._elementRef.nativeElement;return this._isNativeSelect&&(t.multiple||t.size>1)}_iOSKeyupListener=t=>{let i=t.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&Ee("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(gt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),oe("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),ee("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",re]},exportAs:["matInput"],features:[Re([{provide:Bg,useExisting:e}]),Je]})}return e})(),HE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[Ya,Ya,jE,le]})}return e})();var _P=e=>["/raca",e],bP=(e,n)=>["/raca",e,n],SP=(e,n)=>n.name;function DP(e,n){if(e&1&&(S(0,"mat-option",9),je(1),I()),e&2){let t=n.$implicit;be("value",t),E(),Nn(t.label)}}function wP(e,n){if(e&1&&(S(0,"mat-chip-option",17),Ee("click",function(i){return i.stopPropagation()}),je(1),I()),e&2){let t=n.$implicit,i=Te().$implicit;be("routerLink",Zs(3,bP,i.name,t))("selectable",!1),E(),qn(" ",t," ")}}function CP(e,n){if(e&1&&(S(0,"mat-card",11)(1,"mat-card-header")(2,"mat-card-title",12),je(3),I()(),S(4,"mat-card-content")(5,"p"),je(6),I()(),S(7,"mat-card-footer",13)(8,"mat-chip-set",14),_r(9,wP,2,6,"mat-chip-option",15,Ys),I(),S(11,"span",16),je(12,"Veja Mais"),I()()()),e&2){let t=n.$implicit;be("routerLink",Jp(5,_P,t.name)),E(3),Nn(t.name),E(3),qn("Sub-ra\xE7as: ",t.subBreeds.length),E(2),Xl("aria-label",vo(t.name+" traits")),E(),br(t.subBreeds.slice(0,2))}}var Uu=class e{breeds=z([]);dogsService=u(Fo);title=u(Do);router=u(Kt);myControl=new bg("");options=z([]);filteredOptions;constructor(){this.filteredOptions=this.myControl.valueChanges.pipe(rt(""),U(n=>{let t=typeof n=="string"?n:n?.label??"";return this._filter(t)}))}async ngOnInit(){this.title.setTitle("Dogs");let n=await this.dogsService.getAllBreeds();this.breeds.set(Object.entries(n.message).map(([i,r])=>({name:i,subBreeds:r})));let t=[];this.breeds().forEach(i=>{i.subBreeds.forEach(r=>{t.push({label:`${i.name} ${r}`,route:["/raca",i.name,r]})})}),this.options.set(t)}_filter(n){let t=n.toLowerCase();return this.options().filter(i=>i.label.toLowerCase().includes(t))}onOptionSelected(n){this.router.navigate(n.option.value.route)}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=$({type:e,selectors:[["app-home"]],decls:22,vars:5,consts:[["auto","matAutocomplete"],[1,"container"],[1,"filter"],[1,"total-dogs"],[1,"example-form"],["appearance","outline"],["type","text","placeholder","Selecione um cachorro","aria-label","cachorro","matInput","",3,"formControl","matAutocomplete"],["matSuffix",""],[3,"optionSelected"],[3,"value"],[1,"cards-container"],["appearance","outlined",1,"dog-card",3,"routerLink"],[1,"breed-name"],[1,"dog-card-footer"],[3,"aria-label"],[3,"routerLink","selectable"],[1,"see-more"],[3,"click","routerLink","selectable"]],template:function(t,i){if(t&1&&(S(0,"div",1)(1,"div",2)(2,"div")(3,"h2"),je(4,"Clique nos cards para ver fotos bonitas"),I(),S(5,"p",3),je(6),I()(),S(7,"form",4)(8,"mat-form-field",5)(9,"mat-label"),je(10,"Cachorros"),I(),ve(11,"input",6),Fp(),S(12,"mat-icon",7),je(13,"pets"),I(),S(14,"mat-autocomplete",8,0),Ee("optionSelected",function(o){return i.onOptionSelected(o)}),_r(16,DP,2,2,"mat-option",9,Kp),em(18,"async"),I()()()(),S(19,"div",10),_r(20,CP,13,7,"mat-card",11,SP),I()()),t&2){let r=wi(15);E(6),qn("Total: ",i.breeds().length," cachorros"),E(5),be("formControl",i.myControl)("matAutocomplete",r),Pp(),E(5),br(tm(18,3,i.filteredOptions)),E(4),br(i.breeds())}},dependencies:[Mr,Ti,Cw,_w,Sw,ww,Dw,bw,QC,KC,ZC,XC,Iu,xu,Nu,lE,Jw,Zw,lu,Yw,Kw,Ra,Ya,Cu,za,jg,HE,UE,LE,PE,Wo,iv,eC,Sg,mm],styles:[`.container{padding:16px}.filter{display:flex;justify-content:space-between}.total-dogs{font-size:18px;color:#1f1d1d}.example-spacer{flex:1 1 auto}.cards-container{display:flex;flex-wrap:wrap;gap:20px;justify-content:center}.cards-container .dog-card{width:240px}.cards-container .dog-card .breed-name{text-transform:capitalize}.cards-container .dog-card:hover{text-shadow:0 0 5px #fff,0 0 10px #ff007f,0 0 20px #ff007f;box-shadow:5px 5px #db7093;cursor:pointer}.cards-container .dog-card-footer{display:flex;flex-direction:column;flex-grow:1;padding:16px;gap:10px}.cards-container .dog-card-footer mat-chip-set{flex-grow:1}.cards-container .dog-card-footer .mat-mdc-chip-option:hover{box-shadow:2px 2px #db7093}.cards-container .dog-card-footer .mat-mdc-chip-option:hover .mdc-evolution-chip__text-label{text-shadow:0 0 1px #fff,0 0 5px #ff007f,0 0 58px #ff007f}.cards-container .dog-card-footer .see-more{align-self:self-end}
`],encapsulation:2})};var EP=["determinateSpinner"];function xP(e,n){if(e&1&&(dr(),S(0,"svg",11),ve(1,"circle",12),I()),e&2){let t=Te();oe("viewBox",t._viewBox()),E(),Ci("stroke-dasharray",t._strokeCircumference(),"px")("stroke-dashoffset",t._strokeCircumference()/2,"px")("stroke-width",t._circleStrokeWidth(),"%"),oe("r",t._circleRadius())}}var IP=new g("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:zE})}),zE=100,NP=10,$E=(()=>{class e{_elementRef=u(B);_noopAnimations;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;_defaultColor="primary";_determinateCircle;constructor(){let t=u(IP),i=Rg(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!t&&!t._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),t&&(t.color&&(this.color=this._defaultColor=t.color),t.diameter&&(this.diameter=t.diameter),t.strokeWidth&&(this.strokeWidth=t.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(t){this._value=Math.max(0,Math.min(100,t||0))}_value=0;get diameter(){return this._diameter}set diameter(t){this._diameter=t||0}_diameter=zE;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(t){this._strokeWidth=t||0}_strokeWidth;_circleRadius(){return(this.diameter-NP)/2}_viewBox(){let t=this._circleRadius()*2+this.strokeWidth;return`0 0 ${t} ${t}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&at(EP,5),i&2){let o;K(o=Z())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(oe("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),Et("mat-"+r.color),Ci("width",r.diameter,"px")("height",r.diameter,"px")("--%NS%mat-progress-spinner-size",r.diameter+"px")("--%NS%mat-progress-spinner-active-indicator-width",r.diameter+"px"),ee("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",Mn],diameter:[2,"diameter","diameter",Mn],strokeWidth:[2,"strokeWidth","strokeWidth",Mn]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(Si(0,xP,2,8,"ng-template",null,0,Qs),S(2,"div",2,1),dr(),S(4,"svg",3),ve(5,"circle",4),I()(),rl(),S(6,"div",5)(7,"div",6)(8,"div",7),Di(9,8),I(),S(10,"div",9),Di(11,8),I(),S(12,"div",10),Di(13,8),I()()()),i&2){let o=wi(1);E(4),oe("viewBox",r._viewBox()),E(),Ci("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),oe("r",r._circleRadius()),E(4),be("ngTemplateOutlet",o),E(2),be("ngTemplateOutlet",o),E(2),be("ngTemplateOutlet",o)}},dependencies:[ta],styles:[`.mat-mdc-progress-spinner {
  --%NS%mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--%NS%mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --%NS%mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--%NS%mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--%NS%mat-progress-spinner-active-indicator-color, var(--%NS%mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--%NS%mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--%NS%mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--%NS%mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2})}return e})();var GE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();var TP=(e,n)=>["/raca",e,n];function AP(e,n){if(e&1&&(S(0,"a",6),je(1),I()),e&2){let t=n.$implicit,i=Te(2);be("routerLink",Zs(2,TP,i.dogBreed,t)),E(),Nn(t)}}function RP(e,n){if(e&1&&(S(0,"div",4)(1,"h2"),je(2,"Sub Ra\xE7as"),I(),S(3,"div",5),_r(4,AP,2,5,"a",6,Ys),I()()),e&2){let t=Te();E(4),br(t.dogSubBreeds())}}var Ka=class e{title=u(Do);dogBreed="";dogSubBreed="";dogFullName="";dogSubBreeds=z([]);dogImage=z("");route=u(Yt);dogsService=u(Fo);ngOnInit(){this.route.paramMap.subscribe(n=>{this.dogBreed=n.get("breed")??"",this.dogSubBreed=n.get("subBreed"),this.dogBreed&&this.loadData(this.dogBreed,this.dogSubBreed)})}loadData(n,t){this.dogFullName=n,n&&t&&(this.dogFullName=`${n} ${t}`),this.title.setTitle(this.dogFullName),this.dogsService.getBreed(n,t).then(i=>{this.dogImage.set(i.message)}),this.dogsService.getSubBreeds(n).then(i=>{this.dogSubBreeds.set(i.message)})}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=$({type:e,selectors:[["app-dog-page"]],decls:8,vars:6,consts:[[1,"container"],[1,"dog-details"],[1,"dog-image"],[3,"src","alt"],[1,"dog-sub-breeds"],[1,"list"],[3,"routerLink"]],template:function(t,i){t&1&&(S(0,"div",0)(1,"h1"),je(2),I(),S(3,"div",1)(4,"div",2),ve(5,"mat-spinner")(6,"img",3),I(),me(7,RP,6,0,"div",4),I()()),t&2&&(E(2),Nn(i.dogFullName),E(4),be("src",vo(i.dogImage()),Ll)("alt",vo(i.dogFullName)),E(),ge(i.dogSubBreeds().length>0?7:-1))},dependencies:[Mr,Ti,GE,$E],styles:[`.container{padding:16px}h1{text-transform:capitalize}.dog-details{display:flex;gap:40px;justify-content:space-between}.dog-details .dog-image{width:50%;position:relative}.dog-details .dog-image img{width:100%;position:absolute;left:0;top:0;max-width:50em;max-height:50em;height:auto;min-width:30em;min-height:30em;object-fit:contain;object-position:left top}.dog-details .dog-image mat-spinner{position:absolute;left:0;top:0;width:30em!important;height:30em!important;--%NS%mat-progress-spinner-size: 10em!important;--%NS%mat-progress-spinner-active-indicator-color: palevioletred}.dog-details .dog-sub-breeds{display:flex;flex-direction:column;flex:1}.dog-details .dog-sub-breeds .list{display:flex;flex-direction:column}.dog-details .dog-sub-breeds .list a{text-decoration:none;color:#8a2be2;font-size:large;margin-bottom:10px}.dog-details .dog-sub-breeds .list a:hover{color:#ff69b4}
`],encapsulation:2})};var WE=[{path:"",component:Uu},{path:"raca/:breed",component:Ka},{path:"raca/:breed/:subBreed",component:Ka}];var qE={providers:[hh(),lg(WE),km()]};var kP=["*",[["mat-toolbar-row"]]],OP=["*","mat-toolbar-row"],FP=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275dir=k({type:e,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return e})(),YE=(()=>{class e{_elementRef=u(B);_platform=u(we);_document=u(P);color;_toolbarRows;ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||e)};static \u0275cmp=$({type:e,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&kt(o,FP,5),i&2){let s;K(s=Z())&&(r._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(Et(r.color?"mat-"+r.color:""),ee("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:OP,decls:2,vars:0,template:function(i,r){i&1&&(Ae(kP),J(0),J(1,1))},styles:[`.mat-toolbar {
  background: var(--%NS%mat-toolbar-container-background-color, var(--%NS%mat-sys-surface));
  color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--%NS%mat-toolbar-title-text-font, var(--%NS%mat-sys-title-large-font));
  font-size: var(--%NS%mat-toolbar-title-text-size, var(--%NS%mat-sys-title-large-size));
  line-height: var(--%NS%mat-toolbar-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-weight: var(--%NS%mat-toolbar-title-text-weight, var(--%NS%mat-sys-title-large-weight));
  letter-spacing: var(--%NS%mat-toolbar-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --%NS%mat-button-text-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
  --%NS%mat-button-outlined-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2})}return e})();var KE=(()=>{class e{static \u0275fac=function(i){return new(i||e)};static \u0275mod=R({type:e});static \u0275inj=M({imports:[le]})}return e})();var LP=()=>["/"],Hu=class e{title=z("dogs");static \u0275fac=function(t){return new(t||e)};static \u0275cmp=$({type:e,selectors:[["app-root"]],decls:9,vars:2,consts:[["matIconButton","","aria-label","Home button",3,"routerLink"],["aria-hidden","false","aria-label","home icon","fontIcon","home"],[1,"title"],["matIconButton","","aria-label","Translate button"],["aria-hidden","false","aria-label","language icon","fontIcon","g_translate"]],template:function(t,i){t&1&&(S(0,"mat-toolbar")(1,"button",0),ve(2,"mat-icon",1),I(),S(3,"span",2),je(4,"Ra\xE7as de Cachorros"),I(),S(5,"button",3),ve(6,"mat-icon",4),I()(),S(7,"main"),ve(8,"router-outlet"),I()),t&2&&(E(),be("routerLink",Xp(1,LP)))},dependencies:[ba,Mr,Ti,KE,YE,Iu,xu,Nu,zg],styles:["[_nghost-%COMP%]{display:flex;flex-direction:column;margin:0}.title[_ngcontent-%COMP%]{font-size:larger;font-weight:700}.example-spacer[_ngcontent-%COMP%]{flex:1 1 auto}.mat-toolbar[_ngcontent-%COMP%]{height:80px;min-height:80px;width:100%;justify-content:space-between}button[_ngcontent-%COMP%]{width:60px;height:60px;padding:auto}.mat-icon[_ngcontent-%COMP%]{transform:scale(1.5)}h1[_ngcontent-%COMP%]{color:#ff1493}"]})};xm(Hu,qE).catch(e=>console.error(e));
