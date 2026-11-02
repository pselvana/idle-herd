// Run with `node tests/test_local_control/dashboard_smoke.mjs` (no browser dependencies).
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

class Element {
  constructor(tag = '') { this.tag = tag; this.textContent = ''; this.value = ''; this.checked = false; this.children = []; this.handlers = {}; }
  append(child) { this.children.push(child); if (this.tag === 'model' && this.children.length === 1) this.value = child.value; }
  replaceChildren(...children) { this.children = children; if (this.tag === 'model') this.value = ''; }
  addEventListener(name, handler) { this.handlers[name] = handler; }
  set innerHTML(_) { throw new Error('untrusted HTML insertion'); }
}
const nodes = new Map();
const document = {
  getElementById(id) { if (!nodes.has(id)) nodes.set(id, new Element(id)); return nodes.get(id); },
  createElement(tag) { return new Element(tag); },
};
const malicious = '<img src=x onerror=alert(1)>';
const status = {
  version: 1, observed_at: 100, mode: {mode: 'auto'}, idle: {reason: null, activity: 'idle'},
  slots: {running: 0, reserved: 0, quarantined: 0}, waiters: 0, platform: malicious,
  resources: {cpu_percent: null, gpu_percent: null, memory_available_gb: null, on_ac: null,
    battery_percent: null, thermal_state: 'unknown', source: malicious, observed_at: 1},
  backend: {state: 'online'}, coordination: {state: 'disabled', leader: null, quorum: null},
  peer_details: [{local: false, worker_id: malicious, reachable: true, platform: 'unknown',
    backend: 'ollama', models: [malicious], observed_at: 1}], job_history: [], worker_leases: [],
  transfers: [], peers: {known: 1, reachable: 1}, notifications_enabled: false,
};
let pendingStatus;
let deferStatus = false;
const calls = [];
const ok = data => ({ok: true, json: async () => data});
const stream = {
  getReader() {
    let sent = false;
    return {async read() {
      if (sent) return {done: true};
      sent = true;
      return {done: false, value: new TextEncoder().encode(`data: ${JSON.stringify({version: 1, data: status})}\n\n`)};
    }};
  },
};
async function fetch(path, options) {
  calls.push([path, options]);
  if (path === '/v1/node/status' && deferStatus) return await new Promise(resolve => { pendingStatus = resolve; });
  if (path === '/v1/node/status') return ok(status);
  if (path === '/v1/node/events') return {ok: true, body: stream};
  if (path.startsWith('/v1/node/diagnostics')) return ok({checks: [{title: malicious, state: 'ok', detail: malicious}]});
  if (path.startsWith('/v1/cluster/capacity')) return ok({discovery_state: 'enabled', observed_available_slots: 1,
    observed_at: '2026-01-01T00:00:00Z', peers: [{node_id: malicious, reachable: true, fresh: true, available_slots: 1, eligible: true}]});
  throw new Error(`unexpected fetch ${path}`);
}
const timers = [];
const retries = [];
const context = vm.createContext({document, window: {}, fetch, AbortController, TextDecoder, Date, Map, Set,
  setInterval(fn) { timers.push(fn); }, setTimeout(fn) { retries.push(fn); }, clearTimeout, console});
vm.runInContext(fs.readFileSync(new URL('../../src/fleet_manager/local_control/assets/dashboard.js', import.meta.url), 'utf8'), context);
const tick = () => new Promise(resolve => setImmediate(resolve));
document.getElementById('token').value = 'secret';
await nodes.get('connect').handlers.submit({preventDefault() {}});
await tick(); await tick();
assert.match(nodes.get('message').textContent, /Connected/);
assert.equal(nodes.get('token').value, '');
assert.equal(nodes.get('peers').children[0].children[0].textContent, malicious);
assert.equal(nodes.get('model').children[0].textContent, malicious);
assert.equal(nodes.get('capacity').children[0].children[0].textContent, malicious);
assert.match(nodes.get('diagnostics').textContent, /<img/);
assert(calls.some(([path]) => path.includes(encodeURIComponent(malicious))));
assert(calls.every(([, options]) => options.headers.Authorization === 'Bearer secret'));
assert.equal(retries.length, 1, 'event EOF schedules a reconnect');
retries.shift()();
await tick(); await tick();
assert.equal(calls.filter(([path]) => path === '/v1/node/events').length, 2);

// A delayed older snapshot cannot roll back newer peer evidence.
context.newer = {...status, observed_at: 200, platform: 'newer observation'};
context.older = {...status, observed_at: 150, platform: 'outdated observation'};
vm.runInContext('apply(newer, session); apply(older, session)', context);
assert.match(nodes.get('resources').textContent, /newer observation/);

// Failure from a poll started before a newer successful event must not clear that event.
deferStatus = true;
const outdatedPoll = vm.runInContext('refresh(session)', context);
await tick();
context.newest = {...status, observed_at: 300, platform: 'newest observation'};
vm.runInContext('apply(newest, session)', context);
pendingStatus({ok: false, status: 503});
await outdatedPoll;
assert.match(nodes.get('message').textContent, /Connected/);
assert.match(nodes.get('resources').textContent, /newest observation/);

// A delayed authenticated response cannot restore data after disconnect.
deferStatus = true;
const refresh = vm.runInContext('refresh(session)', context);
await tick();
nodes.get('disconnect').handlers.click();
pendingStatus(ok(status));
await refresh;
assert.equal(nodes.get('message').textContent, 'Disconnected');
assert.equal(nodes.get('peers').children.length, 0);
assert.equal(nodes.get('capacity').children.length, 0);
console.log('dashboard smoke: untrusted text, event reconnect, capacity, stale polls and disconnect passed');
