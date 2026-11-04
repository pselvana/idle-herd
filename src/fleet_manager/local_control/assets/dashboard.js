'use strict';
let token = '', session = 0, lastUpdate = 0, controller, notifications = false, inspection = 0;
let displayRevision = 0;
let current = null, failures = 0, lastAlert = 0, previousJobs = new Map();
const el = id => document.getElementById(id);
const show = (id, value) => { el(id).textContent = value; };
const val = value => value === null || value === undefined ? 'unknown' : String(value);
const stamp = value => value ? new Date(value * 1000).toLocaleString() : 'unknown';
function table(id, rows) {
  const body = el(id); body.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement('tr');
    for (const value of row) { const td = document.createElement('td'); td.textContent = val(value); tr.append(td); }
    body.append(tr);
  }
}
function clear(message) {
  ++inspection; ++displayRevision; current = null; lastUpdate = 0; show('message', message);
  for (const id of ['overview', 'resources', 'coordination', 'capacity-note', 'diagnostics']) show(id, 'unknown');
  for (const id of ['peers', 'jobs', 'leases', 'transfers', 'capacity']) table(id, []);
  el('model').replaceChildren();
}
async function get(path, id) {
  const response = await fetch(path, {headers: {Authorization: 'Bearer ' + token},
    cache: 'no-store', redirect: 'error', signal: controller.signal});
  if (!response.ok) throw new Error('Request failed (' + response.status + ')');
  const data = await response.json();
  if (id !== session) throw new Error('Connection changed');
  return data;
}
function apply(data, id) {
  if (id !== session || !token || data.version !== 1) return;
  if (current && data.observed_at < current.observed_at) return;
  ++displayRevision; current = data; lastUpdate = Date.now(); show('message', 'Connected · local observation');
  show('overview', 'Mode: ' + data.mode.mode + ' · Peer background: ' + (data.idle.reason || 'eligible') +
    ' · Running: ' + data.slots.running + ' · Reserved: ' + data.slots.reserved +
    ' · Quarantined: ' + data.slots.quarantined + ' · Waiters: ' + data.waiters);
  const r = data.resources;
  show('resources', data.platform + ' · Backend: ' + data.backend.state + '\nCPU: ' + val(r.cpu_percent) +
    '% · GPU: ' + val(r.gpu_percent) + '% · Available RAM: ' + val(r.memory_available_gb) + ' GB' +
    '\nAC: ' + val(r.on_ac) + ' · Battery: ' + val(r.battery_percent) + '% · Thermal: ' + r.thermal_state +
    '\nActivity: ' + data.idle.activity + ' · Source: ' + r.source + ' · Sample: ' + stamp(r.observed_at));
  const c = data.coordination;
  show('coordination', 'Admin consensus: ' + c.state + ' · Leader: ' + val(c.leader) + ' · Quorum: ' + val(c.quorum) +
    '\nInference is independent of admin consensus. Reachability does not prove a network partition.');
  table('peers', data.peer_details.map(p => [p.hostname || (p.local ? 'This worker' : (p.worker_id || 'Unverified endpoint')),
    p.reachable ? 'reachable' : 'unreachable / stale', p.platform, p.backend,
    p.models.join(', ') || 'none observed', stamp(p.observed_at),
    p.slots ? p.slots.running + ' running · ' + p.slots.reserved + ' reserved · ' +
      p.slots.quarantined + ' quarantined / ' + p.slots.total : 'unknown',
    val(p.waiters), val(p.queued_jobs) + ' queued · ' + val(p.running_jobs) + ' running · ' +
      val(p.replicated_jobs) + ' replicated',
    val(p.activity) + ' · ' + val(p.sensor_source),
    p.eligibility_reason || p.reason || (p.eligible === true ? 'eligible' : 'unknown'),
    stamp(p.diagnostics_observed_at)]));
  table('jobs', data.job_history.map(j => [j.job_id, j.model, j.state, j.worker_id,
    j.effective_durability || 'not accepted', j.durable_copies, j.attempt_count,
    j.possible_duplicates ? 'allowed' : 'not authorized', stamp(j.updated_at)]));
  table('leases', data.worker_leases.map(j => [j.lease_id || j.job_id, j.request_id_hash,
    j.worker_incarnation, j.slot_generation, j.model, j.state,
    j.origin_host || j.origin_worker_id || (j.peer_origin ? 'unknown peer' : 'this worker'),
    j.quarantine_recovery === 'awaiting_backend' ? 'Waiting for confirmed backend completion' :
      j.state === 'quarantined' ? 'Unknown outcome; stopped-backend recovery required' : '—']));
  table('transfers', data.transfers.map(t => [t.model_id, t.state,
    t.completed_bytes + ' / ' + t.total_bytes + ' bytes', t.reason || '—']));
  const selected = el('model').value;
  const names = [...new Set(data.peer_details.flatMap(p => p.models))].sort();
  el('model').replaceChildren();
  for (const name of names) { const option = document.createElement('option'); option.value = name; option.textContent = name; el('model').append(option); }
  if (names.includes(selected)) el('model').value = selected;
  failures = data.backend.state === 'offline' ? failures + 1 : 0;
  const jobFailed = data.job_history.some(j => previousJobs.has(j.job_id) && previousJobs.get(j.job_id) !== j.state && ['failed', 'unknown_outcome'].includes(j.state));
  const diskFailed = data.transfers.some(t => ['failed', 'rejected'].includes(t.state) && /disk|quota|space/.test(t.reason || ''));
  previousJobs = new Map(data.job_history.map(j => [j.job_id, j.state]));
  if (notifications && data.notifications_enabled && (failures >= 3 || jobFailed || diskFailed || (data.peers.known > 0 && data.peers.reachable === 0)) && Date.now() - lastAlert > 300000) {
    lastAlert = Date.now();
    if ('Notification' in window && Notification.permission === 'granted') new Notification('Idle-Herd needs attention', {body: 'Check the local dashboard for backend, peer, job or transfer status.'});
  }
}
async function details(id) {
  const request = ++inspection;
  const selectedModel = el('model').value;
  try {
    const model = selectedModel;
    const diagnostic = await get('/v1/node/diagnostics' + (model ? '?model=' + encodeURIComponent(model) : ''), id);
    if (request !== inspection || selectedModel !== el('model').value) return;
    show('diagnostics', diagnostic.checks.map(c => c.title + ' [' + c.state + ']\n' + c.detail + (c.remedy ? '\n' + c.remedy : '')).join('\n\n'));
    if (!model) { table('capacity', []); show('capacity-note', 'No model observed. Install a compatible backend/model; no capacity is implied.'); return; }
    const view = await get('/v1/cluster/capacity?model=' + encodeURIComponent(model), id);
    if (request !== inspection || selectedModel !== el('model').value) return;
    show('capacity-note', 'Reachable estimate · ' + view.discovery_state + ' · ' + view.observed_available_slots +
      ' slot(s) indicated · Observed: ' + stamp(Date.parse(view.observed_at) / 1000) + '. Live worker admission is required.');
    table('capacity', view.peers.map(p => [p.node_id, p.reachable ? 'reachable' : 'unreachable',
      p.fresh ? 'fresh' : 'stale', p.available_slots, p.eligible, p.reason || '—']));
  } catch (error) { if (id === session && request === inspection) { show('diagnostics', error.message); table('capacity', []); show('capacity-note', 'Unavailable · unknown capacity'); } }
}
async function events(id) {
  try {
    const response = await fetch('/v1/node/events', {headers: {Authorization: 'Bearer ' + token},
      cache: 'no-store', redirect: 'error', signal: controller.signal});
    if (id !== session) return;
    if (!response.ok || !response.body) throw new Error('Event connection unavailable');
    const reader = response.body.getReader(), decoder = new TextDecoder(); let buffer = '';
    while (id === session && token) {
      const chunk = await reader.read(); if (chunk.done) break;
      buffer += decoder.decode(chunk.value, {stream: true});
      if (buffer.length > 2097152) throw new Error('Status event too large');
      let end;
      while ((end = buffer.indexOf('\n\n')) >= 0) {
        const block = buffer.slice(0, end); buffer = buffer.slice(end + 2);
        const line = block.split('\n').find(l => l.startsWith('data: '));
        if (line) { const event = JSON.parse(line.slice(6)); if (event.version === 1) apply(event.data, id); }
      }
    }
  } catch (_) { /* Polling and freshness checks remain independent of event reconnects. */ }
  if (id === session && token) setTimeout(() => { if (id === session && token) events(id); }, 3000);
}
async function refresh(id) {
  const started = displayRevision;
  try { apply(await get('/v1/node/status', id), id); }
  catch (error) { if (id === session && started === displayRevision) clear('Disconnected · ' + error.message); }
}
el('connect').addEventListener('submit', async event => {
  event.preventDefault(); if (controller) controller.abort();
  const id = ++session; controller = new AbortController();
  token = el('token').value.trim(); el('token').value = ''; previousJobs.clear(); failures = 0;
  clear('Connecting…'); await refresh(id);
  if (id === session && token) { if (current) details(id); events(id); }
});
el('disconnect').addEventListener('click', () => {
  ++session; token = ''; if (controller) controller.abort(); notifications = false;
  el('notify').checked = false; clear('Disconnected');
});
el('inspect').addEventListener('click', () => { if (token) details(session); });
el('notify').addEventListener('change', async event => {
  const id = session;
  notifications = false;
  if (!event.target.checked) return;
  if (!current?.notifications_enabled) {
    show('notification-note', 'First enable notifications_enabled with idle-herd preferences. Alerts contain no model names or job content.');
    event.target.checked = false; return;
  }
  if ('Notification' in window) {
    const granted = (await Notification.requestPermission()) === 'granted';
    if (id !== session || !token || !event.target.checked) return;
    notifications = granted;
  }
  event.target.checked = notifications;
  show('notification-note', notifications ? 'Generic health alerts enabled for this connected tab. Job completions stay silent.' : 'Notifications unavailable or denied.');
});
setInterval(() => { if (token) refresh(session); }, 3000);
setInterval(() => { if (token) details(session); }, 15000);
setInterval(() => { if (lastUpdate && Date.now() - lastUpdate > 5000) show('message', 'Disconnected · status stale; reconnecting'); }, 1000);
