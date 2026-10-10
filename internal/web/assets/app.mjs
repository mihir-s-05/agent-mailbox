import { createDemo } from './demo.mjs';
import { keyOf, isOutstanding, reconcile } from './model.mjs';
import { render } from './view.mjs';
import { createMailboxClient, installMailboxTools } from './webmcp.mjs';

const $ = id => document.getElementById(id);
let state, live = null, connectionAttempt = null, composeParent = null, pendingSend = null;
let backendAvailable = false;
function resetDemo(notice = 'Explore the demo, receive messages, or follow a reply.') {
  state = { ...createDemo(), mode: 'demo', identity: null, agent: 'reviewer', view: 'inbox', query: '', priority: '', state: '', selected: null, notice, error: false, actions: false, busy: false, stream: 'Demo simulation', lastRefresh: new Date().toISOString(), webmcp: 'Connect to expose tools' };
  const params = new URL(location.href).searchParams;
  if (state.agents.some(a => a.agent_id === params.get('agent'))) state.agent = params.get('agent');
  if (['inbox', 'observed', 'delivery'].includes(params.get('view'))) state.view = params.get('view');
  $('search').value = ''; $('priority-filter').value = ''; $('state-filter').value = '';
  paint();
}
function paint() {
  const focus = document.activeElement?.dataset.focus;
  render(state, handlers);
  if (focus) document.querySelector(`[data-focus="${CSS.escape(focus)}"]`)?.focus({ preventScroll: true });
}
function notice(text, error = false) { state.notice = text; state.error = error; paint(); }
function viewURL() {
  // Only synthetic view settings belong in the address bar; never live identities.
  const url = new URL(location.href); url.search = '';
  if (state.mode === 'demo') { url.searchParams.set('agent', state.agent); url.searchParams.set('view', state.view); }
  history.replaceState(null, '', url);
}
function demoEvent(m, type) { state.events.push({ message_id: m.message_id, recipient_agent_id: m.recipient_agent_id, event_type: type, event_at: new Date().toISOString() }); state.revision++; }
function toolsClose() { live?.tools?.close(); if (live) live.tools = null; }
function current(connection, agent) { return live === connection && !connection.lifetime.signal.aborted && state.agent === agent; }
async function installTools(connection) {
  toolsClose();
  const definitions = await connection.client.tools();
  if (live !== connection) return;
  // Observers get only tools backed by the stable, read-only HTTP endpoints.
  const exposed = state.actions ? definitions : definitions.filter(d => ['peek_inbox', 'list_agents'].includes(d.name));
  const agent = state.agent;
  const installed = await installMailboxTools(document.modelContext, exposed, async (name, args, signal) => {
    if (!current(connection, agent)) throw new Error('Mailbox selection changed');
    let result;
    if (!state.actions && name === 'peek_inbox') {
      const data = await connection.client.inbox(agent, signal);
      result = { messages: data.messages.filter(m => m.priority >= (args.min_priority || 0)) };
      result.count = result.messages.length;
    } else if (!state.actions && name === 'list_agents') {
      result = { agents: (await connection.client.agents(signal)).filter(a => (args.include_offline || a.online) && (!args.filter_tag || a.tags?.includes(args.filter_tag))) };
    } else {
      result = await connection.client.call(name, args, signal);
      if (current(connection, agent) && name === 'poll_inbox') rememberReceived(result.messages, agent);
    }
    if (current(connection, agent)) await refresh(connection, agent);
    return result;
  });
  if (!current(connection, agent)) { installed.close(); return; }
  connection.tools = installed;
  state.webmcp = installed.supported ? `${state.actions ? 'Mailbox actions' : 'Read-only tools'} registered` : 'Unavailable in this browser';
  paint();
}
async function refresh(connection = live, agent = state.agent) {
  if (!connection || !agent) return;
  // Serialize refreshes so an older response cannot overwrite a streamed update.
  if (connection.refreshing) { connection.refreshAgain = true; return connection.refreshing; }
  connection.refreshing = (async () => {
    do {
      connection.refreshAgain = false;
      const [inbox, events, subscriptions, agents] = await Promise.all([
        connection.client.inbox(agent, connection.selection.signal),
        connection.client.log(agent, connection.selection.signal),
        connection.client.subscriptions(agent, connection.selection.signal).then(data => ({ data }), error => ({ error })),
        connection.client.agents(connection.selection.signal)
      ]);
      if (!current(connection, agent)) return;
      state.messages = reconcile(state.messages, inbox.messages, events, agent);
      state.events = events; state.agents = agents;
      state.subscriptions = subscriptions.data || [];
      state.diagnosticsError = subscriptions.error ? 'Subscription diagnostics could not be read. Refresh to retry.' : '';
      state.lastRefresh = new Date().toISOString(); paint();
    } while (connection.refreshAgain && current(connection, agent));
  })();
  try { await connection.refreshing; } finally { connection.refreshing = null; }
}
async function observeAgent(agent) {
  const connection = live;
  if (!connection) { state.agent = agent; state.selected = null; viewURL(); paint(); return; }
  if (state.actions && agent !== state.agent) { notice('Return to observer mode before selecting another mailbox.'); return; }
  const selected = state.agents.find(a => a.agent_id === agent);
  if (!selected?.is_owned) return;
  toolsClose(); connection.selection.abort();
  if (connection.refreshing) await connection.refreshing.catch(() => {});
  if (live !== connection) return;
  connection.selection = new AbortController(); state.agent = agent; state.messages = []; state.events = []; state.subscriptions = []; state.selected = null; state.stream = 'Connecting…'; state.revision = null; state.busy = true; paint();
  try {
    await refresh(connection, agent);
    if (!current(connection, agent)) return;
    state.busy = false;
    await installTools(connection);
    startWatch(connection, agent);
    notice(`Viewing ${agent}. Delivery state is unchanged.`);
  } catch (error) {
    if (!current(connection, agent)) return;
    state.busy = false; state.stream = 'Unavailable'; state.messages = []; state.events = []; state.subscriptions = [];
    notice(error.message, true);
  }
}
function startWatch(connection, agent) {
  state.stream = 'Connecting…'; paint();
  connection.client.watch(agent, async ({ revision }) => {
    if (!current(connection, agent)) return;
    state.stream = 'Connected'; state.revision = revision;
    await refresh(connection, agent);
  }, connection.selection.signal).catch(error => {
    if (!current(connection, agent) || connection.selection.signal.aborted) return;
    state.stream = 'Disconnected'; notice(`${error.message}. Refresh still reads the inbox.`, true);
  });
}
async function connect(event) {
  event.preventDefault(); const token = $('token').value.trim(); $('token').value = ''; $('connect-error').textContent = '';
  if (!backendAvailable) { $('connect-error').textContent = 'This preview has no live backend. See the connection guide.'; return; }
  const client = createMailboxClient(token); const connection = { client, lifetime: new AbortController(), selection: new AbortController(), tools: null };
  connectionAttempt = connection;
  $('connect-submit').disabled = true;
  try {
    const identity = await client.observe(connection.lifetime.signal);
    const agents = await client.agents(connection.lifetime.signal);
    const requested = $('connect-agent').value.trim();
    const chosen = requested ? agents.find(a => a.agent_id === requested && a.is_owned) : agents.find(a => a.is_owned);
    if (!chosen) throw new Error(requested ? 'This token does not own that mailbox. Register the agent through MCP first.' : 'No owned mailbox is available. Register an agent through MCP first.');
    const messages = await client.inbox(chosen.agent_id, connection.lifetime.signal);
    // Authentication and ownership succeed before demo state is replaced.
    live?.client.close(); live?.lifetime.abort(); toolsClose(); live = connection;
    state = { mode: 'live', identity, agents, messages: [], events: [], subscriptions: [], agent: chosen.agent_id, selected: null, view: 'inbox', query: '', priority: '', state: '', actions: false, busy: false, stream: 'Connecting…', revision: null, webmcp: 'Checking browser support…', notice: '', error: false };
    state.messages = messages.messages.map(m => ({ ...m, recipient_agent_id: chosen.agent_id, outstanding: true }));
    $('search').value = ''; $('priority-filter').value = ''; $('state-filter').value = '';
    $('connect-dialog').close(); viewURL(); paint();
    await observeAgent(chosen.agent_id);
  } catch (error) {
    if (live !== connection) { client.close(); connection.lifetime.abort(); }
    $('connect-error').textContent = error.message;
  } finally { if (connectionAttempt === connection) connectionAttempt = null; $('connect-submit').disabled = false; }
}
async function activate(event) {
  event.preventDefault(); const connection = live, agent = state.agent; if (!connection) return;
  $('activate-error').textContent = ''; const button = event.submitter; button.disabled = true;
  state.busy = true; paint();
  try {
    await connection.client.connect(agent, { replaceExisting: $('replace-session').checked });
    if (!current(connection, agent)) return;
    state.actions = true; $('activate-dialog').close(); await installTools(connection); await refresh(connection, agent);
    state.busy = false; notice(`Message actions active for ${agent}. Receive records delivery; acknowledge records receipt.`);
  } catch (error) { if (current(connection, agent)) { state.busy = false; $('activate-error').textContent = error.message; paint(); } }
  finally { button.disabled = false; }
}
async function deactivate() {
  const connection = live; if (!connection || !state.actions) return;
  state.busy = true; toolsClose(); paint();
  let error;
  try { await connection.client.deactivate(AbortSignal.timeout(5000)); } catch (e) { error = e; }
  if (live !== connection) return;
  state.actions = false; state.busy = false; await installTools(connection); await refresh(connection);
  notice(error ? `Local actions cleared. Server session release failed: ${error.message}` : 'Returned to observer mode. The viewer session was released.', Boolean(error));
}
async function disconnect() {
  const connection = live; if (!connection) return;
  // Clear page/tool data immediately, then best-effort release only our session.
  toolsClose(); connection.selection.abort();
  const release = state.actions ? connection.client.deactivate(AbortSignal.timeout(2000)).catch(() => {}) : Promise.resolve();
  live = null; connection.lifetime.abort(); pendingSend = null; composeParent = null;
  for (const dialog of document.querySelectorAll('dialog[open]')) dialog.close();
  $('token').value = ''; $('connect-agent').value = ''; $('compose-form').reset();
  viewURL(); history.replaceState(null, '', location.pathname);
  resetDemo('Disconnected. Credentials and observed live messages were cleared.');
  await release; connection.client.close();
}
function openCompose(parent = null) {
  if (state.mode === 'live' && !state.actions) return;
  composeParent = parent; pendingSend = null; $('compose-form').reset(); $('compose-error').textContent = '';
  $('compose-title').textContent = parent ? 'Reply to message' : 'New message';
  $('recipient').value = parent ? parent.from_agent_id === state.agent ? parent.to?.agent_id || '' : parent.from_agent_id : '';
  $('topic').value = parent ? parent.topic : ''; $('compose-priority').value = parent ? String(parent.priority) : '1';
  $('compose-context').textContent = `${state.mode === 'demo' ? 'Synthetic demo' : 'Live mailbox'} · from ${state.agent}${parent ? ` · reply to ${parent.message_id}` : ''}`;
  $('compose-dialog').showModal(); $('recipient').focus();
}
async function send(event) {
  event.preventDefault(); $('compose-error').textContent = '';
  const args = { to: { type: 'direct', agent_id: $('recipient').value.trim() }, priority: Number($('compose-priority').value), topic: $('topic').value.trim(), body: $('body').value, require_ack: $('require-ack').checked, ttl_seconds: 3600 };
  if (composeParent) args.in_reply_to = composeParent.message_id;
  const fingerprint = JSON.stringify(args);
  if (!pendingSend || pendingSend.fingerprint !== fingerprint) pendingSend = { fingerprint, key: crypto.randomUUID() };
  args.idempotency_key = pendingSend.key;
  const connection = live, agent = state.agent; event.submitter.disabled = true;
  try {
    let result;
    if (state.mode === 'demo') {
      if (!state.agents.some(a => a.agent_id === args.to.agent_id)) throw new Error('Choose one of the five demo agents.');
      result = { message_id: `demo-${crypto.randomUUID().slice(0, 8)}`, accepted_at: new Date().toISOString() };
    } else { if (!state.actions || !connection) throw new Error('Activate message actions first.'); result = await connection.client.call('send_message', args, connection.lifetime.signal); if (!current(connection, agent)) return; }
    const message = { ...args, message_id: result.message_id, from_agent_id: agent, recipient_agent_id: args.to.agent_id, created_at: result.accepted_at, delivery_state: state.mode === 'demo' || args.to.agent_id === agent ? 'PENDING' : 'ACCEPTED', outstanding: true, attachments: [] };
    state.messages.push(message);
    if (state.mode === 'demo') demoEvent(message, 'ACCEPTED'); else await refresh(connection, agent);
    state.view = 'observed'; state.selected = keyOf(message); viewURL(); $('compose-dialog').close(); pendingSend = null;
    notice(state.mode === 'demo' ? 'Synthetic message accepted. Select its recipient to receive it.' : `Message accepted: ${result.message_id}.`);
  } catch (error) { if (state.mode === 'demo' || current(connection, agent)) $('compose-error').textContent = `${error.message} Retry preserves the idempotency key for unchanged content.`; }
  finally { event.submitter.disabled = false; }
}
async function mutate(name, args, applyDemo, success) {
  if (state.busy || state.mode === 'live' && !state.actions) return;
  const connection = live, agent = state.agent; state.busy = true; paint();
  try {
    if (state.mode === 'demo') applyDemo();
    else { const result = await connection.client.call(name, args, connection.lifetime.signal); if (!current(connection, agent)) return;
      if (name === 'poll_inbox') rememberReceived(result.messages, agent);
      if (name === 'ack_messages' && !(result.acked?.length || result.already_acked?.length)) throw new Error('Message was not acknowledged. Refresh its delivery state.');
      await refresh(connection, agent);
    }
    state.busy = false; notice(success);
  } catch (error) { if (state.mode === 'demo' || current(connection, agent)) { state.busy = false; notice(error.message, true); } }
}
function rememberReceived(messages, agent) {
  for (const m of messages) {
    const normalized = { ...m, recipient_agent_id: agent, outstanding: ['PENDING', 'DELIVERED'].includes(m.delivery_state) };
    const index = state.messages.findIndex(old => keyOf(old) === keyOf(normalized));
    if (index < 0) state.messages.push(normalized); else state.messages[index] = normalized;
  }
}
const handlers = {
  agent: observeAgent,
  select(key) { state.selected = key; paint(); if (matchMedia('(max-width: 560px)').matches) $('detail').scrollIntoView({ behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth', block: 'start' }); },
  threadSelect(key) { state.selected = key; state.view = 'observed'; viewURL(); paint(); },
  compose: openCompose,
  receive() { return mutate('poll_inbox', { max_messages: 100, wait_ms: 0 }, () => { for (const m of state.messages.filter(m => m.recipient_agent_id === state.agent && m.delivery_state === 'PENDING').slice(0, 100)) { m.delivered_at = new Date().toISOString(); m.delivery_state = m.require_ack ? 'DELIVERED' : 'ACKED'; demoEvent(m, 'DELIVERED'); if (!m.require_ack) { m.acked_at = m.delivered_at; demoEvent(m, 'ACKED'); } } }, 'Inbox received. Delivery recorded; work completion is independent.'); },
  async ack(m) { await mutate('ack_messages', { message_ids: [m.message_id], ack_kind: 'ACK' }, () => { m.delivery_state = 'ACKED'; m.acked_at = new Date().toISOString(); demoEvent(m, 'ACKED'); }, 'Acknowledgement recorded. It does not prove completed work.'); if (!state.error) { state.view = 'observed'; state.selected = keyOf(m); viewURL(); paint(); } },
  cancel(m) { return mutate('cancel_message', { message_id: m.message_id }, () => { for (const item of state.messages.filter(item => item.message_id === m.message_id && isOutstanding(item))) { item.delivery_state = 'CANCELLED'; demoEvent(item, 'CANCELLED'); } }, 'Outstanding deliveries cancelled.'); }
};
for (const b of document.querySelectorAll('[data-view]')) b.addEventListener('click', () => { state.view = b.dataset.view; viewURL(); paint(); });
for (const [id, property] of [['search', 'query'], ['priority-filter', 'priority'], ['state-filter', 'state']]) $(id).addEventListener(id === 'search' ? 'input' : 'change', () => { state[property] = $(id).value; paint(); });
$('agent-picker').addEventListener('change', () => observeAgent($('agent-picker').value));
$('reset').addEventListener('click', () => { history.replaceState(null, '', location.pathname); resetDemo('Demo reset. All messages and receipts restored.'); });
$('connect-open').addEventListener('click', () => { $('connect-error').textContent = ''; $('connect-dialog').showModal(); });
$('activate-open').addEventListener('click', () => { $('replace-session').checked = false; $('activate-error').textContent = ''; $('activation-note').textContent = `Create a session for ${state.agent} to send, receive, and acknowledge messages.`; $('activate-dialog').showModal(); });
for (const button of document.querySelectorAll('.close-dialog')) button.addEventListener('click', () => { button.closest('dialog').close(); if (button.closest('dialog').id === 'connect-dialog') $('token').value = ''; });
$('connect-dialog').addEventListener('cancel', () => { $('token').value = ''; });
$('connect-dialog').addEventListener('close', () => {
  if (connectionAttempt && live !== connectionAttempt) { connectionAttempt.lifetime.abort(); connectionAttempt.client.close(); }
});
$('connect-form').addEventListener('submit', connect); $('activate-form').addEventListener('submit', activate); $('compose-form').addEventListener('submit', send);
$('compose-open').addEventListener('click', () => openCompose()); $('disconnect').addEventListener('click', disconnect);
$('deactivate').addEventListener('click', () => deactivate().catch(error => notice(error.message, true)));
$('refresh').addEventListener('click', () => { if (live) refresh().then(() => notice('Mailbox refreshed without recording delivery.')).catch(error => notice(error.message, true)); });
addEventListener('pagehide', () => { toolsClose(); live?.selection.abort(); live?.lifetime.abort(); live?.client.close(); });
resetDemo();
fetch(new URL('./viewer-config.json', import.meta.url), { credentials: 'omit' }).then(response => response.json()).then(config => {
  backendAvailable = config.api_available === true;
  if (!backendAvailable) { $('backend-note').hidden = false; $('backend-note').textContent = 'This preview is demo-only. Attach the Go service to enable a live connection; see the guide.'; $('connect-submit').disabled = true; }
}).catch(() => { $('backend-note').hidden = false; $('backend-note').textContent = 'Live backend configuration could not be read. The demo remains available.'; $('connect-submit').disabled = true; });
