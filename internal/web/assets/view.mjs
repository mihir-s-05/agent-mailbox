import { priorities, states, keyOf, isOutstanding, filterMessages, conversation, formatTime, destination } from './model.mjs';
const $ = id => document.getElementById(id);
export function element(tag, className, text) { const node = document.createElement(tag); if (className) node.className = className; if (text !== undefined) node.textContent = text; return node; }
function badge(state) { const node = element('span', 'state-label'); node.append(element('span', `state-dot ${state.toLowerCase()}`), document.createTextNode(states[state] || state)); return node; }
function pair(label, value) { const block = element('div'); block.append(element('dt', '', label), element('dd', '', value)); return block; }
function action(text, fn, enabled = true, className = '') { const b = element('button', className, text); b.type = 'button'; b.disabled = !enabled; b.addEventListener('click', fn); return b; }
export function render(state, handlers) {
  const live = state.mode === 'live';
  const canAct = !state.busy && (!live || state.actions);
  $('mode-label').textContent = live ? `Live · ${state.identity.team_id}` : 'Demo · synthetic data';
  $('mode-label').classList.toggle('live', live);
  $('scope-label').textContent = live ? `${state.agent || 'No mailbox selected'} · ${state.actions ? 'message actions active' : 'observer mode'}` : 'Five fictional agents. Nothing leaves this page.';
  $('connect-open').hidden = live; $('disconnect').hidden = !live; $('reset').hidden = live;
  $('refresh').hidden = !live; $('refresh').disabled = state.busy;
  $('activate-open').hidden = !live || state.actions; $('activate-open').disabled = state.busy || !state.agent;
  $('deactivate').hidden = !live || !state.actions; $('deactivate').disabled = state.busy;
  $('compose-open').disabled = !canAct || !state.agent;
  $('inbox-count').textContent = state.messages.filter(m => m.recipient_agent_id === state.agent && isOutstanding(m)).length;
  $('notice').textContent = state.notice; $('notice').classList.toggle('error', state.error);
  for (const b of document.querySelectorAll('[data-view]')) b.setAttribute('aria-pressed', String(b.dataset.view === state.view));
  $('agent-total').textContent = state.agents.length;
  $('agents').replaceChildren(); $('agent-picker').replaceChildren(); $('recipient-options').replaceChildren();
  for (const a of state.agents) {
    const locked = live && !a.is_owned;
    const button = action('', () => handlers.agent(a.agent_id), !locked && !state.busy && (!state.actions || a.agent_id === state.agent), 'agent-button');
    button.setAttribute('aria-pressed', String(a.agent_id === state.agent)); button.setAttribute('aria-label', `Select ${a.display_name || a.agent_id}`);
    button.dataset.focus = `agent:${a.agent_id}`;
    const name = element('span', 'agent-name'); name.append(element('span', `presence-dot ${a.online ? 'online' : ''}`), document.createTextNode(a.display_name || a.agent_id));
    button.append(name, element('span', 'agent-meta', locked ? 'Team directory · not owned' : `${a.online ? 'Online' : 'Offline'} · ${a.status || 'no status'}`));
    $('agents').append(button);
    const option = element('option', '', `${a.display_name || a.agent_id} · ${a.online ? 'online' : 'offline'}`); option.value = a.agent_id; option.disabled = locked || (state.actions && a.agent_id !== state.agent); $('agent-picker').append(option);
    const recipient = element('option'); recipient.value = a.agent_id; $('recipient-options').append(recipient);
  }
  $('agent-picker').value = state.agent || '';
  $('presence-note').textContent = live ? 'Presence is reported by the service. Viewing leaves it unchanged.' : 'Fictional presence and work status. Reset restores the original scenario.';
  const diagnostics = state.view === 'delivery';
  $('message-pane').hidden = diagnostics; $('detail').hidden = diagnostics; $('diagnostics').hidden = !diagnostics;
  if (diagnostics) { renderDiagnostics(state); return; }
  const messages = filterMessages(state.messages, state);
  if (!messages.some(m => keyOf(m) === state.selected)) state.selected = messages[0] ? keyOf(messages[0]) : null;
  $('result-count').textContent = messages.length;
  $('list-label').textContent = state.view === 'inbox' ? 'Outstanding messages' : live ? 'Observed in this browser' : 'Synthetic message history';
  $('list-note').textContent = live ? 'Live preview: up to 20 outstanding messages. Observed keeps only messages seen or sent here; disconnect clears it.' : 'Demo receipts and messages stay in memory. “Observed” includes fictional history.';
  const list = $('messages'); list.replaceChildren();
  if (!messages.length) list.append(element('p', 'empty-state', state.query || state.priority || state.state ? 'No messages match these filters.' : state.view === 'inbox' ? 'Your inbox is empty.' : 'No messages observed yet.'));
  for (const m of messages) {
    const b = action('', () => handlers.select(keyOf(m)), true, 'message-button'); b.setAttribute('aria-pressed', String(keyOf(m) === state.selected));
    b.dataset.focus = `message:${keyOf(m)}`;
    const top = element('span', 'message-top'); top.append(element('span', 'message-sender', m.from_agent_id === state.agent ? `To ${destination(m)}` : m.from_agent_id), element('time', '', formatTime(m.created_at)));
    const bottom = element('span', 'message-bottom'); bottom.append(element('span', `priority p${m.priority}`, priorities[m.priority]), badge(m.delivery_state));
    b.append(top, element('span', 'message-topic', m.topic), element('span', 'message-preview', m.body), bottom); list.append(b);
  }
  renderDetail(state, state.messages.find(m => keyOf(m) === state.selected), handlers, canAct);
}
function renderDetail(state, m, handlers, canAct) {
  const detail = $('detail'); detail.replaceChildren();
  if (!m) { detail.append(element('p', 'empty-state', 'Select a message to read its conversation.')); return; }
  const top = element('div', 'detail-top'); top.append(element('p', 'eyebrow', 'MESSAGE & CONVERSATION'), element('span', `priority p${m.priority}`, priorities[m.priority]));
  const title = element('h2', 'detail-title', m.topic);
  const envelope = element('dl', 'envelope'); envelope.append(pair('From', m.from_agent_id), pair('To', destination(m)), pair('Sent', formatTime(m.created_at, true)), pair('Receipt', states[m.delivery_state] || m.delivery_state), pair('Message ID', m.message_id), pair('Expires', formatTime(new Date(Date.parse(m.created_at) + m.ttl_seconds * 1000).toISOString(), true)));
  const actions = element('div', 'detail-actions');
  if (m.recipient_agent_id === state.agent && m.delivery_state === 'PENDING') { const b = action('Receive inbox', handlers.receive, canAct); b.title = 'Fetch up to 100 outstanding messages for this agent and record delivery'; actions.append(b); }
  if (m.recipient_agent_id === state.agent && m.delivery_state === 'DELIVERED' && m.require_ack) actions.append(action('Acknowledge', () => handlers.ack(m), canAct, 'primary'));
  actions.append(action('Reply', () => handlers.compose(m), canAct));
  if (m.from_agent_id === state.agent && ['PENDING', 'DELIVERED', 'ACCEPTED'].includes(m.delivery_state)) actions.append(action('Cancel message', () => handlers.cancel(m), canAct));
  let note = m.require_ack ? 'Acknowledgement records the recipient’s acknowledgement. It does not prove completed work.' : 'No explicit ack required: the service acknowledges this message when the recipient fetches it.';
  if (state.mode === 'live' && !state.actions) note = 'Observer mode. Activate message actions to receive, acknowledge, or reply.';
  if (m.delivery_state === 'UNKNOWN') note = 'This message left the limited preview. Its terminal state has not been observed.';
  const thread = conversation(state.messages, m, state.agent);
  const selectedBody = element('div', 'selected-body'); selectedBody.append(element('p', 'message-body', m.body));
  if (m.attachments?.length) selectedBody.append(element('pre', 'attachment-list', JSON.stringify(m.attachments, null, 2)));
  detail.append(top, title, envelope, selectedBody, actions, element('p', 'context-note', note));
  if (thread.messages.length === 1 && !thread.missingParent) return;
  detail.append(element('h3', 'thread-title', `Conversation · ${thread.messages.length} linked messages`));
  if (thread.missingParent) detail.append(element('p', 'missing-parent', `Earlier message ${thread.missingParent} is not available in this view.`));
  for (const item of thread.messages) {
    if (item.message_id === m.message_id) continue;
    const article = element('article', `thread-message ${item.message_id === m.message_id ? 'selected' : ''}`);
    const header = element('div', 'thread-header'); header.append(element('strong', '', `${item.from_agent_id} → ${destination(item)}`), element('span', '', formatTime(item.created_at)));
    article.append(header);
    if (item.topic !== m.topic) article.append(element('p', 'thread-topic', item.topic));
    article.append(element('p', 'message-body', item.body), badge(item.delivery_state));
    if (item.attachments?.length) article.append(element('pre', 'attachment-list', JSON.stringify(item.attachments, null, 2)));
    if (keyOf(item) !== state.selected) article.append(action('View message', () => handlers.threadSelect(keyOf(item)), true, 'text-button'));
    detail.append(article);
  }
}
function renderDiagnostics(state) {
  const root = $('diagnostics'); root.replaceChildren();
  root.append(element('h2', '', 'Delivery & push'), element('p', 'context-note', state.mode === 'demo' ? 'Synthetic diagnostics illustrate the contract. No stream or webhook is running in demo mode.' : 'Browser updates trigger an inbox read. Webhook subscriptions deliver arrival events independently.'));
  const grid = element('div', 'diagnostic-grid');
  const stream = element('div', 'diagnostic-block'); stream.append(element('h3', '', 'Browser connection'));
  const info = element('dl'); info.append(pair('Inbox updates', state.mode === 'demo' ? 'Demo simulation' : state.stream), pair('Last revision', String(state.revision ?? '—')), pair('Last refresh', formatTime(state.lastRefresh, true)), pair('WebMCP', state.mode === 'demo' ? 'Connect to expose mailbox tools' : state.webmcp)); stream.append(info);
  stream.append(element('p', '', 'An inbox update carries a revision, not the message body. Reading this viewer does not mark messages delivered.'));
  const subscriptions = element('div', 'diagnostic-block'); subscriptions.append(element('h3', '', 'Webhook subscriptions'));
  if (state.diagnosticsError) subscriptions.append(element('p', '', state.diagnosticsError));
  else if (!state.subscriptions.length) subscriptions.append(element('p', '', 'No subscriptions for this mailbox.'));
  for (const sub of state.subscriptions) {
    const block = element('div', 'subscription'); block.append(element('strong', '', sub.id));
    const active = sub.active && Date.parse(sub.expires_at) > Date.now();
    block.append(element('p', '', `${active ? 'Active' : 'Inactive / expired'} · ${sub.pending} pending · ${sub.failed} failed`), element('p', '', `Expires ${formatTime(sub.expires_at, true)}`));
    const details = element('details'); details.append(element('summary', '', 'Replay cursor'), element('code', '', sub.cursor)); block.append(details); subscriptions.append(block);
  }
  subscriptions.append(element('p', '', 'Callback acceptance is independent of mailbox acknowledgement. This viewer reads diagnostics; subscription setup and replay stay in the MCP API.'));
  const audit = element('div', 'diagnostic-block'); audit.append(element('h3', '', 'Recent mailbox lifecycle'));
  const list = element('ol', 'event-log');
  const relevant = state.events.filter(e => state.mode !== 'demo' || state.messages.some(m => m.message_id === e.message_id && (m.from_agent_id === state.agent || m.recipient_agent_id === state.agent)));
  for (const event of [...relevant].sort((a, b) => b.event_at.localeCompare(a.event_at)).slice(0, 40)) { const li = element('li'); const label = element('div', '', states[event.event_type] || event.event_type); label.append(element('code', '', `${event.message_id}${event.recipient_agent_id ? ` → ${event.recipient_agent_id}` : ''}`)); li.append(element('time', '', formatTime(event.event_at)), label); list.append(li); }
  if (!relevant.length) audit.append(element('p', '', 'No recent lifecycle events.'));
  audit.append(list);
  const contract = element('div', 'diagnostic-block'); contract.append(element('h3', '', 'Delivery contract'), element('p', '', 'Arrival webhooks use at-least-once delivery. Consumers deduplicate by event ID and resume using their signed cursor.'), element('p', '', 'Expired or cancelled messages leave the outstanding inbox. Acknowledged messages remain in the server log within its retention window.'), element('p', '', 'Presence and work status are agent reports. A work result belongs in a reply.'));
  grid.append(stream, subscriptions, audit, contract); root.append(grid);
}
