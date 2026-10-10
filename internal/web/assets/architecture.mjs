// An illustration of the existing protocol. This module never calls the service.
const DURATION = 3400;
const STAGES = [
  { operation: 'send_message', caption: 'Writer sends a direct message to reviewer. The service checks the sender’s session, permissions, and team.', receipt: 'Not sent', nodes: ['sender', 'service'], wire: 'send', notes: { sender: 'Sending a direct message', service: 'Checks identity & team', recipient: 'Waiting for a message', database: 'Messages · deliveries · events' } },
  { operation: 'COMMIT', caption: 'PostgreSQL commits the message, recipient delivery, and arrival event together. The message stays pending until the recipient fetches it.', receipt: 'Pending', nodes: ['service', 'database'], wire: 'store', notes: { sender: 'Message accepted', service: 'Commits before notifying', recipient: 'Message waiting in inbox', database: 'Message + delivery + event' } },
  { operation: 'subscriptions/listen', caption: 'A subscribed MCP client receives a resource update and can reread the inbox. The update is a change hint; it does not contain the message body.', receipt: 'Pending', nodes: ['service', 'recipient'], wire: 'notify', notes: { sender: 'Message accepted', service: 'Publishes an inbox change', recipient: 'Client receives a change hint', database: 'Body remains in the inbox' } },
  { operation: 'poll_inbox', caption: 'Reviewer fetches the message body. This records delivery. A peek can inspect the same inbox without changing delivery state.', receipt: 'Delivered', nodes: ['service', 'recipient'], wire: 'fetch', notes: { sender: 'Message accepted', service: 'Returns the message body', recipient: 'Reads “Review this patch”', database: 'Delivery recorded' } },
  { operation: 'ack_messages', caption: 'This example requests an explicit acknowledgement, so reviewer acknowledges after fetching. Receipt is recorded; work completion can be reported in a reply.', receipt: 'Acknowledged', nodes: ['recipient', 'service', 'database'], wire: 'fetch', notes: { sender: 'Message accepted', service: 'Records acknowledgement', recipient: 'Acknowledges receipt', database: 'Receipt recorded · ACKED' } }
];

function stageFor(index, mode) {
  const stage = STAGES[index];
  if (index !== 2) return stage;
  if (mode === 'webhook') return { ...stage, operation: 'mailbox.message.created', caption: 'A subscribed host receives a signed arrival webhook and can fetch the inbox. Retries may repeat an event; the host deduplicates by event ID. The body stays in the inbox.', notes: { ...stage.notes, service: 'Sends a signed arrival event', recipient: 'Via the subscribed host' } };
  if (mode === 'polling') return { ...stage, operation: 'poll_inbox', caption: 'No push subscription is needed. Reviewer checks its inbox on its own schedule, then fetches any available messages.', wire: 'fetch', notes: { ...stage.notes, service: 'Inbox ready to read', recipient: 'Checks on its own schedule' } };
  return stage;
}

export function mountMessageFlow(root) {
  if (!root) return;
  const $ = id => root.querySelector(`#${id}`);
  const graph = $('flow-graph'), svg = $('flow-wires');
  const nodes = Object.fromEntries([...root.querySelectorAll('[data-flow-node]')].map(node => [node.dataset.flowNode, node]));
  const wires = Object.fromEntries(['send', 'store', 'notify', 'fetch'].map(name => [name, $(`flow-${name}`)]));
  const steps = [...root.querySelectorAll('[data-flow-step]')];
  const motion = matchMedia('(prefers-reduced-motion: reduce)');
  let index = 0, elapsed = 0, lastFrame = null, frame = null;
  let playing = !motion.matches, visible = false, mode = $('flow-mode').value;

  function render() {
    const stage = stageFor(index, mode);
    root.dataset.stage = String(index);
    root.dataset.mode = mode;
    root.dataset.playing = String(playing);
    $('flow-operation').textContent = stage.operation;
    $('flow-caption').textContent = stage.caption;
    $('flow-receipt').textContent = stage.receipt;
    $('flow-play').textContent = playing ? 'Pause' : 'Play';
    $('flow-play').setAttribute('aria-label', playing ? 'Pause message animation' : 'Play message animation');
    $('flow-notify-label').textContent = mode === 'polling' ? 'Check' : 'Notify';
    for (const [name, node] of Object.entries(nodes)) {
      node.classList.toggle('is-active', stage.nodes.includes(name));
      $(`flow-${name}-note`).textContent = stage.notes[name];
    }
    for (const [name, wire] of Object.entries(wires)) {
      wire.classList.toggle('is-active', name === stage.wire);
      wire.toggleAttribute('hidden', name === 'notify' && mode === 'polling');
    }
    for (const button of steps) button.setAttribute('aria-pressed', String(Number(button.dataset.flowStep) === index));
    hidePacket();
  }

  function hidePacket() { $('flow-packet').setAttribute('hidden', ''); $('flow-signal').setAttribute('hidden', ''); }

  function drawWires() {
    const bounds = graph.getBoundingClientRect();
    if (!bounds.width || !bounds.height) return;
    svg.setAttribute('viewBox', `0 0 ${bounds.width} ${bounds.height}`);
    const box = name => {
      const rect = nodes[name].getBoundingClientRect();
      return { x: rect.left - bounds.left, y: rect.top - bounds.top, w: rect.width, h: rect.height };
    };
    const sender = box('sender'), service = box('service'), recipient = box('recipient'), database = box('database');
    const narrow = matchMedia('(max-width: 760px)').matches;
    if (narrow) {
      wires.send.setAttribute('d', `M${sender.x + sender.w / 2} ${sender.y + sender.h} V${service.y - 5}`);
      wires.store.setAttribute('d', `M${service.x + service.w} ${service.y + service.h / 2} H${database.x - 5}`);
      wires.notify.setAttribute('d', `M${service.x + service.w * .72} ${service.y + service.h} V${recipient.y - 5}`);
      wires.fetch.setAttribute('d', `M${recipient.x + recipient.w * .28} ${recipient.y} V${service.y + service.h + 5}`);
    } else {
      wires.send.setAttribute('d', `M${sender.x + sender.w} ${sender.y + sender.h / 2} H${service.x - 5}`);
      wires.store.setAttribute('d', `M${service.x + service.w / 2} ${service.y + service.h} V${database.y - 5}`);
      const gap = (recipient.x - service.x - service.w) / 2;
      wires.notify.setAttribute('d', `M${service.x + service.w} ${service.y + 29} C${service.x + service.w + gap} ${service.y - 5} ${recipient.x - gap} ${recipient.y - 5} ${recipient.x - 5} ${recipient.y + 29}`);
      wires.fetch.setAttribute('d', `M${recipient.x} ${recipient.y + recipient.h - 27} C${recipient.x - gap} ${recipient.y + recipient.h + 7} ${service.x + service.w + gap} ${service.y + service.h + 7} ${service.x + service.w + 5} ${service.y + service.h - 27}`);
    }
  }

  function drawPacket() {
    hidePacket();
    if (motion.matches) return;
    const stage = stageFor(index, mode), path = wires[stage.wire];
    // A fetch first sends a request, then returns the body along the same route.
    const cycle = Math.min(elapsed / (DURATION * .8), 1);
    if (cycle >= 1) return;
    let progress = cycle;
    if (index === 3) progress = cycle < .42 ? cycle / .42 : 1 - (cycle - .42) / .58;
    const point = path.getPointAtLength(path.getTotalLength() * progress);
    const packet = stage.wire === 'notify' ? $('flow-signal') : $('flow-packet');
    packet.removeAttribute('hidden');
    packet.setAttribute('transform', `translate(${point.x} ${point.y})`);
  }

  function tick(time) {
    if (lastFrame !== null) elapsed += Math.min(time - lastFrame, 100);
    lastFrame = time;
    if (elapsed >= DURATION) { index = (index + 1) % STAGES.length; elapsed = 0; render(); }
    drawPacket();
    frame = requestAnimationFrame(tick);
  }

  function syncPlayback() {
    if (frame !== null) cancelAnimationFrame(frame);
    frame = null; lastFrame = null;
    if (playing && visible && !document.hidden) frame = requestAnimationFrame(tick);
    else hidePacket();
  }

  function selectStep(step) {
    index = step; elapsed = 0; playing = false;
    render(); syncPlayback();
  }
  for (const button of steps) button.addEventListener('click', () => selectStep(Number(button.dataset.flowStep)));
  $('flow-play').addEventListener('click', () => { playing = !playing; render(); syncPlayback(); });
  $('flow-replay').addEventListener('click', () => { index = 0; elapsed = 0; playing = !motion.matches; render(); syncPlayback(); });
  $('flow-mode').addEventListener('change', () => { mode = $('flow-mode').value; elapsed = 0; render(); syncPlayback(); });
  motion.addEventListener('change', () => { playing = false; render(); syncPlayback(); });
  document.addEventListener('visibilitychange', syncPlayback);
  // Work stops when the illustration is off screen or the tab is hidden.
  new IntersectionObserver(entries => { visible = entries[0].isIntersecting; syncPlayback(); }, { threshold: .15 }).observe(graph);
  new ResizeObserver(drawWires).observe(graph);
  for (const control of root.querySelectorAll('button, select')) control.disabled = false;
  drawWires(); render();
}
