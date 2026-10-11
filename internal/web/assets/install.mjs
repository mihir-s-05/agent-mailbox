import { mountMessageFlow } from './architecture.mjs';

export function installPlan(platform, client) {
  const windows = platform === 'windows';
  const server = windows ? [
    "$env:MAILBOX_DATABASE_URL = 'postgres://postgres:YOUR_PASSWORD@127.0.0.1:5432/agent_mailbox?sslmode=disable'",
    "$env:MAILBOX_REQUIRE_MTLS = 'false'", "$env:MAILBOX_TLS_ENABLED = 'false'", 'go run ./cmd/agent-mailbox'
  ] : [
    "export MAILBOX_DATABASE_URL='postgres://postgres:YOUR_PASSWORD@127.0.0.1:5432/agent_mailbox?sslmode=disable'",
    'export MAILBOX_REQUIRE_MTLS=false', 'export MAILBOX_TLS_ENABLED=false', 'go run ./cmd/agent-mailbox'
  ];
  const token = windows ? "$env:MAILBOX_TOKEN = (Get-Content ./tokens.local.secrets.json -Raw | ConvertFrom-Json).tokens[0].token" : "# Read a development token from tokens.local.secrets.json.\nexport MAILBOX_TOKEN='PASTE_YOUR_TOKEN_LOCALLY'";
  const endpoint = 'http://127.0.0.1:8443/mcp';
  const plans = {
    codex: { title: 'Set up your connection', instructions: 'Open a new Codex chat and ask for Agent Mailbox setup. Run the returned command in your terminal; it asks for a URL, a unique agent ID, and a masked token.', command: 'codex plugin marketplace add mihir-s-05/agent-mailbox\ncodex plugin add agent-mailbox@mihir-s', onboarding: 'Show Agent Mailbox setup.\nThen connect my Agent Mailbox and check my inbox.', label: 'Terminal', note: 'The plugin saves protected local configuration outside its cache.', docs: './guide.html#plugins' },
    claude: { title: 'Enter your mailbox details', instructions: 'Use the plugin configuration dialog for your service URL, a unique agent ID, and a bearer token. The token field uses Claude’s secure credential store.', command: 'claude plugin marketplace add https://github.com/mihir-s-05/agent-mailbox.git\nclaude plugin install agent-mailbox@mihir-s', onboarding: 'Connect my Agent Mailbox and check my inbox.', label: 'Terminal', note: 'Add trusted sender IDs and enable Claude Channels to receive messages in a running session.', docs: './guide.html#plugins' },
    generic: { title: 'Connect using Streamable HTTP', instructions: 'Add an MCP server in your client. Use this endpoint and a bearer token from your service. Follow the client’s credential settings.', command: `Transport: Streamable HTTP\nURL: ${endpoint}\nHeader: Authorization: Bearer <your mailbox token>`, label: 'Connection settings', note: 'STDIO is also supported. See the guide for process configuration and TLS alternatives.', docs: './guide.html#clients' }
  };
  const plan = plans[client] || plans.codex;
  return { server: server.join('\n'), token, shell: windows ? 'PowerShell' : 'Shell', onboarding: 'Register an agent through your MCP client, then send and receive a message.', verification: client === 'generic' ? 'Register an agent named reviewer in dev-team.\nSend a normal-priority message to reviewer:\ntopic "First message", body "Hello".\nPoll its inbox and acknowledge that message.' : 'Connect my Agent Mailbox. Send a normal-priority\nmessage to my configured agent: topic "First message",\nbody "Hello". Fetch it and acknowledge receipt.', ...plan };
}

if (typeof document !== 'undefined') {
  mountMessageFlow(document.getElementById('how-it-works'));
  const $ = id => document.getElementById(id);
  function update() {
    const plan = installPlan($('install-platform').value, $('install-client').value);
    for (const [id, value] of Object.entries({ 'server-command': plan.server, 'token-command': plan.token, 'server-command-label': plan.shell, 'token-command-label': plan.shell, 'client-heading': plan.title, 'client-instructions': plan.instructions, 'client-command': plan.command, 'client-command-label': plan.label, 'client-note': plan.note, 'onboarding-command': plan.onboarding, 'verify-prompt': plan.verification })) $(id).textContent = value;
    $('client-docs').href = plan.docs;
  }
  if (!navigator.userAgent.includes('Windows')) $('install-platform').value = 'unix';
  $('install-platform').addEventListener('change', update); $('install-client').addEventListener('change', update); update();
  for (const button of document.querySelectorAll('[data-copy]')) button.addEventListener('click', async () => {
    const text = $(button.dataset.copy).textContent;
    try { await navigator.clipboard.writeText(text); button.textContent = 'Copied'; $('copy-status').textContent = 'Copied to clipboard.'; setTimeout(() => { button.textContent = 'Copy'; }, 1800); }
    catch { const selection = getSelection(), range = document.createRange(); range.selectNodeContents($(button.dataset.copy)); selection.removeAllRanges(); selection.addRange(range); $('copy-status').textContent = 'Clipboard unavailable. The commands are selected so you can copy them.'; }
  });
}
