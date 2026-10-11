import { readFile, writeFile, mkdir, copyFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const version = '0.1.1', name = 'agent-mailbox';
const repository = 'https://github.com/mihir-s-05/agent-mailbox';
const definitions = JSON.parse(execFileSync('go', ['run', '-buildvcs=false', './cmd/plugin-catalog'], { cwd: root, encoding: 'utf8' }));
const json = async (file, value) => { await mkdir(path.dirname(file), { recursive: true }); await writeFile(file, JSON.stringify(value, null, 2) + '\n'); };
const runtime = ['bridge.mjs', 'client.mjs', 'config.mjs', 'http.mjs', 'channel.mjs', 'setup.mjs'];
const author = { name: 'Mihir S.', url: 'https://mihir-s.com' };
const presentation = { displayName: 'Agent Mailbox', shortDescription: 'Messaging between your agents', longDescription: 'Connect to your Agent Mailbox service to send messages, read an owned inbox, and acknowledge receipt. PostgreSQL and the Go backend run separately. Credentials stay in local configuration. Claude Channel injection is opt-in and restricted to configured senders.', developerName: author.name, category: 'Developer Tools', capabilities: ['Read', 'Write'], websiteURL: repository, composerIcon: './assets/icon.svg', logo: './assets/icon.svg' };
for (const client of ['codex', 'claude']) {
  const target = path.join(root, 'plugins', client);
  await mkdir(path.join(target, 'scripts'), { recursive: true }); await mkdir(path.join(target, 'assets'), { recursive: true });
  for (const file of runtime) await copyFile(path.join(root, 'plugin-src', file), path.join(target, 'scripts', file));
  await json(path.join(target, 'scripts', 'tools.json'), definitions);
  const icon = (await readFile(path.join(root, 'internal/web/assets/favicon.svg'), 'utf8')).replace('viewBox="0 0 32 32"', 'viewBox="0 0 64 64"').replace('<rect ', '<g transform="scale(2)"><rect ').replace('</svg>', '</g></svg>');
  await writeFile(path.join(target, 'assets/icon.svg'), icon);
  if (client === 'codex') {
    // Compatibility format matches currently installed Codex local stdio plugins.
    await json(path.join(target, '.codex-plugin/plugin.json'), { name, version, description: 'Durable messaging for your agents through MCP.', author, repository, homepage: repository, keywords: ['agents', 'mailbox', 'mcp'], mcpServers: './.mcp.json', interface: presentation });
    await json(path.join(target, '.mcp.json'), { mcpServers: { mailbox: { command: 'node', args: ['./scripts/bridge.mjs', '--client=codex'], cwd: '.' } } });
  } else {
    const userConfig = {
      url: { type: 'string', title: 'Mailbox service URL', description: 'Your existing Agent Mailbox service. Remote services require HTTPS.', default: 'http://127.0.0.1:8443/', required: true },
      agent: { type: 'string', title: 'Agent ID', description: 'Choose a distinct ID for each concurrent client.', default: 'claude', required: true },
      token: { type: 'string', title: 'Bearer token', description: 'A credential issued by your service. Kept in Claude’s secure credential store.', sensitive: true, required: true },
      senders: { type: 'string', title: 'Trusted channel senders', description: 'Comma-separated agent IDs allowed to inject messages. Blank disables injection. No wildcards.', default: '' }
    };
    await json(path.join(target, '.claude-plugin/plugin.json'), { name, version, description: 'Agent messaging and opt-in inbox injection through Claude Channels.', author, repository, homepage: repository, keywords: ['agents', 'mailbox', 'mcp', 'channels'], userConfig, channels: [{ server: 'mailbox', displayName: 'Agent Mailbox' }] });
    await json(path.join(target, '.mcp.json'), { mcpServers: { mailbox: { command: 'node', args: ['${CLAUDE_PLUGIN_ROOT}/scripts/bridge.mjs', '--client=claude', '--channel'], env: { MAILBOX_URL: '${user_config.url}', MAILBOX_AGENT_ID: '${user_config.agent}', MAILBOX_TOKEN: '${user_config.token}', MAILBOX_ALLOWED_SENDERS: '${user_config.senders}' } } } });
  }
}
await json(path.join(root, '.agents/plugins/marketplace.json'), { name: 'mihir-s', interface: { displayName: 'Mihir S. · Agent Mailbox' }, plugins: [{ name, source: { source: 'local', path: './plugins/codex' }, policy: { installation: 'AVAILABLE', authentication: 'ON_INSTALL' }, category: 'Developer Tools' }] });
await json(path.join(root, '.claude-plugin/marketplace.json'), { name: 'mihir-s', description: 'Agent Mailbox plugins for authenticated agent messaging.', owner: { name: author.name }, plugins: [{ name, source: './plugins/claude', description: 'Agent messaging with opt-in Claude Channel inbox injection.', version }] });
console.log(`Packaged Agent Mailbox ${version} for Codex and Claude Code. No credentials or instruction files included.`);
