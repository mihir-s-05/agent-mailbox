import { createInterface, emitKeypressEvents } from 'node:readline';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { loadConfig, normalizeConfig, saveConfig } from './config.mjs';
import { createAPI } from './http.mjs';

export async function checkConnection(config, fetch_) {
  const api = createAPI(config, fetch_);
  try {
    const identity = await api.request('api/session');
    const { tools } = await api.request('api/tools');
    if (!identity.team_id || !tools?.some(tool => tool.name === 'send_message')) throw new Error('The endpoint is not a compatible Agent Mailbox service.');
    return { team_id: identity.team_id, agent_id: config.agent, service_url: config.url };
  } finally { api.close(); }
}

export async function configure(client, value, { env = process.env, fetch_ } = {}) {
  const config = normalizeConfig(value);
  const status = await checkConnection(config, fetch_);
  const saved = await saveConfig(client, config, env);
  return { ...status, configuration_file: saved, trusted_senders: config.senders };
}

function secretPrompt() {
  const input = process.stdin, output = process.stdout;
  if (!input.isTTY || !output.isTTY) throw new Error('Use an interactive terminal for a masked token, or --token-stdin with --url and --agent.');
  emitKeypressEvents(input);
  output.write('Bearer token (masked): ');
  const wasRaw = input.isRaw;
  input.setRawMode(true); input.resume();
  return new Promise((resolve, reject) => {
    let value = '';
    function finish(error) { input.off('keypress', onKey); input.setRawMode(Boolean(wasRaw)); input.pause(); output.write('\n'); error ? reject(error) : resolve(value); }
    function onKey(text, key = {}) {
      if (key.ctrl && key.name === 'c') return finish(new Error('Setup cancelled.'));
      if (key.name === 'return' || key.name === 'enter') return finish();
      if (key.name === 'backspace') { if (value.length) { value = value.slice(0, -1); output.write('\b \b'); } return; }
      if (text && !key.ctrl && !key.meta && !/[\x00-\x1f\x7f]/.test(text)) { if (value.length + text.length > 8192) return finish(new Error('Token exceeds the size limit.')); value += text; output.write('*'.repeat(text.length)); }
    }
    input.on('keypress', onKey);
  });
}

export async function main(args = process.argv.slice(2)) {
  const option = name => { const index = args.indexOf(name); return index < 0 ? undefined : args[index + 1]; };
  const client = option('--client') || 'codex';
  if (!['codex', 'claude'].includes(client)) throw new Error('Use --client codex or --client claude.');
  if (args.includes('--check')) {
    const config = await loadConfig(client);
    if (!config) throw new Error('Run setup first.');
    console.log(JSON.stringify(await checkConnection(config), null, 2)); return;
  }
  let value;
  if (args.includes('--token-stdin')) {
    if (!option('--url') || !option('--agent')) throw new Error('--token-stdin requires --url and --agent; the token must never be an argument.');
    let token = '';
    for await (const chunk of process.stdin) { token += chunk; if (token.length > 8192) throw new Error('Token exceeds the size limit.'); }
    value = { url: option('--url'), agent: option('--agent'), token, senders: option('--senders') };
  } else {
    console.log(`Agent Mailbox · ${client} setup\nUse a distinct agent ID for each running client. Credentials stay on this computer.`);
    if (!process.stdin.isTTY) throw new Error('Run setup in an interactive terminal.');
    const prompt = createInterface({ input: process.stdin, output: process.stdout });
    const ask = text => new Promise(resolve => prompt.question(text, resolve));
    const url = (await ask('Service URL [http://127.0.0.1:8443/]: ')).trim() || 'http://127.0.0.1:8443/';
    const agent = (await ask(`Agent ID [${client}]: `)).trim() || client;
    const senders = client === 'claude' ? await ask('Trusted sender IDs for channel injection (comma-separated; blank disables injection): ') : '';
    prompt.close();
    const token = await secretPrompt();
    value = { url, agent, senders, token };
  }
  const out = await configure(client, value);
  console.log(`Connected to team ${out.team_id}. Saved protected configuration for ${out.agent_id}.\nReturn to your client and ask: Connect my Agent Mailbox and check my inbox.`);
  if (client === 'claude') console.log('Channel opt-in: claude --dangerously-load-development-channels plugin:agent-mailbox@mihir-s\nReview Claude’s development-channel prompt. Organization channel policy still applies.');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main().catch(error => { console.error(error.message); process.exitCode = 1; });
