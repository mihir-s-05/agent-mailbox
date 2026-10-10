import { readFile, mkdir, writeFile, rename, chmod, lstat, unlink } from 'node:fs/promises';
import { homedir } from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
const exec = promisify(execFile);

export function configPath(client, env = process.env) {
  const base = process.platform === 'win32' ? (env.LOCALAPPDATA || path.join(homedir(), 'AppData', 'Local')) : (env.XDG_CONFIG_HOME || path.join(homedir(), '.config'));
  return env.MAILBOX_CLIENT_CONFIG || path.join(base, 'agent-mailbox', 'clients', `${client}.json`);
}

export function normalizeConfig(input) {
  const url = new URL(String(input.url || 'http://127.0.0.1:8443/'));
  if (url.username || url.password || url.search || url.hash) throw new Error('Use a service URL without credentials, query, or fragment.');
  const local = ['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname);
  if (url.protocol !== 'https:' && !(url.protocol === 'http:' && local)) throw new Error('Use HTTPS for a remote mailbox, or HTTP on loopback.');
  // Accept the MCP URL copied from the service; browser API routes share its parent.
  url.pathname = url.pathname.replace(/\/mcp\/?$/, '/');
  if (!url.pathname.endsWith('/')) url.pathname += '/';
  const agent = String(input.agent || '').trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(agent)) throw new Error('Choose an agent ID using letters, numbers, dots, underscores, or hyphens.');
  const token = String(input.token || '').trim();
  if (!token || token.length > 8192 || /[\s\x00-\x1f\x7f]/.test(token)) throw new Error('Enter a valid mailbox bearer token.');
  const senders = Array.isArray(input.senders) ? input.senders : String(input.senders || '').split(',');
  const allowedSenders = [...new Set(senders.map(sender => String(sender).trim()).filter(Boolean))];
  if (allowedSenders.length > 100 || allowedSenders.some(sender => !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(sender))) throw new Error('Trusted senders must be specific agent IDs, separated by commas. Wildcards are not accepted.');
  return { url: url.href, agent, token, senders: allowedSenders };
}

export async function loadConfig(client, env = process.env) {
  if (env.MAILBOX_URL || env.MAILBOX_AGENT_ID) return normalizeConfig({ url: env.MAILBOX_URL, agent: env.MAILBOX_AGENT_ID, token: env.MAILBOX_TOKEN, senders: env.MAILBOX_ALLOWED_SENDERS });
  try {
    return normalizeConfig(JSON.parse(await readFile(configPath(client, env), 'utf8')));
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    // Never echo the file or a parser error that could quote a credential.
    throw new Error('Mailbox configuration is invalid. Run setup again.');
  }
}

async function restrictWindows(file) {
  if (process.platform !== 'win32') return;
  const { stdout } = await exec('whoami.exe', ['/user', '/fo', 'csv', '/nh'], { windowsHide: true });
  const sid = stdout.match(/S-1-5-\d+(?:-\d+)+/g)?.at(-1);
  if (!sid) throw new Error('Could not determine the Windows account for credential protection.');
  await exec('icacls.exe', [file, '/inheritance:r', '/grant:r', `*${sid}:(F)`], { windowsHide: true });
}

export async function saveConfig(client, value, env = process.env) {
  const config = normalizeConfig(value), destination = path.resolve(configPath(client, env));
  await mkdir(path.dirname(destination), { recursive: true, mode: 0o700 });
  try { if ((await lstat(destination)).isSymbolicLink()) throw new Error('Refusing a credential-file symlink.'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const temporary = `${destination}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, '', { mode: 0o600, flag: 'wx' });
    // Apply Windows ACLs before writing any credential bytes.
    await restrictWindows(temporary);
    await writeFile(temporary, JSON.stringify(config, null, 2) + '\n', { mode: 0o600 });
    await rename(temporary, destination);
    await chmod(destination, 0o600);
  } finally { await unlink(temporary).catch(() => {}); }
  return destination;
}
