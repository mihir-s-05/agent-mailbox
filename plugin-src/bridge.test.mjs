import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const bridge = fileURLToPath(new URL('../plugins/claude/scripts/bridge.mjs', import.meta.url));
for (const modern of [false, true]) {
  test(`packaged Claude bridge exposes tools with ${modern ? 'required modern cache hints' : 'legacy initialization'}`, async () => {
    const env = { ...process.env, MAILBOX_CLIENT_CONFIG: path.join(tmpdir(), `mailbox-unconfigured-${randomUUID()}.json`) };
    for (const key of ['MAILBOX_URL', 'MAILBOX_AGENT_ID', 'MAILBOX_TOKEN', 'MAILBOX_ALLOWED_SENDERS']) delete env[key];
    const child = spawn(process.execPath, [bridge, '--client=claude', '--channel'], { env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let number = 0;
    const pending = new Map();
    createInterface({ input: child.stdout }).on('line', line => {
      const message = JSON.parse(line);
      pending.get(message.id)?.(message);
    });
    const rpc = (method, params = {}) => new Promise((resolve, reject) => {
      const id = ++number;
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`${method} timed out`)); }, 5000);
      pending.set(id, message => { clearTimeout(timer); pending.delete(id); message.error ? reject(new Error(message.error.message)) : resolve(message.result); });
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
    try {
      const discovery = modern
        ? await rpc('server/discover', { _meta: { 'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {}, 'io.modelcontextprotocol/clientInfo': { name: 'strict-host-test', version: '1' } } })
        : await rpc('initialize', { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'legacy-host-test', version: '1' } });
      assert.deepEqual(discovery.capabilities.experimental['claude/channel'], {});
      const listing = await rpc('tools/list');
      assert.equal(listing.tools.length, 11);
      assert.ok(listing.tools.some(tool => tool.name === 'mailbox_connect'));
      for (const result of [discovery, listing]) {
        assert.equal(result.ttlMs, modern ? 0 : undefined);
        assert.equal(result.cacheScope, modern ? 'private' : undefined);
        assert.equal(result.resultType, modern ? 'complete' : undefined);
      }
      if (modern) assert.ok(discovery.supportedVersions.includes('2026-07-28'));
      else assert.equal(discovery.protocolVersion, '2025-11-25');
    } finally {
      const exited = once(child, 'exit');
      const timer = setTimeout(() => child.kill(), 2500);
      child.stdin.end();
      await exited;
      clearTimeout(timer);
    }
  });
}
