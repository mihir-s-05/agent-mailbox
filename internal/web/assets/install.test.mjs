import test from 'node:test';
import assert from 'node:assert/strict';
import { installPlan } from './install.mjs';
test('installation keeps local HTTP on loopback and uses real service entry points', () => {
  for (const platform of ['windows', 'unix']) {
    const plan = installPlan(platform, 'codex');
    assert.match(plan.server, /MAILBOX_DATABASE_URL/);
    assert.match(plan.server, /127\.0\.0\.1:5432/);
    assert.match(plan.server, /go run \.\/cmd\/agent-mailbox$/);
    assert.match(plan.command, /codex plugin add agent-mailbox@mihir-s/);
    assert.equal(plan.command.includes('PASTE_YOUR_TOKEN'), false);
  }
});
test('Claude installation uses the GitHub plugin and keeps credentials in local onboarding', () => {
  const plan = installPlan('unix', 'claude');
  assert.match(plan.command, /claude plugin marketplace add https:\/\/github\.com\/mihir-s-05\/agent-mailbox\.git#codex\/github-plugins/);
  assert.match(plan.command, /claude plugin install agent-mailbox@mihir-s/);
  assert.match(plan.instructions, /secure credential store/);
  assert.equal(plan.command.includes('Bearer'), false);
});
