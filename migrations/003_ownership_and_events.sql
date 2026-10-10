ALTER TABLE agents ADD COLUMN owner_subject TEXT;
UPDATE agents a SET owner_subject = (
  SELECT NULLIF(btrim(s.principal),'') FROM sessions s
  WHERE s.team_id=a.team_id AND s.agent_id=a.agent_id
  ORDER BY s.active DESC, s.created_at DESC, s.session_id DESC LIMIT 1
);
UPDATE sessions s SET active=false,invalidated_at=now() FROM agents a
WHERE s.team_id=a.team_id AND s.agent_id=a.agent_id AND s.active
AND (a.owner_subject IS NULL OR a.owner_subject<>s.principal);
-- Legacy agents without a registration history stay locked until an administrator assigns an owner.
ALTER TABLE agents ADD COLUMN inbox_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE agents ADD COLUMN replay_floor BIGINT NOT NULL DEFAULT 0;
ALTER TABLE messages ADD COLUMN request_hash TEXT;

CREATE TABLE sender_rates (
  team_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY(team_id, agent_id)
);

CREATE TABLE mailbox_events (
  event_id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  message_id TEXT NOT NULL,
  name TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  data JSONB NOT NULL,
  UNIQUE(team_id, agent_id, revision)
);
CREATE INDEX mailbox_events_replay ON mailbox_events(team_id, agent_id, revision);

CREATE TABLE event_subscriptions (
  id TEXT PRIMARY KEY,
  team_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  owner_subject TEXT NOT NULL,
  token_id TEXT NOT NULL,
  name TEXT NOT NULL,
  arguments JSONB NOT NULL,
  callback_url TEXT NOT NULL,
  secret BYTEA NOT NULL,
  previous_secret BYTEA,
  rotate_until TIMESTAMPTZ,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cursor BIGINT NOT NULL DEFAULT 0,
  active BOOLEAN NOT NULL DEFAULT true
);
CREATE INDEX event_subscriptions_mailbox ON event_subscriptions(team_id, agent_id) WHERE active;

CREATE TABLE event_deliveries (
  subscription_id TEXT NOT NULL REFERENCES event_subscriptions(id) ON DELETE CASCADE,
  event_id TEXT NOT NULL REFERENCES mailbox_events(event_id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'PENDING' CHECK(state IN ('PENDING','ACCEPTED','FAILED')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_until TIMESTAMPTZ,
  lease_token TEXT,
  accepted_at TIMESTAMPTZ,
  last_error TEXT,
  PRIMARY KEY(subscription_id, event_id)
);
CREATE INDEX event_deliveries_ready ON event_deliveries(next_attempt_at) WHERE state='PENDING';
