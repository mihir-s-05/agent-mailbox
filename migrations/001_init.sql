CREATE TABLE IF NOT EXISTS agents (
  team_id        TEXT NOT NULL,
  agent_id       TEXT NOT NULL,
  display_name   TEXT,
  tags           JSONB NOT NULL DEFAULT '[]'::jsonb,
  capabilities   JSONB NOT NULL DEFAULT '[]'::jsonb,
  status         TEXT NOT NULL DEFAULT 'idle',
  status_note    TEXT,
  last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  online         BOOLEAN NOT NULL DEFAULT true,
  PRIMARY KEY (team_id, agent_id)
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id      TEXT PRIMARY KEY,
  team_id         TEXT NOT NULL,
  agent_id        TEXT NOT NULL,
  principal       TEXT NOT NULL,
  active          BOOLEAN NOT NULL DEFAULT true,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  invalidated_at  TIMESTAMPTZ,
  FOREIGN KEY (team_id, agent_id) REFERENCES agents(team_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_lookup ON sessions(team_id, agent_id, active);

CREATE TABLE IF NOT EXISTS messages (
  message_id       TEXT PRIMARY KEY,
  team_id          TEXT NOT NULL,
  from_agent_id    TEXT NOT NULL,
  to_type          TEXT NOT NULL,
  to_value         TEXT,
  priority         INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 3),
  topic            TEXT NOT NULL,
  body             TEXT NOT NULL,
  in_reply_to      TEXT,
  attachments      JSONB NOT NULL DEFAULT '[]'::jsonb,
  require_ack      BOOLEAN NOT NULL DEFAULT true,
  read_receipt     TEXT NOT NULL DEFAULT 'NONE',
  idempotency_key  TEXT,
  created_at       TIMESTAMPTZ NOT NULL,
  ttl_seconds      INTEGER NOT NULL DEFAULT 604800,
  expires_at       TIMESTAMPTZ NOT NULL,
  cancelled        BOOLEAN NOT NULL DEFAULT false
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idem
  ON messages(team_id, from_agent_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_messages_team_time ON messages(team_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_reply ON messages(in_reply_to);
CREATE INDEX IF NOT EXISTS idx_messages_expiry ON messages(expires_at) WHERE cancelled = false;

CREATE TABLE IF NOT EXISTS deliveries (
  message_id          TEXT NOT NULL,
  team_id             TEXT NOT NULL,
  recipient_agent_id  TEXT NOT NULL,
  state               TEXT NOT NULL DEFAULT 'PENDING',
  delivered_at        TIMESTAMPTZ,
  acked_at            TIMESTAMPTZ,
  read_receipt        TEXT NOT NULL DEFAULT 'NONE',
  PRIMARY KEY (message_id, recipient_agent_id),
  FOREIGN KEY (message_id) REFERENCES messages(message_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_inbox
  ON deliveries(team_id, recipient_agent_id, state)
  WHERE state IN ('PENDING', 'DELIVERED');

CREATE TABLE IF NOT EXISTS message_events (
  event_id             TEXT PRIMARY KEY,
  team_id              TEXT NOT NULL,
  message_id           TEXT NOT NULL,
  recipient_agent_id   TEXT,
  event_type           TEXT NOT NULL,
  event_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  details              JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_events_team_time ON message_events(team_id, event_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_message ON message_events(message_id, event_at ASC);

