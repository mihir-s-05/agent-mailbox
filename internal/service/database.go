package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mihir/msg-com/internal/model"
)

func dbError(operation string, err error) *model.APIError {
	return model.NewError("INTERNAL_ERROR", "database operation failed: "+operation, nil)
}

// All mailbox mutations lock agent rows before delivery rows, in stable order.
func lockMailboxes(ctx context.Context, tx pgx.Tx, team string, ids []string) error {
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	for i, id := range ids {
		if i > 0 && id == ids[i-1] {
			continue
		}
		var found string
		if err := tx.QueryRow(ctx, `SELECT agent_id FROM agents WHERE team_id=$1 AND agent_id=$2 FOR UPDATE`, team, id).Scan(&found); err != nil {
			return err
		}
	}
	return nil
}

func notifyInboxTx(ctx context.Context, tx pgx.Tx, team, agent string) (int64, error) {
	var revision int64
	if err := tx.QueryRow(ctx, `UPDATE agents SET inbox_revision=inbox_revision+1 WHERE team_id=$1 AND agent_id=$2 RETURNING inbox_revision`, team, agent).Scan(&revision); err != nil {
		return 0, err
	}
	payload, err := json.Marshal(map[string]any{"team_id": team, "agent_id": agent, "revision": revision})
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, channelName(team), string(payload))
	return revision, err
}

func auditTx(ctx context.Context, tx pgx.Tx, team, message, recipient, kind string, at time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO message_events(event_id,team_id,message_id,recipient_agent_id,event_type,event_at,details) VALUES($1,$2,$3,$4,$5,$6,'{}')`, uuid.NewString(), team, message, nullable(recipient), kind, at)
	return err
}

func (s *Service) publishArrival(ctx context.Context, tx pgx.Tx, sess sessionRow, in SendInput, message string, recipient string, priority int, at, expiry time.Time) error {
	revision, err := notifyInboxTx(ctx, tx, sess.TeamID, recipient)
	if err != nil {
		return err
	}
	data, err := json.Marshal(map[string]any{"message_id": message, "agent_id": recipient, "from_agent_id": sess.AgentID, "priority": priority, "topic": in.Topic, "expires_at": expiry.Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO mailbox_events(event_id,team_id,agent_id,revision,message_id,name,occurred_at,expires_at,data) VALUES($1,$2,$3,$4,$5,'mailbox.message.created',$6,$7,$8)`, uuid.NewString(), sess.TeamID, recipient, revision, message, at, expiry, string(data))
	return err
}

func (s *Service) takeDatabaseRate(ctx context.Context, tx pgx.Tx, sess sessionRow) *model.APIError {
	var count int
	err := tx.QueryRow(ctx, `INSERT INTO sender_rates(team_id,agent_id,window_start,count) VALUES($1,$2,date_trunc('minute',now()),1)
	ON CONFLICT(team_id,agent_id) DO UPDATE SET window_start=EXCLUDED.window_start,count=CASE WHEN sender_rates.window_start=EXCLUDED.window_start THEN sender_rates.count+1 ELSE 1 END RETURNING count`, sess.TeamID, sess.AgentID).Scan(&count)
	if err != nil {
		return dbError("rate limit", err)
	}
	if count > s.cfg.RateLimitPerMinute {
		return model.NewError("RATE_LIMITED", "sender exceeded per-minute rate limit", map[string]any{"retry_after_ms": time.Until(time.Now().Truncate(time.Minute).Add(time.Minute)).Milliseconds()})
	}
	return nil
}

func senderLockKey(sess sessionRow) string {
	return fmt.Sprintf("%d:%s:%s", len(sess.TeamID), sess.TeamID, sess.AgentID)
}
