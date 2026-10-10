package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mihir/msg-com/internal/model"
)

func transitionOnPoll(ctx context.Context, tx pgx.Tx, sess sessionRow, id string, requireAck bool, at time.Time) (string, *time.Time, *time.Time, *model.APIError) {
	state := model.DeliveryDelivered
	if !requireAck {
		state = model.DeliveryAcked
	}
	var delivered *time.Time
	var acked *time.Time
	err := tx.QueryRow(ctx, `UPDATE deliveries d SET state=$1,delivered_at=$2::timestamptz,acked_at=CASE WHEN $1='ACKED' THEN $2::timestamptz ELSE NULL END FROM messages m WHERE d.message_id=m.message_id AND d.team_id=$3 AND d.recipient_agent_id=$4 AND d.message_id=$5 AND d.state='PENDING' AND NOT m.cancelled AND m.expires_at>now() RETURNING d.delivered_at,d.acked_at`, state, at, sess.TeamID, sess.AgentID, id).Scan(&delivered, &acked)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, nil, nil
	}
	if err != nil {
		return "", nil, nil, dbError("mark delivered", err)
	}
	if err = auditTx(ctx, tx, sess.TeamID, id, sess.AgentID, "DELIVERED", at); err != nil {
		return "", nil, nil, dbError("audit delivery", err)
	}
	if !requireAck {
		if err = auditTx(ctx, tx, sess.TeamID, id, sess.AgentID, "ACKED", at); err != nil {
			return "", nil, nil, dbError("audit acknowledgement", err)
		}
	}
	if _, err = notifyInboxTx(ctx, tx, sess.TeamID, sess.AgentID); err != nil {
		return "", nil, nil, dbError("notify delivery", err)
	}
	return state, delivered, acked, nil
}

func (s *Service) AckMessages(ctx context.Context, p model.Principal, in AckInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	if !p.HasScope("poll:self") {
		return nil, model.NewError("FORBIDDEN", "mailbox acknowledgement permission is required", nil)
	}
	if kind := strings.ToUpper(strings.TrimSpace(in.AckKind)); kind != "" && kind != "ACK" {
		return nil, model.NewError("INVALID_INPUT", "ack_kind must be ACK", nil)
	}
	if len(in.MessageIDs) > 100 {
		return nil, model.NewError("INVALID_INPUT", "at most 100 messages may be acknowledged", nil)
	}
	acked, already, missing := []string{}, []string{}, []string{}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, dbError("begin ack", err)
	}
	defer tx.Rollback(ctx)
	if err = lockMailboxes(ctx, tx, sess.TeamID, []string{sess.AgentID}); err != nil {
		return nil, dbError("lock ack", err)
	}
	if aerr = authorizeSessionTx(ctx, tx, sess); aerr != nil {
		return nil, aerr
	}
	for _, id := range in.MessageIDs {
		var found string
		err = tx.QueryRow(ctx, `UPDATE deliveries d SET state='ACKED',acked_at=now() FROM messages m WHERE d.message_id=m.message_id AND d.team_id=$1 AND d.recipient_agent_id=$2 AND d.message_id=$3 AND d.state='DELIVERED' AND NOT m.cancelled AND m.expires_at>now() RETURNING d.message_id`, sess.TeamID, sess.AgentID, id).Scan(&found)
		if errors.Is(err, pgx.ErrNoRows) {
			var state string
			err = tx.QueryRow(ctx, `SELECT state FROM deliveries WHERE team_id=$1 AND recipient_agent_id=$2 AND message_id=$3`, sess.TeamID, sess.AgentID, id).Scan(&state)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, dbError("read acknowledgement", err)
			}
			if state == model.DeliveryAcked {
				already = append(already, id)
			} else {
				missing = append(missing, id)
			}
			continue
		}
		if err != nil {
			return nil, dbError("acknowledge", err)
		}
		if err = auditTx(ctx, tx, sess.TeamID, id, sess.AgentID, "ACKED", time.Now().UTC()); err != nil {
			return nil, dbError("audit acknowledgement", err)
		}
		acked = append(acked, id)
	}
	if len(acked) > 0 {
		if _, err = notifyInboxTx(ctx, tx, sess.TeamID, sess.AgentID); err != nil {
			return nil, dbError("notify ack", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, dbError("commit ack", err)
	}
	return map[string]any{"acked": acked, "already_acked": already, "not_found": missing}, nil
}

func authorizeSessionTx(ctx context.Context, tx pgx.Tx, sess sessionRow) *model.APIError {
	var allowed bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sessions s JOIN agents a USING(team_id,agent_id) WHERE s.session_id=$1 AND s.active AND s.principal=$2 AND a.owner_subject=$2)`, sess.SessionID, sess.Principal).Scan(&allowed)
	if err != nil {
		return dbError("revalidate session", err)
	}
	if !allowed {
		return model.NewError("SESSION_INVALID", "session was invalidated", nil)
	}
	if _, err := tx.Exec(ctx, `UPDATE agents SET last_seen_at=now(),online=true WHERE team_id=$1 AND agent_id=$2 AND owner_subject=$3`, sess.TeamID, sess.AgentID, sess.Principal); err != nil {
		return dbError("refresh agent presence", err)
	}
	return nil
}

func (s *Service) CancelMessage(ctx context.Context, p model.Principal, in CancelInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, "")
	if aerr != nil {
		return nil, aerr
	}
	if !p.HasScope("send:direct") && !p.HasScope("send:broadcast") && !p.HasScope("send:any") {
		return nil, model.NewError("FORBIDDEN", "send permission is required", nil)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, dbError("begin cancellation", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT recipient_agent_id FROM deliveries WHERE team_id=$1 AND message_id=$2 ORDER BY recipient_agent_id`, sess.TeamID, in.MessageID)
	if err != nil {
		return nil, dbError("read recipients", err)
	}
	ids := []string{sess.AgentID}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, dbError("scan recipient", err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbError("iterate recipients", err)
	}
	if err = lockMailboxes(ctx, tx, sess.TeamID, ids); err != nil {
		return nil, dbError("lock cancellation", err)
	}
	if aerr = authorizeSessionTx(ctx, tx, sess); aerr != nil {
		return nil, aerr
	}
	var sender string
	err = tx.QueryRow(ctx, `SELECT from_agent_id FROM messages WHERE team_id=$1 AND message_id=$2 FOR UPDATE`, sess.TeamID, in.MessageID).Scan(&sender)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[string]any{"status": "NOT_FOUND"}, nil
	}
	if err != nil {
		return nil, dbError("read message", err)
	}
	if sender != sess.AgentID {
		return nil, model.NewError("FORBIDDEN", "only the sender may cancel", nil)
	}
	rows, err = tx.Query(ctx, `UPDATE deliveries SET state='CANCELLED' WHERE team_id=$1 AND message_id=$2 AND state IN ('PENDING','DELIVERED') RETURNING recipient_agent_id`, sess.TeamID, in.MessageID)
	if err != nil {
		return nil, dbError("cancel deliveries", err)
	}
	changed := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, dbError("scan cancellation", err)
		}
		changed = append(changed, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbError("iterate cancellations", err)
	}
	var acked int
	if err = tx.QueryRow(ctx, `SELECT count(*)::int FROM deliveries WHERE message_id=$1 AND state='ACKED'`, in.MessageID).Scan(&acked); err != nil {
		return nil, dbError("count acknowledgements", err)
	}
	if len(changed) > 0 {
		if _, err = tx.Exec(ctx, `UPDATE messages SET cancelled=true WHERE message_id=$1`, in.MessageID); err != nil {
			return nil, dbError("cancel message", err)
		}
		for _, id := range changed {
			if err = auditTx(ctx, tx, sess.TeamID, in.MessageID, id, "CANCELLED", time.Now().UTC()); err != nil {
				return nil, dbError("audit cancellation", err)
			}
			if _, err = notifyInboxTx(ctx, tx, sess.TeamID, id); err != nil {
				return nil, dbError("notify cancellation", err)
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, dbError("commit cancellation", err)
	}
	status := "TOO_LATE"
	if len(changed) > 0 {
		status = "CANCELLED"
		if acked > 0 {
			status = "PARTIAL"
		}
	}
	return map[string]any{"status": status, "cancelled_count": len(changed), "already_acked_count": acked}, nil
}
