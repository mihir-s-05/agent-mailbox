package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mihir/msg-com/internal/model"
)

func (s *Service) SendMessage(ctx context.Context, p model.Principal, in SendInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	route, ok := in.To["type"].(string)
	if !ok || (route != "direct" && route != "broadcast") {
		return nil, model.NewError("INVALID_INPUT", "to.type must be direct or broadcast", nil)
	}
	requiredScope := "send:direct"
	if route == "broadcast" {
		requiredScope = "send:broadcast"
	}
	if !p.HasScope(requiredScope) && !p.HasScope("send:any") {
		return nil, model.NewError("FORBIDDEN", "missing "+requiredScope+" scope", nil)
	}
	if strings.TrimSpace(in.Body) == "" || strings.TrimSpace(in.Topic) == "" {
		return nil, model.NewError("INVALID_INPUT", "topic and body are required", nil)
	}
	if len(in.Body) > s.cfg.MaxBodyBytes {
		return nil, model.NewError("BODY_TOO_LARGE", "body exceeds maximum size", nil)
	}
	if len(in.Topic) > 512 || len(in.InReplyTo) > 128 || len(in.IdempotencyKey) > 256 {
		return nil, model.NewError("INVALID_INPUT", "message metadata exceeds maximum size", nil)
	}
	attachments, err := json.Marshal(in.Attachments)
	if err != nil || len(in.Attachments) > s.cfg.MaxAttachments || len(attachments) > 32*1024 {
		return nil, model.NewError("INVALID_INPUT", "invalid or oversized attachments", nil)
	}
	if in.Attachments == nil {
		in.Attachments = []map[string]any{}
		attachments = []byte("[]")
	}
	priority, err := model.ParsePriority(in.Priority)
	if err != nil {
		return nil, model.NewError("INVALID_PRIORITY", "invalid priority", nil)
	}
	readReceipt, err := model.NormalizeReadReceipt(in.ReadReceipt)
	if err != nil {
		return nil, model.NewError("INVALID_INPUT", "invalid read_receipt", nil)
	}
	if in.TTLSeconds < 0 {
		return nil, model.NewError("INVALID_INPUT", "ttl_seconds cannot be negative", nil)
	}
	if in.TTLSeconds == 0 {
		in.TTLSeconds = s.cfg.DefaultTTLSeconds
	}
	if in.TTLSeconds > s.cfg.MaxTTLSeconds {
		in.TTLSeconds = s.cfg.MaxTTLSeconds
	}
	in.Topic = strings.TrimSpace(in.Topic)
	in.Priority = priority
	in.ReadReceipt = readReceipt
	// Credential rotation and explicit team checks do not alter request identity.
	fingerprintInput := in
	fingerprintInput.SessionID = ""
	fingerprintInput.TeamID = ""
	fingerprintInput.IdempotencyKey = ""
	canonical, err := json.Marshal(fingerprintInput)
	if err != nil {
		return nil, model.NewError("INVALID_INPUT", "invalid message", nil)
	}
	hash := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(hash[:])
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, dbError("begin send", err)
	}
	defer tx.Rollback(ctx)
	// A sender lock makes same-key concurrent sends and rate accounting deterministic.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, senderLockKey(sess)); err != nil {
		return nil, dbError("lock sender", err)
	}
	if in.IdempotencyKey != "" {
		var id string
		var originalHash *string
		var at time.Time
		var recipients int
		err = tx.QueryRow(ctx, `SELECT message_id,request_hash,created_at,(SELECT count(*) FROM deliveries d WHERE d.message_id=m.message_id)::int FROM messages m WHERE team_id=$1 AND from_agent_id=$2 AND idempotency_key=$3`, sess.TeamID, sess.AgentID, in.IdempotencyKey).Scan(&id, &originalHash, &at, &recipients)
		if err == nil {
			if err = lockMailboxes(ctx, tx, sess.TeamID, []string{sess.AgentID}); err != nil {
				return nil, dbError("lock retry sender", err)
			}
			if aerr = authorizeSessionTx(ctx, tx, sess); aerr != nil {
				return nil, aerr
			}
			if originalHash != nil && *originalHash != fingerprint {
				return nil, model.NewError("IDEMPOTENCY_CONFLICT", "idempotency key was used with different message data", nil)
			}
			if err = tx.Commit(ctx); err != nil {
				return nil, dbError("commit retry", err)
			}
			return sendResult(id, at, recipients, true), nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, dbError("deduplicate send", err)
		}
	}
	recipients, toType, toValue, aerr := s.resolveRecipients(ctx, tx, sess.TeamID, sess.AgentID, in.To, in.IncludeSelf)
	if aerr != nil {
		return nil, aerr
	}
	scope := "send:direct"
	if toType != "direct" {
		scope = "send:broadcast"
	}
	if !p.HasScope(scope) && !p.HasScope("send:any") {
		return nil, model.NewError("FORBIDDEN", "missing "+scope+" scope", nil)
	}
	if toType != "direct" && len(recipients) > s.cfg.MaxBroadcastRecipients {
		return nil, model.NewError("BROADCAST_TOO_WIDE", "broadcast exceeds recipient limit", nil)
	}
	ids := []string{sess.AgentID}
	for _, r := range recipients {
		ids = append(ids, r.AgentID)
	}
	if err = lockMailboxes(ctx, tx, sess.TeamID, ids); err != nil {
		return nil, dbError("lock recipients", err)
	}
	if aerr = authorizeSessionTx(ctx, tx, sess); aerr != nil {
		return nil, aerr
	}
	for _, r := range recipients {
		var count int
		err = tx.QueryRow(ctx, `SELECT count(*)::int FROM deliveries d JOIN messages m USING(message_id) WHERE d.team_id=$1 AND d.recipient_agent_id=$2 AND d.state IN ('PENDING','DELIVERED') AND NOT m.cancelled AND m.expires_at>now()`, sess.TeamID, r.AgentID).Scan(&count)
		if err != nil {
			return nil, dbError("check capacity", err)
		}
		if count >= s.cfg.MailboxCap {
			return nil, model.NewError("MAILBOX_FULL", "recipient mailbox at capacity", map[string]any{"recipient_agent_id": r.AgentID})
		}
	}
	if aerr = s.takeDatabaseRate(ctx, tx, sess); aerr != nil {
		return nil, aerr
	}
	if in.InReplyTo != "" {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM messages WHERE team_id=$1 AND message_id=$2)`, sess.TeamID, in.InReplyTo).Scan(&exists); err != nil {
			return nil, dbError("check reply", err)
		}
		if !exists {
			return nil, model.NewError("INVALID_INPUT", "in_reply_to does not reference a team message", nil)
		}
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := newULID(now)
	expiry := now.Add(time.Duration(in.TTLSeconds) * time.Second)
	_, err = tx.Exec(ctx, `INSERT INTO messages(message_id,team_id,from_agent_id,to_type,to_value,priority,topic,body,in_reply_to,attachments,require_ack,read_receipt,idempotency_key,created_at,ttl_seconds,expires_at,cancelled,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,false,$17)`, id, sess.TeamID, sess.AgentID, toType, toValue, priority, in.Topic, in.Body, nullable(in.InReplyTo), string(attachments), in.RequireAck, readReceipt, nullable(in.IdempotencyKey), now, in.TTLSeconds, expiry, fingerprint)
	if err != nil {
		return nil, dbError("insert message", err)
	}
	if err = auditTx(ctx, tx, sess.TeamID, id, "", "ACCEPTED", now); err != nil {
		return nil, dbError("audit send", err)
	}
	for _, r := range recipients {
		if _, err = tx.Exec(ctx, `INSERT INTO deliveries(message_id,team_id,recipient_agent_id,state,read_receipt) VALUES($1,$2,$3,'PENDING',$4)`, id, sess.TeamID, r.AgentID, readReceipt); err != nil {
			return nil, dbError("insert delivery", err)
		}
		if err = s.publishArrival(ctx, tx, sess, in, id, r.AgentID, priority, now, expiry); err != nil {
			return nil, dbError("publish arrival", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, dbError("commit send", err)
	}
	return sendResult(id, now, len(recipients), false), nil
}

func sendResult(id string, at time.Time, count int, deduplicated bool) map[string]any {
	return map[string]any{"message_id": id, "accepted_at": at.UTC().Format(time.RFC3339Nano), "recipient_count": count, "deduplicated": deduplicated}
}
