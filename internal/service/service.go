package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"

	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/model"
)

type Service struct {
	db  *pgxpool.Pool
	cfg config.Config

	rlMu       sync.Mutex
	rateWindow map[string]rateState
}

type rateState struct {
	WindowStart time.Time
	Count       int
}

type RegisterInput struct {
	TeamID                 string
	AgentID                string
	DisplayName            string
	Tags                   []string
	Capabilities           []string
	ReplaceExistingSession bool
}

type SendInput struct {
	SessionID      string
	TeamID         string
	To             map[string]any
	Priority       any
	Topic          string
	Body           string
	InReplyTo      string
	Attachments    []map[string]any
	TTLSeconds     int
	RequireAck     bool
	ReadReceipt    string
	IncludeSelf    bool
	IdempotencyKey string
}

type PollInput struct {
	SessionID   string
	TeamID      string
	MaxMessages int
	MinPriority any
	WaitMS      int
	Cursor      string
}

type AckInput struct {
	SessionID  string
	TeamID     string
	MessageIDs []string
	AckKind    string
}

type ListAgentsInput struct {
	SessionID      string
	TeamID         string
	FilterTag      string
	IncludeOffline bool
}

type DeregisterInput struct {
	SessionID string
	TeamID    string
	Reason    string
}

type CancelInput struct {
	SessionID string
	MessageID string
}

type StatusInput struct {
	SessionID string
	Status    string
	Note      string
}

type LogInput struct {
	SessionID string
	Since     string
	Limit     int
	InReplyTo string
}

type sessionRow struct {
	SessionID string
	TeamID    string
	AgentID   string
	Principal string
}

type recipient struct {
	AgentID string
}

func New(db *pgxpool.Pool, cfg config.Config) *Service {
	return &Service{
		db:         db,
		cfg:        cfg,
		rateWindow: map[string]rateState{},
	}
}

func (s *Service) RegisterAgent(ctx context.Context, p model.Principal, in RegisterInput) (map[string]any, *model.APIError) {
	in.TeamID = strings.TrimSpace(in.TeamID)
	in.AgentID = strings.TrimSpace(in.AgentID)
	if in.TeamID == "" || in.AgentID == "" {
		return nil, model.NewError("INVALID_INPUT", "team_id and agent_id are required", nil)
	}
	if p.TeamID != "" && p.TeamID != in.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match requested team", map[string]any{
			"expected_team":  p.TeamID,
			"requested_team": in.TeamID,
		})
	}

	now := time.Now().UTC()
	sessionID := uuid.NewString()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to begin transaction", map[string]any{"error": err.Error()})
	}
	defer tx.Rollback(ctx)

	tagsJSON, _ := json.Marshal(normalizeStringSlice(in.Tags))
	capsJSON, _ := json.Marshal(normalizeStringSlice(in.Capabilities))
	if _, err := tx.Exec(ctx, `
		INSERT INTO agents(team_id, agent_id, display_name, tags, capabilities, last_seen_at, registered_at, online)
		VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$6,true)
		ON CONFLICT (team_id,agent_id)
		DO UPDATE SET
			display_name = EXCLUDED.display_name,
			tags = EXCLUDED.tags,
			capabilities = EXCLUDED.capabilities,
			last_seen_at = EXCLUDED.last_seen_at,
			online = true`,
		in.TeamID, in.AgentID, strings.TrimSpace(in.DisplayName), string(tagsJSON), string(capsJSON), now); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to upsert agent", map[string]any{"error": err.Error()})
	}

	var activeSessionID string
	err = tx.QueryRow(ctx, `
		SELECT session_id
		FROM sessions
		WHERE team_id=$1 AND agent_id=$2 AND active=true
		FOR UPDATE`, in.TeamID, in.AgentID).Scan(&activeSessionID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, model.NewError("INTERNAL_ERROR", "failed to check active session", map[string]any{"error": err.Error()})
	}
	if activeSessionID != "" && !in.ReplaceExistingSession {
		return nil, model.NewError("SESSION_CONFLICT", "active session exists; set replace_existing_session=true to rotate", map[string]any{
			"session_id": activeSessionID,
		})
	}
	if activeSessionID != "" && in.ReplaceExistingSession {
		if _, err := tx.Exec(ctx, `
			UPDATE sessions SET active=false, invalidated_at=$1
			WHERE team_id=$2 AND agent_id=$3 AND active=true`,
			now, in.TeamID, in.AgentID); err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to invalidate old session", map[string]any{"error": err.Error()})
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sessions(session_id, team_id, agent_id, principal, active, created_at)
		VALUES($1,$2,$3,$4,true,$5)`,
		sessionID, in.TeamID, in.AgentID, p.Subject, now); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to create session", map[string]any{"error": err.Error()})
	}

	var mailboxSize int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int
		FROM deliveries
		WHERE team_id=$1 AND recipient_agent_id=$2 AND state IN ('PENDING','DELIVERED')`,
		in.TeamID, in.AgentID).Scan(&mailboxSize); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to compute mailbox size", map[string]any{"error": err.Error()})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to commit registration", map[string]any{"error": err.Error()})
	}

	return map[string]any{
		"session_id":           sessionID,
		"recommended_poll_ms":  s.cfg.DefaultPollMS,
		"server_time":          now.Format(time.RFC3339Nano),
		"mailbox_size":         mailboxSize,
		"protocol_version":     "mailbox-mcp/1.0",
		"inactivity_threshold": s.cfg.InactivityThreshold.String(),
	}, nil
}

func (s *Service) SendMessage(ctx context.Context, p model.Principal, in SendInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	if !s.takeRateLimit(sess.TeamID + ":" + sess.AgentID) {
		return nil, model.NewError("RATE_LIMITED", "sender exceeded per-minute rate limit", map[string]any{
			"retry_after_ms": 15000,
		})
	}

	if len(in.Body) == 0 {
		return nil, model.NewError("INVALID_INPUT", "body is required", nil)
	}
	if len(in.Body) > s.cfg.MaxBodyBytes {
		return nil, model.NewError("BODY_TOO_LARGE", "body exceeds maximum size", map[string]any{
			"max_bytes": s.cfg.MaxBodyBytes,
		})
	}
	if len(in.Attachments) > s.cfg.MaxAttachments {
		return nil, model.NewError("INVALID_INPUT", "too many attachments", map[string]any{
			"max_attachments": s.cfg.MaxAttachments,
		})
	}

	priority, err := model.ParsePriority(in.Priority)
	if err != nil {
		return nil, model.NewError("INVALID_PRIORITY", "invalid priority", map[string]any{"error": err.Error()})
	}
	readReceipt, err := model.NormalizeReadReceipt(in.ReadReceipt)
	if err != nil {
		return nil, model.NewError("INVALID_INPUT", "invalid read_receipt", map[string]any{"error": err.Error()})
	}
	if in.TTLSeconds <= 0 {
		in.TTLSeconds = s.cfg.DefaultTTLSeconds
	}
	if in.TTLSeconds > s.cfg.MaxTTLSeconds {
		in.TTLSeconds = s.cfg.MaxTTLSeconds
	}

	recipients, toType, toValue, aerr := s.resolveRecipients(ctx, sess.TeamID, sess.AgentID, in.To, in.IncludeSelf)
	if aerr != nil {
		return nil, aerr
	}
	if toType == "direct" {
		if !p.HasScope("send:any") && !p.HasScope("send:direct") {
			return nil, model.NewError("INVALID_INPUT", "missing send:direct scope", nil)
		}
	} else {
		if !p.HasScope("send:any") && !p.HasScope("send:broadcast") {
			return nil, model.NewError("INVALID_INPUT", "missing send:broadcast scope", nil)
		}
	}
	if len(recipients) > s.cfg.MaxBroadcastRecipients && toType != "direct" {
		return nil, model.NewError("BROADCAST_TOO_WIDE", "broadcast exceeds recipient limit", map[string]any{
			"max_recipients":     s.cfg.MaxBroadcastRecipients,
			"matched_recipients": len(recipients),
		})
	}
	if aerr := s.ensureMailboxCapacity(ctx, sess.TeamID, recipients); aerr != nil {
		return nil, aerr
	}

	now := time.Now().UTC()
	if in.IdempotencyKey != "" {
		var existing string
		var count int
		err := s.db.QueryRow(ctx, `
			SELECT m.message_id, COALESCE((SELECT count(*)::int FROM deliveries d WHERE d.message_id=m.message_id),0)
			FROM messages m
			WHERE m.team_id=$1 AND m.from_agent_id=$2 AND m.idempotency_key=$3`,
			sess.TeamID, sess.AgentID, in.IdempotencyKey).Scan(&existing, &count)
		if err == nil {
			return map[string]any{
				"message_id":      existing,
				"accepted_at":     now.Format(time.RFC3339Nano),
				"recipient_count": count,
				"deduplicated":    true,
			}, nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, model.NewError("INTERNAL_ERROR", "idempotency lookup failed", map[string]any{"error": err.Error()})
		}
	}

	msgID := newULID(now)
	attJSON, _ := json.Marshal(in.Attachments)
	expiresAt := now.Add(time.Duration(in.TTLSeconds) * time.Second)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to begin transaction", map[string]any{"error": err.Error()})
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO messages(
			message_id, team_id, from_agent_id, to_type, to_value, priority, topic, body, in_reply_to,
			attachments, require_ack, read_receipt, idempotency_key, created_at, ttl_seconds, expires_at, cancelled
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,false)`,
		msgID, sess.TeamID, sess.AgentID, toType, toValue, priority, strings.TrimSpace(in.Topic), in.Body,
		nullable(in.InReplyTo), string(attJSON), in.RequireAck, readReceipt, nullable(in.IdempotencyKey), now, in.TTLSeconds, expiresAt); err != nil {
		if in.IdempotencyKey != "" && isUniqueIdempotencyViolation(err) {
			var existing string
			var count int
			lookupErr := tx.QueryRow(ctx, `
				SELECT m.message_id, COALESCE((SELECT count(*)::int FROM deliveries d WHERE d.message_id=m.message_id),0)
				FROM messages m
				WHERE m.team_id=$1 AND m.from_agent_id=$2 AND m.idempotency_key=$3`,
				sess.TeamID, sess.AgentID, in.IdempotencyKey).Scan(&existing, &count)
			if lookupErr == nil {
				_ = tx.Rollback(ctx)
				return map[string]any{
					"message_id":      existing,
					"accepted_at":     now.Format(time.RFC3339Nano),
					"recipient_count": count,
					"deduplicated":    true,
				}, nil
			}
		}
		return nil, model.NewError("INTERNAL_ERROR", "failed to insert message", map[string]any{"error": err.Error()})
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events(event_id, team_id, message_id, recipient_agent_id, event_type, event_at, details)
		VALUES($1,$2,$3,NULL,'ACCEPTED',$4,'{}'::jsonb)`,
		uuid.NewString(), sess.TeamID, msgID, now); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to record message event", map[string]any{"error": err.Error()})
	}

	for _, r := range recipients {
		if _, err := tx.Exec(ctx, `
			INSERT INTO deliveries(message_id, team_id, recipient_agent_id, state, read_receipt)
			VALUES($1,$2,$3,'PENDING',$4)`,
			msgID, sess.TeamID, r.AgentID, readReceipt); err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to insert delivery", map[string]any{
				"error":              err.Error(),
				"recipient_agent_id": r.AgentID,
			})
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to commit message", map[string]any{"error": err.Error()})
	}

	return map[string]any{
		"message_id":      msgID,
		"accepted_at":     now.Format(time.RFC3339Nano),
		"recipient_count": len(recipients),
		"deduplicated":    false,
	}, nil
}

func (s *Service) PollInbox(ctx context.Context, p model.Principal, in PollInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	if !p.HasScope("*") && !p.HasScope("poll:self") {
		return nil, model.NewError("INVALID_INPUT", "missing poll:self scope", nil)
	}

	maxMessages := in.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 20
	}
	if maxMessages > 100 {
		maxMessages = 100
	}
	minPriority, err := model.ParsePriority(defaultPriority(in.MinPriority))
	if err != nil {
		return nil, model.NewError("INVALID_PRIORITY", "invalid min_priority", map[string]any{"error": err.Error()})
	}
	waitMS := in.WaitMS
	if waitMS < 0 {
		waitMS = 0
	}
	if waitMS > 25000 {
		waitMS = 25000
	}

	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	var rows []map[string]any
	var nextCursor string
	var snapshotAt time.Time
	for {
		rows, nextCursor, snapshotAt, aerr = s.fetchPollPage(ctx, sess, minPriority, maxMessages, in.Cursor)
		if aerr != nil {
			return nil, aerr
		}
		if len(rows) > 0 || waitMS == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	var hasUrgent bool
	if err := s.db.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM deliveries d
			JOIN messages m ON m.message_id=d.message_id
			WHERE d.team_id=$1
			  AND d.recipient_agent_id=$2
			  AND d.state IN ('PENDING','DELIVERED')
			  AND m.priority=3
			  AND m.expires_at > now()
			  AND m.cancelled=false
		)`,
		sess.TeamID, sess.AgentID).Scan(&hasUrgent); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to compute has_urgent", map[string]any{"error": err.Error()})
	}

	recommended := s.cfg.DefaultPollMS
	if hasUrgent {
		recommended = 250
	}

	return map[string]any{
		"messages":             rows,
		"next_cursor":          nullable(nextCursor),
		"snapshot_at":          snapshotAt.Format(time.RFC3339Nano),
		"has_urgent":           hasUrgent,
		"recommended_poll_ms":  recommended,
		"protocol_description": "snapshot-scoped keyset pagination",
	}, nil
}

func (s *Service) AckMessages(ctx context.Context, p model.Principal, in AckInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	if strings.ToUpper(strings.TrimSpace(in.AckKind)) != "" && strings.ToUpper(strings.TrimSpace(in.AckKind)) != "ACK" {
		return nil, model.NewError("INVALID_INPUT", "ack_kind must be ACK in v1", nil)
	}
	if len(in.MessageIDs) == 0 {
		return map[string]any{"acked": []string{}, "not_found": []string{}, "already_acked": []string{}}, nil
	}

	now := time.Now().UTC()
	acked := []string{}
	already := []string{}
	notFound := []string{}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to begin ack transaction", map[string]any{"error": err.Error()})
	}
	defer tx.Rollback(ctx)

	for _, id := range in.MessageIDs {
		var state string
		err := tx.QueryRow(ctx, `
			SELECT state FROM deliveries
			WHERE team_id=$1 AND recipient_agent_id=$2 AND message_id=$3`,
			sess.TeamID, sess.AgentID, id).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			notFound = append(notFound, id)
			continue
		}
		if err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to read delivery state", map[string]any{"error": err.Error()})
		}

		if state == model.DeliveryAcked {
			already = append(already, id)
			continue
		}
		if state == model.DeliveryPending {
			notFound = append(notFound, id)
			continue
		}
		if state == model.DeliveryCancelled || state == model.DeliveryExpired {
			notFound = append(notFound, id)
			continue
		}

		if _, err := tx.Exec(ctx, `
			UPDATE deliveries
			SET state='ACKED', acked_at=$1
			WHERE team_id=$2 AND recipient_agent_id=$3 AND message_id=$4`,
			now, sess.TeamID, sess.AgentID, id); err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to ack message", map[string]any{"error": err.Error()})
		}
		_, _ = tx.Exec(ctx, `
			INSERT INTO message_events(event_id, team_id, message_id, recipient_agent_id, event_type, event_at, details)
			VALUES($1,$2,$3,$4,'ACKED',$5,'{}'::jsonb)`,
			uuid.NewString(), sess.TeamID, id, sess.AgentID, now)
		acked = append(acked, id)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to commit ack transaction", map[string]any{"error": err.Error()})
	}
	return map[string]any{
		"acked":         acked,
		"not_found":     notFound,
		"already_acked": already,
	}, nil
}

func (s *Service) ListAgents(ctx context.Context, p model.Principal, in ListAgentsInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	if !p.HasScope("*") && !p.HasScope("list:agents") {
		return nil, model.NewError("INVALID_INPUT", "missing list:agents scope", nil)
	}

	args := []any{sess.TeamID}
	query := `
		SELECT agent_id, COALESCE(display_name,''), tags::text, capabilities::text, online, last_seen_at, COALESCE(status,'')
		FROM agents
		WHERE team_id=$1`
	if !in.IncludeOffline {
		query += ` AND online=true`
	}
	if strings.TrimSpace(in.FilterTag) != "" {
		args = append(args, in.FilterTag)
		query += fmt.Sprintf(` AND tags ? $%d`, len(args))
	}
	query += ` ORDER BY agent_id ASC`

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to list agents", map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	out := []map[string]any{}
	for rows.Next() {
		var (
			agentID, displayName, tagsRaw, capsRaw, status string
			online                                         bool
			lastSeen                                       time.Time
		)
		if err := rows.Scan(&agentID, &displayName, &tagsRaw, &capsRaw, &online, &lastSeen, &status); err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to scan agent row", map[string]any{"error": err.Error()})
		}
		var tags []string
		var caps []string
		_ = json.Unmarshal([]byte(tagsRaw), &tags)
		_ = json.Unmarshal([]byte(capsRaw), &caps)
		out = append(out, map[string]any{
			"agent_id":     agentID,
			"display_name": nullable(displayName),
			"tags":         tags,
			"capabilities": caps,
			"online":       online,
			"last_seen_at": lastSeen.Format(time.RFC3339Nano),
			"status":       nullable(status),
		})
	}
	return map[string]any{"agents": out}, nil
}

func (s *Service) DeregisterAgent(ctx context.Context, p model.Principal, in DeregisterInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, in.TeamID)
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	now := time.Now().UTC()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to begin deregister transaction", map[string]any{"error": err.Error()})
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET active=false, invalidated_at=$1
		WHERE session_id=$2`, now, sess.SessionID); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to invalidate session", map[string]any{"error": err.Error()})
	}
	if _, err := tx.Exec(ctx, `
		UPDATE agents
		SET online=false, status_note=$1
		WHERE team_id=$2 AND agent_id=$3`,
		nullable(in.Reason), sess.TeamID, sess.AgentID); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to set agent offline", map[string]any{"error": err.Error()})
	}
	var pending int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int
		FROM deliveries
		WHERE team_id=$1 AND recipient_agent_id=$2 AND state IN ('PENDING','DELIVERED')`,
		sess.TeamID, sess.AgentID).Scan(&pending); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to compute pending messages", map[string]any{"error": err.Error()})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to commit deregistration", map[string]any{"error": err.Error()})
	}

	return map[string]any{
		"agent_id":         sess.AgentID,
		"pending_messages": pending,
		"deregistered_at":  now.Format(time.RFC3339Nano),
	}, nil
}

func (s *Service) CancelMessage(ctx context.Context, p model.Principal, in CancelInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, "")
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}

	var teamID, fromAgent string
	err := s.db.QueryRow(ctx, `
		SELECT team_id, from_agent_id FROM messages WHERE message_id=$1`,
		in.MessageID).Scan(&teamID, &fromAgent)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[string]any{"status": "NOT_FOUND"}, nil
	}
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to fetch message", map[string]any{"error": err.Error()})
	}
	if teamID != sess.TeamID || fromAgent != sess.AgentID {
		return map[string]any{"status": "TOO_LATE"}, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to begin cancellation transaction", map[string]any{"error": err.Error()})
	}
	defer tx.Rollback(ctx)

	cmd, err := tx.Exec(ctx, `
		UPDATE deliveries
		SET state='CANCELLED'
		WHERE message_id=$1 AND state IN ('PENDING','DELIVERED')`,
		in.MessageID)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to cancel deliveries", map[string]any{"error": err.Error()})
	}
	cancelledCount := int(cmd.RowsAffected())

	var ackedCount int
	if err := tx.QueryRow(ctx, `SELECT count(*)::int FROM deliveries WHERE message_id=$1 AND state='ACKED'`, in.MessageID).Scan(&ackedCount); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to count acked deliveries", map[string]any{"error": err.Error()})
	}
	if cancelledCount > 0 {
		_, _ = tx.Exec(ctx, `UPDATE messages SET cancelled=true WHERE message_id=$1`, in.MessageID)
		_, _ = tx.Exec(ctx, `
			INSERT INTO message_events(event_id, team_id, message_id, recipient_agent_id, event_type, event_at, details)
			VALUES($1,$2,$3,NULL,'CANCELLED',$4,$5::jsonb)`,
			uuid.NewString(), teamID, in.MessageID, time.Now().UTC(), `{"scope":"bulk"}`)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to commit cancellation", map[string]any{"error": err.Error()})
	}

	status := "TOO_LATE"
	switch {
	case cancelledCount > 0 && ackedCount == 0:
		status = "CANCELLED"
	case cancelledCount > 0 && ackedCount > 0:
		status = "PARTIAL"
	}
	return map[string]any{
		"status":              status,
		"cancelled_count":     cancelledCount,
		"already_acked_count": ackedCount,
	}, nil
}

func (s *Service) SetAgentStatus(ctx context.Context, p model.Principal, in StatusInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, "")
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	status := strings.ToLower(strings.TrimSpace(in.Status))
	if status == "" {
		return nil, model.NewError("INVALID_INPUT", "status is required", nil)
	}
	switch status {
	case "idle", "busy", "blocked":
	default:
		return nil, model.NewError("INVALID_INPUT", "status must be one of idle|busy|blocked", nil)
	}
	if _, err := s.db.Exec(ctx, `
		UPDATE agents SET status=$1, status_note=$2, last_seen_at=$3, online=true
		WHERE team_id=$4 AND agent_id=$5`,
		status, nullable(in.Note), time.Now().UTC(), sess.TeamID, sess.AgentID); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to set status", map[string]any{"error": err.Error()})
	}
	return map[string]any{"ok": true}, nil
}

func (s *Service) GetMessageLog(ctx context.Context, p model.Principal, in LogInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, in.SessionID, "")
	if aerr != nil {
		return nil, aerr
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args := []any{sess.TeamID}
	query := `
		SELECT e.event_id, e.message_id, COALESCE(e.recipient_agent_id,''), e.event_type, e.event_at, e.details::text, COALESCE(m.in_reply_to,'')
		FROM message_events e
		LEFT JOIN messages m ON m.message_id=e.message_id
		WHERE e.team_id=$1`
	if strings.TrimSpace(in.Since) != "" {
		t, err := time.Parse(time.RFC3339, in.Since)
		if err != nil {
			return nil, model.NewError("INVALID_INPUT", "invalid since timestamp", map[string]any{"error": err.Error()})
		}
		args = append(args, t.UTC())
		query += fmt.Sprintf(` AND e.event_at >= $%d`, len(args))
	}
	if strings.TrimSpace(in.InReplyTo) != "" {
		args = append(args, in.InReplyTo)
		query += fmt.Sprintf(` AND m.in_reply_to = $%d`, len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY e.event_at DESC LIMIT $%d`, len(args))

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to read event log", map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	out := []map[string]any{}
	for rows.Next() {
		var eventID, messageID, recipient, eventType, detailsRaw, inReplyTo string
		var eventAt time.Time
		if err := rows.Scan(&eventID, &messageID, &recipient, &eventType, &eventAt, &detailsRaw, &inReplyTo); err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to scan event log", map[string]any{"error": err.Error()})
		}
		details := map[string]any{}
		_ = json.Unmarshal([]byte(detailsRaw), &details)
		out = append(out, map[string]any{
			"event_id":            eventID,
			"message_id":          messageID,
			"recipient_agent_id":  nullable(recipient),
			"event_type":          eventType,
			"event_at":            eventAt.Format(time.RFC3339Nano),
			"details":             details,
			"message_in_reply_to": nullable(inReplyTo),
		})
	}
	return map[string]any{"events": out}, nil
}

func (s *Service) SweepInactivity(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `
		UPDATE agents
		SET online=false
		WHERE online=true AND last_seen_at < now() - $1::interval`,
		fmt.Sprintf("%.0f seconds", s.cfg.InactivityThreshold.Seconds()))
	return err
}

func (s *Service) SweepExpiry(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE deliveries d
		SET state='EXPIRED'
		FROM messages m
		WHERE d.message_id=m.message_id
		  AND d.state IN ('PENDING','DELIVERED')
		  AND m.expires_at < now()
		RETURNING d.team_id, d.message_id, d.recipient_agent_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	now := time.Now().UTC()
	for rows.Next() {
		var teamID, messageID, recipient string
		if err := rows.Scan(&teamID, &messageID, &recipient); err != nil {
			return err
		}
		_, _ = tx.Exec(ctx, `
			INSERT INTO message_events(event_id, team_id, message_id, recipient_agent_id, event_type, event_at, details)
			VALUES($1,$2,$3,$4,'EXPIRED',$5,'{}'::jsonb)`,
			uuid.NewString(), teamID, messageID, recipient, now)
	}
	return tx.Commit(ctx)
}

func (s *Service) SweepRetention(ctx context.Context) error {
	interval := fmt.Sprintf("%.0f seconds", s.cfg.RetentionWindow.Seconds())
	_, err := s.db.Exec(ctx, `
		DELETE FROM deliveries
		WHERE message_id IN (
			SELECT message_id
			FROM messages
			WHERE expires_at < now() - $1::interval
		)`, interval)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `
		DELETE FROM messages
		WHERE expires_at < now() - $1::interval`, interval)
	return err
}

func (s *Service) requireSession(ctx context.Context, sessionID, explicitTeam string) (sessionRow, *model.APIError) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return sessionRow{}, model.NewError("INVALID_INPUT", "session_id is required", nil)
	}
	var sess sessionRow
	err := s.db.QueryRow(ctx, `
		SELECT session_id, team_id, agent_id, principal
		FROM sessions
		WHERE session_id=$1 AND active=true`, sessionID).
		Scan(&sess.SessionID, &sess.TeamID, &sess.AgentID, &sess.Principal)
	if errors.Is(err, pgx.ErrNoRows) {
		return sessionRow{}, model.NewError("SESSION_INVALID", "session_id not recognized", nil)
	}
	if err != nil {
		return sessionRow{}, model.NewError("INTERNAL_ERROR", "failed to load session", map[string]any{"error": err.Error()})
	}
	if explicitTeam != "" && explicitTeam != sess.TeamID {
		return sessionRow{}, model.NewError("TEAM_MISMATCH", "team_id does not match session team", map[string]any{
			"requested_team": explicitTeam,
			"session_team":   sess.TeamID,
		})
	}

	_, _ = s.db.Exec(ctx, `
		UPDATE agents
		SET last_seen_at=$1, online=true
		WHERE team_id=$2 AND agent_id=$3`,
		time.Now().UTC(), sess.TeamID, sess.AgentID)
	return sess, nil
}

func (s *Service) resolveRecipients(ctx context.Context, teamID, senderID string, to map[string]any, includeSelf bool) ([]recipient, string, string, *model.APIError) {
	toType := strings.TrimSpace(asString(to["type"]))
	switch toType {
	case "direct":
		agentID := strings.TrimSpace(asString(to["agent_id"]))
		if agentID == "" {
			return nil, "", "", model.NewError("INVALID_INPUT", "to.agent_id is required for direct messages", nil)
		}
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agents WHERE team_id=$1 AND agent_id=$2)`, teamID, agentID).Scan(&exists); err != nil {
			return nil, "", "", model.NewError("INTERNAL_ERROR", "failed to check direct recipient", map[string]any{"error": err.Error()})
		}
		if !exists {
			return nil, "", "", model.NewError("AGENT_NOT_FOUND", "direct recipient does not exist", map[string]any{"agent_id": agentID})
		}
		return []recipient{{AgentID: agentID}}, "direct", agentID, nil
	case "broadcast":
		scope := strings.TrimSpace(asString(to["scope"]))
		var (
			query string
			args  []any
		)
		switch scope {
		case "team":
			query = `SELECT agent_id FROM agents WHERE team_id=$1`
			args = []any{teamID}
		case "tag":
			tag := strings.TrimSpace(asString(to["tag"]))
			if tag == "" {
				return nil, "", "", model.NewError("INVALID_INPUT", "to.tag is required for tag broadcasts", nil)
			}
			query = `SELECT agent_id FROM agents WHERE team_id=$1 AND tags ? $2`
			args = []any{teamID, tag}
		default:
			return nil, "", "", model.NewError("INVALID_INPUT", "broadcast scope must be team or tag", nil)
		}
		rows, err := s.db.Query(ctx, query, args...)
		if err != nil {
			return nil, "", "", model.NewError("INTERNAL_ERROR", "failed to resolve broadcast recipients", map[string]any{"error": err.Error()})
		}
		defer rows.Close()
		out := []recipient{}
		for rows.Next() {
			var agentID string
			if err := rows.Scan(&agentID); err != nil {
				return nil, "", "", model.NewError("INTERNAL_ERROR", "failed to scan broadcast recipient", map[string]any{"error": err.Error()})
			}
			if !includeSelf && agentID == senderID {
				continue
			}
			out = append(out, recipient{AgentID: agentID})
		}
		toValue := ""
		if scope == "tag" {
			toValue = asString(to["tag"])
		}
		if scope == "team" {
			return out, "broadcast_team", toValue, nil
		}
		return out, "broadcast_tag", toValue, nil
	default:
		return nil, "", "", model.NewError("INVALID_INPUT", "to.type must be direct or broadcast", nil)
	}
}

func (s *Service) fetchPollPage(ctx context.Context, sess sessionRow, minPriority, max int, cursor string) ([]map[string]any, string, time.Time, *model.APIError) {
	snapshotAt := time.Now().UTC()
	lastPriority := 0
	lastCreated := time.Time{}
	lastMessageID := ""
	if strings.TrimSpace(cursor) != "" {
		c, err := model.DecodeCursor(cursor)
		if err != nil {
			return nil, "", time.Time{}, model.NewError("INVALID_INPUT", "invalid cursor", map[string]any{"error": err.Error()})
		}
		snapshotAt = c.SnapshotAt.UTC()
		lastPriority = c.LastPriority
		lastCreated = c.LastCreatedAt.UTC()
		lastMessageID = c.LastMessageID
	}

	query := `
		SELECT
			d.message_id,
			m.from_agent_id,
			m.priority,
			m.topic,
			m.body,
			COALESCE(m.in_reply_to,''),
			m.attachments::text,
			m.created_at,
			m.ttl_seconds,
			m.require_ack,
			m.read_receipt,
			m.to_type,
			COALESCE(m.to_value,''),
			d.state,
			d.delivered_at,
			d.acked_at
		FROM deliveries d
		JOIN messages m ON m.message_id=d.message_id
		WHERE d.team_id=$1
		  AND d.recipient_agent_id=$2
		  AND d.state IN ('PENDING','DELIVERED')
		  AND m.cancelled=false
		  AND m.expires_at > now()
		  AND m.priority >= $3
		  AND m.created_at <= $4`
	args := []any{sess.TeamID, sess.AgentID, minPriority, snapshotAt}
	if cursor != "" {
		query += `
		  AND (
			m.priority < $5
			OR (m.priority = $5 AND m.created_at > $6)
			OR (m.priority = $5 AND m.created_at = $6 AND m.message_id > $7)
		  )`
		args = append(args, lastPriority, lastCreated, lastMessageID)
	}
	query += ` ORDER BY m.priority DESC, m.created_at ASC, m.message_id ASC LIMIT $` + fmt.Sprint(len(args)+1)
	args = append(args, max+1)

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, "", time.Time{}, model.NewError("INTERNAL_ERROR", "failed to poll inbox", map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	type polled struct {
		MessageID   string
		FromAgentID string
		Priority    int
		Topic       string
		Body        string
		InReplyTo   string
		AttachRaw   string
		CreatedAt   time.Time
		TTLSeconds  int
		RequireAck  bool
		ReadReceipt string
		ToType      string
		ToValue     string
		State       string
		DeliveredAt *time.Time
		AckedAt     *time.Time
	}
	buf := []polled{}
	for rows.Next() {
		var x polled
		if err := rows.Scan(
			&x.MessageID, &x.FromAgentID, &x.Priority, &x.Topic, &x.Body, &x.InReplyTo,
			&x.AttachRaw, &x.CreatedAt, &x.TTLSeconds, &x.RequireAck, &x.ReadReceipt,
			&x.ToType, &x.ToValue, &x.State, &x.DeliveredAt, &x.AckedAt); err != nil {
			return nil, "", time.Time{}, model.NewError("INTERNAL_ERROR", "failed to decode poll row", map[string]any{"error": err.Error()})
		}
		buf = append(buf, x)
	}

	hasMore := len(buf) > max
	if hasMore {
		buf = buf[:max]
	}
	nextCursor := ""
	if hasMore {
		last := buf[len(buf)-1]
		cur, err := model.EncodeCursor(model.PollCursor{
			SnapshotAt:    snapshotAt,
			LastPriority:  last.Priority,
			LastCreatedAt: last.CreatedAt,
			LastMessageID: last.MessageID,
		})
		if err != nil {
			return nil, "", time.Time{}, model.NewError("INTERNAL_ERROR", "failed to encode next cursor", map[string]any{"error": err.Error()})
		}
		nextCursor = cur
	}

	now := time.Now().UTC()
	out := make([]map[string]any, 0, len(buf))
	for _, item := range buf {
		state := item.State
		deliveredAt := item.DeliveredAt
		ackedAt := item.AckedAt

		if item.State == model.DeliveryPending {
			state, deliveredAt, ackedAt = s.applyPendingDeliveryTransition(
				ctx, sess, item.MessageID, item.RequireAck, ackedAt, now,
			)
		}

		attachments := []map[string]any{}
		_ = json.Unmarshal([]byte(item.AttachRaw), &attachments)

		to := map[string]any{}
		switch item.ToType {
		case "direct":
			to["type"] = "direct"
			to["agent_id"] = item.ToValue
		case "broadcast_team":
			to["type"] = "broadcast"
			to["scope"] = "team"
		case "broadcast_tag":
			to["type"] = "broadcast"
			to["scope"] = "tag"
			to["tag"] = item.ToValue
		}

		out = append(out, map[string]any{
			"message_id":     item.MessageID,
			"team_id":        sess.TeamID,
			"from_agent_id":  item.FromAgentID,
			"to":             to,
			"priority":       item.Priority,
			"topic":          item.Topic,
			"body":           item.Body,
			"in_reply_to":    nullable(item.InReplyTo),
			"attachments":    attachments,
			"created_at":     item.CreatedAt.Format(time.RFC3339Nano),
			"ttl_seconds":    item.TTLSeconds,
			"delivery_state": state,
			"delivered_at":   toRFC3339(deliveredAt),
			"acked_at":       toRFC3339(ackedAt),
			"read_receipt":   item.ReadReceipt,
		})
	}

	return out, nextCursor, snapshotAt, nil
}

func (s *Service) ensureMailboxCapacity(ctx context.Context, teamID string, recipients []recipient) *model.APIError {
	if len(recipients) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(recipients))
	agentIDs := make([]string, 0, len(recipients))
	for _, r := range recipients {
		if _, ok := seen[r.AgentID]; ok {
			continue
		}
		seen[r.AgentID] = struct{}{}
		agentIDs = append(agentIDs, r.AgentID)
	}

	rows, err := s.db.Query(ctx, `
		SELECT recipient_agent_id, count(*)::int
		FROM deliveries
		WHERE team_id=$1
		  AND recipient_agent_id = ANY($2)
		  AND state IN ('PENDING','DELIVERED')
		GROUP BY recipient_agent_id`,
		teamID, agentIDs)
	if err != nil {
		return model.NewError("INTERNAL_ERROR", "failed to inspect mailbox depth", map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	counts := make(map[string]int, len(agentIDs))
	for rows.Next() {
		var agentID string
		var pending int
		if err := rows.Scan(&agentID, &pending); err != nil {
			return model.NewError("INTERNAL_ERROR", "failed to inspect mailbox depth", map[string]any{"error": err.Error()})
		}
		counts[agentID] = pending
	}

	for _, agentID := range agentIDs {
		if counts[agentID] >= s.cfg.MailboxCap {
			return model.NewError("MAILBOX_FULL", "recipient mailbox at capacity", map[string]any{
				"recipient_agent_id": agentID,
				"mailbox_cap":        s.cfg.MailboxCap,
			})
		}
	}

	return nil
}

func (s *Service) applyPendingDeliveryTransition(
	ctx context.Context,
	sess sessionRow,
	messageID string,
	requireAck bool,
	ackedAt *time.Time,
	now time.Time,
) (string, *time.Time, *time.Time) {
	if requireAck {
		deliveredAt := s.markDeliveredOnPoll(ctx, sess, messageID, now)
		return model.DeliveryDelivered, deliveredAt, ackedAt
	}
	deliveredAt, autoAckedAt := s.autoAckOnPoll(ctx, sess, messageID, now)
	return model.DeliveryAcked, deliveredAt, autoAckedAt
}

func (s *Service) markDeliveredOnPoll(ctx context.Context, sess sessionRow, messageID string, now time.Time) *time.Time {
	_, _ = s.db.Exec(ctx, `
		UPDATE deliveries
		SET state='DELIVERED', delivered_at=COALESCE(delivered_at,$1)
		WHERE message_id=$2 AND recipient_agent_id=$3 AND team_id=$4`,
		now, messageID, sess.AgentID, sess.TeamID)
	_, _ = s.db.Exec(ctx, `
		INSERT INTO message_events(event_id, team_id, message_id, recipient_agent_id, event_type, event_at, details)
		VALUES($1,$2,$3,$4,'DELIVERED',$5,'{}'::jsonb)`,
		uuid.NewString(), sess.TeamID, messageID, sess.AgentID, now)
	deliveredAt := now
	return &deliveredAt
}

func (s *Service) autoAckOnPoll(ctx context.Context, sess sessionRow, messageID string, now time.Time) (*time.Time, *time.Time) {
	_, _ = s.db.Exec(ctx, `
		UPDATE deliveries
		SET state='ACKED',
		    delivered_at=COALESCE(delivered_at,$1),
		    acked_at=COALESCE(acked_at,$1)
		WHERE message_id=$2 AND recipient_agent_id=$3 AND team_id=$4`,
		now, messageID, sess.AgentID, sess.TeamID)
	_, _ = s.db.Exec(ctx, `
		INSERT INTO message_events(event_id, team_id, message_id, recipient_agent_id, event_type, event_at, details)
		VALUES($1,$2,$3,$4,'DELIVERED',$5,'{}'::jsonb),($6,$2,$3,$4,'ACKED',$5,'{}'::jsonb)`,
		uuid.NewString(), sess.TeamID, messageID, sess.AgentID, now, uuid.NewString())
	deliveredAt := now
	ackedAt := now
	return &deliveredAt, &ackedAt
}

func (s *Service) takeRateLimit(key string) bool {
	s.rlMu.Lock()
	defer s.rlMu.Unlock()

	now := time.Now().UTC()
	b := s.rateWindow[key]
	if b.WindowStart.IsZero() || now.Sub(b.WindowStart) >= time.Minute {
		s.rateWindow[key] = rateState{WindowStart: now, Count: 1}
		return true
	}
	if b.Count >= s.cfg.RateLimitPerMinute {
		return false
	}
	b.Count++
	s.rateWindow[key] = b
	return true
}

func newULID(now time.Time) string {
	entropy := ulid.Monotonic(rand.Reader, 0)
	return ulid.MustNew(ulid.Timestamp(now), entropy).String()
}

func normalizeStringSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func defaultPriority(v any) any {
	if v == nil {
		return 0
	}
	return v
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func nullable(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func toRFC3339(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func isUniqueIdempotencyViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	if pgErr.Code != "23505" {
		return false
	}
	return strings.EqualFold(pgErr.ConstraintName, "idx_messages_idem")
}
