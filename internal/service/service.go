package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	db       *pgxpool.Pool
	cfg      config.Config
	notifier *Notifier
}

type RegisterInput struct {
	TeamID                 string   `json:"team_id"`
	AgentID                string   `json:"agent_id"`
	DisplayName            string   `json:"display_name"`
	Tags                   []string `json:"tags"`
	Capabilities           []string `json:"capabilities"`
	ReplaceExistingSession bool     `json:"replace_existing_session"`
}

type SendInput struct {
	SessionID      string           `json:"session_id"`
	TeamID         string           `json:"team_id"`
	To             map[string]any   `json:"to"`
	Priority       any              `json:"priority"`
	Topic          string           `json:"topic"`
	Body           string           `json:"body"`
	InReplyTo      string           `json:"in_reply_to"`
	Attachments    []map[string]any `json:"attachments"`
	TTLSeconds     int              `json:"ttl_seconds"`
	RequireAck     bool             `json:"require_ack"`
	ReadReceipt    string           `json:"read_receipt"`
	IncludeSelf    bool             `json:"include_self"`
	IdempotencyKey string           `json:"idempotency_key"`
}

type PollInput struct {
	SessionID   string `json:"session_id"`
	TeamID      string `json:"team_id"`
	MaxMessages int    `json:"max_messages"`
	MinPriority any    `json:"min_priority"`
	WaitMS      int    `json:"wait_ms"`
	Cursor      string `json:"cursor"`
}

type AckInput struct {
	SessionID  string   `json:"session_id"`
	TeamID     string   `json:"team_id"`
	MessageIDs []string `json:"message_ids"`
	AckKind    string   `json:"ack_kind"`
}

type ListAgentsInput struct {
	SessionID      string `json:"session_id"`
	TeamID         string `json:"team_id"`
	FilterTag      string `json:"filter_tag"`
	IncludeOffline bool   `json:"include_offline"`
}

type DeregisterInput struct {
	SessionID string `json:"session_id"`
	TeamID    string `json:"team_id"`
	Reason    string `json:"reason"`
}

type CancelInput struct {
	SessionID string `json:"session_id"`
	MessageID string `json:"message_id"`
}

type StatusInput struct {
	SessionID string `json:"session_id"`
	Status    string `json:"status"`
	Note      string `json:"note"`
}

type LogInput struct {
	SessionID string `json:"session_id"`
	Since     string `json:"since"`
	Limit     int    `json:"limit"`
	InReplyTo string `json:"in_reply_to"`
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

func New(db *pgxpool.Pool, cfg config.Config, notifier *Notifier) *Service {
	return &Service{
		db:       db,
		cfg:      cfg,
		notifier: notifier,
	}
}

func (s *Service) RegisterAgent(ctx context.Context, p model.Principal, in RegisterInput) (map[string]any, *model.APIError) {
	in.TeamID = strings.TrimSpace(in.TeamID)
	in.AgentID = strings.TrimSpace(in.AgentID)
	if p.TeamID == "" || p.Subject == "" {
		return nil, model.NewError("FORBIDDEN", "authenticated team and subject are required", nil)
	}
	if len(in.TeamID) > 128 || len(in.AgentID) > 128 || len(in.DisplayName) > 256 || len(in.Tags) > 32 || len(in.Capabilities) > 32 {
		return nil, model.NewError("INVALID_INPUT", "agent metadata exceeds limits", nil)
	}
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

	var existingOwner *string
	err = tx.QueryRow(ctx, `SELECT owner_subject FROM agents WHERE team_id=$1 AND agent_id=$2 FOR UPDATE`, in.TeamID, in.AgentID).Scan(&existingOwner)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, dbError("read agent owner", err)
	}
	if err == nil && (existingOwner == nil || *existingOwner != p.Subject) {
		if !p.HasScope("agents:takeover") || !in.ReplaceExistingSession {
			return nil, model.NewError("FORBIDDEN", "agent belongs to a different principal; explicit takeover permission and replacement are required", nil)
		}
	}
	// Insert first to serialize concurrent initial registration. The conditional conflict
	// update prevents a second principal from changing metadata when it loses the race.
	tagsJSON, _ := json.Marshal(normalizeStringSlice(in.Tags))
	capsJSON, _ := json.Marshal(normalizeStringSlice(in.Capabilities))
	if _, err := tx.Exec(ctx, `
		INSERT INTO agents(team_id, agent_id, display_name, tags, capabilities, last_seen_at, registered_at, online,owner_subject)
		VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$6,true,$7)
		ON CONFLICT (team_id,agent_id)
		DO UPDATE SET
			display_name = EXCLUDED.display_name,
			tags = EXCLUDED.tags,
			capabilities = EXCLUDED.capabilities,
			last_seen_at = EXCLUDED.last_seen_at,
			online = true, owner_subject=EXCLUDED.owner_subject
		WHERE agents.owner_subject=EXCLUDED.owner_subject OR $8`,
		in.TeamID, in.AgentID, strings.TrimSpace(in.DisplayName), string(tagsJSON), string(capsJSON), now, p.Subject, p.HasScope("agents:takeover") && in.ReplaceExistingSession); err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to upsert agent", map[string]any{"error": err.Error()})
	}

	var owner string
	if err := tx.QueryRow(ctx, `SELECT owner_subject FROM agents WHERE team_id=$1 AND agent_id=$2`, in.TeamID, in.AgentID).Scan(&owner); err != nil {
		return nil, dbError("verify owner", err)
	}
	if owner != p.Subject {
		return nil, model.NewError("FORBIDDEN", "agent belongs to a different principal", nil)
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
		return nil, model.NewError("SESSION_CONFLICT", "active session exists; set replace_existing_session=true to rotate", nil)
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
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "idx_sessions_one_active" {
			return nil, model.NewError("SESSION_CONFLICT", "active session exists; set replace_existing_session=true to rotate", nil)
		}
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

	if s.notifier != nil {
		s.notifier.EnsureTeam(ctx, in.TeamID)
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

func (s *Service) PollInbox(ctx context.Context, p model.Principal, in PollInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, in.TeamID)
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
	if in.Cursor != "" {
		waitMS = 0
	}
	if waitMS > 25000 {
		waitMS = 25000
	}

	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	var rows []map[string]any
	var nextCursor string
	var snapshotAt time.Time

	if s.notifier != nil {
		s.notifier.EnsureTeam(ctx, sess.TeamID)
	}

	for {
		var waitCh <-chan struct{}
		if s.notifier != nil && !s.notifier.IsFallback() {
			waitCh = s.notifier.WaitCh(sess.TeamID, sess.AgentID)
			defer s.notifier.ReleaseWait(sess.TeamID, sess.AgentID, waitCh)
		}

		sess, aerr = s.requireSession(ctx, p, in.SessionID, in.TeamID)
		if aerr != nil {
			return nil, aerr
		}
		rows, nextCursor, snapshotAt, aerr = s.fetchPollPage(ctx, sess, minPriority, maxMessages, in.Cursor)
		if aerr != nil {
			return nil, aerr
		}
		if len(rows) > 0 || waitMS == 0 || time.Now().After(deadline) {
			break
		}
		if waitCh != nil {
			waitCtx, waitCancel := context.WithDeadline(ctx, deadline)
			select {
			case <-waitCh:
			case <-time.After(time.Second):
			case <-waitCtx.Done():
			}
			waitCancel()
		} else {
			if err := sleepCtx(ctx, 200*time.Millisecond); err != nil {
				return nil, model.NewError("CANCELLED", "poll cancelled", nil)
			}
		}
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
			  AND EXISTS(SELECT 1 FROM agents a JOIN sessions s USING(team_id,agent_id) WHERE a.team_id=$1 AND a.agent_id=$2 AND a.owner_subject=$3 AND s.session_id=$4 AND s.active)
		)`,
		sess.TeamID, sess.AgentID, p.Subject, sess.SessionID).Scan(&hasUrgent); err != nil {
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

func (s *Service) PeekInbox(ctx context.Context, p model.Principal, sessionID string, minPriority int) ([]map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, sessionID, "")
	if aerr != nil {
		return nil, aerr
	}
	return s.PeekMailbox(ctx, p, sess.AgentID, minPriority)
}

func (s *Service) PeekMailbox(ctx context.Context, p model.Principal, agent string, minPriority int) ([]map[string]any, *model.APIError) {
	if aerr := s.AuthorizeMailbox(ctx, p, agent); aerr != nil {
		return nil, aerr
	}
	sess := sessionRow{TeamID: p.TeamID, AgentID: agent}

	const limit = 20
	rows, err := s.db.Query(ctx, `
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
		  AND EXISTS(SELECT 1 FROM agents a WHERE a.team_id=$1 AND a.agent_id=$2 AND a.owner_subject=$5)
		ORDER BY m.priority DESC, m.created_at ASC, m.message_id ASC
		LIMIT $4`,
		sess.TeamID, sess.AgentID, minPriority, limit, p.Subject)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to peek inbox", map[string]any{"error": err.Error()})
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var (
			messageID, fromAgentID, topic, body, inReplyTo string
			attachRaw, readReceipt, toType, toValue, state string
			priority, ttlSeconds                           int
			requireAck                                     bool
			createdAt                                      time.Time
			deliveredAt, ackedAt                           *time.Time
		)
		if err := rows.Scan(
			&messageID, &fromAgentID, &priority, &topic, &body, &inReplyTo,
			&attachRaw, &createdAt, &ttlSeconds, &requireAck, &readReceipt,
			&toType, &toValue, &state, &deliveredAt, &ackedAt); err != nil {
			return nil, model.NewError("INTERNAL_ERROR", "failed to scan peek row", map[string]any{"error": err.Error()})
		}

		attachments := []map[string]any{}
		_ = json.Unmarshal([]byte(attachRaw), &attachments)

		to := map[string]any{}
		switch toType {
		case "direct":
			to["type"] = "direct"
			to["agent_id"] = toValue
		case "broadcast_team":
			to["type"] = "broadcast"
			to["scope"] = "team"
		case "broadcast_tag":
			to["type"] = "broadcast"
			to["scope"] = "tag"
			to["tag"] = toValue
		}

		out = append(out, map[string]any{
			"message_id":     messageID,
			"team_id":        sess.TeamID,
			"from_agent_id":  fromAgentID,
			"to":             to,
			"priority":       priority,
			"topic":          topic,
			"body":           body,
			"in_reply_to":    nullable(inReplyTo),
			"attachments":    attachments,
			"created_at":     createdAt.Format(time.RFC3339Nano),
			"ttl_seconds":    ttlSeconds,
			"delivery_state": state,
			"delivered_at":   toRFC3339(deliveredAt),
			"acked_at":       toRFC3339(ackedAt),
			"read_receipt":   readReceipt,
			"require_ack":    requireAck,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("iterate inbox", err)
	}
	return out, nil
}

func (s *Service) ListAgents(ctx context.Context, p model.Principal, in ListAgentsInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, in.TeamID)
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
	return s.AgentDirectory(ctx, p, in)
}

// AgentDirectory permits observation without registering or replacing a session.
// A poll:self credential without list:agents sees only its own mailboxes.
func (s *Service) AgentDirectory(ctx context.Context, p model.Principal, in ListAgentsInput) (map[string]any, *model.APIError) {
	if p.TeamID == "" || p.Subject == "" || (!p.HasScope("list:agents") && !p.HasScope("poll:self")) {
		return nil, model.NewError("FORBIDDEN", "agent directory permission is required", nil)
	}
	args := []any{p.TeamID, p.Subject}
	query := `
		SELECT agent_id, COALESCE(display_name,''), tags::text, capabilities::text, online, last_seen_at, COALESCE(status,''), COALESCE(owner_subject=$2,false)
		FROM agents
		WHERE team_id=$1`
	if !p.HasScope("list:agents") {
		query += ` AND owner_subject=$2`
	}
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
			online, owned                                  bool
			lastSeen                                       time.Time
		)
		if err := rows.Scan(&agentID, &displayName, &tagsRaw, &capsRaw, &online, &lastSeen, &status, &owned); err != nil {
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
			"is_owned":     owned,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("iterate agents", err)
	}
	return map[string]any{"agents": out}, nil
}

func (s *Service) DeregisterAgent(ctx context.Context, p model.Principal, in DeregisterInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, in.TeamID)
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

	if err = lockMailboxes(ctx, tx, sess.TeamID, []string{sess.AgentID}); err != nil {
		return nil, dbError("lock deregistration", err)
	}
	if aerr = authorizeSessionTx(ctx, tx, sess); aerr != nil {
		return nil, aerr
	}
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

func (s *Service) SetAgentStatus(ctx context.Context, p model.Principal, in StatusInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, "")
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
	cmd, err := s.db.Exec(ctx, `
		UPDATE agents SET status=$1, status_note=$2, last_seen_at=$3, online=true
		WHERE team_id=$4 AND agent_id=$5 AND owner_subject=$6 AND EXISTS(SELECT 1 FROM sessions WHERE session_id=$7 AND active)`,
		status, nullable(in.Note), time.Now().UTC(), sess.TeamID, sess.AgentID, p.Subject, sess.SessionID)
	if err != nil {
		return nil, model.NewError("INTERNAL_ERROR", "failed to set status", map[string]any{"error": err.Error()})
	}
	if cmd.RowsAffected() == 0 {
		return nil, model.NewError("SESSION_INVALID", "session was invalidated", nil)
	}
	return map[string]any{"ok": true}, nil
}

func (s *Service) GetMessageLog(ctx context.Context, p model.Principal, in LogInput) (map[string]any, *model.APIError) {
	sess, aerr := s.requireSession(ctx, p, in.SessionID, "")
	if aerr != nil {
		return nil, aerr
	}
	if !p.HasScope("poll:self") {
		return nil, model.NewError("FORBIDDEN", "mailbox read permission is required", nil)
	}
	if p.TeamID != "" && p.TeamID != sess.TeamID {
		return nil, model.NewError("TEAM_MISMATCH", "token team does not match session team", map[string]any{
			"expected_team": p.TeamID,
			"session_team":  sess.TeamID,
		})
	}
	return s.mailboxLog(ctx, p, sess.AgentID, sess.SessionID, in)
}

// PeekMailboxLog reads lifecycle metadata only, scoped to the owned mailbox.
func (s *Service) PeekMailboxLog(ctx context.Context, p model.Principal, agent string, in LogInput) (map[string]any, *model.APIError) {
	if err := s.AuthorizeMailbox(ctx, p, agent); err != nil {
		return nil, err
	}
	return s.mailboxLog(ctx, p, agent, "", in)
}

func (s *Service) mailboxLog(ctx context.Context, p model.Principal, agent, sessionID string, in LogInput) (map[string]any, *model.APIError) {
	limit := in.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args := []any{p.TeamID, agent, p.Subject, sessionID}
	query := `
		SELECT e.event_id, e.message_id, COALESCE(e.recipient_agent_id,''), e.event_type, e.event_at, e.details::text, COALESCE(m.in_reply_to,'')
		FROM message_events e
		LEFT JOIN messages m ON m.message_id=e.message_id
		WHERE e.team_id=$1 AND (m.from_agent_id=$2 OR e.recipient_agent_id=$2 OR EXISTS(SELECT 1 FROM deliveries d WHERE d.message_id=e.message_id AND d.recipient_agent_id=$2 AND d.team_id=$1)) AND EXISTS(SELECT 1 FROM agents a WHERE a.team_id=$1 AND a.agent_id=$2 AND a.owner_subject=$3 AND ($4='' OR EXISTS(SELECT 1 FROM sessions s WHERE s.team_id=$1 AND s.agent_id=$2 AND s.session_id=$4 AND s.active)))`
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
	if err := rows.Err(); err != nil {
		return nil, dbError("iterate audit log", err)
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

func (s *Service) SweepRetention(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var acquired bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(840174252)`).Scan(&acquired); err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	interval := fmt.Sprintf("%.0f seconds", s.cfg.RetentionWindow.Seconds())
	for _, query := range []string{
		`WITH floors AS (SELECT team_id,agent_id,max(revision) AS floor FROM mailbox_events WHERE occurred_at<now()-$1::interval GROUP BY team_id,agent_id) UPDATE agents a SET replay_floor=GREATEST(a.replay_floor,f.floor) FROM floors f WHERE a.team_id=f.team_id AND a.agent_id=f.agent_id`,
		`DELETE FROM mailbox_events WHERE occurred_at<now()-$1::interval`,
		`DELETE FROM message_events WHERE event_at<now()-$1::interval`,
		`DELETE FROM deliveries WHERE message_id IN(SELECT message_id FROM messages WHERE expires_at<now()-$1::interval)`,
		`DELETE FROM messages WHERE expires_at<now()-$1::interval`,
		`DELETE FROM event_subscriptions WHERE expires_at<now()-$1::interval`,
		`DELETE FROM sender_rates WHERE window_start<now()-$1::interval`,
	} {
		if _, err = tx.Exec(ctx, query, interval); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Service) resolveRecipients(ctx context.Context, tx pgx.Tx, teamID, senderID string, to map[string]any, includeSelf bool) ([]recipient, string, string, *model.APIError) {
	toType := strings.TrimSpace(asString(to["type"]))
	switch toType {
	case "direct":
		agentID := strings.TrimSpace(asString(to["agent_id"]))
		if agentID == "" {
			return nil, "", "", model.NewError("INVALID_INPUT", "to.agent_id is required for direct messages", nil)
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agents WHERE team_id=$1 AND agent_id=$2)`, teamID, agentID).Scan(&exists); err != nil {
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
		rows, err := tx.Query(ctx, query, args...)
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
		if err := rows.Err(); err != nil {
			return nil, "", "", dbError("iterate recipients", err)
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
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, "", time.Time{}, dbError("begin poll", err)
	}
	defer tx.Rollback(ctx)
	if err = lockMailboxes(ctx, tx, sess.TeamID, []string{sess.AgentID}); err != nil {
		return nil, "", time.Time{}, dbError("lock inbox", err)
	}
	if aerr := authorizeSessionTx(ctx, tx, sess); aerr != nil {
		return nil, "", time.Time{}, aerr
	}

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

	rows, err := tx.Query(ctx, query, args...)
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

	if err := rows.Err(); err != nil {
		return nil, "", time.Time{}, dbError("iterate poll page", err)
	}
	rows.Close()
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
			var apiErr *model.APIError
			state, deliveredAt, ackedAt, apiErr = transitionOnPoll(ctx, tx, sess, item.MessageID, item.RequireAck, now)
			if apiErr != nil {
				return nil, "", time.Time{}, apiErr
			}
			if state == "" {
				continue
			}
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
			"require_ack":    item.RequireAck,
		})
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, "", time.Time{}, dbError("commit poll", err)
	}
	return out, nextCursor, snapshotAt, nil
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
