package events

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mihir/msg-com/internal/model"
)

const EventName = "mailbox.message.created"

type AuthorizeFunc func(context.Context, model.Principal, string) *model.APIError
type PrincipalFunc func(string, string, string) (model.Principal, bool)
type Service struct {
	db             *pgxpool.Pool
	key            []byte
	client         *http.Client
	authorize      AuthorizeFunc
	principal      PrincipalFunc
	heartbeat      atomic.Int64
	verificationMu sync.Mutex
	verified       map[[32]byte]time.Time
}

func New(db *pgxpool.Pool, key []byte, authorize AuthorizeFunc, principal PrincipalFunc) (*Service, error) {
	if len(key) != 32 {
		return nil, errors.New("event encryption key must be 32 bytes")
	}
	return &Service{db: db, key: append([]byte(nil), key...), client: safeHTTPClient(), authorize: authorize, principal: principal, verified: map[[32]byte]time.Time{}}, nil
}

type Delivery struct {
	Mode   string `json:"mode"`
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
}
type SubscribeInput struct {
	Name      string          `json:"name"`
	Arguments map[string]any  `json:"arguments"`
	Delivery  Delivery        `json:"delivery"`
	Cursor    *string         `json:"cursor,omitempty"`
	TTLMS     json.RawMessage `json:"ttlMs,omitempty"`
}
type SubscriptionResult struct {
	ID            string `json:"id"`
	RefreshBefore string `json:"refreshBefore"`
	Cursor        string `json:"cursor"`
	Truncated     bool   `json:"truncated"`
}
type Filters struct {
	AgentID     string `json:"agent_id"`
	MinPriority int    `json:"min_priority"`
	Topic       string `json:"topic,omitempty"`
}
type ProtocolError struct {
	Code    int64
	Message string
	Reason  string
}

func (e *ProtocolError) Error() string { return e.Message }
func invalid(message string) error     { return &ProtocolError{-32602, message, "invalid_params"} }
func denied() error                    { return &ProtocolError{-32024, "not authorized for this mailbox", "forbidden"} }

func Catalog() []map[string]any {
	return []map[string]any{{"name": EventName, "description": "A message arrived in the specified agent mailbox. Fetch the inbox to read and acknowledge it.", "delivery": []string{"webhook"}, "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"agent_id": map[string]any{"type": "string", "minLength": 1, "maxLength": 128}, "min_priority": map[string]any{"type": "integer", "minimum": 0, "maximum": 3}, "topic": map[string]any{"type": "string", "maxLength": 512}}, "required": []string{"agent_id"}, "additionalProperties": false}, "payloadSchema": map[string]any{"type": "object", "properties": map[string]any{"message_id": map[string]any{"type": "string"}, "agent_id": map[string]any{"type": "string"}, "from_agent_id": map[string]any{"type": "string"}, "priority": map[string]any{"type": "integer"}, "topic": map[string]any{"type": "string"}, "expires_at": map[string]any{"type": "string", "format": "date-time"}}, "required": []string{"message_id", "agent_id", "from_agent_id", "priority", "topic", "expires_at"}, "additionalProperties": false}}}
}

func filters(in SubscribeInput) (Filters, error) {
	if in.Name != EventName {
		return Filters{}, &ProtocolError{-32023, "unknown event", "event"}
	}
	if in.Delivery.Mode != "webhook" {
		return Filters{}, &ProtocolError{-32026, "only webhook delivery is supported", "deliveryMode"}
	}
	if err := validateURL(in.Delivery.URL); err != nil {
		return Filters{}, invalid(err.Error())
	}
	for key := range in.Arguments {
		if key != "agent_id" && key != "min_priority" && key != "topic" {
			return Filters{}, invalid("unknown event filter")
		}
	}
	agent, ok := in.Arguments["agent_id"].(string)
	if !ok || strings.TrimSpace(agent) == "" || len(agent) > 128 {
		return Filters{}, invalid("agent_id is required")
	}
	f := Filters{AgentID: agent}
	if value, ok := in.Arguments["min_priority"]; ok {
		priority, err := model.ParsePriority(value)
		if err != nil {
			return f, invalid("min_priority must be an integer from 0 to 3")
		}
		if _, isString := value.(string); isString {
			return f, invalid("min_priority must be an integer")
		}
		f.MinPriority = priority
	}
	if value, ok := in.Arguments["topic"]; ok {
		topic, ok := value.(string)
		if !ok || len(topic) > 512 {
			return f, invalid("invalid topic")
		}
		f.Topic = topic
	}
	return f, nil
}

func subscriptionID(p model.Principal, in SubscribeInput, f Filters) string {
	identity, _ := json.Marshal([]any{p.TeamID, p.Subject, in.Delivery.URL, in.Name, f})
	sum := sha256.Sum256(identity)
	return "sub_" + hex.EncodeToString(sum[:])
}

func (s *Service) cursor(team, agent string, revision int64) string {
	payload, _ := json.Marshal([]any{team, agent, revision})
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func (s *Service) parseCursor(cursor, team, agent string) (int64, error) {
	parts := strings.Split(cursor, ".")
	if len(parts) != 2 {
		return 0, invalid("invalid event cursor")
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(parts[0]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		return 0, invalid("invalid event cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, invalid("invalid event cursor")
	}
	var values []json.RawMessage
	if json.Unmarshal(payload, &values) != nil || len(values) != 3 {
		return 0, invalid("invalid event cursor")
	}
	var t, a string
	var revision int64
	if json.Unmarshal(values[0], &t) != nil || json.Unmarshal(values[1], &a) != nil || json.Unmarshal(values[2], &revision) != nil || t != team || a != agent || revision < 0 {
		return 0, invalid("cursor does not belong to this mailbox")
	}
	return revision, nil
}

func (s *Service) Subscribe(ctx context.Context, p model.Principal, in SubscribeInput) (SubscriptionResult, error) {
	f, err := filters(in)
	if err != nil {
		return SubscriptionResult{}, err
	}
	if p.Token == "" || s.authorize(ctx, p, f.AgentID) != nil {
		return SubscriptionResult{}, denied()
	}
	key, err := signingKey(in.Delivery.Secret)
	if err != nil {
		return SubscriptionResult{}, invalid(err.Error())
	}
	lifetime := time.Hour
	if len(in.TTLMS) > 0 && string(in.TTLMS) != "null" {
		var ms int64
		if json.Unmarshal(in.TTLMS, &ms) != nil || ms <= 0 {
			return SubscriptionResult{}, invalid("ttlMs must be positive milliseconds or null")
		}
		if ms > 24*60*60*1000 {
			ms = 24 * 60 * 60 * 1000
		}
		if ms < 60*1000 {
			ms = 60 * 1000
		}
		lifetime = time.Duration(ms) * time.Millisecond
	}
	id := subscriptionID(p, in, f)
	if err = s.verifyFor(ctx, p, in.Delivery.URL, id, key); err != nil {
		return SubscriptionResult{}, &ProtocolError{-32015, "callback verification failed", "challenge_failed"}
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return SubscriptionResult{}, err
	}
	defer tx.Rollback(ctx)
	// Serialize owner quota checks and mailbox watermark capture.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,1))`, p.TeamID+":"+p.Subject); err != nil {
		return SubscriptionResult{}, err
	}
	var revision, floor int64
	var owner *string
	err = tx.QueryRow(ctx, `SELECT inbox_revision,replay_floor,owner_subject FROM agents WHERE team_id=$1 AND agent_id=$2 FOR UPDATE`, p.TeamID, f.AgentID).Scan(&revision, &floor, &owner)
	if err != nil {
		return SubscriptionResult{}, err
	}
	if owner == nil || *owner != p.Subject {
		return SubscriptionResult{}, denied()
	}
	start := revision
	truncated := false
	if in.Cursor != nil {
		start, err = s.parseCursor(*in.Cursor, p.TeamID, f.AgentID)
		if err != nil {
			return SubscriptionResult{}, err
		}
		if start > revision {
			return SubscriptionResult{}, invalid("cursor is ahead of mailbox")
		}
		if start < floor {
			start = floor
			truncated = true
		}
	}
	var oldEncrypted []byte
	var previousCursor int64
	var wasActive bool
	err = tx.QueryRow(ctx, `SELECT secret,cursor,active AND expires_at>now() FROM event_subscriptions WHERE id=$1 FOR UPDATE`, id).Scan(&oldEncrypted, &previousCursor, &wasActive)
	if err == nil && in.Cursor == nil {
		start = previousCursor
		if start < floor {
			start = floor
			truncated = true
		}
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return SubscriptionResult{}, err
	}
	if err == nil && in.Cursor != nil && start > previousCursor {
		var gap bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mailbox_events e WHERE e.team_id=$1 AND e.agent_id=$2 AND e.name=$3 AND e.revision>$4 AND e.revision<=$5 AND (e.data->>'priority')::int>=$6 AND ($7='' OR e.data->>'topic'=$7) AND NOT EXISTS(SELECT 1 FROM event_deliveries d WHERE d.subscription_id=$8 AND d.event_id=e.event_id AND d.state='ACCEPTED'))`, p.TeamID, f.AgentID, in.Name, previousCursor, start, f.MinPriority, f.Topic, id).Scan(&gap); err != nil {
			return SubscriptionResult{}, err
		}
		if gap {
			return SubscriptionResult{}, invalid("cursor would skip undelivered events")
		}
	}
	if !wasActive {
		var count int
		if err = tx.QueryRow(ctx, `SELECT count(*)::int FROM event_subscriptions WHERE team_id=$1 AND owner_subject=$2 AND active AND expires_at>now()`, p.TeamID, p.Subject).Scan(&count); err != nil {
			return SubscriptionResult{}, err
		}
		if count >= 100 {
			return SubscriptionResult{}, &ProtocolError{-32025, "subscription limit reached", "subscriptions"}
		}
	}
	encrypted, err := encrypt(s.key, key)
	if err != nil {
		return SubscriptionResult{}, err
	}
	var old []byte
	var rotate *time.Time
	if len(oldEncrypted) > 0 {
		oldKey, err := decrypt(s.key, oldEncrypted)
		if err != nil {
			return SubscriptionResult{}, err
		}
		if !hmac.Equal(oldKey, key) {
			old = oldEncrypted
			until := time.Now().UTC().Add(5 * time.Minute)
			rotate = &until
		}
	}
	args, _ := json.Marshal(f)
	expiry := time.Now().UTC().Add(lifetime)
	_, err = tx.Exec(ctx, `INSERT INTO event_subscriptions(id,team_id,agent_id,owner_subject,token_id,name,arguments,callback_url,secret,previous_secret,rotate_until,expires_at,cursor,active) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,true) ON CONFLICT(id) DO UPDATE SET token_id=EXCLUDED.token_id,secret=EXCLUDED.secret,previous_secret=CASE WHEN EXCLUDED.previous_secret IS NOT NULL THEN EXCLUDED.previous_secret ELSE event_subscriptions.previous_secret END,rotate_until=CASE WHEN EXCLUDED.previous_secret IS NOT NULL THEN EXCLUDED.rotate_until ELSE event_subscriptions.rotate_until END,expires_at=EXCLUDED.expires_at,cursor=EXCLUDED.cursor,active=true`, id, p.TeamID, f.AgentID, p.Subject, p.Token, in.Name, string(args), in.Delivery.URL, encrypted, old, rotate, expiry, start)
	if err != nil {
		return SubscriptionResult{}, err
	}
	// A replay request may intentionally revisit previously accepted events.
	if in.Cursor != nil {
		if _, err = tx.Exec(ctx, `UPDATE event_deliveries d SET state='PENDING',attempts=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,accepted_at=NULL FROM mailbox_events e WHERE d.subscription_id=$1 AND d.event_id=e.event_id AND e.revision>$2`, id, start); err != nil {
			return SubscriptionResult{}, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE event_deliveries SET state='PENDING',attempts=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error=NULL WHERE subscription_id=$1 AND state='FAILED'`, id); err != nil {
		return SubscriptionResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return SubscriptionResult{}, err
	}
	return SubscriptionResult{ID: id, RefreshBefore: expiry.Format(time.RFC3339Nano), Cursor: s.cursor(p.TeamID, f.AgentID, start), Truncated: truncated}, nil
}

func (s *Service) Unsubscribe(ctx context.Context, p model.Principal, in SubscribeInput) error {
	f, err := filters(in)
	if err != nil {
		return err
	}
	if s.authorize(ctx, p, f.AgentID) != nil {
		return denied()
	}
	id := subscriptionID(p, in, f)
	_, err = s.db.Exec(ctx, `UPDATE event_subscriptions SET active=false WHERE id=$1 AND team_id=$2 AND owner_subject=$3`, id, p.TeamID, p.Subject)
	return err
}

// Diagnostic returns owner-scoped delivery state without secrets or callbacks.
func (s *Service) Diagnostic(ctx context.Context, p model.Principal, agent string) ([]map[string]any, error) {
	if s.authorize(ctx, p, agent) != nil {
		return nil, denied()
	}
	rows, err := s.db.Query(ctx, `SELECT s.id,s.active,s.expires_at,s.cursor,count(d.event_id) FILTER(WHERE d.state='PENDING'),count(d.event_id) FILTER(WHERE d.state='FAILED') FROM event_subscriptions s LEFT JOIN event_deliveries d ON d.subscription_id=s.id WHERE s.team_id=$1 AND s.owner_subject=$2 AND s.agent_id=$3 GROUP BY s.id ORDER BY s.id`, p.TeamID, p.Subject, agent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id string
		var active bool
		var expiry time.Time
		var cursor, pending, failed int64
		if err = rows.Scan(&id, &active, &expiry, &cursor, &pending, &failed); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "active": active, "expires_at": expiry.Format(time.RFC3339Nano), "cursor": s.cursor(p.TeamID, agent, cursor), "pending": pending, "failed": failed})
	}
	return out, rows.Err()
}
