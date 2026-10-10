package service

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/testutil"
)

type urgencyTracer struct {
	ready, resume chan struct{}
	once          atomic.Bool
}

func (tr *urgencyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "AND m.priority=3") && tr.once.CompareAndSwap(false, true) {
		close(tr.ready)
		select {
		case <-tr.resume:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*urgencyTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestMailboxIntegration(t *testing.T) {
	db := testutil.Database(t)
	ctx := context.Background()
	cfg := config.Config{DefaultPollMS: 10, MaxBodyBytes: 32768, MaxAttachments: 8, MaxBroadcastRecipients: 200, MailboxCap: 100, RateLimitPerMinute: 1000, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400, RetentionWindow: 24 * time.Hour, InactivityThreshold: time.Minute}
	svc := New(db, cfg, nil)
	principal := func(subject string) model.Principal {
		return model.Principal{TeamID: "team", Subject: subject, Token: subject + "-token", Scopes: map[string]bool{"send:direct": true, "send:broadcast": true, "poll:self": true, "list:agents": true}}
	}
	alice, bob := principal("alice"), principal("bob")
	register := func(t *testing.T, p model.Principal, agent string) string {
		t.Helper()
		out, err := svc.RegisterAgent(ctx, p, RegisterInput{TeamID: p.TeamID, AgentID: agent})
		if err != nil {
			t.Fatal(err)
		}
		return out["session_id"].(string)
	}
	send := func(t *testing.T, p model.Principal, session, agent, key string) string {
		t.Helper()
		out, err := svc.SendMessage(ctx, p, SendInput{SessionID: session, To: map[string]any{"type": "direct", "agent_id": agent}, Priority: 1, Topic: "test", Body: "message", RequireAck: true, IdempotencyKey: key})
		if err != nil {
			t.Fatal(err)
		}
		return out["message_id"].(string)
	}
	t.Run("borrowed sessions have no access or presence side effects", func(t *testing.T) {
		session := register(t, alice, "private")
		sender := register(t, bob, "sender")
		message := send(t, bob, sender, "private", "private-message")
		before := time.Now().UTC().Add(-time.Hour)
		if _, err := db.Exec(ctx, `UPDATE agents SET online=false,last_seen_at=$1 WHERE agent_id='private'`, before); err != nil {
			t.Fatal(err)
		}
		for _, attacker := range []model.Principal{bob, {TeamID: "other", Subject: alice.Subject, Scopes: map[string]bool{"*": true}}, {TeamID: "team", Subject: "", Scopes: map[string]bool{"*": true}}} {
			calls := map[string]func() *model.APIError{
				"send": func() *model.APIError {
					_, e := svc.SendMessage(ctx, attacker, SendInput{SessionID: session})
					return e
				},
				"poll": func() *model.APIError { _, e := svc.PollInbox(ctx, attacker, PollInput{SessionID: session}); return e },
				"peek": func() *model.APIError { _, e := svc.PeekInbox(ctx, attacker, session, 0); return e },
				"ack": func() *model.APIError {
					_, e := svc.AckMessages(ctx, attacker, AckInput{SessionID: session, MessageIDs: []string{message}})
					return e
				},
				"list": func() *model.APIError {
					_, e := svc.ListAgents(ctx, attacker, ListAgentsInput{SessionID: session})
					return e
				},
				"deregister": func() *model.APIError {
					_, e := svc.DeregisterAgent(ctx, attacker, DeregisterInput{SessionID: session})
					return e
				},
				"cancel": func() *model.APIError {
					_, e := svc.CancelMessage(ctx, attacker, CancelInput{SessionID: session, MessageID: message})
					return e
				},
				"status": func() *model.APIError {
					_, e := svc.SetAgentStatus(ctx, attacker, StatusInput{SessionID: session, Status: "busy"})
					return e
				},
				"log": func() *model.APIError {
					_, e := svc.GetMessageLog(ctx, attacker, LogInput{SessionID: session})
					return e
				},
			}
			for name, call := range calls {
				if err := call(); err == nil {
					t.Fatalf("%s accepted borrowed session", name)
				}
			}
		}
		var online bool
		var seen time.Time
		if err := db.QueryRow(ctx, `SELECT online,last_seen_at FROM agents WHERE agent_id='private'`).Scan(&online, &seen); err != nil {
			t.Fatal(err)
		}
		if online || seen.Sub(before).Abs() > time.Microsecond {
			t.Fatal("denied caller altered presence")
		}
		rotated := alice
		rotated.Token = "rotated-credential"
		if _, err := svc.PeekInbox(ctx, rotated, session, 0); err != nil {
			t.Fatalf("same subject credential rotation: %v", err)
		}
		if _, err := svc.RegisterAgent(ctx, bob, RegisterInput{TeamID: "team", AgentID: "private", ReplaceExistingSession: true}); err == nil || err.Details != nil {
			t.Fatal("unauthorized registration disclosed state or succeeded")
		}
		if _, err := svc.DeregisterAgent(ctx, alice, DeregisterInput{SessionID: session}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.RegisterAgent(ctx, bob, RegisterInput{TeamID: "team", AgentID: "private"}); err == nil {
			t.Fatal("inactive mailbox lost ownership")
		}
		if _, err := svc.RegisterAgent(ctx, rotated, RegisterInput{TeamID: "team", AgentID: "private"}); err != nil {
			t.Fatal("owner cannot resume", err)
		}
	})
	t.Run("takeover invalidates a waiting poll", func(t *testing.T) {
		presenceSession := register(t, alice, "presence")
		db.Exec(ctx, `UPDATE agents SET last_seen_at=now()-interval '2 hours',online=false WHERE agent_id='presence'`)
		if _, err := svc.PollInbox(ctx, alice, PollInput{SessionID: presenceSession}); err != nil {
			t.Fatal(err)
		}
		if err := svc.SweepInactivity(ctx); err != nil {
			t.Fatal(err)
		}
		var online bool
		db.QueryRow(ctx, `SELECT online FROM agents WHERE agent_id='presence'`).Scan(&online)
		if !online {
			t.Fatal("legitimate polling failed to refresh presence")
		}
		session := register(t, alice, "transfer")
		done := make(chan *model.APIError, 1)
		go func() { _, err := svc.PollInbox(ctx, alice, PollInput{SessionID: session, WaitMS: 4000}); done <- err }()
		time.Sleep(100 * time.Millisecond)
		admin := bob
		admin.Scopes = map[string]bool{"*": true}
		out, err := svc.RegisterAgent(ctx, admin, RegisterInput{TeamID: "team", AgentID: "transfer", ReplaceExistingSession: true})
		if err != nil {
			t.Fatal(err)
		}
		send(t, admin, out["session_id"].(string), "transfer", "after-transfer")
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("old waiting session saw transferred mailbox")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("old poll did not terminate")
		}
		if _, err := svc.PeekMailbox(ctx, alice, "transfer", 0); err == nil {
			t.Fatal("old owner retained resource access")
		}
	})
	t.Run("concurrent idempotency and capacity are transactional", func(t *testing.T) {
		sender := register(t, alice, "dedup-sender")
		register(t, bob, "dedup-recipient")
		in := SendInput{SessionID: sender, To: map[string]any{"type": "direct", "agent_id": "dedup-recipient"}, Priority: 1, Topic: "test", Body: "same", RequireAck: true, IdempotencyKey: "retry"}
		results := make(chan map[string]any, 20)
		errs := make(chan *model.APIError, 20)
		var wg sync.WaitGroup
		for range 20 {
			wg.Add(1)
			go func() { defer wg.Done(); out, err := svc.SendMessage(ctx, alice, in); results <- out; errs <- err }()
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		var id, accepted string
		for out := range results {
			if id == "" {
				id = out["message_id"].(string)
				accepted = out["accepted_at"].(string)
			}
			if out["message_id"] != id || out["accepted_at"] != accepted {
				t.Fatal("retry changed identity or acceptance time")
			}
		}
		var count int
		db.QueryRow(ctx, `SELECT count(*) FROM mailbox_events WHERE message_id=$1`, id).Scan(&count)
		if count != 1 {
			t.Fatalf("duplicate arrival events: %d", count)
		}
		in.Body = "changed"
		if _, err := svc.SendMessage(ctx, alice, in); err == nil || err.Code != "IDEMPOTENCY_CONFLICT" {
			t.Fatal("changed content accepted", err)
		}
		capCfg := cfg
		capCfg.MailboxCap = 1
		capService := New(db, capCfg, nil)
		recipient := register(t, bob, "cap-recipient")
		successes := 0
		var mu sync.Mutex
		for i := range 20 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				copy := in
				copy.Body = "capacity"
				copy.To = map[string]any{"type": "direct", "agent_id": "cap-recipient"}
				copy.IdempotencyKey = fmt.Sprint(i)
				_, err := capService.SendMessage(ctx, alice, copy)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successes++
				} else if err.Code != "MAILBOX_FULL" {
					t.Error(err)
				}
			}(i)
		}
		wg.Wait()
		if successes != 1 {
			t.Fatalf("capacity accepted %d", successes)
		}
		if _, err := svc.PeekInbox(ctx, bob, recipient, 0); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("post-page urgency metadata cannot cross takeover", func(t *testing.T) {
		oldSession := register(t, alice, "urgency-transfer")
		tracer := &urgencyTracer{ready: make(chan struct{}), resume: make(chan struct{})}
		tracedCfg := db.Config()
		tracedCfg.ConnConfig.Tracer = tracer
		traced, err := pgxpool.NewWithConfig(ctx, tracedCfg)
		if err != nil {
			t.Fatal(err)
		}
		defer traced.Close()
		reader := New(traced, cfg, nil)
		done := make(chan map[string]any, 1)
		go func() {
			out, err := reader.PollInbox(ctx, alice, PollInput{SessionID: oldSession})
			if err != nil {
				out = map[string]any{"error": err}
			}
			done <- out
		}()
		select {
		case <-tracer.ready:
		case <-time.After(3 * time.Second):
			close(tracer.resume)
			t.Fatal("metadata query never reached")
		}
		admin := bob
		admin.Scopes = map[string]bool{"*": true}
		out, aerr := svc.RegisterAgent(ctx, admin, RegisterInput{TeamID: admin.TeamID, AgentID: "urgency-transfer", ReplaceExistingSession: true})
		if aerr != nil {
			close(tracer.resume)
			t.Fatal(aerr)
		}
		if _, aerr = svc.SendMessage(ctx, admin, SendInput{SessionID: out["session_id"].(string), To: map[string]any{"type": "direct", "agent_id": "urgency-transfer"}, Priority: 3, Topic: "private", Body: "new owner", RequireAck: true}); aerr != nil {
			close(tracer.resume)
			t.Fatal(aerr)
		}
		close(tracer.resume)
		result := <-done
		if result["has_urgent"] == true {
			t.Fatal("old poll disclosed new owner urgency", result)
		}
	})
	t.Run("audit failures roll back delivery and terminal states stay terminal", func(t *testing.T) {
		sender := register(t, alice, "state-sender")
		recipient := register(t, bob, "state-recipient")
		id := send(t, alice, sender, "state-recipient", "state-first")
		if _, err := db.Exec(ctx, `CREATE FUNCTION reject_delivery_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='DELIVERED' THEN RAISE EXCEPTION 'audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_delivery BEFORE INSERT ON message_events FOR EACH ROW EXECUTE FUNCTION reject_delivery_audit()`); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.PollInbox(ctx, bob, PollInput{SessionID: recipient}); err == nil {
			t.Fatal("poll reported success without durable audit")
		}
		var state string
		db.QueryRow(ctx, `SELECT state FROM deliveries WHERE message_id=$1`, id).Scan(&state)
		if state != "PENDING" {
			t.Fatal("failed transaction altered state", state)
		}
		db.Exec(ctx, `DROP TRIGGER reject_delivery ON message_events; DROP FUNCTION reject_delivery_audit()`)
		if _, err := svc.PollInbox(ctx, bob, PollInput{SessionID: recipient}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.CancelMessage(ctx, alice, CancelInput{SessionID: sender, MessageID: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.AckMessages(ctx, bob, AckInput{SessionID: recipient, MessageIDs: []string{id}}); err != nil {
			t.Fatal(err)
		}
		db.QueryRow(ctx, `SELECT state FROM deliveries WHERE message_id=$1`, id).Scan(&state)
		if state != "CANCELLED" {
			t.Fatal("ack overwrote cancellation", state)
		}
		expired := send(t, alice, sender, "state-recipient", "state-expired")
		db.Exec(ctx, `UPDATE messages SET expires_at=now()-interval '1 second' WHERE message_id=$1`, expired)
		if err := svc.SweepExpiry(ctx); err != nil {
			t.Fatal(err)
		}
		db.QueryRow(ctx, `SELECT state FROM deliveries WHERE message_id=$1`, expired).Scan(&state)
		if state != "EXPIRED" {
			t.Fatal("expiry did not persist", state)
		}
	})
}
