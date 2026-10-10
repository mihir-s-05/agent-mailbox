package events

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/testutil"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestWebhookBoundaries(t *testing.T) {
	if _, err := safeHTTPClient().Post("https://127.0.0.1:443/callback", "application/json", strings.NewReader("{}")); err == nil || !strings.Contains(err.Error(), "not public") {
		t.Fatal("connection-time private destination was not blocked", err)
	}
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.64.0.1", "192.0.2.1", "::1", "::ffff:127.0.0.1", "fe80::1", "fc00::1", "224.0.0.1", "0.0.0.0"} {
		if publicAddress(net.ParseIP(ip)) {
			t.Errorf("accepted nonpublic IP %s", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !publicAddress(net.ParseIP(ip)) {
			t.Errorf("rejected public IP %s", ip)
		}
	}
	for _, url := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com/#fragment", "https://example.com:1234/"} {
		if validateURL(url) == nil {
			t.Errorf("accepted invalid callback %s", url)
		}
	}
	key := bytes.Repeat([]byte{42}, 32)
	ciphertext, err := encrypt(key, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := decrypt(key, ciphertext)
	if err != nil || string(plain) != "secret" {
		t.Fatal(err)
	}
	ciphertext[0] ^= 1
	if _, err = decrypt(key, ciphertext); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}
	path := filepath.Join(t.TempDir(), "events.key")
	first, err := LoadKey(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadKey(path)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("restart changed encryption key", err)
	}
	s := &Service{key: key}
	cursor := s.cursor("team", "agent", 3)
	if _, err := s.parseCursor(cursor, "team", "other"); err == nil {
		t.Fatal("cursor crossed mailbox")
	}
	if _, err := s.parseCursor(cursor+"x", "team", "agent"); err == nil {
		t.Fatal("tampered cursor accepted")
	}
	// Compare against an independent Standard Webhooks HMAC implementation.
	body := []byte(`{"hello":"world"}`)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("evt.1700000000."))
	mac.Write(body)
	expected := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if signature(key, "evt", 1700000000, body) != expected {
		t.Fatal("signature format mismatch")
	}
}

func TestEventsIntegration(t *testing.T) {
	db := testutil.Database(t)
	ctx := context.Background()
	p := model.Principal{TeamID: "events-team", Subject: "owner", Token: "token", Scopes: map[string]bool{"*": true}}
	cfg := config.Config{MaxBodyBytes: 32768, MaxAttachments: 8, MailboxCap: 100, RateLimitPerMinute: 1000, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400}
	svc := service.New(db, cfg, nil)
	out, aerr := svc.RegisterAgent(ctx, p, service.RegisterInput{TeamID: p.TeamID, AgentID: "inbox"})
	if aerr != nil {
		t.Fatal(aerr)
	}
	session := out["session_id"].(string)
	var allowed atomic.Bool
	allowed.Store(true)
	principal := func(team, subject, token string) (model.Principal, bool) {
		return p, allowed.Load() && team == p.TeamID && subject == p.Subject && token == p.Token
	}
	encryptionKey := bytes.Repeat([]byte{7}, 32)
	s, err := New(db, encryptionKey, svc.AuthorizeMailbox, principal)
	if err != nil {
		t.Fatal(err)
	}
	signing := bytes.Repeat([]byte{5}, 32)
	oldSigning := append([]byte(nil), signing...)
	var mu sync.Mutex
	var ids []string
	var eventBodies [][]byte
	status := 200
	responseBody := ""
	challengeOK := true
	block := false
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		timestamp, _ := strconv.ParseInt(r.Header.Get("webhook-timestamp"), 10, 64)
		id := r.Header.Get("webhook-id")
		var message map[string]any
		if err := json.Unmarshal(body, &message); err != nil {
			t.Error(err)
		}
		mu.Lock()
		defer mu.Unlock()
		signatures := strings.Fields(r.Header.Get("webhook-signature"))
		valid := false
		for _, candidate := range signatures {
			if hmac.Equal([]byte(candidate), []byte(signature(signing, id, timestamp, body))) {
				valid = true
			}
		}
		if !valid {
			t.Error("invalid webhook signature")
		}
		if r.Header.Get("X-MCP-Subscription-Id") == "" {
			t.Error("missing subscription ID")
		}
		if message["type"] == "verification" {
			echo := "incorrect"
			if challengeOK {
				echo = message["challenge"].(string)
			}
			response, _ := json.Marshal(map[string]string{"challenge": echo})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(response)), Header: make(http.Header)}, nil
		}
		if message["eventId"] != id || message["name"] != EventName {
			t.Error("invalid event envelope")
		}
		if _, ok := message["data"].(map[string]any)["body"]; ok {
			t.Error("event leaked full message")
		}
		ids = append(ids, id)
		eventBodies = append(eventBodies, append([]byte(nil), body...))
		if !bytes.Equal(signing, oldSigning) {
			if len(signatures) != 2 || signatures[1] != signature(oldSigning, id, timestamp, body) {
				t.Error("rotation did not include old signature")
			}
		}
		if block {
			mu.Unlock()
			entered <- struct{}{}
			<-release
			mu.Lock()
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(responseBody)), Header: make(http.Header)}, nil
	})
	s.client = &http.Client{Transport: transport}
	in := SubscribeInput{Name: EventName, Arguments: map[string]any{"agent_id": "inbox", "min_priority": float64(2)}, Delivery: Delivery{Mode: "webhook", URL: "https://receiver.example/events", Secret: "whsec_" + base64.StdEncoding.EncodeToString(signing)}}
	attacker := p
	attacker.Subject = "attacker"
	if _, err := s.Subscribe(ctx, attacker, in); err == nil {
		t.Fatal("unauthorized subscription accepted")
	}
	challengeOK = false
	if _, err := s.Subscribe(ctx, p, in); err == nil {
		t.Fatal("wrong challenge accepted")
	}
	challengeOK = true
	sub, err := s.Subscribe(ctx, p, in)
	if err != nil {
		t.Fatal(err)
	}
	refreshed, err := s.Subscribe(ctx, p, in)
	if err != nil || refreshed.ID != sub.ID || refreshed.Cursor != sub.Cursor {
		t.Fatal("refresh created duplicate or moved watermark", err)
	}
	var encrypted []byte
	db.QueryRow(ctx, `SELECT secret FROM event_subscriptions WHERE id=$1`, sub.ID).Scan(&encrypted)
	if bytes.Equal(encrypted, signing) || bytes.Contains(encrypted, signing) {
		t.Fatal("plaintext signing secret stored")
	}
	send := func(priority int) string {
		t.Helper()
		out, err := svc.SendMessage(ctx, p, service.SendInput{SessionID: session, To: map[string]any{"type": "direct", "agent_id": "inbox"}, Priority: priority, Topic: "topic", Body: "untrusted message", RequireAck: true})
		if err != nil {
			t.Fatal(err)
		}
		return out["message_id"].(string)
	}
	send(1)
	send(3)
	send(2)
	if err = s.Materialize(ctx); err != nil {
		t.Fatal(err)
	}
	ahead := s.cursor(p.TeamID, "inbox", 3)
	skip := in
	skip.Cursor = &ahead
	if _, err := s.Subscribe(ctx, p, skip); err == nil {
		t.Fatal("refresh cursor skipped undelivered matching events")
	}
	status = 429
	if worked, err := s.DeliverOne(ctx); err != nil || !worked {
		t.Fatal("first delivery missing", err)
	}
	if worked, err := s.DeliverOne(ctx); err != nil || worked {
		t.Fatal("new event bypassed failed earlier event", err)
	}
	db.Exec(ctx, `UPDATE event_deliveries SET next_attempt_at=now() WHERE subscription_id=$1`, sub.ID)
	status = 200
	restarted, _ := New(db, encryptionKey, svc.AuthorizeMailbox, principal)
	restarted.client = s.client
	if worked, err := restarted.DeliverOne(ctx); err != nil || !worked {
		t.Fatal("restart lost retry", err)
	}
	if len(ids) != 2 || ids[0] != ids[1] || !bytes.Equal(eventBodies[0], eventBodies[1]) {
		t.Fatal("retry changed event ID or body")
	}
	responseBody = strings.Repeat("x", 5000)
	if worked, err := restarted.DeliverOne(ctx); err != nil || !worked {
		t.Fatal("next event missing", err)
	}
	responseBody = ""
	refreshed, err = s.Subscribe(ctx, p, in)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := s.parseCursor(refreshed.Cursor, p.TeamID, "inbox")
	if err != nil || revision < 3 {
		t.Fatal("refresh reset cursor", err, revision)
	}
	signing = bytes.Repeat([]byte{6}, 32)
	in.Delivery.Secret = "whsec_" + base64.StdEncoding.EncodeToString(signing)
	if _, err = s.Subscribe(ctx, p, in); err != nil {
		t.Fatal(err)
	}
	send(3)
	s.Materialize(ctx)
	block = true
	done := make(chan error, 1)
	go func() { _, err := s.DeliverOne(ctx); done <- err }()
	<-entered
	if worked, err := restarted.DeliverOne(ctx); err != nil || worked {
		t.Fatal("second worker stole in-flight lease", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	block = false
	send(3)
	s.Materialize(ctx)
	status = 410
	if _, err = s.DeliverOne(ctx); err != nil {
		t.Fatal(err)
	}
	var active bool
	db.QueryRow(ctx, `SELECT active FROM event_subscriptions WHERE id=$1`, sub.ID).Scan(&active)
	if active {
		t.Fatal("410 did not stop subscription")
	}
	status = 200
	if _, err = s.Subscribe(ctx, p, in); err != nil {
		t.Fatal(err)
	}
	if worked, err := s.DeliverOne(ctx); err != nil || !worked {
		t.Fatal("refresh failed to redrive dead letter", err)
	}
	send(3)
	s.Materialize(ctx)
	allowed.Store(false)
	before := len(ids)
	if _, err = s.DeliverOne(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ids) != before {
		t.Fatal("revoked credential received an event")
	}
	allowed.Store(true)
	in.Cursor = &sub.Cursor
	if _, err = s.Subscribe(ctx, p, in); err != nil {
		t.Fatal("explicit replay rejected", err)
	}
	if worked, err := s.DeliverOne(ctx); err != nil || !worked {
		t.Fatal("replay did not deliver", err)
	}
	if err = s.Unsubscribe(ctx, p, in); err != nil {
		t.Fatal(err)
	}
	if err = s.Unsubscribe(ctx, p, in); err != nil {
		t.Fatal("unsubscribe not idempotent", err)
	}
	if worked, err := s.DeliverOne(ctx); err != nil || worked {
		t.Fatal("unsubscribed event delivered", err)
	}
	diagnostic, err := s.Diagnostic(ctx, p, "inbox")
	if err != nil || len(diagnostic) != 1 {
		t.Fatal("diagnostic missing", err)
	}
	encoded := fmt.Sprint(diagnostic)
	if strings.Contains(encoded, "whsec_") || strings.Contains(encoded, "receiver.example") {
		t.Fatal("diagnostic leaked callback credentials")
	}
}
