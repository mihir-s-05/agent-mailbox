package web_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/events"
	"github.com/mihir/msg-com/internal/mailboxapi"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/testutil"
	"github.com/mihir/msg-com/internal/viewerpreview"
	"github.com/mihir/msg-com/internal/web"
)

func TestBrowserMailboxFlow(t *testing.T) {
	if os.Getenv("MAILBOX_BROWSER_TESTS") != "1" {
		t.Skip("set MAILBOX_BROWSER_TESTS=1 and PLAYWRIGHT_MODULE for browser integration")
	}
	db := testutil.Database(t)
	svc := service.New(db, config.Config{MaxBodyBytes: 32768, MaxAttachments: 8, MailboxCap: 100, RateLimitPerMinute: 100, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400}, nil)
	p := model.Principal{TeamID: "browser", Subject: "qa", Token: "qa-token", Scopes: map[string]bool{"*": true}}
	validate := func(team, subject, token string) (model.Principal, bool) {
		return p, team == p.TeamID && subject == p.Subject && token == p.Token
	}
	eventService, _ := events.New(db, bytes.Repeat([]byte{1}, 32), svc.AuthorizeMailbox, validate)
	api := mailboxapi.New(svc)
	ctx := context.Background()
	if _, err := svc.RegisterAgent(ctx, p, service.RegisterInput{TeamID: p.TeamID, AgentID: "browser-agent", DisplayName: "Browser agent"}); err != nil {
		t.Fatal(err)
	}
	sender, registerErr := svc.RegisterAgent(ctx, p, service.RegisterInput{TeamID: p.TeamID, AgentID: "sender", DisplayName: "Test sender"})
	if registerErr != nil {
		t.Fatal(registerErr)
	}
	mux := http.NewServeMux()
	mux.Handle("/", web.Assets())
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer qa-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		web.New(api, svc, eventService, validate).ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	}))
	server := httptest.NewServer(mux)
	defer server.Close()
	defer server.CloseClientConnections()
	cmd := exec.CommandContext(context.Background(), "node", filepath.Join("tests", "browser.mjs"))
	u, _ := viewerpreview.ParseBackend(server.URL)
	proxy, err := viewerpreview.Handler(u, "/agent-mailbox/")
	if err != nil {
		t.Fatal(err)
	}
	project := httptest.NewServer(proxy)
	defer project.Close()
	cmd.Env = append(os.Environ(), "MAILBOX_BROWSER_URL="+project.URL+"/agent-mailbox/", "MAILBOX_QA_SENDER_SESSION="+sender["session_id"].(string))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser QA failed: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}

func TestObserverAuthorizationAndNoSideEffects(t *testing.T) {
	db := testutil.Database(t)
	ctx := context.Background()
	svc := service.New(db, config.Config{MaxBodyBytes: 32768, MaxAttachments: 8, MailboxCap: 100, RateLimitPerMinute: 100, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400}, nil)
	p := model.Principal{TeamID: "team", Subject: "owner", Token: "owned", Scopes: map[string]bool{"*": true}}
	other := model.Principal{TeamID: "team", Subject: "other", Token: "other", Scopes: map[string]bool{"*": true}}
	foreign := model.Principal{TeamID: "foreign", Subject: "owner", Token: "foreign", Scopes: map[string]bool{"*": true}}
	register := func(principal model.Principal, name string) string {
		result, err := svc.RegisterAgent(ctx, principal, service.RegisterInput{TeamID: principal.TeamID, AgentID: name})
		if err != nil {
			t.Fatal(err)
		}
		return result["session_id"].(string)
	}
	ownedSession := register(p, "inbox")
	otherSession := register(other, "private")
	register(foreign, "foreign-only")
	if _, err := svc.SendMessage(ctx, other, service.SendInput{SessionID: otherSession, To: map[string]any{"type": "direct", "agent_id": "inbox"}, Priority: 1, Topic: "owned message", Body: "visible to owner", RequireAck: true}); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	if _, err := db.Exec(ctx, `UPDATE agents SET online=false,last_seen_at=$1 WHERE team_id='team' AND agent_id='inbox'`, before); err != nil {
		t.Fatal(err)
	}
	valid := true
	validate := func(team, subject, token string) (model.Principal, bool) {
		return p, valid && team == p.TeamID && subject == p.Subject && token == p.Token
	}
	eventService, _ := events.New(db, bytes.Repeat([]byte{1}, 32), svc.AuthorizeMailbox, validate)
	handler := web.New(mailboxapi.New(svc), svc, eventService, validate)
	get := func(path string, principal *model.Principal) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", path, nil)
		if principal != nil {
			r = r.WithContext(auth.WithPrincipal(ctx, *principal))
		}
		handler.ServeHTTP(w, r)
		return w
	}
	if w := get("/api/agents", nil); w.Code != 401 {
		t.Fatal("unauthenticated directory exposed")
	}
	for _, path := range []string{"/api/inbox?agent_id=private", "/api/log?agent_id=private", "/api/subscriptions?agent_id=private", "/api/inbox?agent_id=foreign-only"} {
		if w := get(path, &p); w.Code != 403 {
			t.Fatalf("inaccessible mailbox %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/api/agents", "/api/inbox?agent_id=inbox", "/api/log?agent_id=inbox", "/api/subscriptions?agent_id=inbox"} {
		w := get(path, &p)
		if w.Code != 200 {
			t.Fatalf("owned %s: %d %s", path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "foreign-only") || strings.Contains(w.Body.String(), "owner_subject") {
			t.Fatal("directory leaked foreign or owner data")
		}
	}
	var online, active bool
	var lastSeen time.Time
	var delivery string
	if err := db.QueryRow(ctx, `SELECT online,last_seen_at FROM agents WHERE team_id='team' AND agent_id='inbox'`).Scan(&online, &lastSeen); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow(ctx, `SELECT active FROM sessions WHERE session_id=$1`, ownedSession).Scan(&active)
	_ = db.QueryRow(ctx, `SELECT state FROM deliveries WHERE team_id='team' AND recipient_agent_id='inbox'`).Scan(&delivery)
	if online || !lastSeen.Equal(before) || !active || delivery != "PENDING" {
		t.Fatal("observer changed presence, session, or delivery")
	}
	limited := p
	limited.Scopes = map[string]bool{"poll:self": true}
	validate = func(team, subject, token string) (model.Principal, bool) {
		return limited, valid && team == p.TeamID && subject == p.Subject && token == p.Token
	}
	handler = web.New(mailboxapi.New(svc), svc, eventService, validate)
	if w := get("/api/agents", &limited); w.Code != 200 || strings.Contains(w.Body.String(), "private") {
		t.Fatal("limited directory exposed another owner's agent")
	}
	valid = false
	if w := get("/api/inbox?agent_id=inbox", &p); w.Code != 401 {
		t.Fatal("revoked credential retained access")
	}
}
