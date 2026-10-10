package app

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/testutil"
	"github.com/mihir/msg-com/internal/tokens"
)

func TestRuntimeStartupAndShutdown(t *testing.T) {
	db := testutil.Database(t)
	dsn := db.Config().ConnString()
	schema := db.Config().ConnConfig.RuntimeParams["search_path"]
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	directory := t.TempDir()
	t.Chdir(filepath.Join("..", ".."))
	expiry := time.Now().Add(time.Hour)
	cfg := config.Config{DatabaseURL: dsn, EventKeyFile: filepath.Join(directory, "events.key"), Tokens: []config.TokenRecord{{ID: "runtime-test", TeamID: "runtime", Subject: "owner", TokenHash: tokens.Hash("runtime-test-token"), Scopes: []string{"*"}, ExpiresAt: &expiry}}, InlineTokenSource: true, TokenReloadInterval: time.Second, DefaultPollMS: 10, MaxBodyBytes: 32768, MaxAttachments: 8, MailboxCap: 100, RateLimitPerMinute: 100, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400, InactivityThreshold: time.Minute, RetentionWindow: time.Hour, ExpirySweepInterval: time.Second, RetentionSweepInterval: time.Second, InactivitySweepEvery: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime, err := Start(ctx, cfg, "runtime-test-token")
	if err != nil {
		t.Fatal(err)
	}
	p, ok := runtime.Auth.LookupToken("runtime-test-token")
	if !ok {
		t.Fatal("runtime token missing")
	}
	if _, err := runtime.Service.RegisterAgent(ctx, p, service.RegisterInput{TeamID: p.TeamID, AgentID: "runtime-agent"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !runtime.Events.Healthy() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !runtime.Events.Healthy() {
		t.Fatal("event worker never ready")
	}
	done := make(chan struct{})
	go func() { runtime.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("workers did not stop on close")
	}
}
