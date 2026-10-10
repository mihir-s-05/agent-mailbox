package pluginclient_test

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

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/events"
	"github.com/mihir/msg-com/internal/mailboxapi"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/testutil"
	"github.com/mihir/msg-com/internal/web"
)

func TestLiveChannelAndMailbox(t *testing.T) {
	db := testutil.Database(t)
	svc := service.New(db, config.Config{MaxBodyBytes: 32768, MaxAttachments: 8, MaxBroadcastRecipients: 200, MailboxCap: 100, RateLimitPerMinute: 100, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400}, nil)
	p := model.Principal{TeamID: "plugin-test", Subject: "owner", Token: "qa-plugin-token", Scopes: map[string]bool{"*": true}}
	other := p
	other.Subject, other.Token = "other", "qa-other-owner"
	validate := func(team, subject, token string) (model.Principal, bool) {
		for _, candidate := range []model.Principal{p, other} {
			if candidate.TeamID == team && candidate.Subject == subject && candidate.Token == token {
				return candidate, true
			}
		}
		return model.Principal{}, false
	}
	eventService, err := events.New(db, bytes.Repeat([]byte{1}, 32), svc.AuthorizeMailbox, validate)
	if err != nil {
		t.Fatal(err)
	}
	api := web.New(mailboxapi.New(svc), svc, eventService, validate)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var principal model.Principal
		switch r.Header.Get("Authorization") {
		case "Bearer " + p.Token:
			principal = p
		case "Bearer " + other.Token:
			principal = other
		default:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		api.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	command := exec.CommandContext(context.Background(), "node", filepath.Join("..", "..", "plugin-src", "integration.mjs"))
	command.Env = append(os.Environ(), "MAILBOX_PLUGIN_TEST_URL="+server.URL+"/")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("plugin integration failed: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}
