package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/tokens"
)

func TestMiddlewareWritesStructuredAuthErrorEnvelope(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	reg := NewRegistry(config.Config{
		RequireMTLS: false,
		Tokens: []config.TokenRecord{
			{
				ID:        "test",
				TokenHash: tokens.Hash("t1"),
				TeamID:    "team",
				Subject:   "subj",
				Scopes:    []string{"poll:self"},
				ExpiresAt: &expires,
			},
		},
	})

	h := reg.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON envelope: %v", err)
	}

	errorObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("response missing error envelope: %#v", body)
	}
	if _, ok := errorObj["code"].(string); !ok {
		t.Fatalf("error.code missing: %#v", errorObj)
	}
	if _, ok := errorObj["message"].(string); !ok {
		t.Fatalf("error.message missing: %#v", errorObj)
	}
	if _, ok := errorObj["details"]; !ok {
		t.Fatalf("error.details missing: %#v", errorObj)
	}
}

func TestAuthenticateRejectsExpiredToken(t *testing.T) {
	expired := time.Now().UTC().Add(-time.Minute)
	reg := NewRegistry(config.Config{
		RequireMTLS: false,
		Tokens: []config.TokenRecord{
			{
				ID:        "expired",
				TokenHash: tokens.Hash("expired-token"),
				TeamID:    "team",
				Subject:   "subj",
				Scopes:    []string{"poll:self"},
				ExpiresAt: &expired,
			},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer expired-token")
	_, aerr := reg.Authenticate(req)
	if aerr == nil || aerr.Code != "AUTH_INVALID" {
		t.Fatalf("expected AUTH_INVALID for expired token, got %#v", aerr)
	}
}

func TestReloadAppliesRevokedFlag(t *testing.T) {
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "tokens.json")
	expires := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	hash := tokens.Hash("rotate-me")

	initial := `{"tokens":[{"id":"t1","token_hash":"` + hash + `","team_id":"team","subject":"s","scopes":["poll:self"],"expires_at":"` + expires + `"}]}`
	if err := os.WriteFile(tokenFile, []byte(initial), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	cfg := config.Config{
		RequireMTLS:         false,
		TokensJSONFile:      tokenFile,
		TokenReloadInterval: 0,
		Tokens: []config.TokenRecord{
			{
				ID:        "t1",
				TokenHash: hash,
				TeamID:    "team",
				Subject:   "s",
				Scopes:    []string{"poll:self"},
				ExpiresAt: timePtr(time.Now().UTC().Add(time.Hour)),
			},
		},
	}
	reg := NewRegistry(cfg)
	if _, ok := reg.LookupToken("rotate-me"); !ok {
		t.Fatalf("expected token to be valid before revoke")
	}

	revoked := `{"tokens":[{"id":"t1","token_hash":"` + hash + `","team_id":"team","subject":"s","scopes":["poll:self"],"expires_at":"` + expires + `","revoked":true}]}`
	if err := os.WriteFile(tokenFile, []byte(revoked), 0o600); err != nil {
		t.Fatalf("write revoked token file: %v", err)
	}
	if err := reg.Reload(); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if _, ok := reg.LookupToken("rotate-me"); ok {
		t.Fatalf("expected token to be invalid after revoke")
	}
}

func timePtr(v time.Time) *time.Time {
	return &v
}
