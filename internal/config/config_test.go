package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mihir/msg-com/internal/tokens"
)

func TestLoadAutoGeneratesTokensWithTTL(t *testing.T) {
	tmp := t.TempDir()
	tokenFile := filepath.Join(tmp, "tokens.local.json")
	secretFile := filepath.Join(tmp, "tokens.local.secrets.json")

	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_TOKENS", "")
	setenv(t, "MAILBOX_ALLOW_PLAINTEXT_TOKENS", "false")
	setenv(t, "MAILBOX_TOKENS_JSON_FILE", tokenFile)
	setenv(t, "MAILBOX_BOOTSTRAP_TOKENS_FILE", secretFile)
	setenv(t, "MAILBOX_AUTO_GENERATE_TOKENS", "true")
	setenv(t, "MAILBOX_BOOTSTRAP_TOKEN_COUNT", "2")
	setenv(t, "MAILBOX_BOOTSTRAP_TOKEN_TTL", "2h")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Tokens) != 2 {
		t.Fatalf("expected 2 bootstrap tokens, got %d", len(cfg.Tokens))
	}
	for _, tok := range cfg.Tokens {
		if tok.TokenHash == "" {
			t.Fatalf("expected token hash to be set")
		}
		if tok.ExpiresAt == nil || tok.ExpiresAt.Before(time.Now().UTC()) {
			t.Fatalf("expected non-expired token")
		}
		if tok.Token != "" {
			t.Fatalf("expected plaintext token to be stripped from server token set")
		}
	}

	tokenBody, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("read token file: %v", err)
	}
	if strings.Contains(string(tokenBody), `"token":`) {
		t.Fatalf("token file should not contain plaintext tokens")
	}
	if !strings.Contains(string(tokenBody), `"token_hash"`) {
		t.Fatalf("token file must contain token_hash")
	}

	secretBody, err := os.ReadFile(secretFile)
	if err != nil {
		t.Fatalf("read bootstrap secrets file: %v", err)
	}
	if !strings.Contains(string(secretBody), `"token"`) {
		t.Fatalf("bootstrap secrets file must include plaintext token output")
	}
}

func TestLoadRejectsEmptyScopes(t *testing.T) {
	tmp := t.TempDir()
	tokenFile := filepath.Join(tmp, "tokens.json")
	expires := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	hash := tokens.Hash("abc")
	content := `{"tokens":[{"id":"bad","token_hash":"` + hash + `","team_id":"team","subject":"s","scopes":[],"expires_at":"` + expires + `"}]}`
	if err := os.WriteFile(tokenFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_TOKENS", "")
	setenv(t, "MAILBOX_TOKENS_JSON_FILE", tokenFile)
	setenv(t, "MAILBOX_AUTO_GENERATE_TOKENS", "false")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "at least one scope") {
		t.Fatalf("expected scope validation error, got %v", err)
	}
}

func TestLoadAllowsExpiredTokenWithoutCreatedAtWhenOtherTokensAreActive(t *testing.T) {
	tmp := t.TempDir()
	tokenFile := filepath.Join(tmp, "tokens.json")

	expired := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	active := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	content := `{"tokens":[` +
		`{"id":"expired","token_hash":"` + tokens.Hash("expired") + `","team_id":"team","subject":"s1","scopes":["poll:self"],"expires_at":"` + expired + `"},` +
		`{"id":"active","token_hash":"` + tokens.Hash("active") + `","team_id":"team","subject":"s2","scopes":["poll:self"],"expires_at":"` + active + `"}` +
		`]}`
	if err := os.WriteFile(tokenFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_TOKENS", "")
	setenv(t, "MAILBOX_TOKENS_JSON_FILE", tokenFile)
	setenv(t, "MAILBOX_AUTO_GENERATE_TOKENS", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() should allow expired token entries when active tokens exist: %v", err)
	}
	if len(cfg.Tokens) != 2 {
		t.Fatalf("expected 2 token records, got %d", len(cfg.Tokens))
	}
}

func TestLoadBootstrapSecretWriteFailureDoesNotLeaveTokenFile(t *testing.T) {
	tmp := t.TempDir()
	tokenFile := filepath.Join(tmp, "tokens.local.json")

	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_TOKENS", "")
	setenv(t, "MAILBOX_ALLOW_PLAINTEXT_TOKENS", "false")
	setenv(t, "MAILBOX_TOKENS_JSON_FILE", tokenFile)
	setenv(t, "MAILBOX_BOOTSTRAP_TOKENS_FILE", tmp)
	setenv(t, "MAILBOX_AUTO_GENERATE_TOKENS", "true")
	setenv(t, "MAILBOX_BOOTSTRAP_TOKEN_COUNT", "2")
	setenv(t, "MAILBOX_BOOTSTRAP_TOKEN_TTL", "2h")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "write bootstrap token secrets file") {
		t.Fatalf("expected secret write failure, got %v", err)
	}
	if _, statErr := os.Stat(tokenFile); !os.IsNotExist(statErr) {
		t.Fatalf("token file should not exist after bootstrap secret write failure, statErr=%v", statErr)
	}
}

func TestReadTokensForInlineSourceKeepsFixedExpiry(t *testing.T) {
	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_ALLOW_PLAINTEXT_TOKENS", "true")
	setenv(t, "MAILBOX_TOKENS", "inline-token=dev-team:poll:self|1h")
	setenv(t, "MAILBOX_AUTO_GENERATE_TOKENS", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.InlineTokenSource {
		t.Fatalf("expected inline token source to be true")
	}
	if len(cfg.Tokens) != 1 || cfg.Tokens[0].ExpiresAt == nil {
		t.Fatalf("expected one inline token with expiry")
	}

	firstExpiry := cfg.Tokens[0].ExpiresAt.UTC()
	reloaded, err := ReadTokens(cfg)
	if err != nil {
		t.Fatalf("ReadTokens() error = %v", err)
	}
	if len(reloaded) != 1 || reloaded[0].ExpiresAt == nil {
		t.Fatalf("expected one reloaded token with expiry")
	}
	if !reloaded[0].ExpiresAt.UTC().Equal(firstExpiry) {
		t.Fatalf("expected fixed inline expiry; got %s want %s", reloaded[0].ExpiresAt.UTC(), firstExpiry)
	}
}

func TestLoadFailsOnInvalidDurationEnv(t *testing.T) {
	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_BOOTSTRAP_TOKEN_TTL", "not-a-duration")
	setenv(t, "MAILBOX_TOKENS", "inline-token=dev-team:poll:self|1h")
	setenv(t, "MAILBOX_ALLOW_PLAINTEXT_TOKENS", "true")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "MAILBOX_BOOTSTRAP_TOKEN_TTL has invalid duration") {
		t.Fatalf("expected invalid duration error, got %v", err)
	}
}

func TestLoadFailsOnInvalidIntegerEnv(t *testing.T) {
	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "false")
	setenv(t, "MAILBOX_BOOTSTRAP_TOKEN_COUNT", "oops")
	setenv(t, "MAILBOX_TOKENS", "inline-token=dev-team:poll:self|1h")
	setenv(t, "MAILBOX_ALLOW_PLAINTEXT_TOKENS", "true")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "MAILBOX_BOOTSTRAP_TOKEN_COUNT has invalid integer") {
		t.Fatalf("expected invalid integer error, got %v", err)
	}
}

func TestLoadFailsOnInvalidBooleanEnv(t *testing.T) {
	setenv(t, "MAILBOX_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/agent_mailbox?sslmode=disable")
	setenv(t, "MAILBOX_REQUIRE_MTLS", "maybe")
	setenv(t, "MAILBOX_TOKENS", "inline-token=dev-team:poll:self|1h")
	setenv(t, "MAILBOX_ALLOW_PLAINTEXT_TOKENS", "true")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "MAILBOX_REQUIRE_MTLS has invalid boolean") {
		t.Fatalf("expected invalid boolean error, got %v", err)
	}
}

func setenv(t *testing.T, key, value string) {
	t.Helper()
	previous, had := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("setenv %s: %v", key, err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, previous)
			return
		}
		_ = os.Unsetenv(key)
	})
}
