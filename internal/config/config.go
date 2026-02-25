package config

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mihir/msg-com/internal/tokens"
)

type TokenRecord struct {
	ID        string     `json:"id,omitempty"`
	Token     string     `json:"token,omitempty"`
	TokenHash string     `json:"token_hash,omitempty"`
	TeamID    string     `json:"team_id"`
	Subject   string     `json:"subject"`
	Scopes    []string   `json:"scopes"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Revoked   bool       `json:"revoked,omitempty"`
}

type tokenFile struct {
	Tokens []TokenRecord `json:"tokens"`
}

type BootstrapTokenSecret struct {
	ID        string    `json:"id"`
	Token     string    `json:"token"`
	TeamID    string    `json:"team_id"`
	Subject   string    `json:"subject"`
	Scopes    []string  `json:"scopes"`
	ExpiresAt time.Time `json:"expires_at"`
}

type bootstrapSecretsFile struct {
	GeneratedAt time.Time              `json:"generated_at"`
	TeamID      string                 `json:"team_id"`
	Tokens      []BootstrapTokenSecret `json:"tokens"`
}

type Config struct {
	Address                string
	BasePath               string
	DatabaseURL            string
	DefaultPollMS          int
	InactivityThreshold    time.Duration
	RetentionWindow        time.Duration
	ExpirySweepInterval    time.Duration
	InactivitySweepEvery   time.Duration
	RetentionSweepInterval time.Duration
	MaxBodyBytes           int
	MaxAttachments         int
	MaxBroadcastRecipients int
	MailboxCap             int
	RateLimitPerMinute     int
	DefaultTTLSeconds      int
	MaxTTLSeconds          int
	RequireMTLS            bool
	TLSCertFile            string
	TLSKeyFile             string
	TLSClientCAFile        string

	Tokens                []TokenRecord
	TokensJSONFile        string
	BootstrapTokensFile   string
	AutoGenerateTokens    bool
	BootstrapTeamID       string
	BootstrapTokenCount   int
	BootstrapTokenTTL     time.Duration
	BootstrapTokenScopes  []string
	TokenReloadInterval   time.Duration
	AllowPlaintextTokens  bool
	BootstrapTokenSecrets []BootstrapTokenSecret
	InlineTokenSource     bool
}

func Load() (Config, error) {
	defaultPollMS, err := envIntStrict("MAILBOX_DEFAULT_POLL_MS", 1200)
	if err != nil {
		return Config{}, err
	}
	maxBodyBytes, err := envIntStrict("MAILBOX_MAX_BODY_BYTES", 32*1024)
	if err != nil {
		return Config{}, err
	}
	maxAttachments, err := envIntStrict("MAILBOX_MAX_ATTACHMENTS", 8)
	if err != nil {
		return Config{}, err
	}
	maxBroadcastRecipients, err := envIntStrict("MAILBOX_MAX_BROADCAST_RECIPIENTS", 200)
	if err != nil {
		return Config{}, err
	}
	mailboxCap, err := envIntStrict("MAILBOX_MAILBOX_CAP", 5000)
	if err != nil {
		return Config{}, err
	}
	rateLimitPerMinute, err := envIntStrict("MAILBOX_RATE_LIMIT_PER_MIN", 60)
	if err != nil {
		return Config{}, err
	}
	defaultTTLSeconds, err := envIntStrict("MAILBOX_DEFAULT_TTL_SECONDS", 7*24*60*60)
	if err != nil {
		return Config{}, err
	}
	maxTTLSeconds, err := envIntStrict("MAILBOX_MAX_TTL_SECONDS", 30*24*60*60)
	if err != nil {
		return Config{}, err
	}
	requireMTLS, err := envBoolStrict("MAILBOX_REQUIRE_MTLS", true)
	if err != nil {
		return Config{}, err
	}
	autoGenerateTokens, err := envBoolStrict("MAILBOX_AUTO_GENERATE_TOKENS", true)
	if err != nil {
		return Config{}, err
	}
	bootstrapTokenCount, err := envIntStrict("MAILBOX_BOOTSTRAP_TOKEN_COUNT", 2)
	if err != nil {
		return Config{}, err
	}
	allowPlaintextTokens, err := envBoolStrict("MAILBOX_ALLOW_PLAINTEXT_TOKENS", false)
	if err != nil {
		return Config{}, err
	}

	inactivityThreshold, err := envDurationStrict("MAILBOX_INACTIVITY_THRESHOLD", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	retentionWindow, err := envDurationStrict("MAILBOX_RETENTION_WINDOW", 7*24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	expirySweepInterval, err := envDurationStrict("MAILBOX_EXPIRY_SWEEP_INTERVAL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	inactivitySweepEvery, err := envDurationStrict("MAILBOX_INACTIVITY_SWEEP_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	retentionSweepInterval, err := envDurationStrict("MAILBOX_RETENTION_SWEEP_INTERVAL", time.Hour)
	if err != nil {
		return Config{}, err
	}
	bootstrapTokenTTL, err := envDurationStrict("MAILBOX_BOOTSTRAP_TOKEN_TTL", 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	tokenReloadInterval, err := envDurationStrict("MAILBOX_TOKEN_RELOAD_INTERVAL", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Address:                envString("MAILBOX_ADDR", ":8443"),
		BasePath:               envString("MAILBOX_BASE_PATH", "/mcp"),
		DatabaseURL:            strings.TrimSpace(os.Getenv("MAILBOX_DATABASE_URL")),
		DefaultPollMS:          defaultPollMS,
		InactivityThreshold:    inactivityThreshold,
		RetentionWindow:        retentionWindow,
		ExpirySweepInterval:    expirySweepInterval,
		InactivitySweepEvery:   inactivitySweepEvery,
		RetentionSweepInterval: retentionSweepInterval,
		MaxBodyBytes:           maxBodyBytes,
		MaxAttachments:         maxAttachments,
		MaxBroadcastRecipients: maxBroadcastRecipients,
		MailboxCap:             mailboxCap,
		RateLimitPerMinute:     rateLimitPerMinute,
		DefaultTTLSeconds:      defaultTTLSeconds,
		MaxTTLSeconds:          maxTTLSeconds,
		RequireMTLS:            requireMTLS,
		TLSCertFile:            strings.TrimSpace(os.Getenv("MAILBOX_TLS_CERT_FILE")),
		TLSKeyFile:             strings.TrimSpace(os.Getenv("MAILBOX_TLS_KEY_FILE")),
		TLSClientCAFile:        strings.TrimSpace(os.Getenv("MAILBOX_TLS_CLIENT_CA_FILE")),
		TokensJSONFile:         envString("MAILBOX_TOKENS_JSON_FILE", "tokens.local.json"),
		BootstrapTokensFile:    envString("MAILBOX_BOOTSTRAP_TOKENS_FILE", "tokens.local.secrets.json"),
		AutoGenerateTokens:     autoGenerateTokens,
		BootstrapTeamID:        envString("MAILBOX_BOOTSTRAP_TEAM_ID", "dev-team"),
		BootstrapTokenCount:    bootstrapTokenCount,
		BootstrapTokenTTL:      bootstrapTokenTTL,
		BootstrapTokenScopes:   envCSV("MAILBOX_BOOTSTRAP_TOKEN_SCOPES", []string{"send:direct", "poll:self", "list:agents"}),
		TokenReloadInterval:    tokenReloadInterval,
		AllowPlaintextTokens:   allowPlaintextTokens,
	}

	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("MAILBOX_DATABASE_URL is required")
	}
	if !strings.HasPrefix(cfg.BasePath, "/") {
		cfg.BasePath = "/" + cfg.BasePath
	}
	if cfg.RequireMTLS {
		if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.TLSClientCAFile == "" {
			return Config{}, errors.New("MAILBOX_REQUIRE_MTLS=true requires MAILBOX_TLS_CERT_FILE, MAILBOX_TLS_KEY_FILE, and MAILBOX_TLS_CLIENT_CA_FILE")
		}
	}
	if cfg.BootstrapTokenCount <= 0 {
		return Config{}, errors.New("MAILBOX_BOOTSTRAP_TOKEN_COUNT must be greater than zero")
	}
	if cfg.DefaultPollMS <= 0 {
		return Config{}, errors.New("MAILBOX_DEFAULT_POLL_MS must be greater than zero")
	}
	if cfg.MaxBodyBytes <= 0 {
		return Config{}, errors.New("MAILBOX_MAX_BODY_BYTES must be greater than zero")
	}
	if cfg.MaxAttachments < 0 {
		return Config{}, errors.New("MAILBOX_MAX_ATTACHMENTS must be zero or greater")
	}
	if cfg.MaxBroadcastRecipients < 0 {
		return Config{}, errors.New("MAILBOX_MAX_BROADCAST_RECIPIENTS must be zero or greater")
	}
	if cfg.MailboxCap <= 0 {
		return Config{}, errors.New("MAILBOX_MAILBOX_CAP must be greater than zero")
	}
	if cfg.RateLimitPerMinute <= 0 {
		return Config{}, errors.New("MAILBOX_RATE_LIMIT_PER_MIN must be greater than zero")
	}
	if cfg.DefaultTTLSeconds <= 0 {
		return Config{}, errors.New("MAILBOX_DEFAULT_TTL_SECONDS must be greater than zero")
	}
	if cfg.MaxTTLSeconds <= 0 {
		return Config{}, errors.New("MAILBOX_MAX_TTL_SECONDS must be greater than zero")
	}
	if cfg.MaxTTLSeconds < cfg.DefaultTTLSeconds {
		return Config{}, errors.New("MAILBOX_MAX_TTL_SECONDS must be greater than or equal to MAILBOX_DEFAULT_TTL_SECONDS")
	}
	if cfg.TokenReloadInterval < 0 {
		return Config{}, errors.New("MAILBOX_TOKEN_RELOAD_INTERVAL must be zero or greater")
	}
	if cfg.BootstrapTokenTTL <= 0 {
		return Config{}, errors.New("MAILBOX_BOOTSTRAP_TOKEN_TTL must be greater than zero")
	}
	if len(cfg.BootstrapTokenScopes) == 0 {
		return Config{}, errors.New("MAILBOX_BOOTSTRAP_TOKEN_SCOPES must include at least one scope")
	}

	tokenSet, secrets, inlineSource, err := loadTokens(cfg, true)
	if err != nil {
		return Config{}, err
	}
	cfg.Tokens = tokenSet
	cfg.BootstrapTokenSecrets = secrets
	cfg.InlineTokenSource = inlineSource

	if !hasActiveToken(cfg.Tokens, time.Now().UTC()) {
		return Config{}, errors.New("no active tokens configured; all tokens are expired or revoked")
	}
	return cfg, nil
}

func ReadTokens(cfg Config) ([]TokenRecord, error) {
	if cfg.InlineTokenSource {
		return cloneTokenRecords(cfg.Tokens), nil
	}
	tokenSet, _, _, err := loadTokens(cfg, false)
	if err != nil {
		return nil, err
	}
	return tokenSet, nil
}

func loadTokens(cfg Config, allowBootstrap bool) ([]TokenRecord, []BootstrapTokenSecret, bool, error) {
	now := time.Now().UTC()

	raw := strings.TrimSpace(os.Getenv("MAILBOX_TOKENS"))
	if raw != "" {
		if !cfg.AllowPlaintextTokens {
			return nil, nil, false, errors.New("MAILBOX_TOKENS is disabled unless MAILBOX_ALLOW_PLAINTEXT_TOKENS=true")
		}
		inline, err := parseInlineTokens(raw, cfg.BootstrapTokenTTL, now)
		if err != nil {
			return nil, nil, false, err
		}
		normalized, err := normalizeTokenRecords(inline, cfg)
		return normalized, nil, true, err
	}

	b, err := os.ReadFile(cfg.TokensJSONFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && allowBootstrap && cfg.AutoGenerateTokens {
			toks, secrets, genErr := generateBootstrapTokenFiles(cfg, now)
			return toks, secrets, false, genErr
		}
		return nil, nil, false, fmt.Errorf("read token file %q: %w", cfg.TokensJSONFile, err)
	}
	var tf tokenFile
	if err := json.Unmarshal(b, &tf); err != nil {
		return nil, nil, false, fmt.Errorf("parse token file %q: %w", cfg.TokensJSONFile, err)
	}
	if len(tf.Tokens) == 0 && allowBootstrap && cfg.AutoGenerateTokens {
		toks, secrets, genErr := generateBootstrapTokenFiles(cfg, now)
		return toks, secrets, false, genErr
	}
	normalized, err := normalizeTokenRecords(tf.Tokens, cfg)
	if err != nil {
		return nil, nil, false, err
	}
	return normalized, nil, false, nil
}

func parseInlineTokens(raw string, defaultTTL time.Duration, now time.Time) ([]TokenRecord, error) {
	parts := strings.Split(raw, ";")
	out := make([]TokenRecord, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		tokenSpec := p
		tokenTTL := defaultTTL
		if pipe := strings.LastIndex(p, "|"); pipe > 0 && pipe < len(p)-1 {
			d, err := time.ParseDuration(strings.TrimSpace(p[pipe+1:]))
			if err != nil {
				return nil, fmt.Errorf("invalid MAILBOX_TOKENS ttl in %q: %w", p, err)
			}
			if d <= 0 {
				return nil, fmt.Errorf("MAILBOX_TOKENS ttl must be > 0 in %q", p)
			}
			tokenTTL = d
			tokenSpec = strings.TrimSpace(p[:pipe])
		}

		eq := strings.Index(tokenSpec, "=")
		colon := strings.Index(tokenSpec, ":")
		if eq <= 0 || colon <= eq {
			return nil, fmt.Errorf("invalid MAILBOX_TOKENS entry %q", p)
		}
		rawToken := strings.TrimSpace(tokenSpec[:eq])
		team := strings.TrimSpace(tokenSpec[eq+1 : colon])
		scopeList := strings.TrimSpace(tokenSpec[colon+1:])
		if rawToken == "" || team == "" || scopeList == "" {
			return nil, fmt.Errorf("invalid MAILBOX_TOKENS entry %q", p)
		}
		scopes := []string{}
		for _, s := range strings.Split(scopeList, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				scopes = append(scopes, s)
			}
		}
		if len(scopes) == 0 {
			return nil, fmt.Errorf("MAILBOX_TOKENS entry %q must include at least one scope", p)
		}
		expires := now.Add(tokenTTL)
		created := now
		out = append(out, TokenRecord{
			ID:        fmt.Sprintf("inline-%d", i+1),
			Token:     rawToken,
			TeamID:    team,
			Subject:   fmt.Sprintf("inline-%d", i+1),
			Scopes:    scopes,
			CreatedAt: &created,
			ExpiresAt: &expires,
		})
	}
	return out, nil
}

func normalizeTokenRecords(records []TokenRecord, cfg Config) ([]TokenRecord, error) {
	if len(records) == 0 {
		return nil, errors.New("no tokens configured")
	}

	seen := map[string]bool{}
	normalized := make([]TokenRecord, 0, len(records))
	for i, in := range records {
		id := strings.TrimSpace(in.ID)
		if id == "" {
			id = fmt.Sprintf("token-%d", i+1)
		}

		teamID := strings.TrimSpace(in.TeamID)
		if teamID == "" {
			return nil, fmt.Errorf("token %q missing team_id", id)
		}
		subject := strings.TrimSpace(in.Subject)
		if subject == "" {
			subject = id
		}

		scopes := normalizeScopes(in.Scopes)
		if len(scopes) == 0 {
			return nil, fmt.Errorf("token %q must include at least one scope", id)
		}

		hash := strings.TrimSpace(in.TokenHash)
		rawToken := strings.TrimSpace(in.Token)
		if hash == "" {
			if rawToken == "" {
				return nil, fmt.Errorf("token %q must define token_hash (or token when MAILBOX_ALLOW_PLAINTEXT_TOKENS=true)", id)
			}
			if !cfg.AllowPlaintextTokens {
				return nil, fmt.Errorf("token %q uses plaintext token; set MAILBOX_ALLOW_PLAINTEXT_TOKENS=true only for migration and switch to token_hash", id)
			}
			hash = tokens.Hash(rawToken)
		}
		hash, err := normalizeTokenHash(hash)
		if err != nil {
			return nil, fmt.Errorf("token %q has invalid token_hash: %w", id, err)
		}
		if seen[hash] {
			return nil, fmt.Errorf("duplicate token hash for token %q", id)
		}
		seen[hash] = true

		if in.ExpiresAt == nil {
			return nil, fmt.Errorf("token %q missing expires_at", id)
		}
		expires := in.ExpiresAt.UTC()

		var created *time.Time
		if in.CreatedAt != nil {
			c := in.CreatedAt.UTC()
			created = &c
			if expires.Before(c) {
				return nil, fmt.Errorf("token %q has expires_at before created_at", id)
			}
		}

		normalized = append(normalized, TokenRecord{
			ID:        id,
			TokenHash: hash,
			TeamID:    teamID,
			Subject:   subject,
			Scopes:    scopes,
			CreatedAt: created,
			ExpiresAt: &expires,
			Revoked:   in.Revoked,
		})
	}

	return normalized, nil
}

func generateBootstrapTokenFiles(cfg Config, now time.Time) ([]TokenRecord, []BootstrapTokenSecret, error) {
	records := make([]TokenRecord, 0, cfg.BootstrapTokenCount)
	secrets := make([]BootstrapTokenSecret, 0, cfg.BootstrapTokenCount)
	for i := 0; i < cfg.BootstrapTokenCount; i++ {
		rawToken, err := tokens.Generate()
		if err != nil {
			return nil, nil, err
		}
		id := fmt.Sprintf("bootstrap-%d", i+1)
		expiresAt := now.Add(cfg.BootstrapTokenTTL).UTC()
		createdAt := now.UTC()

		records = append(records, TokenRecord{
			ID:        id,
			TokenHash: tokens.Hash(rawToken),
			TeamID:    cfg.BootstrapTeamID,
			Subject:   id,
			Scopes:    append([]string(nil), cfg.BootstrapTokenScopes...),
			CreatedAt: &createdAt,
			ExpiresAt: &expiresAt,
		})
		secrets = append(secrets, BootstrapTokenSecret{
			ID:        id,
			Token:     rawToken,
			TeamID:    cfg.BootstrapTeamID,
			Subject:   id,
			Scopes:    append([]string(nil), cfg.BootstrapTokenScopes...),
			ExpiresAt: expiresAt,
		})
	}

	if cfg.BootstrapTokensFile != "" && filepath.Clean(cfg.BootstrapTokensFile) == filepath.Clean(cfg.TokensJSONFile) {
		return nil, nil, errors.New("MAILBOX_BOOTSTRAP_TOKENS_FILE must be different from MAILBOX_TOKENS_JSON_FILE")
	}
	if cfg.BootstrapTokensFile != "" {
		secretPayload := bootstrapSecretsFile{
			GeneratedAt: now.UTC(),
			TeamID:      cfg.BootstrapTeamID,
			Tokens:      secrets,
		}
		if err := writeJSONSecure(cfg.BootstrapTokensFile, secretPayload); err != nil {
			return nil, nil, fmt.Errorf("write bootstrap token secrets file %q: %w", cfg.BootstrapTokensFile, err)
		}
	}
	payload := tokenFile{Tokens: records}
	if err := writeJSONSecure(cfg.TokensJSONFile, payload); err != nil {
		if cfg.BootstrapTokensFile != "" {
			_ = os.Remove(cfg.BootstrapTokensFile)
		}
		return nil, nil, fmt.Errorf("write bootstrap token file %q: %w", cfg.TokensJSONFile, err)
	}
	return records, secrets, nil
}

func cloneTokenRecords(in []TokenRecord) []TokenRecord {
	out := make([]TokenRecord, 0, len(in))
	for _, rec := range in {
		cp := rec
		cp.Scopes = append([]string(nil), rec.Scopes...)
		if rec.CreatedAt != nil {
			t := rec.CreatedAt.UTC()
			cp.CreatedAt = &t
		}
		if rec.ExpiresAt != nil {
			t := rec.ExpiresAt.UTC()
			cp.ExpiresAt = &t
		}
		out = append(out, cp)
	}
	return out
}

func writeJSONSecure(path string, payload any) error {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return err
	}
	_ = os.Chmod(path, 0o600)
	return nil
}

func normalizeScopes(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]bool{}
	for _, s := range in {
		s = strings.TrimSpace(strings.ToLower(s))
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func normalizeTokenHash(v string) (string, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	if !strings.HasPrefix(v, "sha256:") {
		return "", errors.New("token_hash must use sha256:<hex>")
	}
	hexPart := strings.TrimPrefix(v, "sha256:")
	if len(hexPart) != 64 {
		return "", errors.New("sha256 hash must be 64 hex characters")
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return "", fmt.Errorf("hash is not valid hex: %w", err)
	}
	return "sha256:" + hexPart, nil
}

func hasActiveToken(records []TokenRecord, now time.Time) bool {
	for _, r := range records {
		if r.Revoked {
			continue
		}
		if r.ExpiresAt != nil && now.Before(r.ExpiresAt.UTC()) {
			return true
		}
	}
	return false
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envIntStrict(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid integer %q: %w", key, v, err)
	}
	return n, nil
}

func envBoolStrict(key string, fallback bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true, nil
	case "0", "false", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s has invalid boolean %q (expected true/false)", key, v)
	}
}

func envDurationStrict(key string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid duration %q: %w", key, v, err)
	}
	return d, nil
}

func envCSV(key string, fallback []string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return append([]string(nil), fallback...)
	}
	out := []string{}
	for _, item := range strings.Split(v, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, strings.ToLower(item))
		}
	}
	if len(out) == 0 {
		return append([]string(nil), fallback...)
	}
	return out
}
