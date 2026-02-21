package auth

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/tokens"
)

type contextKey string

const principalContextKey contextKey = "principal"

type tokenEntry struct {
	principal model.Principal
	expiresAt time.Time
	revoked   bool
}

type Registry struct {
	requireMTLS bool
	reloadEvery time.Duration
	cfg         config.Config
	now         func() time.Time

	mu     sync.RWMutex
	tokens map[string]tokenEntry
}

func NewRegistry(cfg config.Config) *Registry {
	r := &Registry{
		requireMTLS: cfg.RequireMTLS,
		reloadEvery: cfg.TokenReloadInterval,
		cfg:         cfg,
		now:         time.Now,
	}
	r.replaceTokens(cfg.Tokens)
	return r
}

func (r *Registry) Start(ctx context.Context) {
	if r.reloadEvery <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(r.reloadEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.Reload(); err != nil {
					log.Printf("token reload error: %v", err)
				}
			}
		}
	}()
}

func (r *Registry) Reload() error {
	tokenSet, err := config.ReadTokens(r.cfg)
	if err != nil {
		return err
	}
	r.replaceTokens(tokenSet)
	return nil
}

func (r *Registry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		principal, err := r.Authenticate(req)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		ctx := context.WithValue(req.Context(), principalContextKey, principal)
		next.ServeHTTP(w, req.WithContext(ctx))
	})
}

func (r *Registry) Authenticate(req *http.Request) (model.Principal, *model.APIError) {
	if r.requireMTLS {
		if err := validateClientCert(req.TLS); err != nil {
			return model.Principal{}, model.NewError("AUTH_INVALID", err.Error(), nil)
		}
	}

	authz := strings.TrimSpace(req.Header.Get("Authorization"))
	if authz == "" {
		return model.Principal{}, model.NewError("AUTH_INVALID", "missing bearer token", nil)
	}
	if len(authz) < 7 || !strings.EqualFold(authz[:7], "bearer ") {
		return model.Principal{}, model.NewError("AUTH_INVALID", "authorization header must be Bearer token", nil)
	}
	token := strings.TrimSpace(authz[7:])
	if token == "" {
		return model.Principal{}, model.NewError("AUTH_INVALID", "missing bearer token", nil)
	}

	principal, ok, expired, revoked := r.lookup(token)
	if !ok {
		return model.Principal{}, model.NewError("AUTH_INVALID", "invalid bearer token", nil)
	}
	if revoked {
		return model.Principal{}, model.NewError("AUTH_INVALID", "revoked bearer token", nil)
	}
	if expired {
		return model.Principal{}, model.NewError("AUTH_INVALID", "expired bearer token", nil)
	}
	return principal, nil
}

func (r *Registry) LookupToken(token string) (model.Principal, bool) {
	principal, ok, expired, revoked := r.lookup(token)
	if !ok || expired || revoked {
		return model.Principal{}, false
	}
	return principal, true
}

func (r *Registry) lookup(token string) (model.Principal, bool, bool, bool) {
	hash := tokens.Hash(token)
	now := r.now().UTC()

	r.mu.RLock()
	entry, ok := r.tokens[hash]
	r.mu.RUnlock()
	if !ok {
		return model.Principal{}, false, false, false
	}
	expired := !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt)
	return entry.principal, true, expired, entry.revoked
}

func (r *Registry) replaceTokens(records []config.TokenRecord) {
	next := make(map[string]tokenEntry, len(records))
	for _, t := range records {
		scopeSet := map[string]bool{}
		for _, scope := range t.Scopes {
			scope = strings.TrimSpace(strings.ToLower(scope))
			if scope != "" {
				scopeSet[scope] = true
			}
		}
		entry := tokenEntry{
			principal: model.Principal{
				Token:   t.ID,
				TeamID:  t.TeamID,
				Subject: t.Subject,
				Scopes:  scopeSet,
			},
			revoked: t.Revoked,
		}
		if t.ExpiresAt != nil {
			entry.expiresAt = t.ExpiresAt.UTC()
		}
		next[t.TokenHash] = entry
	}

	r.mu.Lock()
	r.tokens = next
	r.mu.Unlock()
}

func FromContext(ctx context.Context) (model.Principal, bool) {
	p, ok := ctx.Value(principalContextKey).(model.Principal)
	return p, ok
}

func validateClientCert(cs *tls.ConnectionState) error {
	if cs == nil {
		return errors.New("mTLS required but no TLS state")
	}
	if len(cs.PeerCertificates) == 0 {
		return errors.New("mTLS required but no client certificate")
	}
	return nil
}

func writeAPIError(w http.ResponseWriter, err *model.APIError) {
	details := err.Details
	if details == nil {
		details = map[string]any{}
	}
	envelope := map[string]any{
		"error": map[string]any{
			"code":    err.Code,
			"message": err.Message,
			"details": details,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusForCode(err.Code))
	_ = json.NewEncoder(w).Encode(envelope)
}

func statusForCode(code string) int {
	switch code {
	case "AUTH_INVALID":
		return http.StatusUnauthorized
	case "TEAM_MISMATCH":
		return http.StatusForbidden
	case "TEAM_NOT_FOUND", "AGENT_NOT_FOUND":
		return http.StatusNotFound
	default:
		return http.StatusBadRequest
	}
}
