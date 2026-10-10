// Package web exposes the shared mailbox API to an authenticated browser page.
package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/events"
	"github.com/mihir/msg-com/internal/mailboxapi"
	"github.com/mihir/msg-com/internal/service"
)

func New(api *mailboxapi.API, svc *service.Service, eventService *events.Service, validate events.PrincipalFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/session", func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		respond(w, http.StatusOK, map[string]any{"team_id": p.TeamID, "subject": p.Subject})
	})
	mux.HandleFunc("GET /api/agents", func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		out, err := svc.AgentDirectory(r.Context(), p, service.ListAgentsInput{IncludeOffline: true})
		if err != nil {
			respond(w, http.StatusForbidden, map[string]any{"error": "agent directory access denied"})
			return
		}
		respond(w, http.StatusOK, out)
	})
	mux.HandleFunc("GET /api/inbox", func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		messages, err := svc.PeekMailbox(r.Context(), p, r.URL.Query().Get("agent_id"), 0)
		if err != nil {
			respond(w, http.StatusForbidden, map[string]any{"error": "mailbox access denied"})
			return
		}
		respond(w, http.StatusOK, map[string]any{"messages": messages, "limit": 20})
	})
	mux.HandleFunc("GET /api/log", func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		out, err := svc.PeekMailboxLog(r.Context(), p, r.URL.Query().Get("agent_id"), service.LogInput{Limit: 100})
		if err != nil {
			respond(w, http.StatusForbidden, map[string]any{"error": "mailbox log access denied"})
			return
		}
		respond(w, http.StatusOK, out)
	})
	mux.HandleFunc("GET /api/tools", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]any{"tools": api.Definitions()})
	})
	mux.HandleFunc("POST /api/call", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 512*1024)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var in struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if dec.Decode(&in) != nil || dec.Decode(new(any)) != io.EOF {
			respond(w, http.StatusBadRequest, map[string]any{"error": "invalid request"})
			return
		}
		p, _ := auth.FromContext(r.Context())
		out, err := api.Call(r.Context(), p, in.Name, in.Arguments)
		if err != nil {
			respond(w, http.StatusBadRequest, map[string]any{"error": err})
			return
		}
		respond(w, http.StatusOK, out)
	})
	mux.HandleFunc("GET /api/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		out, err := eventService.Diagnostic(r.Context(), p, r.URL.Query().Get("agent_id"))
		if err != nil {
			respond(w, http.StatusForbidden, map[string]any{"error": "subscription access denied"})
			return
		}
		respond(w, http.StatusOK, map[string]any{"subscriptions": out})
	})
	mux.HandleFunc("GET /api/inbox/stream", func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		agent := r.URL.Query().Get("agent_id")
		if svc.AuthorizeMailbox(r.Context(), p, agent) != nil {
			respond(w, http.StatusForbidden, map[string]any{"error": "mailbox access denied"})
			return
		}
		controller := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var revision int64 = -1
		heartbeat := time.Now()
		for {
			current, ok := validate(p.TeamID, p.Subject, p.Token)
			if !ok {
				return
			}
			next, err := svc.InboxRevision(r.Context(), current, agent)
			if err != nil {
				return
			}
			if next != revision {
				if _, err = fmt.Fprintf(w, "event: inbox_changed\ndata: {\"revision\":%d}\n\n", next); err != nil {
					return
				}
				revision = next
				heartbeat = time.Now()
			} else if time.Since(heartbeat) > 15*time.Second {
				if _, err = fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
					return
				}
				heartbeat = time.Now()
			}
			if controller.Flush() != nil {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		p, ok := auth.FromContext(r.Context())
		if !ok {
			respond(w, http.StatusUnauthorized, map[string]any{"error": "authentication required"})
			return
		}
		current, ok := validate(p.TeamID, p.Subject, p.Token)
		if !ok {
			respond(w, http.StatusUnauthorized, map[string]any{"error": "credential no longer active"})
			return
		}
		mux.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), current)))
	})
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
