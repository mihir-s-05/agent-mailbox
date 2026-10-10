package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/events"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/testutil"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func TestProtocolIntegration(t *testing.T) {
	db := testutil.Database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owner := model.Principal{TeamID: "protocol", Subject: "owner", Token: "owner", Scopes: map[string]bool{"*": true}}
	other := owner
	other.Subject = "other"
	other.Token = "other"
	var revoked atomic.Bool
	lookup := func(token string) (model.Principal, bool) {
		switch token {
		case "owner":
			return owner, !revoked.Load()
		case "other":
			return other, true
		}
		return model.Principal{}, false
	}
	validate := func(team, subject, token string) (model.Principal, bool) {
		p, ok := lookup(token)
		return p, ok && p.TeamID == team && p.Subject == subject
	}
	svc := service.New(db, config.Config{MaxBodyBytes: 32768, MaxAttachments: 8, MaxBroadcastRecipients: 200, MailboxCap: 100, RateLimitPerMinute: 100, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400}, nil)
	out, err := svc.RegisterAgent(ctx, owner, service.RegisterInput{TeamID: owner.TeamID, AgentID: "agent/escaped"})
	if err != nil {
		t.Fatal(err)
	}
	session := out["session_id"].(string)
	eventService, _ := events.New(db, bytes.Repeat([]byte{1}, 32), svc.AuthorizeMailbox, validate)
	wrapper := NewMailboxServer(svc, auth.FromContext, eventService)
	wrapper.SetPrincipalValidator(validate)
	wrapper.SetBearerResolver(lookup)
	protocol := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return wrapper.MCP }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, PropagateRequestCancellation: true})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := lookup(r.Header.Get("Authorization")[7:])
		if !ok {
			http.Error(w, "unauthorized", 401)
			return
		}
		protocol.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	}))
	defer httpServer.Close()
	defer httpServer.CloseClientConnections()
	rpc := func(t *testing.T, token, method string, params map[string]any) map[string]any {
		t.Helper()
		if params == nil {
			params = map[string]any{}
		}
		params["_meta"] = map[string]any{mcp.MetaKeyProtocolVersion: "2026-07-28", mcp.MetaKeyClientInfo: map[string]any{"name": "test", "version": "1"}, mcp.MetaKeyClientCapabilities: map[string]any{}}
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequestWithContext(ctx, "POST", httpServer.URL, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("MCP-Protocol-Version", "2026-07-28")
		req.Header.Set("Mcp-Method", method)
		if name, ok := params["name"].(string); ok {
			req.Header.Set("Mcp-Name", name)
		} else if uri, ok := params["uri"].(string); ok {
			req.Header.Set("Mcp-Name", uri)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusUnauthorized {
			return map[string]any{"error": map[string]any{"code": -32001}}
		}
		var result map[string]any
		if json.Unmarshal(raw, &result) != nil {
			t.Fatalf("invalid protocol response %d: %s", resp.StatusCode, raw)
		}
		return result
	}
	discovery := rpc(t, "owner", "server/discover", nil)
	result, ok := discovery["result"].(map[string]any)
	if !ok {
		t.Fatal(discovery)
	}
	if _, ok := result["capabilities"].(map[string]any)["events"]; !ok {
		t.Fatal("event discovery missing", result)
	}
	if result["_meta"].(map[string]any)[mcp.MetaKeyServerInfo] == nil {
		t.Fatal("discovery lost server metadata")
	}
	if result["resultType"] != "complete" {
		t.Fatal("discovery lost complete result type", result)
	}
	catalog := rpc(t, "owner", "events/list", nil)
	if catalog["error"] != nil {
		t.Fatal(catalog)
	}
	if catalog["result"].(map[string]any)["_meta"].(map[string]any)[mcp.MetaKeyServerInfo] == nil {
		t.Fatal("custom result lost server metadata")
	}
	legacyBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"legacy","version":"1"}}}`)
	legacyReq, _ := http.NewRequestWithContext(ctx, "POST", httpServer.URL, bytes.NewReader(legacyBody))
	legacyReq.Header.Set("Authorization", "Bearer owner")
	legacyReq.Header.Set("Content-Type", "application/json")
	legacyReq.Header.Set("Accept", "application/json, text/event-stream")
	legacyReq.Header.Set("MCP-Protocol-Version", "2025-11-25")
	legacyResp, legacyErr := http.DefaultClient.Do(legacyReq)
	if legacyErr != nil {
		t.Fatal(legacyErr)
	}
	var legacyResult map[string]any
	decodeErr := json.NewDecoder(legacyResp.Body).Decode(&legacyResult)
	legacyResp.Body.Close()
	if decodeErr != nil || legacyResult["error"] != nil {
		t.Fatal("legacy initialization failed", decodeErr, legacyResult)
	}
	called := rpc(t, "owner", "tools/call", map[string]any{"name": "peek_inbox", "arguments": map[string]any{"session_id": session}})
	tool, ok := called["result"].(map[string]any)
	if !ok {
		t.Fatal(called)
	}
	if tool["structuredContent"] == nil || tool["content"].([]any)[0].(map[string]any)["text"] == "ok" {
		t.Fatal("missing structured or text JSON", tool)
	}
	borrowed := rpc(t, "other", "tools/call", map[string]any{"name": "peek_inbox", "arguments": map[string]any{"session_id": session}})
	if borrowed["result"].(map[string]any)["isError"] != true {
		t.Fatal("borrowed RPC session accepted", borrowed)
	}
	stable := StableURI(owner.TeamID, "agent/escaped")
	for _, uri := range []string{stable, stable + "/urgent", "mailbox://inbox/" + session, "mailbox://inbox/" + session + "/urgent"} {
		if _, _, err := wrapper.authorizeURI(ctx, owner, uri); err != nil {
			t.Fatal("valid resource URI rejected", uri, err)
		}
		owned := rpc(t, "owner", "resources/read", map[string]any{"uri": uri})
		if owned["error"] != nil || owned["result"] == nil {
			t.Fatal("owned resource RPC rejected", uri, owned)
		}
		if _, _, err := wrapper.authorizeURI(ctx, other, uri); err == nil {
			t.Fatal("resource crossed principal", uri)
		}
		response := rpc(t, "other", "resources/read", map[string]any{"uri": uri})
		if response["error"] == nil {
			t.Fatal("resource RPC crossed principal", response)
		}
	}
	updates := make(chan string, 8)
	client := mcp.NewClient(&mcp.Implementation{Name: "subscriber", Version: "1"}, &mcp.ClientOptions{ResourceUpdatedHandler: func(_ context.Context, req *mcp.ResourceUpdatedNotificationRequest) { updates <- req.Params.URI }})
	subscriber, connectErr := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL, HTTPClient: &http.Client{Transport: bearerTransport{"owner"}}}, nil)
	if connectErr != nil {
		t.Fatal(connectErr)
	}
	defer subscriber.Close()
	if err := subscriber.Subscribe(ctx, &mcp.SubscribeParams{URI: stable}); err != nil {
		t.Fatal(err)
	}
	wrapper.MCP.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: stable})
	select {
	case uri := <-updates:
		if uri != stable {
			t.Fatal(uri)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscribed inbox never received update")
	}
	// A new owner of the same URI must not notify an old subscription.
	if _, err := svc.RegisterAgent(ctx, other, service.RegisterInput{TeamID: owner.TeamID, AgentID: "agent/escaped", ReplaceExistingSession: true}); err != nil {
		t.Fatal(err)
	}
	wrapper.MCP.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: stable})
	select {
	case uri := <-updates:
		t.Fatal("old owner received update after transfer", uri)
	case <-time.After(150 * time.Millisecond):
	}
	revoked.Store(true)
	response := rpc(t, "owner", "tools/list", nil)
	if response["error"] == nil {
		t.Fatal("revoked credential accepted", response)
	}
}
