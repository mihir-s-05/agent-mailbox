package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/events"
	"github.com/mihir/msg-com/internal/mailboxapi"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type PrincipalResolver func(context.Context) (model.Principal, bool)
type Server struct {
	MCP      *mcp.Server
	API      *mailboxapi.API
	svc      *service.Service
	resolver PrincipalResolver
	events   *events.Service
	validate events.PrincipalFunc
	bearer   func(string) (model.Principal, bool)
	mu       sync.Mutex
	watches  map[string]map[*mcp.ServerSession]watch
}
type watch struct {
	principal model.Principal
	agent     string
	revision  int64
	seen      bool
	urgent    bool
}

func NewMailboxServer(svc *service.Service, resolver PrincipalResolver, eventService *events.Service) *Server {
	wrapper := &Server{svc: svc, resolver: resolver, events: eventService, API: mailboxapi.New(svc), watches: map[string]map[*mcp.ServerSession]watch{}}
	resolve := wrapper.principal
	subscribe := func(ctx context.Context, req *mcp.SubscribeRequest) error {
		p, ok := resolve(ctx)
		if !ok {
			return fmt.Errorf("unauthenticated")
		}
		agent, urgent, err := wrapper.authorizeURI(ctx, p, req.Params.URI)
		if err != nil {
			return err
		}
		wrapper.mu.Lock()
		if wrapper.watches[req.Params.URI] == nil {
			wrapper.watches[req.Params.URI] = map[*mcp.ServerSession]watch{}
		}
		wrapper.watches[req.Params.URI][req.Session] = watch{principal: p, agent: agent, urgent: urgent}
		wrapper.mu.Unlock()
		return nil
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "agent-mailbox", Version: "2.0.0"}, &mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{Resources: &mcp.ResourceCapabilities{Subscribe: true}}, SubscribeHandler: subscribe, UnsubscribeHandler: func(_ context.Context, req *mcp.UnsubscribeRequest) error {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		delete(wrapper.watches[req.Params.URI], req.Session)
		if len(wrapper.watches[req.Params.URI]) == 0 {
			delete(wrapper.watches, req.Params.URI)
		}
		return nil
	}})
	wrapper.MCP = server
	for _, definition := range wrapper.API.Definitions() {
		server.AddTool(&mcp.Tool{Name: definition.Name, Description: definition.Description, InputSchema: definition.InputSchema, OutputSchema: map[string]any{"type": "object"}, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: definition.ReadOnly}}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			p, ok := resolve(ctx)
			if !ok {
				return toolResult(nil, model.NewError("AUTH_INVALID", "unauthenticated", nil)), nil
			}
			out, err := wrapper.API.Call(ctx, p, req.Params.Name, req.Params.Arguments)
			return toolResult(out, err), nil
		})
	}
	for _, template := range []string{"mailbox://inbox/{session_id}", "mailbox://inbox/{session_id}/urgent", "mailbox://teams/{team_id}/agents/{agent_id}/inbox", "mailbox://teams/{team_id}/agents/{agent_id}/inbox/urgent"} {
		server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: template, Name: "Agent Inbox", Description: "Read outstanding mailbox messages without changing delivery state.", MIMEType: "application/json"}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			p, ok := resolve(ctx)
			if !ok {
				return nil, fmt.Errorf("unauthenticated")
			}
			agent, urgent, err := wrapper.authorizeURI(ctx, p, req.Params.URI)
			if err != nil {
				return nil, err
			}
			minimum := 0
			if urgent {
				minimum = 3
			}
			messages, aerr := svc.PeekMailbox(ctx, p, agent, minimum)
			if aerr != nil {
				return nil, aerr
			}
			data, _ := json.Marshal(map[string]any{"messages": messages, "count": len(messages)})
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: req.Params.URI, MIMEType: "application/json", Text: string(data)}}}, nil
		})
	}
	if eventService != nil {
		wrapper.registerEvents()
	}
	// Authenticate every RPC. The SDK passes HTTP request headers in RequestExtra,
	// allowing callers to install a credential resolver without transport sessions.
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			p, ok := resolve(ctx)
			if extra := req.GetExtra(); extra != nil && extra.Header != nil && wrapper.bearer != nil {
				raw := strings.TrimSpace(extra.Header.Get("Authorization"))
				ok = false
				if len(raw) > 7 && strings.EqualFold(raw[:7], "Bearer ") {
					p, ok = wrapper.bearer(strings.TrimSpace(raw[7:]))
				}
			}
			if !ok {
				return nil, &jsonrpc.Error{Code: -32001, Message: "unauthenticated"}
			}
			ctx = auth.WithPrincipal(ctx, p)
			if method == "subscriptions/listen" {
				params, valid := req.GetParams().(*mcp.SubscriptionsListenParams)
				if !valid || params.Notifications == nil {
					return nil, &jsonrpc.Error{Code: -32602, Message: "notifications are required"}
				}
				for _, uri := range params.Notifications.ResourceSubscriptions {
					if _, _, err := wrapper.authorizeURI(ctx, p, uri); err != nil {
						return nil, err
					}
				}
				stream, cancel := context.WithCancel(ctx)
				defer cancel()
				go wrapper.guardStream(stream, cancel, p, params.Notifications.ResourceSubscriptions)
				ctx = stream
			}
			result, err := next(ctx, method, req)
			if err == nil && method == "server/discover" && eventService != nil {
				if discovery, ok := result.(*mcp.DiscoverResult); ok {
					return &discoveryResult{DiscoverResult: discovery}, nil
				}
			}
			return result, err
		}
	})
	// The SDK fans out by URI. Check each subscriber at the sending boundary,
	// including ownership transfers and credential revocation after subscription.
	server.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "notifications/resources/updated" {
				params, ok := req.GetParams().(*mcp.ResourceUpdatedNotificationParams)
				if !ok {
					return nil, nil
				}
				session, ok := req.GetSession().(*mcp.ServerSession)
				if !ok {
					return nil, nil
				}
				wrapper.mu.Lock()
				w, ok := wrapper.watches[params.URI][session]
				wrapper.mu.Unlock()
				if !ok {
					return nil, nil
				}
				p, ok := wrapper.current(w.principal)
				if !ok {
					return nil, nil
				}
				if _, _, err := wrapper.authorizeURI(ctx, p, params.URI); err != nil {
					return nil, nil
				}
			}
			return next(ctx, method, req)
		}
	})
	return wrapper
}
func (s *Server) principal(ctx context.Context) (model.Principal, bool) {
	if p, ok := auth.FromContext(ctx); ok {
		return s.current(p)
	}
	if s.resolver != nil {
		if p, ok := s.resolver(ctx); ok {
			return s.current(p)
		}
	}
	return auth.FromContext(ctx)
}
func (s *Server) SetPrincipalValidator(validate events.PrincipalFunc)            { s.validate = validate }
func (s *Server) SetBearerResolver(resolve func(string) (model.Principal, bool)) { s.bearer = resolve }
func (s *Server) current(p model.Principal) (model.Principal, bool) {
	if s.validate != nil {
		return s.validate(p.TeamID, p.Subject, p.Token)
	}
	return p, p.TeamID != "" && p.Subject != ""
}
func (s *Server) guardStream(ctx context.Context, cancel context.CancelFunc, p model.Principal, uris []string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current, ok := s.current(p)
			if !ok {
				cancel()
				return
			}
			for _, uri := range uris {
				if _, _, err := s.authorizeURI(ctx, current, uri); err != nil {
					cancel()
					return
				}
			}
		}
	}
}
func toolResult(out map[string]any, err *model.APIError) *mcp.CallToolResult {
	if err != nil {
		out = map[string]any{"error": map[string]any{"code": err.Code, "message": err.Message, "details": err.Details}}
	}
	data, _ := json.Marshal(out)
	return &mcp.CallToolResult{IsError: err != nil, StructuredContent: out, Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}}
}
func StableURI(team, agent string) string {
	return "mailbox://teams/" + url.PathEscape(team) + "/agents/" + url.PathEscape(agent) + "/inbox"
}
func (s *Server) authorizeURI(ctx context.Context, p model.Principal, raw string) (string, bool, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "mailbox" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", false, fmt.Errorf("invalid mailbox URI")
	}
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	urgent := false
	if len(parts) > 0 && parts[len(parts)-1] == "urgent" {
		urgent = true
		parts = parts[:len(parts)-1]
	}
	if u.Host == "inbox" && len(parts) == 1 {
		id, err := url.PathUnescape(parts[0])
		if err != nil {
			return "", false, err
		}
		agent, aerr := s.svc.AuthorizedSession(ctx, p, id)
		if aerr != nil {
			return "", false, aerr
		}
		if aerr = s.svc.AuthorizeMailbox(ctx, p, agent); aerr != nil {
			return "", false, aerr
		}
		return agent, urgent, nil
	}
	if u.Host != "teams" || len(parts) != 4 || parts[1] != "agents" || parts[3] != "inbox" {
		return "", false, fmt.Errorf("invalid mailbox URI")
	}
	team, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", false, err
	}
	agent, err := url.PathUnescape(parts[2])
	if err != nil {
		return "", false, err
	}
	if team != p.TeamID {
		return "", false, fmt.Errorf("mailbox team mismatch")
	}
	if err := s.svc.AuthorizeMailbox(ctx, p, agent); err != nil {
		return "", false, err
	}
	return agent, urgent, nil
}

// Resource subscriptions are transient; periodic revision reconciliation also
// repairs missed LISTEN notifications across separate server processes.
func (s *Server) RunResourceNotifications(ctx context.Context, validate func(string, string, string) (model.Principal, bool)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		live := map[*mcp.ServerSession]bool{}
		for session := range s.MCP.Sessions() {
			live[session] = true
		}
		s.mu.Lock()
		snapshot := make(map[string]map[*mcp.ServerSession]watch, len(s.watches))
		for uri, subscribers := range s.watches {
			snapshot[uri] = map[*mcp.ServerSession]watch{}
			for session, w := range subscribers {
				if !live[session] {
					delete(subscribers, session)
					continue
				}
				snapshot[uri][session] = w
			}
			if len(subscribers) == 0 {
				delete(s.watches, uri)
			}
		}
		s.mu.Unlock()
		for uri, subscribers := range snapshot {
			for session, w := range subscribers {
				p, ok := validate(w.principal.TeamID, w.principal.Subject, w.principal.Token)
				if !ok || s.svc.AuthorizeMailbox(ctx, p, w.agent) != nil {
					continue
				}
				revision, err := s.svc.InboxRevision(ctx, p, w.agent)
				if err != nil {
					continue
				}
				if !w.seen || revision != w.revision {
					if err = s.MCP.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: uri}); err == nil {
						s.mu.Lock()
						latest, exists := s.watches[uri][session]
						latest.revision = revision
						latest.seen = true
						if exists {
							s.watches[uri][session] = latest
						}
						s.mu.Unlock()
					}
				}
			}
		}
	}
}

type eventParams struct {
	mcp.ParamsBase
	Name      string          `json:"name,omitempty"`
	Arguments map[string]any  `json:"arguments,omitempty"`
	Delivery  events.Delivery `json:"delivery,omitempty"`
	Cursor    *string         `json:"cursor,omitempty"`
	TTLMS     json.RawMessage `json:"ttlMs,omitempty"`
}
type wireResult struct {
	mcp.ResultBase
	Data map[string]any
}

// Preserve the SDK's result metadata, completeness and cache fields while adding
// the Events extension, which is not yet a field on ServerCapabilities.
type discoveryResult struct{ *mcp.DiscoverResult }

func (r *discoveryResult) MarshalJSON() ([]byte, error) {
	raw, err := json.Marshal(r.DiscoverResult)
	if err != nil {
		return nil, err
	}
	var data map[string]any
	if err = json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	if caps, ok := data["capabilities"].(map[string]any); ok {
		caps["events"] = map[string]any{}
	}
	return json.Marshal(data)
}

func (r *wireResult) MarshalJSON() ([]byte, error) {
	data := make(map[string]any, len(r.Data)+1)
	for key, value := range r.Data {
		data[key] = value
	}
	if meta := r.GetMeta(); len(meta) > 0 {
		data["_meta"] = meta
	}
	return json.Marshal(data)
}
func (s *Server) registerEvents() {
	for _, method := range []string{"events/list", "events/subscribe", "events/unsubscribe"} {
		handler := func(ctx context.Context, _ *mcp.ServerSession, in *eventParams) (*wireResult, error) {
			p, ok := s.principal(ctx)
			if !ok {
				return nil, &jsonrpc.Error{Code: -32001, Message: "unauthenticated"}
			}
			if in == nil {
				in = &eventParams{}
			}
			if method == "events/list" {
				if !p.HasScope("poll:self") {
					return &wireResult{Data: map[string]any{"events": []any{}}}, nil
				}
				if in.Cursor != nil {
					return nil, &jsonrpc.Error{Code: -32602, Message: "unknown event catalog cursor"}
				}
				return &wireResult{Data: map[string]any{"events": events.Catalog()}}, nil
			}
			input := events.SubscribeInput{Name: in.Name, Arguments: in.Arguments, Delivery: in.Delivery, Cursor: in.Cursor, TTLMS: in.TTLMS}
			if method == "events/unsubscribe" {
				if err := s.events.Unsubscribe(ctx, p, input); err != nil {
					return nil, eventError(err)
				}
				return &wireResult{Data: map[string]any{}}, nil
			}
			result, err := s.events.Subscribe(ctx, p, input)
			if err != nil {
				return nil, eventError(err)
			}
			raw, _ := json.Marshal(result)
			var data map[string]any
			_ = json.Unmarshal(raw, &data)
			return &wireResult{Data: data}, nil
		}
		if err := mcp.AddReceivingCustomMethod(s.MCP, method, handler); err != nil {
			panic(err)
		}
	}
}
func eventError(err error) error {
	var protocol *events.ProtocolError
	if errors.As(err, &protocol) {
		return &jsonrpc.Error{Code: protocol.Code, Message: protocol.Message, Data: json.RawMessage(fmt.Sprintf(`{"reason":%q}`, protocol.Reason))}
	}
	return &jsonrpc.Error{Code: -32603, Message: "event operation failed"}
}
