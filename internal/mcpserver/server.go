package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
)

type PrincipalResolver func(context.Context) (model.Principal, bool)

func NewMailboxServer(svc *service.Service, resolver PrincipalResolver) *server.MCPServer {
	s := server.NewMCPServer("agent-mailbox", "1.0.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(true, true),
	)

	s.AddTool(newRegisterTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.RegisterInput{
			TeamID:                 asString(req.GetArguments()["team_id"]),
			AgentID:                asString(req.GetArguments()["agent_id"]),
			DisplayName:            asString(req.GetArguments()["display_name"]),
			Tags:                   asStringSlice(req.GetArguments()["tags"]),
			Capabilities:           asStringSlice(req.GetArguments()["capabilities"]),
			ReplaceExistingSession: asBool(req.GetArguments()["replace_existing_session"], false),
		}
		return svc.RegisterAgent(ctx, p, in)
	}))

	s.AddTool(newSendTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.SendInput{
			SessionID:      asString(req.GetArguments()["session_id"]),
			TeamID:         asString(req.GetArguments()["team_id"]),
			To:             asMap(req.GetArguments()["to"]),
			Priority:       req.GetArguments()["priority"],
			Topic:          asString(req.GetArguments()["topic"]),
			Body:           asString(req.GetArguments()["body"]),
			InReplyTo:      asString(req.GetArguments()["in_reply_to"]),
			Attachments:    asMapSlice(req.GetArguments()["attachments"]),
			TTLSeconds:     asInt(req.GetArguments()["ttl_seconds"], 0),
			RequireAck:     asBool(req.GetArguments()["require_ack"], true),
			ReadReceipt:    asString(req.GetArguments()["read_receipt"]),
			IncludeSelf:    asBool(req.GetArguments()["include_self"], false),
			IdempotencyKey: asString(req.GetArguments()["idempotency_key"]),
		}
		return svc.SendMessage(ctx, p, in)
	}))

	s.AddTool(newPollTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.PollInput{
			SessionID:   asString(req.GetArguments()["session_id"]),
			TeamID:      asString(req.GetArguments()["team_id"]),
			MaxMessages: asInt(req.GetArguments()["max_messages"], 20),
			MinPriority: req.GetArguments()["min_priority"],
			WaitMS:      asInt(req.GetArguments()["wait_ms"], 0),
			Cursor:      asString(req.GetArguments()["cursor"]),
		}
		return svc.PollInbox(ctx, p, in)
	}))

	s.AddTool(newAckTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.AckInput{
			SessionID:  asString(req.GetArguments()["session_id"]),
			TeamID:     asString(req.GetArguments()["team_id"]),
			MessageIDs: asStringSlice(req.GetArguments()["message_ids"]),
			AckKind:    asString(req.GetArguments()["ack_kind"]),
		}
		return svc.AckMessages(ctx, p, in)
	}))

	s.AddTool(newListAgentsTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.ListAgentsInput{
			SessionID:      asString(req.GetArguments()["session_id"]),
			TeamID:         asString(req.GetArguments()["team_id"]),
			FilterTag:      asString(req.GetArguments()["filter_tag"]),
			IncludeOffline: asBool(req.GetArguments()["include_offline"], false),
		}
		return svc.ListAgents(ctx, p, in)
	}))

	s.AddTool(newDeregisterTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.DeregisterInput{
			SessionID: asString(req.GetArguments()["session_id"]),
			TeamID:    asString(req.GetArguments()["team_id"]),
			Reason:    asString(req.GetArguments()["reason"]),
		}
		return svc.DeregisterAgent(ctx, p, in)
	}))

	s.AddTool(newCancelTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.CancelInput{
			SessionID: asString(req.GetArguments()["session_id"]),
			MessageID: asString(req.GetArguments()["message_id"]),
		}
		return svc.CancelMessage(ctx, p, in)
	}))

	s.AddTool(newSetStatusTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.StatusInput{
			SessionID: asString(req.GetArguments()["session_id"]),
			Status:    asString(req.GetArguments()["status"]),
			Note:      asString(req.GetArguments()["note"]),
		}
		return svc.SetAgentStatus(ctx, p, in)
	}))

	s.AddTool(newLogTool(), withEnvelope(resolver, func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError) {
		in := service.LogInput{
			SessionID: asString(req.GetArguments()["session_id"]),
			Since:     asString(req.GetArguments()["since"]),
			Limit:     asInt(req.GetArguments()["limit"], 100),
			InReplyTo: asString(req.GetArguments()["in_reply_to"]),
		}
		return svc.GetMessageLog(ctx, p, in)
	}))

	s.AddResourceTemplate(
		mcp.NewResourceTemplate(
			"mailbox://inbox/{session_id}",
			"Agent Inbox",
			mcp.WithTemplateDescription("Pending messages in the agent's mailbox (read-only, no state changes)."),
			mcp.WithTemplateMIMEType("application/json"),
		),
		inboxResourceHandler(svc, resolver, 0),
	)
	s.AddResourceTemplate(
		mcp.NewResourceTemplate(
			"mailbox://inbox/{session_id}/urgent",
			"Urgent Inbox",
			mcp.WithTemplateDescription("Only urgent (priority=3) pending messages."),
			mcp.WithTemplateMIMEType("application/json"),
		),
		inboxResourceHandler(svc, resolver, 3),
	)

	return s
}

func inboxResourceHandler(svc *service.Service, resolver PrincipalResolver, minPriority int) server.ResourceTemplateHandlerFunc {
	return func(ctx context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		sessionID := extractSessionID(req.Params.URI)
		if sessionID == "" {
			return nil, fmt.Errorf("missing session_id in resource URI")
		}

		var (
			p  model.Principal
			ok bool
		)
		if resolver != nil {
			p, ok = resolver(ctx)
		}
		if !ok {
			p, ok = auth.FromContext(ctx)
		}
		if !ok {
			return nil, fmt.Errorf("unauthenticated")
		}

		messages, apiErr := svc.PeekInbox(ctx, p, sessionID, minPriority)
		if apiErr != nil {
			return nil, fmt.Errorf("%s: %s", apiErr.Code, apiErr.Message)
		}

		data, _ := json.Marshal(map[string]any{
			"messages": messages,
			"count":    len(messages),
		})
		return []mcp.ResourceContents{
			mcp.TextResourceContents{
				URI:      req.Params.URI,
				MIMEType: "application/json",
				Text:     string(data),
			},
		}, nil
	}
}

func extractSessionID(uri string) string {
	const prefix = "mailbox://inbox/"
	if !strings.HasPrefix(uri, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(uri, prefix)
	rest = strings.TrimSuffix(rest, "/urgent")
	rest = strings.TrimSpace(rest)
	if rest == "" || strings.Contains(rest, "/") {
		return ""
	}
	return rest
}

type toolFn func(ctx context.Context, req mcp.CallToolRequest, p model.Principal) (map[string]any, *model.APIError)

func withEnvelope(resolver PrincipalResolver, fn toolFn) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var (
			p  model.Principal
			ok bool
		)
		if resolver != nil {
			p, ok = resolver(ctx)
		}
		if !ok {
			p, ok = auth.FromContext(ctx)
		}
		if !ok {
			return toolError(model.NewError("SESSION_INVALID", "request is missing authenticated principal", nil)), nil
		}
		out, err := fn(ctx, req, p)
		if err != nil {
			return toolError(err), nil
		}
		result := mcp.NewToolResultStructured(out, "ok")
		return result, nil
	}
}

func toolError(err *model.APIError) *mcp.CallToolResult {
	envelope := map[string]any{
		"error": map[string]any{
			"code":    err.Code,
			"message": err.Message,
			"details": err.Details,
		},
	}
	result := mcp.NewToolResultStructured(envelope, fmt.Sprintf("%s: %s", err.Code, err.Message))
	result.IsError = true
	return result
}

func newRegisterTool() mcp.Tool {
	return mcp.NewTool("register_agent",
		mcp.WithDescription("Register or reclaim an agent mailbox session."),
		mcp.WithString("team_id", mcp.Required(), mcp.Description("Team namespace")),
		mcp.WithString("agent_id", mcp.Required(), mcp.Description("Stable agent id")),
		mcp.WithString("display_name", mcp.Description("Optional human-readable name")),
		mcp.WithArray("tags", mcp.Description("Optional tags"), mcp.WithStringItems()),
		mcp.WithArray("capabilities", mcp.Description("Optional free-form capabilities"), mcp.WithStringItems()),
		mcp.WithBoolean("replace_existing_session", mcp.Description("Invalidate active session if present")),
	)
}

func newSendTool() mcp.Tool {
	return mcp.NewTool("send_message",
		mcp.WithDescription("Send a direct or broadcast message."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("team_id", mcp.Description("Optional explicit team consistency check")),
		mcp.WithObject("to", mcp.Required(), mcp.Description("Routing object: direct or broadcast")),
		mcp.WithAny("priority", mcp.Required(), mcp.Description("0-3 or LOW|NORMAL|HIGH|URGENT")),
		mcp.WithString("topic", mcp.Required()),
		mcp.WithString("body", mcp.Required()),
		mcp.WithString("in_reply_to"),
		mcp.WithArray("attachments"),
		mcp.WithNumber("ttl_seconds"),
		mcp.WithBoolean("require_ack"),
		mcp.WithString("read_receipt"),
		mcp.WithBoolean("include_self"),
		mcp.WithString("idempotency_key"),
	)
}

func newPollTool() mcp.Tool {
	return mcp.NewTool("poll_inbox",
		mcp.WithDescription("Poll inbox with keyset cursor and optional long-poll."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("team_id"),
		mcp.WithNumber("max_messages"),
		mcp.WithAny("min_priority"),
		mcp.WithNumber("wait_ms"),
		mcp.WithString("cursor"),
	)
}

func newAckTool() mcp.Tool {
	return mcp.NewTool("ack_messages",
		mcp.WithDescription("Acknowledge one or more delivered messages."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("team_id"),
		mcp.WithArray("message_ids", mcp.Required(), mcp.WithStringItems()),
		mcp.WithString("ack_kind"),
	)
}

func newListAgentsTool() mcp.Tool {
	return mcp.NewTool("list_agents",
		mcp.WithDescription("List online or all agents in the team."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("team_id"),
		mcp.WithString("filter_tag"),
		mcp.WithBoolean("include_offline"),
	)
}

func newDeregisterTool() mcp.Tool {
	return mcp.NewTool("deregister_agent",
		mcp.WithDescription("Mark agent offline and invalidate session."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("team_id"),
		mcp.WithString("reason"),
	)
}

func newCancelTool() mcp.Tool {
	return mcp.NewTool("cancel_message",
		mcp.WithDescription("Cancel unacked deliveries for a message sent by this session."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("message_id", mcp.Required()),
	)
}

func newSetStatusTool() mcp.Tool {
	return mcp.NewTool("set_agent_status",
		mcp.WithDescription("Set lightweight status: idle|busy|blocked."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("status", mcp.Required()),
		mcp.WithString("note"),
	)
}

func newLogTool() mcp.Tool {
	return mcp.NewTool("get_message_log",
		mcp.WithDescription("Read message event log for auditing/debugging."),
		mcp.WithString("session_id", mcp.Required()),
		mcp.WithString("since"),
		mcp.WithNumber("limit"),
		mcp.WithString("in_reply_to"),
	)
}

func asMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	b, _ := json.Marshal(v)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	return out
}

func asMapSlice(v any) []map[string]any {
	if v == nil {
		return nil
	}
	if raw, ok := v.([]any); ok {
		out := make([]map[string]any, 0, len(raw))
		for _, item := range raw {
			out = append(out, asMap(item))
		}
		return out
	}
	b, _ := json.Marshal(v)
	var out []map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", x)
	}
}

func asStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	if arr, ok := v.([]any); ok {
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			s := strings.TrimSpace(asString(item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	if arr, ok := v.([]string); ok {
		out := make([]string, 0, len(arr))
		for _, s := range arr {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func asBool(v any, fallback bool) bool {
	if v == nil {
		return fallback
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		default:
			return fallback
		}
	case float64:
		return x != 0
	default:
		return fallback
	}
}

func asInt(v any, fallback int) int {
	if v == nil {
		return fallback
	}
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return fallback
		}
		return int(n)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return fallback
		}
		return n
	default:
		return fallback
	}
}
