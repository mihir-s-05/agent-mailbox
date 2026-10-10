package mailboxapi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
)

type Definition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	ReadOnly    bool           `json:"readOnly"`
	invoke      func(context.Context, model.Principal, json.RawMessage) (map[string]any, *model.APIError)
	schema      *jsonschema.Resolved
}
type API struct {
	definitions map[string]Definition
	ordered     []Definition
}

func text(max int) map[string]any { return map[string]any{"type": "string", "maxLength": max} }
func integer(minimum, maximum int) map[string]any {
	return map[string]any{"type": "integer", "minimum": minimum, "maximum": maximum}
}
func boolean() map[string]any { return map[string]any{"type": "boolean"} }
func stringsArray(max int) map[string]any {
	return map[string]any{"type": "array", "maxItems": max, "items": text(128)}
}
func object(properties map[string]any, required ...string) map[string]any {
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}
func session(properties map[string]any) map[string]any {
	properties["session_id"] = text(128)
	properties["team_id"] = text(128)
	return properties
}
func priority() map[string]any {
	return map[string]any{"oneOf": []any{integer(0, 3), map[string]any{"type": "string", "enum": []string{"LOW", "NORMAL", "HIGH", "URGENT"}}}}
}
func decode[T any](fn func(context.Context, model.Principal, T) (map[string]any, *model.APIError)) func(context.Context, model.Principal, json.RawMessage) (map[string]any, *model.APIError) {
	return func(ctx context.Context, p model.Principal, raw json.RawMessage) (map[string]any, *model.APIError) {
		var in T
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, model.NewError("INVALID_INPUT", "invalid arguments", nil)
		}
		return fn(ctx, p, in)
	}
}

func New(svc *service.Service) *API {
	routing := map[string]any{"oneOf": []any{
		object(map[string]any{"type": map[string]any{"const": "direct"}, "agent_id": text(128)}, "type", "agent_id"),
		object(map[string]any{"type": map[string]any{"const": "broadcast"}, "scope": map[string]any{"const": "team"}}, "type", "scope"),
		object(map[string]any{"type": map[string]any{"const": "broadcast"}, "scope": map[string]any{"const": "tag"}, "tag": text(128)}, "type", "scope", "tag"),
	}}
	definitions := []Definition{
		{Name: "register_agent", Description: "Register or resume an agent mailbox. Replacement invalidates the previous session; other owners require agents:takeover.", InputSchema: object(map[string]any{"team_id": text(128), "agent_id": text(128), "display_name": text(256), "tags": stringsArray(32), "capabilities": stringsArray(32), "replace_existing_session": boolean()}, "team_id", "agent_id"), invoke: decode(svc.RegisterAgent)},
		{Name: "send_message", Description: "Send a durable direct or broadcast message. Reuse idempotency_key for retries of the same content.", InputSchema: object(session(map[string]any{"to": routing, "priority": priority(), "topic": text(512), "body": text(32768), "in_reply_to": text(128), "attachments": map[string]any{"type": "array", "maxItems": 8, "items": map[string]any{"type": "object"}}, "ttl_seconds": integer(0, 2592000), "require_ack": boolean(), "read_receipt": map[string]any{"type": "string", "enum": []string{"NONE", "ON_DELIVER", "ON_ACK"}}, "include_self": boolean(), "idempotency_key": text(256)}), "session_id", "to", "priority", "topic", "body"), invoke: decode(svc.SendMessage)},
		{Name: "poll_inbox", Description: "Fetch outstanding messages and record delivery. Acknowledgement records receipt, not proof of completed work. Pagination cursors apply only to one snapshot.", InputSchema: object(session(map[string]any{"max_messages": integer(1, 100), "min_priority": priority(), "wait_ms": integer(0, 25000), "cursor": text(2048)}), "session_id"), invoke: decode(svc.PollInbox)},
		{Name: "ack_messages", Description: "Acknowledge up to 100 delivered messages. Duplicate acknowledgements are harmless.", InputSchema: object(session(map[string]any{"message_ids": stringsArray(100), "ack_kind": map[string]any{"type": "string", "enum": []string{"ACK"}}}), "session_id", "message_ids"), invoke: decode(svc.AckMessages)},
		{Name: "list_agents", Description: "List team agents, optionally filtered by tag and including offline mailboxes.", ReadOnly: true, InputSchema: object(session(map[string]any{"filter_tag": text(128), "include_offline": boolean()}), "session_id"), invoke: decode(svc.ListAgents)},
		{Name: "deregister_agent", Description: "Mark this agent offline and invalidate its session. Outstanding messages remain durable.", InputSchema: object(session(map[string]any{"reason": text(512)}), "session_id"), invoke: decode(svc.DeregisterAgent)},
		{Name: "cancel_message", Description: "Cancel outstanding deliveries of a message sent by this agent.", InputSchema: object(session(map[string]any{"message_id": text(128)}), "session_id", "message_id"), invoke: decode(svc.CancelMessage)},
		{Name: "set_agent_status", Description: "Set this agent's current work status.", InputSchema: object(session(map[string]any{"status": map[string]any{"type": "string", "enum": []string{"idle", "busy", "blocked"}}, "note": text(512)}), "session_id", "status"), invoke: decode(svc.SetAgentStatus)},
		{Name: "get_message_log", Description: "Read lifecycle events for messages sent or received by this mailbox.", ReadOnly: true, InputSchema: object(session(map[string]any{"since": text(64), "limit": integer(1, 1000), "in_reply_to": text(128)}), "session_id"), invoke: decode(svc.GetMessageLog)},
		{Name: "peek_inbox", Description: "Read up to 20 outstanding messages without changing delivery state.", ReadOnly: true, InputSchema: object(session(map[string]any{"min_priority": integer(0, 3)}), "session_id"), invoke: decode(func(ctx context.Context, p model.Principal, in service.PollInput) (map[string]any, *model.APIError) {
			minimum := 0
			if in.MinPriority != nil {
				var err error
				minimum, err = model.ParsePriority(in.MinPriority)
				if err != nil {
					return nil, model.NewError("INVALID_INPUT", "invalid priority", nil)
				}
			}
			messages, err := svc.PeekInbox(ctx, p, in.SessionID, minimum)
			return map[string]any{"messages": messages, "count": len(messages)}, err
		})},
	}
	a := &API{definitions: map[string]Definition{}}
	for _, d := range definitions {
		raw, _ := json.Marshal(d.InputSchema)
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			panic(err)
		}
		resolved, err := schema.Resolve(nil)
		if err != nil {
			panic(err)
		}
		d.schema = resolved
		a.definitions[d.Name] = d
		a.ordered = append(a.ordered, d)
	}
	return a
}
func (a *API) Definitions() []Definition { return append([]Definition(nil), a.ordered...) }
func (a *API) Call(ctx context.Context, p model.Principal, name string, raw json.RawMessage) (map[string]any, *model.APIError) {
	d, ok := a.definitions[name]
	if !ok {
		return nil, model.NewError("NOT_FOUND", "unknown mailbox tool", nil)
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	var input any
	if json.Unmarshal(raw, &input) != nil {
		return nil, model.NewError("INVALID_INPUT", "arguments must be JSON", nil)
	}
	if err := d.schema.Validate(input); err != nil {
		return nil, model.NewError("INVALID_INPUT", fmt.Sprintf("invalid arguments: %s", err), nil)
	}
	if name == "send_message" {
		args := input.(map[string]any)
		if _, ok := args["require_ack"]; !ok {
			args["require_ack"] = true
		}
		raw, _ = json.Marshal(args)
	}
	return d.invoke(ctx, p, raw)
}
