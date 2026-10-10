package mailboxapi

import (
	"context"
	"encoding/json"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"testing"
)

func TestInvalidArgumentsNeverReachService(t *testing.T) {
	api := New(&service.Service{})
	for _, raw := range []string{`{"session_id":"s","to":{"type":"direct","agent_id":"a"},"priority":1.5,"topic":"t","body":"b"}`, `{"session_id":"s","to":{"type":"broadcast","scope":"tag"},"priority":1,"topic":"t","body":"b"}`, `{"session_id":"s","to":{"type":"direct","agent_id":"a"},"priority":1,"topic":"t","body":"b","unknown":true}`} {
		if _, err := api.Call(context.Background(), model.Principal{}, "send_message", json.RawMessage(raw)); err == nil || err.Code != "INVALID_INPUT" {
			t.Fatal("invalid schema accepted", err)
		}
	}
}
