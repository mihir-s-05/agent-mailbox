package pluginclient_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestPackagedPluginWithOfficialSDK(t *testing.T) {
	for _, clientName := range []string{"codex", "claude"} {
		t.Run(clientName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			root, err := filepath.Abs(filepath.Join("..", "..", "plugins", clientName, "scripts"))
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command("node", filepath.Join(root, "bridge.mjs"), "--client="+clientName)
			// Isolated absent configuration; no real credential or client config is read.
			command.Env = []string{"MAILBOX_CLIENT_CONFIG=" + filepath.Join(t.TempDir(), "absent.json")}
			client := mcp.NewClient(&mcp.Implementation{Name: "plugin-wire-test", Version: "1"}, nil)
			session, err := client.Connect(ctx, &mcp.CommandTransport{Command: command}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			tools, err := session.ListTools(ctx, nil)
			if err != nil || len(tools.Tools) != 11 {
				t.Fatalf("tools: %v, %v", tools, err)
			}
			out, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "mailbox_setup", Arguments: map[string]any{}})
			if err != nil || out.IsError || out.StructuredContent == nil {
				t.Fatalf("setup: %v, %v", out, err)
			}
			if session.InitializeResult().ServerInfo.Name != "agent-mailbox-plugin" {
				t.Fatal("plugin identity mismatch")
			}
		})
	}
}
