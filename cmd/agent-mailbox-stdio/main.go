package main

import (
	"context"
	"github.com/mihir/msg-com/internal/app"
	"github.com/mihir/msg-com/internal/config"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	token := strings.TrimSpace(os.Getenv("MAILBOX_TOKEN"))
	if token == "" {
		log.Fatal("MAILBOX_TOKEN is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runtime, err := app.Start(ctx, cfg, token)
	if err != nil {
		log.Fatalf("startup: %v", err)
	}
	if err = runtime.Mailbox.MCP.Run(ctx, &mcp.StdioTransport{}); err != nil {
		log.Printf("stdio: %v", err)
	}
	cancel()
	runtime.Close()
}
