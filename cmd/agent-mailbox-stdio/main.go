package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/mcpserver"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	if len(cfg.BootstrapTokenSecrets) > 0 {
		log.Printf("generated %d bootstrap token(s); plaintext secrets written to %s", len(cfg.BootstrapTokenSecrets), cfg.BootstrapTokensFile)
	}

	ctx := context.Background()
	st, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect error: %v", err)
	}
	defer st.Close()

	if err := st.MigrateFromDir(ctx, filepath.Join(".", "migrations")); err != nil {
		log.Fatalf("migration error: %v", err)
	}

	svc := service.New(st.DB, cfg)
	reg := auth.NewRegistry(cfg)
	reg.Start(ctx)

	token := strings.TrimSpace(os.Getenv("MAILBOX_TOKEN"))
	resolver := func(context.Context) (model.Principal, bool) {
		return reg.LookupToken(token)
	}
	mcpSvc := mcpserver.NewMailboxServer(svc, resolver)

	go runTicker(ctx, cfg.InactivitySweepEvery, func(ctx context.Context) {
		if err := svc.SweepInactivity(ctx); err != nil {
			log.Printf("inactivity sweep error: %v", err)
		}
	})
	go runTicker(ctx, cfg.ExpirySweepInterval, func(ctx context.Context) {
		if err := svc.SweepExpiry(ctx); err != nil {
			log.Printf("expiry sweep error: %v", err)
		}
	})
	go runTicker(ctx, cfg.RetentionSweepInterval, func(ctx context.Context) {
		if err := svc.SweepRetention(ctx); err != nil {
			log.Printf("retention sweep error: %v", err)
		}
	})

	if err := server.ServeStdio(mcpSvc); err != nil {
		log.Fatalf("stdio server error: %v", err)
	}
}

func runTicker(ctx context.Context, every time.Duration, fn func(context.Context)) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}
