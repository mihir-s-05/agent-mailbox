package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/mcpserver"
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db connect error: %v", err)
	}
	defer st.Close()

	migrationsDir := filepath.Join(".", "migrations")
	if err := st.MigrateFromDir(ctx, migrationsDir); err != nil {
		log.Fatalf("migration error: %v", err)
	}

	notifier := service.NewNotifier(st.DB)
	svc := service.New(st.DB, cfg, notifier)
	mcpSvc := mcpserver.NewMailboxServer(svc, auth.FromContext)
	notifier.SetMCPServer(mcpSvc)
	httpMCP := server.NewStreamableHTTPServer(
		mcpSvc,
		server.WithEndpointPath(cfg.BasePath),
		server.WithStateLess(true),
	)
	authRegistry := auth.NewRegistry(cfg)
	authRegistry.Start(ctx)

	mux := http.NewServeMux()
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	mux.Handle(cfg.BasePath, authRegistry.Middleware(httpMCP))
	if cfg.BasePath != "/" {
		mux.Handle(cfg.BasePath+"/", authRegistry.Middleware(httpMCP))
	}

	httpSrv := &http.Server{
		Addr:              cfg.Address,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if cfg.RequireMTLS {
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			log.Fatalf("tls config error: %v", err)
		}
		httpSrv.TLSConfig = tlsCfg
	}

	var bgWG sync.WaitGroup
	startBackground := func(fn func()) {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			fn()
		}()
	}

	startBackground(func() { notifier.Run(ctx) })

	startBackground(func() {
		runTicker(ctx, cfg.InactivitySweepEvery, func(ctx context.Context) {
			if err := svc.SweepInactivity(ctx); err != nil {
				log.Printf("inactivity sweep error: %v", err)
			}
		})
	})
	startBackground(func() {
		runTicker(ctx, cfg.ExpirySweepInterval, func(ctx context.Context) {
			if err := svc.SweepExpiry(ctx); err != nil {
				log.Printf("expiry sweep error: %v", err)
			}
		})
	})
	startBackground(func() {
		runTicker(ctx, cfg.RetentionSweepInterval, func(ctx context.Context) {
			if err := svc.SweepRetention(ctx); err != nil {
				log.Printf("retention sweep error: %v", err)
			}
		})
	})

	startBackground(func() {
		var err error
		if cfg.RequireMTLS {
			log.Printf("agent-mailbox listening with mTLS on https://127.0.0.1%s%s", cfg.Address, cfg.BasePath)
			err = httpSrv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			log.Printf("agent-mailbox listening on http://127.0.0.1%s%s", cfg.Address, cfg.BasePath)
			err = httpSrv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	})

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	bgWG.Wait()
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

func buildTLSConfig(cfg config.Config) (*tls.Config, error) {
	caPEM, err := os.ReadFile(cfg.TLSClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("failed to parse client CA")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
	}, nil
}
