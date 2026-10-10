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
	"syscall"
	"time"

	"github.com/mihir/msg-com/internal/app"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/web"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runtime, err := app.Start(ctx, cfg, "")
	if err != nil {
		log.Fatalf("startup: %v", err)
	}
	defer runtime.Close()
	protocol := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return runtime.Mailbox.MCP }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, MaxRequestBodyBytes: 512 * 1024, PropagateRequestCancellation: true})
	mux := http.NewServeMux()
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	mux.Handle("/readyz", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		check, cancel := context.WithTimeout(req.Context(), time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		if runtime.Store.DB.Ping(check) != nil || !runtime.Events.Healthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"ready":false}`))
			return
		}
		w.Write([]byte(`{"ready":true}`))
	}))
	protected := runtime.Auth.Middleware(http.NewCrossOriginProtection().Handler(protocol))
	mux.Handle(cfg.BasePath, protected)
	if cfg.BasePath != "/" {
		mux.Handle(cfg.BasePath+"/", protected)
	}
	browser := web.New(runtime.Mailbox.API, runtime.Service, runtime.Events, runtime.Auth.PrincipalForID)
	mux.Handle("/api/", runtime.Auth.Middleware(http.NewCrossOriginProtection().Handler(browser)))
	mux.Handle("/", web.Assets())
	srv := &http.Server{Addr: cfg.Address, Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 32 * 1024}
	if cfg.UseTLS {
		srv.TLSConfig, err = buildTLSConfig(cfg)
		if err != nil {
			log.Fatalf("TLS: %v", err)
		}
	}
	go func() {
		scheme := "http"
		if cfg.UseTLS {
			scheme = "https"
		}
		log.Printf("agent-mailbox listening on %s://%s%s", scheme, cfg.Address, cfg.BasePath)
		var err error
		if cfg.UseTLS {
			err = srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server: %v", err)
			cancel()
		}
	}()
	<-ctx.Done()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := srv.Shutdown(shutdown); err != nil {
		srv.Close()
	}
}
func buildTLSConfig(cfg config.Config) (*tls.Config, error) {
	result := &tls.Config{MinVersion: tls.VersionTLS13}
	if !cfg.RequireMTLS {
		return result, nil
	}
	ca, err := os.ReadFile(cfg.TLSClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid client CA")
	}
	result.ClientAuth = tls.RequireAndVerifyClientCert
	result.ClientCAs = pool
	return result, nil
}
