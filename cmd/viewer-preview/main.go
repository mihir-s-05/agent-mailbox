package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mihir/msg-com/internal/viewerpreview"
	"github.com/mihir/msg-com/internal/web"
)

func main() {
	address := flag.String("addr", "127.0.0.1:4174", "loopback preview address")
	backend := flag.String("backend", "", "optional fixed Go service origin; no TLS verification bypass")
	prefix := flag.String("base-path", "/", "mount path, beginning and ending with /")
	export := flag.String("export", "", "export public demo assets to this directory and exit")
	flag.Parse()
	if *export != "" {
		if err := web.Export(*export); err != nil {
			log.Fatal(err)
		}
		log.Printf("exported public demo assets to %s", *export)
		return
	}
	host, _, err := net.SplitHostPort(*address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		log.Fatal("preview address must be a loopback IP and port")
	}
	upstream, err := viewerpreview.ParseBackend(*backend)
	if err != nil {
		log.Fatal(err)
	}
	handler, err := viewerpreview.Handler(upstream, *prefix)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server := &http.Server{Addr: *address, Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("Agent Mailbox viewer: http://%s%s (live backend attached: %t)", *address, *prefix, upstream != nil)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
