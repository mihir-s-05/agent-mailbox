package app

import (
	"context"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mihir/msg-com/internal/auth"
	"github.com/mihir/msg-com/internal/config"
	"github.com/mihir/msg-com/internal/events"
	"github.com/mihir/msg-com/internal/mcpserver"
	"github.com/mihir/msg-com/internal/model"
	"github.com/mihir/msg-com/internal/service"
	"github.com/mihir/msg-com/internal/store"
)

type Runtime struct {
	Store   *store.Store
	Auth    *auth.Registry
	Mailbox *mcpserver.Server
	Events  *events.Service
	Service *service.Service
	cancel  context.CancelFunc
	workers sync.WaitGroup
}

func Start(ctx context.Context, cfg config.Config, stdioToken string) (*Runtime, error) {
	ctx, cancel := context.WithCancel(ctx)
	st, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		cancel()
		return nil, err
	}
	ready := false
	defer func() {
		if !ready {
			cancel()
			st.Close()
		}
	}()
	if err = st.MigrateFromDir(ctx, filepath.Join(".", "migrations")); err != nil {
		return nil, err
	}
	key, err := events.LoadKey(cfg.EventKeyFile)
	if err != nil {
		return nil, err
	}
	reg := auth.NewRegistry(cfg)
	reg.Start(ctx)
	notifier := service.NewNotifier(st.DB)
	svc := service.New(st.DB, cfg, notifier)
	eventService, err := events.New(st.DB, key, svc.AuthorizeMailbox, reg.PrincipalForID)
	if err != nil {
		return nil, err
	}
	resolver := auth.FromContext
	if stdioToken != "" {
		resolver = func(context.Context) (model.Principal, bool) { return reg.LookupToken(strings.TrimSpace(stdioToken)) }
	}
	mailbox := mcpserver.NewMailboxServer(svc, resolver, eventService)
	mailbox.SetPrincipalValidator(reg.PrincipalForID)
	mailbox.SetBearerResolver(reg.LookupToken)
	r := &Runtime{Store: st, Auth: reg, Mailbox: mailbox, Events: eventService, Service: svc, cancel: cancel}
	r.run(func() { notifier.Run(ctx) })
	r.run(func() { eventService.Run(ctx) })
	r.run(func() { mailbox.RunResourceNotifications(ctx, reg.PrincipalForID) })
	r.run(func() { ticker(ctx, cfg.InactivitySweepEvery, svc.SweepInactivity) })
	r.run(func() { ticker(ctx, cfg.ExpirySweepInterval, svc.SweepExpiry) })
	r.run(func() { ticker(ctx, cfg.RetentionSweepInterval, svc.SweepRetention) })
	ready = true
	return r, nil
}
func (r *Runtime) run(fn func()) { r.workers.Add(1); go func() { defer r.workers.Done(); fn() }() }
func (r *Runtime) Close()        { r.cancel(); r.workers.Wait(); r.Store.Close() }
func ticker(ctx context.Context, every time.Duration, fn func(context.Context) error) {
	if every <= 0 {
		return
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := fn(ctx); err != nil {
				log.Printf("mailbox sweep: %v", err)
			}
		}
	}
}
