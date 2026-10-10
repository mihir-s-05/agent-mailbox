package service

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Notifier is an acceleration channel. Durable inbox revisions repair missed notifications.
type Notifier struct {
	mu       sync.Mutex
	waiters  map[string]*waiter
	pool     *pgxpool.Pool
	fallback atomic.Bool
	publish  func(context.Context, string, string)
}
type waiter struct {
	channel chan struct{}
	users   int
}

func NewNotifier(pool *pgxpool.Pool) *Notifier {
	n := &Notifier{pool: pool, waiters: map[string]*waiter{}}
	n.fallback.Store(true)
	return n
}
func (n *Notifier) SetPublisher(publish func(context.Context, string, string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.publish = publish
}
func (n *Notifier) IsFallback() bool                   { return n.fallback.Load() }
func (n *Notifier) EnsureTeam(context.Context, string) {}
func (n *Notifier) WaitCh(team, agent string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := team + "\x00" + agent
	w := n.waiters[key]
	if w == nil {
		w = &waiter{channel: make(chan struct{})}
		n.waiters[key] = w
	}
	w.users++
	return w.channel
}
func (n *Notifier) ReleaseWait(team, agent string, ch <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := team + "\x00" + agent
	w := n.waiters[key]
	if w != nil && w.channel == ch {
		w.users--
		if w.users == 0 {
			delete(n.waiters, key)
		}
	}
}
func (n *Notifier) changed(ctx context.Context, team, agent string) {
	n.mu.Lock()
	key := team + "\x00" + agent
	if w := n.waiters[key]; w != nil {
		close(w.channel)
		delete(n.waiters, key)
	}
	publish := n.publish
	n.mu.Unlock()
	if publish != nil {
		publish(ctx, team, agent)
	}
}
func (n *Notifier) Run(ctx context.Context) {
	for ctx.Err() == nil {
		n.runOnce(ctx)
		if sleepCtx(ctx, 2*time.Second) != nil {
			return
		}
	}
}
func (n *Notifier) runOnce(ctx context.Context) {
	defer n.fallback.Store(true)
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `LISTEN mailbox_changed`); err != nil {
		log.Printf("notifier listen: %v", err)
		return
	}
	n.fallback.Store(false)
	for ctx.Err() == nil {
		wait, cancel := context.WithTimeout(ctx, 2*time.Second)
		event, err := conn.Conn().WaitForNotification(wait)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			return
		}
		var payload struct {
			Team  string `json:"team_id"`
			Agent string `json:"agent_id"`
		}
		if event != nil && json.Unmarshal([]byte(event.Payload), &payload) == nil && payload.Team != "" && payload.Agent != "" {
			n.changed(ctx, payload.Team, payload.Agent)
		}
	}
}
func channelName(string) string { return "mailbox_changed" }
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
