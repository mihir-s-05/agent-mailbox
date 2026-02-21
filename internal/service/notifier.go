package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type Notifier struct {
	mu           sync.Mutex
	waiters      map[string]chan struct{}
	teams        map[string]struct{}
	pendingTeams map[string]struct{}

	pool     *pgxpool.Pool
	fallback atomic.Bool

	mcpMu     sync.Mutex
	mcpServer *server.MCPServer
}

func NewNotifier(pool *pgxpool.Pool) *Notifier {
	n := &Notifier{
		waiters:      make(map[string]chan struct{}),
		teams:        make(map[string]struct{}),
		pendingTeams: make(map[string]struct{}),
		pool:         pool,
	}
	n.fallback.Store(true)
	return n
}

func (n *Notifier) SetMCPServer(s *server.MCPServer) {
	n.mcpMu.Lock()
	defer n.mcpMu.Unlock()
	n.mcpServer = s
}

func (n *Notifier) getMCPServer() *server.MCPServer {
	n.mcpMu.Lock()
	defer n.mcpMu.Unlock()
	return n.mcpServer
}

func (n *Notifier) IsFallback() bool {
	return n.fallback.Load()
}

func (n *Notifier) EnsureTeam(_ context.Context, teamID string) {
	if teamID == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.teams[teamID]; exists {
		return
	}
	n.teams[teamID] = struct{}{}
	n.pendingTeams[teamID] = struct{}{}
}

func (n *Notifier) WaitCh(teamID, agentID string) <-chan struct{} {
	key := teamID + "\x00" + agentID
	n.mu.Lock()
	defer n.mu.Unlock()
	ch, ok := n.waiters[key]
	if !ok {
		ch = make(chan struct{})
		n.waiters[key] = ch
	}
	return ch
}

func (n *Notifier) broadcast(teamID, agentID string) {
	key := teamID + "\x00" + agentID

	n.mu.Lock()
	if ch, ok := n.waiters[key]; ok {
		close(ch)
		n.waiters[key] = make(chan struct{})
	}
	n.mu.Unlock()
}

func (n *Notifier) SendResourceNotification(sessionID string) {
	if mcpSrv := n.getMCPServer(); mcpSrv != nil {
		base := "mailbox://inbox/" + sessionID
		mcpSrv.SendNotificationToAllClients(
			string(mcp.MethodNotificationResourceUpdated),
			map[string]any{"uri": base},
		)
		mcpSrv.SendNotificationToAllClients(
			string(mcp.MethodNotificationResourceUpdated),
			map[string]any{"uri": base + "/urgent"},
		)
	}
}

func (n *Notifier) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		n.runOnce(ctx)
		if err := sleepCtx(ctx, 2*time.Second); err != nil {
			return
		}
	}
}

func (n *Notifier) runOnce(ctx context.Context) {
	defer func() {
		n.fallback.Store(true)
	}()

	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("notifier: acquire connection: %v", err)
		}
		return
	}
	defer conn.Release()

	n.seedTeamsFromActiveSessions(ctx, conn)

	n.mu.Lock()
	for t := range n.teams {
		n.pendingTeams[t] = struct{}{}
	}
	n.mu.Unlock()

	if err := n.listenPendingTeams(ctx, conn); err != nil {
		log.Printf("notifier: LISTEN bootstrap failed: %v", err)
		return
	}

	n.fallback.Store(false)
	log.Printf("notifier: listening on %d team channel(s)", len(n.teams))

	for {
		if ctx.Err() != nil {
			return
		}

		if err := n.listenPendingTeams(ctx, conn); err != nil {
			log.Printf("notifier: LISTEN update failed: %v", err)
			return
		}

		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		notification, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			log.Printf("notifier: wait error: %v", err)
			return
		}

		if notification != nil {
			var payload struct {
				TeamID  string `json:"team_id"`
				AgentID string `json:"agent_id"`
			}
			if json.Unmarshal([]byte(notification.Payload), &payload) == nil && payload.TeamID != "" && payload.AgentID != "" {
				n.broadcast(payload.TeamID, payload.AgentID)
			}
		}
	}
}

func (n *Notifier) listenPendingTeams(ctx context.Context, conn *pgxpool.Conn) error {
	n.mu.Lock()
	if len(n.pendingTeams) == 0 {
		n.mu.Unlock()
		return nil
	}
	pending := make([]string, 0, len(n.pendingTeams))
	for t := range n.pendingTeams {
		pending = append(pending, t)
	}
	n.mu.Unlock()

	for _, t := range pending {
		ch := channelName(t)
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
		n.mu.Lock()
		delete(n.pendingTeams, t)
		n.mu.Unlock()
	}
	return nil
}

func (n *Notifier) seedTeamsFromActiveSessions(ctx context.Context, conn *pgxpool.Conn) {
	rows, err := conn.Query(ctx, `SELECT DISTINCT team_id FROM sessions WHERE active=true`)
	if err != nil {
		log.Printf("notifier: seed teams from sessions failed: %v", err)
		return
	}
	defer rows.Close()

	teams := make([]string, 0)
	for rows.Next() {
		var teamID string
		if err := rows.Scan(&teamID); err != nil {
			log.Printf("notifier: seed team scan failed: %v", err)
			continue
		}
		if teamID != "" {
			teams = append(teams, teamID)
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("notifier: seed team iteration failed: %v", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, teamID := range teams {
		n.teams[teamID] = struct{}{}
		n.pendingTeams[teamID] = struct{}{}
	}
}

func channelName(teamID string) string {
	h := sha256.Sum256([]byte(teamID))
	return "mbx_" + hex.EncodeToString(h[:])[:58]
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
