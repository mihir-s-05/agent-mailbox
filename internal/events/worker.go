package events

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Service) Run(ctx context.Context) {
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			tick := time.NewTicker(200 * time.Millisecond)
			defer tick.Stop()
			for ctx.Err() == nil {
				worked, err := s.DeliverOne(ctx)
				if err != nil && ctx.Err() == nil {
					log.Printf("events: delivery: %v", err)
				}
				if worked && err == nil {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		}()
	}
	defer workers.Wait()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if err := s.Materialize(ctx); err != nil {
			if ctx.Err() == nil {
				log.Printf("events: materialize: %v", err)
			}
		} else {
			s.heartbeat.Store(time.Now().UnixNano())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Service) Healthy() bool {
	last := s.heartbeat.Load()
	return last != 0 && time.Since(time.Unix(0, last)) < 10*time.Second
}

func (s *Service) Materialize(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `INSERT INTO event_deliveries(subscription_id,event_id)
	 SELECT s.id,e.event_id FROM event_subscriptions s JOIN mailbox_events e ON e.team_id=s.team_id AND e.agent_id=s.agent_id AND e.name=s.name
	 WHERE s.active AND s.expires_at>now() AND e.revision>s.cursor
	 AND (e.data->>'priority')::int>=COALESCE((s.arguments->>'min_priority')::int,0)
	 AND (COALESCE(s.arguments->>'topic','')='' OR e.data->>'topic'=s.arguments->>'topic')
	 AND NOT EXISTS(SELECT 1 FROM event_deliveries d WHERE d.subscription_id=s.id AND d.event_id=e.event_id)
	 ORDER BY e.occurred_at,e.event_id LIMIT 1000 ON CONFLICT DO NOTHING`)
	return err
}

type job struct {
	id, event, team, agent, owner, token, url, name, lease string
	key, old, data                                         []byte
	rotation                                               *time.Time
	revision                                               int64
	at                                                     time.Time
	attempt                                                int
}

// DeliverOne never holds a database transaction while calling a remote callback.
// The oldest outstanding event blocks newer ones, including while its lease is held.
func (s *Service) DeliverOne(ctx context.Context) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var j job
	err = tx.QueryRow(ctx, `SELECT s.id,e.event_id,s.team_id,s.agent_id,s.owner_subject,s.token_id,s.callback_url,s.secret,s.previous_secret,s.rotate_until,e.name,e.revision,e.occurred_at,e.data::text,d.attempts
	 FROM event_deliveries d JOIN event_subscriptions s ON s.id=d.subscription_id JOIN mailbox_events e ON e.event_id=d.event_id
	 WHERE d.state='PENDING' AND d.next_attempt_at<=now() AND (d.lease_until IS NULL OR d.lease_until<now()) AND s.active AND s.expires_at>now()
	 AND NOT EXISTS(SELECT 1 FROM mailbox_events prior WHERE prior.team_id=s.team_id AND prior.agent_id=s.agent_id AND prior.name=s.name AND prior.revision>s.cursor AND prior.revision<e.revision
	 AND (prior.data->>'priority')::int>=COALESCE((s.arguments->>'min_priority')::int,0) AND (COALESCE(s.arguments->>'topic','')='' OR prior.data->>'topic'=s.arguments->>'topic')
	 AND NOT EXISTS(SELECT 1 FROM event_deliveries done WHERE done.subscription_id=s.id AND done.event_id=prior.event_id AND done.state='ACCEPTED'))
	 ORDER BY d.next_attempt_at,e.revision FOR UPDATE OF d SKIP LOCKED LIMIT 1`).Scan(&j.id, &j.event, &j.team, &j.agent, &j.owner, &j.token, &j.url, &j.key, &j.old, &j.rotation, &j.name, &j.revision, &j.at, &j.data, &j.attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	j.lease = uuid.NewString()
	j.attempt++
	if _, err = tx.Exec(ctx, `UPDATE event_deliveries SET lease_until=now()+interval '30 seconds',lease_token=$3,attempts=attempts+1 WHERE subscription_id=$1 AND event_id=$2`, j.id, j.event, j.lease); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	p, allowed := s.principal(j.team, j.owner, j.token)
	if !allowed || s.authorize(ctx, p, j.agent) != nil {
		_, err = s.db.Exec(ctx, `UPDATE event_subscriptions SET active=false WHERE id=$1 AND token_id=$2`, j.id, j.token)
		return true, err
	}
	// Unsubscribe, expiration, and refresh can race with leasing; recheck before sending.
	var current bool
	if err = s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM event_subscriptions WHERE id=$1 AND active AND expires_at>now() AND token_id=$2 AND secret=$3)`, j.id, j.token, j.key).Scan(&current); err != nil {
		return true, err
	}
	if !current {
		return true, nil
	}
	key, err := decrypt(s.key, j.key)
	if err != nil {
		return true, s.finish(ctx, j, 0, err)
	}
	var old []byte
	if len(j.old) > 0 && j.rotation != nil && time.Now().Before(*j.rotation) {
		old, err = decrypt(s.key, j.old)
		if err != nil {
			return true, s.finish(ctx, j, 0, err)
		}
	}
	body, err := json.Marshal(struct {
		ID        string          `json:"eventId"`
		Name      string          `json:"name"`
		Timestamp string          `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
		Cursor    string          `json:"cursor"`
	}{j.event, j.name, j.at.UTC().Format(time.RFC3339Nano), json.RawMessage(j.data), s.cursor(j.team, j.agent, j.revision)})
	if err != nil {
		return true, s.finish(ctx, j, 0, err)
	}
	status, _, deliveryErr := s.post(ctx, j.url, j.event, j.id, body, key, old)
	return true, s.finish(ctx, j, status, deliveryErr)
}

func (s *Service) finish(ctx context.Context, j job, status int, deliveryErr error) error {
	// Request cancellation leaves the lease for another worker to recover.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// A 2xx status acknowledges receipt regardless of an unused response body.
	// Verification challenges validate their response separately before activation.
	accepted := status >= 200 && status < 300
	// Refresh/replay locks the subscription before its delivery rows. Keep the
	// same order when completing a callback to avoid a refresh/receipt deadlock.
	var subscription string
	if err = tx.QueryRow(ctx, `SELECT id FROM event_subscriptions WHERE id=$1 FOR UPDATE`, j.id).Scan(&subscription); errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	if accepted {
		cmd, err := tx.Exec(ctx, `UPDATE event_deliveries SET state='ACCEPTED',accepted_at=now(),lease_until=NULL,lease_token=NULL,last_error=NULL WHERE subscription_id=$1 AND event_id=$2 AND lease_token=$3`, j.id, j.event, j.lease)
		if err != nil {
			return err
		}
		if cmd.RowsAffected() > 0 {
			if _, err = tx.Exec(ctx, `UPDATE event_subscriptions SET cursor=GREATEST(cursor,$2) WHERE id=$1`, j.id, j.revision); err != nil {
				return err
			}
		}
	} else {
		permanent := status == 410 || status == 413 || (status >= 300 && status < 500 && status != 408 && status != 429) || j.attempt >= 8
		state := "PENDING"
		if permanent {
			state = "FAILED"
		}
		category := "http_error"
		if deliveryErr != nil {
			category = "connection_error"
		}
		backoff := time.Duration(1<<min(j.attempt, 10))*time.Second + time.Duration(rand.IntN(1000))*time.Millisecond
		cmd, err := tx.Exec(ctx, `UPDATE event_deliveries SET state=$4,next_attempt_at=$5,lease_until=NULL,lease_token=NULL,last_error=$6 WHERE subscription_id=$1 AND event_id=$2 AND lease_token=$3`, j.id, j.event, j.lease, state, time.Now().Add(backoff), category)
		if err != nil {
			return err
		}
		if permanent && cmd.RowsAffected() > 0 {
			if _, err = tx.Exec(ctx, `UPDATE event_subscriptions SET active=false WHERE id=$1`, j.id); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}
