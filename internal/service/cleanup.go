package service

import (
	"context"
	"time"
)

func (s *Service) SweepExpiry(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// A single sweeper per database avoids duplicate work across STDIO and HTTP processes.
	var acquired bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(840174251)`).Scan(&acquired); err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT d.team_id,d.recipient_agent_id FROM deliveries d JOIN messages m USING(message_id) WHERE d.state IN ('PENDING','DELIVERED') AND m.expires_at<=now() ORDER BY d.team_id,d.recipient_agent_id`)
	if err != nil {
		return err
	}
	type mailbox struct{ team, agent string }
	boxes := []mailbox{}
	for rows.Next() {
		var b mailbox
		if err = rows.Scan(&b.team, &b.agent); err != nil {
			rows.Close()
			return err
		}
		boxes = append(boxes, b)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, b := range boxes {
		if err = lockMailboxes(ctx, tx, b.team, []string{b.agent}); err != nil {
			return err
		}
	}
	rows, err = tx.Query(ctx, `UPDATE deliveries d SET state='EXPIRED' FROM messages m WHERE d.message_id=m.message_id AND d.state IN ('PENDING','DELIVERED') AND m.expires_at<=now() RETURNING d.team_id,d.message_id,d.recipient_agent_id`)
	if err != nil {
		return err
	}
	type expired struct{ team, message, agent string }
	items := []expired{}
	for rows.Next() {
		var x expired
		if err = rows.Scan(&x.team, &x.message, &x.agent); err != nil {
			rows.Close()
			return err
		}
		items = append(items, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, x := range items {
		if err = auditTx(ctx, tx, x.team, x.message, x.agent, "EXPIRED", time.Now().UTC()); err != nil {
			return err
		}
	}
	for _, b := range boxes {
		if _, err = notifyInboxTx(ctx, tx, b.team, b.agent); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
