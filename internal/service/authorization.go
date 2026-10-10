package service

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mihir/msg-com/internal/model"
)

func (s *Service) requireSession(ctx context.Context, p model.Principal, id, team string) (sessionRow, *model.APIError) {
	id = strings.TrimSpace(id)
	if id == "" {
		return sessionRow{}, model.NewError("INVALID_INPUT", "session_id is required", nil)
	}
	var sess sessionRow
	var owner *string
	err := s.db.QueryRow(ctx, `SELECT s.session_id,s.team_id,s.agent_id,s.principal,a.owner_subject FROM sessions s JOIN agents a USING(team_id,agent_id) WHERE s.session_id=$1 AND s.active`, id).Scan(&sess.SessionID, &sess.TeamID, &sess.AgentID, &sess.Principal, &owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return sess, model.NewError("SESSION_INVALID", "session_id not recognized", nil)
	}
	if err != nil {
		return sess, dbError("authorize session", err)
	}
	if p.TeamID == "" || p.TeamID != sess.TeamID || (team != "" && team != sess.TeamID) {
		return sessionRow{}, model.NewError("TEAM_MISMATCH", "token and session teams differ", nil)
	}
	if p.Subject == "" || sess.Principal != p.Subject || owner == nil || *owner != p.Subject {
		return sessionRow{}, model.NewError("FORBIDDEN", "session belongs to a different principal", nil)
	}
	return sess, nil
}

// AuthorizeMailbox is shared by stable resources, browser APIs, and event delivery.
// Subject identity is retained across bearer-token rotation.
func (s *Service) AuthorizeMailbox(ctx context.Context, p model.Principal, agent string) *model.APIError {
	if p.TeamID == "" || p.Subject == "" || !p.HasScope("poll:self") {
		return model.NewError("FORBIDDEN", "mailbox read permission is required", nil)
	}
	var owner *string
	err := s.db.QueryRow(ctx, `SELECT owner_subject FROM agents WHERE team_id=$1 AND agent_id=$2`, p.TeamID, agent).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.NewError("AGENT_NOT_FOUND", "agent not found", nil)
	}
	if err != nil {
		return dbError("authorize mailbox", err)
	}
	if owner == nil || *owner != p.Subject {
		return model.NewError("FORBIDDEN", "mailbox belongs to a different principal", nil)
	}
	return nil
}

func (s *Service) InboxRevision(ctx context.Context, p model.Principal, agent string) (int64, error) {
	if err := s.AuthorizeMailbox(ctx, p, agent); err != nil {
		return 0, err
	}
	var revision int64
	err := s.db.QueryRow(ctx, `SELECT inbox_revision FROM agents WHERE team_id=$1 AND agent_id=$2 AND owner_subject=$3`, p.TeamID, agent, p.Subject).Scan(&revision)
	return revision, err
}

func (s *Service) AuthorizedSession(ctx context.Context, p model.Principal, id string) (string, *model.APIError) {
	sess, err := s.requireSession(ctx, p, id, "")
	return sess.AgentID, err
}
