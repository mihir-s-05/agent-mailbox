UPDATE sessions s
SET    active = false,
       invalidated_at = NOW()
WHERE  active = true
  AND  EXISTS (
    SELECT 1
    FROM   sessions s2
    WHERE  s2.team_id  = s.team_id
      AND  s2.agent_id = s.agent_id
      AND  s2.active   = true
      AND  (
             s2.created_at > s.created_at
             OR (s2.created_at = s.created_at AND s2.session_id > s.session_id)
           )
  );

CREATE UNIQUE INDEX idx_sessions_one_active
  ON sessions(team_id, agent_id) WHERE active = true;
