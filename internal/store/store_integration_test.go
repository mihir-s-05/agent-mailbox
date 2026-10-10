package store_test

import (
	"context"
	"github.com/mihir/msg-com/internal/store"
	"github.com/mihir/msg-com/internal/testutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOwnershipUpgradeAndConcurrentMigrations(t *testing.T) {
	db := testutil.DatabaseUnmigrated(t)
	ctx := context.Background()
	legacy := t.TempDir()
	for _, name := range []string{"001_init.sql", "002_unique_active_session.sql"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(legacy, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	st := &store.Store{DB: db}
	if err := st.MigrateFromDir(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO agents(team_id,agent_id) VALUES('t','active'),('t','inactive'),('t','blank'),('t','orphan'); INSERT INTO sessions(session_id,team_id,agent_id,principal,active,created_at) VALUES('old','t','active','old',false,now()-interval '1 day'),('current','t','active','owner',true,now()),('inactive-old','t','inactive','old',false,now()-interval '2 days'),('inactive-new','t','inactive','latest',false,now()-interval '1 day'),('blank','t','blank','   ',true,now())`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- st.MigrateFromDir(ctx, filepath.Join("..", "..", "migrations")) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for agent, want := range map[string]string{"active": "owner", "inactive": "latest", "blank": "", "orphan": ""} {
		var owner *string
		if err := db.QueryRow(ctx, `SELECT owner_subject FROM agents WHERE agent_id=$1`, agent).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if (want == "" && owner != nil) || (want != "" && (owner == nil || *owner != want)) {
			t.Fatalf("%s owner %v want %s", agent, owner, want)
		}
	}
	var active bool
	db.QueryRow(ctx, `SELECT active FROM sessions WHERE session_id='blank'`).Scan(&active)
	if active {
		t.Fatal("blank historical owner session remained active")
	}
}
