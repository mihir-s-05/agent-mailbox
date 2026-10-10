// Package testutil provides isolated real PostgreSQL databases for integration tests.
package testutil

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mihir/msg-com/internal/store"
)

func Database(t *testing.T) *pgxpool.Pool {
	return database(t, true)
}
func DatabaseUnmigrated(t *testing.T) *pgxpool.Pool { return database(t, false) }
func database(t *testing.T, migrate bool) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("MAILBOX_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("MAILBOX_INTEGRATION_TESTS") != "1" {
			t.Skip("set MAILBOX_TEST_DATABASE_URL or MAILBOX_INTEGRATION_TESTS=1 for PostgreSQL integration tests")
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		cacheRoot, err := os.UserCacheDir()
		if err != nil {
			t.Fatal(err)
		}
		temp := t.TempDir()
		postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Version(embeddedpostgres.V16).Port(uint32(port)).CachePath(filepath.Join(cacheRoot, "agent-mailbox", "postgres")).RuntimePath(filepath.Join(temp, "runtime")).DataPath(filepath.Join(temp, "data")).Logger(io.Discard).StartTimeout(30 * time.Second))
		if err = postgres.Start(); err != nil {
			t.Fatalf("start isolated PostgreSQL: %v", err)
		}
		t.Cleanup(func() {
			if err := postgres.Stop(); err != nil {
				t.Errorf("stop isolated PostgreSQL: %v", err)
			}
		})
		dsn = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + fmt.Sprintf("%x", uuid.New())
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
		if err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, source, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(source), "..", "..", "migrations")
	if migrate {
		if err = (&store.Store{DB: pool}).MigrateFromDir(ctx, migrations); err != nil {
			t.Fatalf("migrations: %v", err)
		}
	}
	return pool
}
