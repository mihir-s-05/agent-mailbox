package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/mihir/msg-com/internal/store"
)

func main() {
	databaseURL := strings.TrimSpace(os.Getenv("MAILBOX_DATABASE_URL"))
	if databaseURL == "" {
		log.Fatalf("config error: MAILBOX_DATABASE_URL is required")
	}
	ctx := context.Background()
	st, err := store.Connect(ctx, databaseURL)
	if err != nil {
		log.Fatalf("db connect error: %v", err)
	}
	defer st.Close()

	if err := st.MigrateFromDir(ctx, filepath.Join(".", "migrations")); err != nil {
		log.Fatalf("migration error: %v", err)
	}
	log.Println("migrations applied")
}
