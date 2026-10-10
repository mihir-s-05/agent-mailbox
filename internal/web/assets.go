package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/mihir/msg-com/internal/mailboxapi"
)

// Explicit allowlist: tests, configuration, credentials and repository files are never served.
//
//go:embed assets/index.html assets/mailbox.html assets/install.mjs assets/architecture.mjs assets/style.css assets/app.mjs assets/view.mjs assets/model.mjs assets/demo.mjs assets/webmcp.mjs assets/favicon.svg assets/guide.html
var assets embed.FS

func Assets() http.Handler { return AssetsWithBackend(true) }

func AssetsWithBackend(available bool) http.Handler {
	root, _ := fs.Sub(assets, "assets")
	files := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/viewer-config.json":
			respond(w, http.StatusOK, map[string]any{"api_available": available})
		case "/tool-catalog.json":
			respond(w, http.StatusOK, map[string]any{"tools": mailboxapi.New(nil).Definitions()})
		default:
			// Windows can register .mjs as text/plain; modules need JavaScript MIME.
			if strings.HasSuffix(r.URL.Path, ".mjs") {
				w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
			}
			files.ServeHTTP(w, r)
		}
	})
}

// Export writes only embedded public files. It never copies the checkout.
func Export(directory string) error {
	root, _ := fs.Sub(assets, "assets")
	allowed := map[string]bool{"viewer-config.json": true, "tool-catalog.json": true}
	entries, err := fs.ReadDir(root, ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		allowed[entry.Name()] = true
	}
	// Refuse a contaminated output directory instead of publishing stale files.
	if existing, err := os.ReadDir(directory); err == nil {
		for _, entry := range existing {
			if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !allowed[entry.Name()] {
				return fmt.Errorf("export directory contains an unexpected file: %s", entry.Name())
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	if err := fs.WalkDir(root, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := fs.ReadFile(root, name)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(directory, name), data, 0644)
	}); err != nil {
		return err
	}
	config, _ := json.Marshal(map[string]any{"api_available": false})
	if err := os.WriteFile(filepath.Join(directory, "viewer-config.json"), config, 0644); err != nil {
		return err
	}
	catalog, _ := json.Marshal(map[string]any{"tools": mailboxapi.New(nil).Definitions()})
	return os.WriteFile(filepath.Join(directory, "tool-catalog.json"), catalog, 0644)
}
