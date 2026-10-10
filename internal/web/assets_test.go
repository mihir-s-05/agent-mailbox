package web

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicAssetsAndExport(t *testing.T) {
	handler := AssetsWithBackend(false)
	for _, name := range []string{"/.env", "/tokens.local.json", "/webmcp.test.mjs", "/tests/browser.mjs", "/../go.mod"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", name, nil))
		if w.Code != 404 && w.Code != 301 {
			t.Fatalf("private path %s: %d", name, w.Code)
		}
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/app.mjs", nil))
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "javascript") || !strings.Contains(w.Header().Get("Content-Security-Policy"), "connect-src 'self'") {
		t.Fatal("module MIME or CSP missing")
	}
	directory := t.TempDir()
	if err := Export(directory); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(directory)
	if len(files) != 14 {
		t.Fatalf("unexpected public output: %v", files)
	}
	data, _ := os.ReadFile(filepath.Join(directory, "viewer-config.json"))
	var config map[string]bool
	_ = json.Unmarshal(data, &config)
	if config["api_available"] {
		t.Fatal("static export must disable live mode")
	}
	if err := os.WriteFile(filepath.Join(directory, ".env"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Export(directory); err == nil {
		t.Fatal("export must refuse stale private files")
	}
}
