package viewerpreview

import (
	"bufio"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestPrefixProxyAndStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/inbox/stream" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("incorrect forwarding path or credential")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: inbox_changed\ndata: {\"revision\":42}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	h, err := Handler(u, "/agent-mailbox/")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	response, err := http.Get(server.URL + "/agent-mailbox/viewer-config.json")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("prefix mount unavailable")
	}
	req, _ := http.NewRequest("GET", server.URL+"/agent-mailbox/api/inbox/stream?agent_id=owned", nil)
	req.Header.Set("Authorization", "Bearer fixture")
	response, err = server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	response.Body.Close()
	if err != nil || line != "event: inbox_changed\n" {
		t.Fatal("SSE must flush before upstream closes")
	}
	req, _ = http.NewRequest("POST", server.URL+"/agent-mailbox/api/call", strings.NewReader("{}"))
	req.Header.Set("Origin", "https://untrusted.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	response, err = server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatal("cross-origin unsafe request should be rejected")
	}
}
func TestPreviewConfigurationRejectsUnsafeOrigins(t *testing.T) {
	for _, raw := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com/api", "https://example.com?token=secret", "file:///tmp"} {
		if _, err := ParseBackend(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, prefix := range []string{"bad", "/no-trailing-slash", "/../", "/x?y/"} {
		if _, err := Handler(nil, prefix); err == nil {
			t.Fatalf("accepted invalid prefix %s", prefix)
		}
	}
}
