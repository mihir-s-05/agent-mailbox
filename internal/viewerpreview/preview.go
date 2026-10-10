// Package viewerpreview mounts the embedded viewer and an optional fixed backend.
package viewerpreview

import (
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"

	"github.com/mihir/msg-com/internal/web"
)

func ParseBackend(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("backend must be an HTTP(S) origin without credentials, path, query, or fragment")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || (u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return nil, errors.New("HTTP backend must be loopback; remote backends require HTTPS")
		}
	}
	u.Path = ""
	return u, nil
}

func Handler(backend *url.URL, prefix string) (http.Handler, error) {
	if !strings.HasPrefix(prefix, "/") || !strings.HasSuffix(prefix, "/") || path.Clean(prefix)+"/" != prefix && prefix != "/" || strings.ContainsAny(prefix, "?#%\\") {
		return nil, errors.New("base path must be a clean path beginning and ending with /, such as /agent-mailbox/")
	}
	mux := http.NewServeMux()
	mux.Handle("/", web.AssetsWithBackend(backend != nil))
	if backend != nil {
		proxy := httputil.NewSingleHostReverseProxy(backend)
		proxy.FlushInterval = -1
		proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "mailbox backend unavailable", http.StatusBadGateway)
		}
		protected := http.NewCrossOriginProtection().Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, 512*1024)
			proxy.ServeHTTP(w, r)
		}))
		mux.Handle("/api/", protected)
		mux.Handle("/mcp", protected)
	}
	if prefix == "/" {
		return mux, nil
	}
	outer := http.NewServeMux()
	outer.Handle(prefix, http.StripPrefix(strings.TrimSuffix(prefix, "/"), mux))
	outer.HandleFunc(strings.TrimSuffix(prefix, "/"), func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, prefix, http.StatusPermanentRedirect)
	})
	return outer, nil
}
