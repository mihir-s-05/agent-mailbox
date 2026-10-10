package events

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mihir/msg-com/internal/model"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxPayload = 256 * 1024

func LoadKey(path string) ([]byte, error) {
	key, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return nil, err
		}
		file, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(createErr, os.ErrExist) {
			return LoadKey(path)
		}
		if createErr != nil {
			return nil, createErr
		}
		_, err = file.Write(key)
		closeErr := file.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("event encryption key must contain exactly 32 bytes")
	}
	return key, nil
}

func encrypt(key, plain []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plain, nil), nil
}
func decrypt(key, encrypted []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(encrypted) < gcm.NonceSize() {
		return nil, errors.New("invalid encrypted secret")
	}
	return gcm.Open(nil, encrypted[:gcm.NonceSize()], encrypted[gcm.NonceSize():], nil)
}
func signingKey(secret string) ([]byte, error) {
	if !strings.HasPrefix(secret, "whsec_") {
		return nil, errors.New("signing secret requires whsec_ prefix")
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil || len(key) < 24 || len(key) > 64 {
		return nil, errors.New("signing key must decode to 24-64 bytes")
	}
	return key, nil
}
func signature(key []byte, id string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, key)
	fmt.Fprintf(mac, "%s.%d.", id, timestamp)
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return errors.New("callback must be an absolute HTTPS URL without credentials or fragment")
	}
	if u.Port() != "" && u.Port() != "443" {
		return errors.New("callback HTTPS port must be 443")
	}
	return nil
}

var forbiddenNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("2001::/32"),
}

func publicAddress(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range forbiddenNetworks {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func safeHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, DisableKeepAlives: true, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errors.New("callback has no addresses")
		}
		for _, ip := range ips {
			if !publicAddress(ip.IP) {
				return nil, errors.New("callback destination is not public")
			}
		}
		var last error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}}
	return &http.Client{Timeout: 10 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func (s *Service) post(ctx context.Context, callback, id, subscription string, body, key, oldKey []byte) (int, []byte, error) {
	if err := validateURL(callback); err != nil {
		return 0, nil, err
	}
	if len(body) > maxPayload {
		return 413, nil, errors.New("event payload too large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callback, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	now := time.Now().Unix()
	sig := signature(key, id, now, body)
	if len(oldKey) > 0 {
		sig += " " + signature(oldKey, id, now, body)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("webhook-id", id)
	req.Header.Set("webhook-timestamp", strconv.FormatInt(now, 10))
	req.Header.Set("webhook-signature", sig)
	req.Header.Set("X-MCP-Subscription-Id", subscription)
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if len(response) > 4096 {
		return resp.StatusCode, nil, errors.New("callback response too large")
	}
	return resp.StatusCode, response, err
}

func (s *Service) verify(ctx context.Context, callback, subscription string, key []byte) error {
	challengeBytes := make([]byte, 32)
	if _, err := rand.Read(challengeBytes); err != nil {
		return err
	}
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes)
	body, _ := json.Marshal(map[string]string{"type": "verification", "challenge": challenge})
	status, response, err := s.post(ctx, callback, "verification_"+challenge, subscription, body, key, nil)
	if err != nil {
		return err
	}
	var echo struct {
		Challenge string `json:"challenge"`
	}
	if status < 200 || status >= 300 || json.Unmarshal(response, &echo) != nil || !hmac.Equal([]byte(echo.Challenge), []byte(challenge)) {
		return errors.New("callback challenge failed")
	}
	return nil
}

// Cache successful callback verification briefly, bound to principal and signing
// key. Connection-time address validation still applies on every event POST.
func (s *Service) verifyFor(ctx context.Context, p model.Principal, callback, subscription string, key []byte) error {
	identity, _ := json.Marshal([]any{p.TeamID, p.Subject, callback, key})
	digest := sha256.Sum256(identity)
	now := time.Now()
	s.verificationMu.Lock()
	expiry := s.verified[digest]
	s.verificationMu.Unlock()
	if now.Before(expiry) {
		return nil
	}
	if err := s.verify(ctx, callback, subscription, key); err != nil {
		return err
	}
	s.verificationMu.Lock()
	defer s.verificationMu.Unlock()
	for id, until := range s.verified {
		if !now.Before(until) {
			delete(s.verified, id)
		}
	}
	if len(s.verified) < 256 {
		s.verified[digest] = now.Add(5 * time.Minute)
	}
	return nil
}
