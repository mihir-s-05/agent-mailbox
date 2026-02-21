package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

type requestEnvelope struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      string         `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

func main() {
	var (
		url        string
		token      string
		method     string
		paramsJSON string
		caFile     string
		certFile   string
		keyFile    string
	)

	flag.StringVar(&url, "url", "", "MCP endpoint URL")
	flag.StringVar(&token, "token", "", "Bearer token")
	flag.StringVar(&method, "method", "", "JSON-RPC method")
	flag.StringVar(&paramsJSON, "params", "{}", "JSON object for params")
	flag.StringVar(&caFile, "ca", "", "Path to CA certificate PEM")
	flag.StringVar(&certFile, "cert", "", "Path to client certificate PEM")
	flag.StringVar(&keyFile, "key", "", "Path to client key PEM")
	flag.Parse()

	if url == "" || token == "" || method == "" {
		fmt.Fprintln(os.Stderr, "missing required flags: --url, --token, --method")
		os.Exit(2)
	}

	params := map[string]any{}
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		fmt.Fprintf(os.Stderr, "invalid --params JSON: %v\n", err)
		os.Exit(2)
	}

	envelope := requestEnvelope{
		JSONRPC: "2.0",
		ID:      "1",
		Method:  method,
		Params:  params,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal envelope: %v\n", err)
		os.Exit(1)
	}

	client := &http.Client{}
	if caFile != "" && certFile != "" && keyFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read CA: %v\n", err)
			os.Exit(1)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			fmt.Fprintln(os.Stderr, "failed to parse CA bundle")
			os.Exit(1)
		}

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load client cert/key: %v\n", err)
			os.Exit(1)
		}
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:   tls.VersionTLS13,
				RootCAs:      caPool,
				Certificates: []tls.Certificate{cert},
			},
		}
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "build request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "status=%d body=%s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}
	fmt.Println(string(respBody))
}
