// Package sources contains the network clients for the two upstream agencies.
// Every call goes through obs.NewHTTPClient, so the correlation ID is forwarded
// and the latency of each outbound call is logged.
package sources

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// SourceError marks a failure of the upstream itself (network, timeout, non-2xx,
// unreadable body). The poller counts these against the circuit breaker; a
// record that merely fails validation is a mapping problem, not a source error.
type SourceError struct {
	Source string
	Status int // 0 when there was no HTTP response
	Err    error
}

func (e *SourceError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s: HTTP %d: %v", e.Source, e.Status, e.Err)
	}
	return fmt.Sprintf("%s: %v", e.Source, e.Err)
}

func (e *SourceError) Unwrap() error { return e.Err }

// IsAuth reports a credential problem (401/403) - worth a distinct log line.
func (e *SourceError) IsAuth() bool {
	return e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden
}

// getArray performs GET base+path?since=... and splits the JSON array response
// into its elements without decoding them (SplitArray): every element keeps its
// exact bytes, and a broken element is returned with its Problem instead of
// failing the whole response.
func getArray(ctx context.Context, hc *http.Client, lim Limits, source, base, path string, since time.Time, hdr map[string]string) ([]Element, error) {
	u := strings.TrimRight(base, "/") + path
	if !since.IsZero() {
		u += "?" + url.Values{"since": {since.UTC().Format(time.RFC3339Nano)}}.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, &SourceError{Source: source, Err: err}
	}
	for k, v := range hdr {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, &SourceError{Source: source, Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, &SourceError{Source: source, Status: resp.StatusCode, Err: errors.New(strings.TrimSpace(string(snippet)))}
	}
	out, err := SplitArray(resp.Body, lim)
	if err != nil {
		return nil, &SourceError{Source: source, Status: resp.StatusCode, Err: fmt.Errorf("reading response: %w", err)}
	}
	return out, nil
}

// BMKG is the client of the BMKG mock (credential: X-BMKG-Key).
type BMKG struct {
	HTTP    *http.Client
	BaseURL string
	APIKey  string
	Caller  string // instance name sent as X-Caller, so the upstream log shows who asked
	Limits  Limits
}

func (c *BMKG) hdr() map[string]string {
	return map[string]string{"X-BMKG-Key": c.APIKey, obs.HeaderCaller: c.Caller}
}

func (c *BMKG) SeismicEvents(ctx context.Context, since time.Time) ([]Element, error) {
	return getArray(ctx, c.HTTP, c.Limits, "bmkg", c.BaseURL, "/seismic-events", since, c.hdr())
}

func (c *BMKG) TsunamiWarnings(ctx context.Context, since time.Time) ([]Element, error) {
	return getArray(ctx, c.HTTP, c.Limits, "bmkg", c.BaseURL, "/tsunami-warnings", since, c.hdr())
}

// PVMBG is the client of the PVMBG mock (credential: Authorization: Bearer).
type PVMBG struct {
	HTTP    *http.Client
	BaseURL string
	Token   string
	Caller  string
	Limits  Limits
}

func (c *PVMBG) VolcanicReports(ctx context.Context, since time.Time) ([]Element, error) {
	return getArray(ctx, c.HTTP, c.Limits, "pvmbg", c.BaseURL, "/volcanic-reports", since,
		map[string]string{"Authorization": "Bearer " + c.Token, obs.HeaderCaller: c.Caller})
}
