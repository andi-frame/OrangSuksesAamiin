package sources

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// Coastal is the client of Sensor Pantai (Lampiran C; credential:
// Authorization: Bearer <PANTAI_AUTH_TOKEN>). It has its own http.Client
// (connection pool, timeout): a hanging sensor cannot use up connections that
// BMKG and PVMBG need (bulkhead).
type Coastal struct {
	HTTP     *http.Client
	BaseURL  string
	Token    string
	Caller   string
	MaxBytes int64 // response limit, default 8 MiB
}

// CoastalPage is one /v1/observations response. Items keep their exact bytes.
type CoastalPage struct {
	GeneratedAt time.Time
	Items       []json.RawMessage
	NextCursor  string
	Raw         []byte // the whole body (kept when it must be set aside)
}

// ErrBadCursor: the sensor rejected our cursor (400), e.g. after it restarted
// from scratch. The poller starts over; storing is idempotent.
var ErrBadCursor = errors.New("sensor rejected the cursor")

// MalformedResponse is a 2xx body that is not the contract's JSON object. The
// body is kept so the poller can set it aside; NextCursor is salvaged when it
// is readable, so one broken page does not block the stream forever.
type MalformedResponse struct {
	Reason     string
	Raw        []byte
	NextCursor string
	// Items salvaged from the "items" array with the same splitter as BMKG and
	// PVMBG: one broken observation does not take the valid ones of its page
	// with it. Broken elements carry their Problem.
	Items       []Element
	GeneratedAt time.Time
}

func (e *MalformedResponse) Error() string { return "malformed response: " + e.Reason }

var (
	nextCursorRe  = regexp.MustCompile(`"next_cursor"\s*:\s*"([A-Za-z0-9_\-=+/.]{1,512})"`)
	generatedAtRe = regexp.MustCompile(`"generated_at"\s*:\s*"([0-9TZ:.+\-]{10,40})"`)
	itemsKeyRe    = regexp.MustCompile(`"items"\s*:\s*\[`)
)

func (c *Coastal) do(ctx context.Context, path string, q url.Values) (*http.Response, error) {
	u := strings.TrimRight(c.BaseURL, "/") + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, &SourceError{Source: "sensor-pantai", Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set(obs.HeaderCaller, c.Caller)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, &SourceError{Source: "sensor-pantai", Err: err}
	}
	return resp, nil
}

func (c *Coastal) limit() int64 {
	if c.MaxBytes <= 0 {
		return 8 << 20
	}
	return c.MaxBytes
}

// readBody reads at most limit bytes; a longer body is an error, not a silent cut.
func (c *Coastal) readBody(resp *http.Response) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(resp.Body, c.limit()+1))
	if err != nil {
		return nil, &SourceError{Source: "sensor-pantai", Status: resp.StatusCode, Err: fmt.Errorf("reading response: %w", err)}
	}
	if int64(len(b)) > c.limit() {
		return b[:c.limit()], &MalformedResponse{Reason: fmt.Sprintf("too_large: more than %d bytes", c.limit()), Raw: b[:min(len(b), 64<<10)]}
	}
	return b, nil
}

func statusError(resp *http.Response) error {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	return &SourceError{Source: "sensor-pantai", Status: resp.StatusCode, Err: errors.New(strings.TrimSpace(string(snippet)))}
}

// Stations returns GET /v1/stations by station_id.
func (c *Coastal) Stations(ctx context.Context) ([]json.RawMessage, error) {
	resp, err := c.do(ctx, "/v1/stations", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, statusError(resp)
	}
	body, err := c.readBody(resp)
	if err != nil {
		return nil, err
	}
	var v struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, &SourceError{Source: "sensor-pantai", Status: resp.StatusCode, Err: fmt.Errorf("stations: %w", err)}
	}
	return v.Items, nil
}

// Observations returns one page after cursor ("" = from the beginning).
func (c *Coastal) Observations(ctx context.Context, cursor string, limit int) (*CoastalPage, error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	resp, err := c.do(ctx, "/v1/observations", q)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest && cursor != "" {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, ErrBadCursor
	}
	if resp.StatusCode/100 != 2 {
		return nil, statusError(resp)
	}
	body, err := c.readBody(resp)
	if err != nil {
		return nil, err
	}
	var v struct {
		GeneratedAt *string           `json:"generated_at"`
		Items       []json.RawMessage `json:"items"`
		NextCursor  *string           `json:"next_cursor"`
	}
	malformed := func(reason string) error {
		m := &MalformedResponse{Reason: reason, Raw: body}
		if mm := nextCursorRe.FindSubmatch(body); mm != nil {
			m.NextCursor = string(mm[1])
		}
		if mm := generatedAtRe.FindSubmatch(body); mm != nil {
			if t, err := time.Parse(time.RFC3339Nano, string(mm[1])); err == nil {
				m.GeneratedAt = t.UTC()
			}
		}
		if loc := itemsKeyRe.FindIndex(body); loc != nil {
			if els, err := SplitArray(bytes.NewReader(body[loc[1]-1:]), Limits{}); err == nil {
				for _, el := range els {
					// an element that does not even start like an object is debris
					// of the broken page (the whole page is kept anyway)
					if t := bytes.TrimSpace(el.Raw); len(t) > 0 && t[0] == '{' {
						m.Items = append(m.Items, el)
					}
				}
			}
		}
		return m
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, malformed("invalid_json: " + err.Error())
	}
	if v.Items == nil {
		return nil, malformed("items missing or not an array")
	}
	p := &CoastalPage{Items: v.Items, Raw: body}
	if v.NextCursor != nil {
		p.NextCursor = *v.NextCursor
	}
	if v.GeneratedAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *v.GeneratedAt)
		if err != nil {
			return nil, malformed("generated_at is not RFC 3339")
		}
		p.GeneratedAt = t.UTC()
	}
	return p, nil
}
