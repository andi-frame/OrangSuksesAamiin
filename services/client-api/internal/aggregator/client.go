package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Freshness struct {
	State         string     `json:"state"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

type ListPage struct {
	Items      []json.RawMessage
	NextCursor string
	Freshness  map[string]Freshness
	StatusCode int
}

type Client struct {
	Base string
	Key  string
	HTTP *http.Client
}

func New(base, key string, log *slog.Logger, timeout time.Duration) *Client {
	return &Client{
		Base: strings.TrimRight(base, "/"),
		Key:  key,
		HTTP: obs.NewHTTPClient(log, "aggregator", timeout),
	}
}

func (c *Client) List(ctx context.Context, q url.Values) (ListPage, error) {
	u := c.Base + "/internal/hazards"
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	return c.getList(ctx, u)
}

func (c *Client) Get(ctx context.Context, hazardID string) ([]byte, int, error) {
	u := c.Base + "/internal/hazards/" + url.PathEscape(hazardID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	c.setHeaders(req)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}

func (c *Client) SourcesStatus(ctx context.Context) ([]byte, int, error) {
	u := c.Base + "/internal/sources/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	c.setHeaders(req)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}

func (c *Client) getList(ctx context.Context, u string) (ListPage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ListPage{}, err
	}
	c.setHeaders(req)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return ListPage{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ListPage{}, err
	}
	out := ListPage{StatusCode: resp.StatusCode}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("aggregator list: %d: %s", resp.StatusCode, string(body))
	}
	var parsed struct {
		Items      []json.RawMessage    `json:"items"`
		NextCursor string               `json:"next_cursor"`
		Freshness  map[string]Freshness `json:"freshness"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return out, err
	}
	out.Items = parsed.Items
	out.NextCursor = parsed.NextCursor
	out.Freshness = parsed.Freshness
	return out, nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("X-Service-Key", c.Key)
	if id := obs.CorrelationID(req.Context()); id != "" {
		req.Header.Set(obs.HeaderCorrelationID, id)
	}
}
