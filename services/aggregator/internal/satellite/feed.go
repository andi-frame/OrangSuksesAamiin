package satellite

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type event struct {
	id, kind string
	data     []byte
	received time.Time
}

type session struct {
	Group   string `json:"group"`
	Profile string `json:"profile"`
	ID      string `json:"session_id"`
	Latest  int64  `json:"latest_offset"`
}

type responseError int

func (e responseError) Error() string { return fmt.Sprintf("satellite feed returned HTTP %d", int(e)) }

func (r *Runner) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(r.URL, "/")+path, nil)
	if err != nil {
		return nil, errors.New("invalid satellite feed URL")
	}
	req.Header.Set("Authorization", "Bearer "+r.Token)
	if strings.HasPrefix(path, "/v1/stream") {
		req.Header.Set("Accept", "text/event-stream")
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, responseError(resp.StatusCode)
	}
	return resp, nil
}

func (r *Runner) session(ctx context.Context) (session, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := r.get(ctx, "/v1/session")
	if err != nil {
		return session{}, err
	}
	defer resp.Body.Close()
	var s session
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&s)
	if err == nil && (s.Group == "" || s.Profile == "") {
		err = errors.New("invalid satellite session identity")
	}
	return s, err
}

func streamURL(offset *int64) string {
	q := url.Values{}
	if offset != nil {
		// Request the boundary too. This accepts feeds with either inclusive
		// or exclusive from semantics and the journal discards duplicates.
		q.Set("from", strconv.FormatInt(*offset, 10))
	} else {
		q.Set("from", "0")
	}
	return "/v1/stream?" + q.Encode()
}

func readEvents(ctx context.Context, body io.Reader, events chan<- event) error {
	const maxEvent = 1 << 20
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), maxEvent)
	var e event
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(e.data) > 0 {
				e.received = time.Now().UTC().Truncate(time.Microsecond)
				e.data = e.data[:len(e.data)-1]
				select {
				case events <- e:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			e = event{}
			continue
		}
		key, val, _ := strings.Cut(line, ":")
		val = strings.TrimPrefix(val, " ")
		switch key {
		case "event":
			e.kind = val
		case "id":
			e.id = val
		case "data":
			if len(e.data)+len(val)+1 > maxEvent {
				return errors.New("satellite SSE event exceeds 1 MiB")
			}
			e.data = append(append(e.data, val...), '\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

func rawText(b json.RawMessage) string {
	if len(b) == 0 {
		return ""
	}
	if b[0] != '"' {
		return string(b)
	}
	var v string
	_ = json.Unmarshal(b, &v)
	return v
}

func (p *processor) decodeChange(kind string, data []byte) (detection, string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return detection{}, "", err
	}
	old := rawText(fields["previous_key"])
	if old == "" {
		old = rawText(fields["detection_key"])
	}
	if old == "" {
		old = rawText(fields["key"])
	}
	if kind == "retract" && old != "" {
		return detection{}, old, nil
	}
	if kind == "update" && len(fields["changes"]) > 0 {
		var changes map[string]json.RawMessage
		if err := json.Unmarshal(fields["changes"], &changes); err != nil {
			return detection{}, "", err
		}
		prior, ok := p.state.Detections[old]
		if !ok {
			prior, ok = p.state.Inactive[old]
		}
		if !ok {
			if p.state.Excluded[old] {
				return detection{}, old, nil
			}
			return detection{}, "", errors.New("satellite update references unknown detection")
		}
		if update, ok := changes["frp"]; ok {
			frp, err := strconv.ParseFloat(rawText(update), 64)
			if err != nil || math.IsNaN(frp) || math.IsInf(frp, 0) || frp < 0 {
				return detection{}, "", errors.New("invalid satellite FRP update")
			}
			prior.FRP = frp
		}
		if update, ok := changes["confidence"]; ok {
			conf, err := confidence(rawText(update))
			if err != nil {
				return detection{}, "", err
			}
			prior.Confidence = conf
		}
		return prior, old, nil
	}
	if payload := fields["detection"]; len(payload) > 0 {
		if err := json.Unmarshal(payload, &fields); err != nil {
			return detection{}, "", err
		}
	}
	if kind != "detection" && kind != "update" && kind != "retract" {
		return detection{}, "", fmt.Errorf("unknown satellite event kind %q", kind)
	}
	sat, date, hhmm := rawText(fields["satellite"]), rawText(fields["acq_date"]), rawText(fields["acq_time"])
	lat, lon := rawText(fields["latitude"]), rawText(fields["longitude"])
	key := strings.Join([]string{sat, date, hhmm, lat, lon}, "|")
	if kind == "retract" && sat != "" && date != "" && hhmm != "" && lat != "" && lon != "" {
		return detection{}, key, nil
	}
	if len(hhmm) < 1 || len(hhmm) > 4 || sat == "" || strings.Contains(sat, "|") {
		return detection{}, "", errors.New("invalid satellite detection key")
	}
	at, timeErr := time.Parse("2006-01-02 1504", date+" "+strings.Repeat("0", 4-len(hhmm))+hhmm)
	phi, latErr := strconv.ParseFloat(lat, 64)
	lambda, lonErr := strconv.ParseFloat(lon, 64)
	frp, frpErr := strconv.ParseFloat(rawText(fields["frp"]), 64)
	conf, confErr := confidence(rawText(fields["confidence"]))
	if timeErr != nil || latErr != nil || lonErr != nil || frpErr != nil ||
		math.IsNaN(phi) || math.Abs(phi) > 90 || math.IsNaN(lambda) || math.Abs(lambda) > 180 ||
		math.IsNaN(frp) || math.IsInf(frp, 0) || frp < 0 || confErr != nil {
		return detection{}, "", errors.New("invalid satellite detection")
	}
	return detection{Key: key, Satellite: sat, At: at, Lat: phi, Lon: lambda, FRP: frp, Confidence: conf}, old, nil
}

func confidence(raw string) (string, error) {
	switch strings.ToLower(raw) {
	case "l", "low":
		return "l", nil
	case "n", "nominal":
		return "n", nil
	case "h", "high":
		return "h", nil
	default:
		return "", errors.New("invalid satellite confidence")
	}
}

func offsetOf(e event) (int64, bool, error) {
	raw := e.id
	if raw == "" && len(e.data) > 0 {
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(bytes.TrimSpace(e.data), &payload); err != nil {
			return 0, false, err
		}
		raw = rawText(payload["offset"])
	}
	if raw == "" && e.kind == "heartbeat" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0, false, errors.New("invalid satellite offset")
	}
	return n, true, nil
}
