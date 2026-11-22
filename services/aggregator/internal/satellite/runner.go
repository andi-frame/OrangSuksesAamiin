// Package satellite consumes the elected Aggregator's VIIRS event stream.
package satellite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

type Runner struct {
	Repo       ports.Repository
	Store      ports.SatelliteStore
	Client     *http.Client
	Gate       func(context.Context) error
	URL, Token string
	Lateness   time.Duration
	StartFrom  string // "latest": a session without checkpoint starts at the feed head
	Provinces  []province
	Log        *slog.Logger
}

func (r *Runner) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := r.consume(ctx)
		if errors.Is(err, ports.ErrFenced) || ctx.Err() != nil {
			return
		}
		metricDisconnects.Inc()
		r.Log.WarnContext(ctx, "satellite_stream_disconnected", "error", err)
		var code responseError
		if errors.As(err, &code) && (code == 400 || code == 401 || code == 403) {
			backoff = 30 * time.Second
		}
		if time.Since(start) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff + rand.N(backoff/4)):
		}
		backoff = min(30*time.Second, backoff*2)
	}
}

func (r *Runner) consume(ctx context.Context) error {
	if r.Gate != nil {
		if err := r.Gate(ctx); err != nil {
			return err
		}
	}
	session, err := r.session(ctx)
	if err != nil {
		return err
	}
	identity := strings.Join([]string{session.Group, session.Profile, session.ID}, "|")
	key := strings.TrimRight(r.URL, "/") + "|" + identity
	s := newState()
	data, err := r.Store.LoadSatellite(ctx, key)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		if err = json.Unmarshal(data, &s); err != nil {
			return err
		}
	}
	// never for the exam profile: its whole stream is compared with the assistants' reference
	if s.Offset == nil && r.StartFrom == "latest" && session.Profile != "exam" && session.Latest > 0 {
		start := session.Latest - 1 // the stream is inclusive: event Latest-1 is read and skipped as already seen
		s.Offset, s.Applied = &start, &start
		r.Log.InfoContext(ctx, "satellite_start_at_head", "offset", session.Latest)
	}
	if len(s.Pending) > 0 || (s.Offset != nil && (s.Applied == nil || *s.Applied != *s.Offset)) {
		if err := r.flush(ctx, key, &s, session.Latest); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := r.get(ctx, streamURL(s.Offset))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	events := make(chan event, 128)
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- readEvents(ctx, resp.Body, events)
		close(events)
	}()
	var head atomic.Int64
	head.Store(session.Latest)
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current, err := r.session(ctx)
				if err == nil && current.Group == session.Group && current.Profile == session.Profile && current.ID == session.ID {
					head.Store(current.Latest)
					metricHead.Set(float64(current.Latest))
				}
			}
		}
	}()
	p := newProcessor(&s, r.Provinces, r.Lateness, identity)
	// The checkpoint holds the whole clustering state and grows with the session, so it is written at most once
	// per second or every 512 events: a burst is stored in large batches instead of one state write per event.
	// Resuming replays at most those events from the stored offset (idempotent: versions already stored are skipped).
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	idle := time.NewTimer(45 * time.Second)
	defer idle.Stop()
	dirty := 0
	flush := func() error {
		if dirty == 0 {
			return nil
		}
		if err := r.flush(ctx, key, &s, head.Load()); err != nil {
			return err
		}
		dirty = 0
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-idle.C:
			if err := flush(); err != nil {
				return err
			}
			return errors.New("satellite feed idle timeout")
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case e, ok := <-events:
			if ok {
				metricEvents.Inc(e.kind)
			}
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				return <-streamErr
			}
			idle.Reset(45 * time.Second)
			offset, numbered, err := offsetOf(e)
			if err != nil {
				return err
			}
			if !numbered {
				continue
			}
			if s.Offset != nil && offset <= *s.Offset {
				continue
			}
			if s.Offset != nil && offset != *s.Offset+1 || s.Offset == nil && offset > 1 {
				return fmt.Errorf("satellite offset gap before %d", offset)
			}
			e.id = strconv.FormatInt(offset, 10)
			if err := p.apply(e); err != nil {
				// apply only fails on the content of this one event (unparseable, invalid values, an update of a
				// detection never seen). Stopping here would retry the same offset forever and stall the whole
				// feed, so the event is counted, logged with its raw text and skipped; the offset still advances.
				s.Rejected++
				metricRejected.Inc()
				raw := string(e.data)
				if len(raw) > 512 {
					raw = raw[:512]
				}
				r.Log.WarnContext(ctx, "satellite_event_rejected", "offset", offset, "kind", e.kind,
					"error", err.Error(), "raw", raw)
			}
			s.Offset = &offset
			dirty++
			if dirty >= 512 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Runner) flush(ctx context.Context, key string, s *state, head int64) error {
	ctx, cancel := context.WithTimeout(obs.WithCorrelationID(ctx, obs.NewCorrelationID()), 15*time.Second)
	defer cancel()
	if r.Gate != nil {
		if err := r.Gate(ctx); err != nil {
			return err
		}
	}
	save := func() error {
		b, err := json.Marshal(s)
		if err != nil {
			return err
		}
		return r.Store.StoreSatellite(ctx, key, b)
	}
	if len(s.Pending) > 0 {
		if err := save(); err != nil {
			return err
		}
	}
	for _, h := range s.Pending {
		written, err := r.saveChange(ctx, h)
		if err != nil {
			return err
		}
		if written {
			metricStoreLatency.ObserveSince(h.UpdatedAt)
			r.Log.InfoContext(ctx, "satellite_hazard_applied", "hazard_id", h.HazardID, "version", h.Version,
				"store_latency_ms", time.Since(h.UpdatedAt).Milliseconds(), "retired", h.Retired)
		}
	}
	s.Pending = nil
	s.Applied = s.Offset
	if err := save(); err != nil {
		return err
	}
	singletons := len(s.Detections) - len(s.Assignment)
	if s.Applied == nil {
		return nil
	}
	lag := max(0, head-*s.Applied)
	metricHead.Set(float64(head))
	metricApplied.Set(float64(*s.Applied))
	metricLag.Set(float64(lag))
	metricLate.Set(float64(s.Late))
	metricSingletons.Set(float64(singletons))
	metricActiveFires.Set(float64(activeFires(s)))
	r.Log.InfoContext(ctx, "satellite_checkpoint", "applied_offset", *s.Applied,
		"last_session_head", head, "lag_offsets", lag, "late_detections", s.Late, "singletons", singletons, "rejected_events", s.Rejected)
	status, err := json.Marshal(map[string]any{
		"source": domain.SourceSatellite, "state": "up", "breaker": "closed",
		"last_success_at": time.Now().UTC(), "applied_offset": *s.Applied,
		"last_session_head": head, "lag_offsets": lag, "late_detections": s.Late, "singletons": singletons, "rejected_events": s.Rejected,
	})
	if err == nil {
		err = r.Repo.PutSourceStatus(ctx, string(domain.SourceSatellite), status)
	}
	if err != nil && ctx.Err() == nil {
		r.Log.WarnContext(ctx, "satellite_status_store_failed", "error", err)
	}
	return nil
}

func (r *Runner) saveChange(ctx context.Context, h domain.HazardEvent) (bool, error) {
	prior, err := r.Repo.Get(ctx, h.HazardID)
	if err != nil && !errors.Is(err, ports.ErrNotFound) {
		return false, err
	}
	if prior != nil && prior.Version >= h.Version {
		// An interrupted COMMIT can leave a visible row on the primary while
		// it is still waiting for WAL to reach the standby. A fenced WAL
		// barrier confirms durability before advancing the applied offset.
		if barrier, ok := r.Repo.(ports.HazardDurability); ok {
			return false, barrier.ConfirmHazard(ctx, h.HazardID, h.Version)
		}
		return false, nil
	}
	kind := domain.KindUpdated
	if h.Version == 1 {
		kind = domain.KindCreated
	}
	if h.Retired {
		kind = domain.KindRetired
	}
	id := domain.EventID(h.HazardID, h.Version)
	payload, err := json.Marshal(domain.Envelope{EventID: id, Kind: kind, EmittedAt: h.UpdatedAt, Hazard: h, SchemaName: domain.SchemaName})
	if err != nil {
		return false, err
	}
	err = r.Repo.Save(ctx, h, h.Version-1, ports.OutboxEntry{EventID: id, HazardID: h.HazardID, Version: h.Version,
		Payload: payload, CorrelationID: obs.CorrelationID(ctx), CreatedAt: h.UpdatedAt})
	return err == nil, err
}

// activeFires counts the fires that are not retired (a fire that shrank below two detections is retired).
func activeFires(s *state) int {
	n := 0
	for _, h := range s.Fires {
		if !h.Retired {
			n++
		}
	}
	return n
}
