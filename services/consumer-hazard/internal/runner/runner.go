package runner

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/consumer-hazard/internal/dedup"
)

type Stats struct {
	Processed    int64  `json:"processed"`
	Deduplicated int64  `json:"deduplicated"`
	LastEventID  string `json:"last_event_id"`
	LastHazardID string `json:"last_hazard_id"`
	LastKind     string `json:"last_kind"`
	Consumer     string `json:"consumer"`
	Group        string `json:"group"`
}

type Runner struct {
	client *kgo.Client
	dedup  *dedup.Store
	log    *slog.Logger
	name   string
	group  string

	processed    atomic.Int64
	deduplicated atomic.Int64
	lastEventID  atomic.Value // string
	lastHazard   atomic.Value // string
	lastKind     atomic.Value // string

	alertMu sync.Mutex
	alerted map[string]bool
	alerts  []Alert
}

func New(ctx context.Context, brokers []string, topic, group, name string, log *slog.Logger) (*Runner, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.Ping(pctx); err != nil {
		cl.Close()
		return nil, err
	}
	r := &Runner{client: cl, dedup: dedup.New(), log: log, name: name, group: group}
	r.lastEventID.Store("")
	r.lastHazard.Store("")
	r.lastKind.Store("")
	return r, nil
}

func (r *Runner) Close() { r.client.Close() }

func (r *Runner) Stats() Stats {
	return Stats{
		Processed:    r.processed.Load(),
		Deduplicated: r.deduplicated.Load(),
		LastEventID:  r.lastEventID.Load().(string),
		LastHazardID: r.lastHazard.Load().(string),
		LastKind:     r.lastKind.Load().(string),
		Consumer:     r.name,
		Group:        r.group,
	}
}

func (r *Runner) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := r.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err != nil {
					r.log.WarnContext(ctx, "kafka_fetch_error", "error", fe.Err.Error())
				}
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			r.handle(ctx, rec)
			r.client.MarkCommitRecords(rec)
		})
	}
}

func (r *Runner) handle(ctx context.Context, rec *kgo.Record) {
	h := map[string]string{}
	for _, rh := range rec.Headers {
		h[rh.Key] = string(rh.Value)
	}
	ctx = obs.ExtractHeaders(ctx, h)
	ctx, span := obs.StartSpan(ctx, "consume "+rec.Topic, obs.SpanConsumer,
		"consumer", r.name, "partition", int(rec.Partition), "offset", rec.Offset)
	defer span.End()

	var env struct {
		EventID   string    `json:"event_id"`
		Kind      string    `json:"kind"`
		EmittedAt time.Time `json:"emitted_at"`
		Hazard    struct {
			HazardID   string         `json:"hazard_id"`
			Source     string         `json:"source"`
			Severity   string         `json:"severity"`
			Attributes map[string]any `json:"attributes"`
		} `json:"hazard"`
	}
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		consumed.Inc(r.name, "bad_message")
		r.log.WarnContext(ctx, "consumer_bad_message", "error", err.Error())
		return
	}
	if env.EventID == "" {
		consumed.Inc(r.name, "bad_message")
		r.log.WarnContext(ctx, "consumer_bad_message", "error", "missing event_id")
		return
	}
	span.SetAttr("event_id", env.EventID)
	if !r.dedup.Mark(env.EventID) {
		consumed.Inc(r.name, "duplicate")
		r.deduplicated.Add(1)
		return
	}
	consumed.Inc(r.name, "processed")
	if !env.EmittedAt.IsZero() {
		delivery.Observe(time.Since(env.EmittedAt).Seconds(), r.name, env.Hazard.Source)
	}
	if crit, _ := env.Hazard.Attributes["critical"].(bool); crit {
		r.critical(ctx, env.EventID, env.Hazard.HazardID, env.Hazard.Source, env.Hazard.Attributes, env.EmittedAt)
	}
	r.processed.Add(1)
	r.lastEventID.Store(env.EventID)
	r.lastHazard.Store(env.Hazard.HazardID)
	r.lastKind.Store(env.Kind)
	r.log.InfoContext(ctx, "consumer_event",
		"consumer", r.name, "group", r.group,
		"event_id", env.EventID, "kind", env.Kind, "hazard_id", env.Hazard.HazardID)
}

var (
	consumed = obs.NewCounter("consumer_events_total",
		"Events read from the broker per outcome (processed, duplicate, bad_message).", "consumer", "outcome")
	delivery = obs.NewHistogram("consumer_delivery_seconds",
		"Time from the hazard write in the Aggregator (envelope emitted_at) to this consumer.",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 7.5, 10, 30}, "consumer", "source")
	criticalAlerts = obs.NewCounter("consumer_critical_alerts_total",
		"Critical alerts (attributes.critical=true) handed to the Tim Lapangan notification channel.", "consumer", "source")
	criticalLatency = obs.NewHistogram("consumer_critical_alert_seconds",
		"Critical alerts: time from the hazard write to the notification (SLO: p95 <= 5 s).",
		[]float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 7.5, 10, 30}, "consumer")
)

// Alert is one critical notification for Tim Lapangan (kept for GET /alerts).
type Alert struct {
	EventID    string         `json:"event_id"`
	HazardID   string         `json:"hazard_id"`
	Source     string         `json:"source"`
	NotifiedAt time.Time      `json:"notified_at"`
	LatencyMs  float64        `json:"latency_ms"`
	Attributes map[string]any `json:"attributes"`
}

// critical delivers a critical alert. In this PoC the channel to Tim Lapangan is
// the structured log line "critical_alert" plus GET /alerts; a real deployment
// would push (SMS, app) from here. Alerts are keyed by hazard_id: a later
// version of the same hazard does not alert twice.
func (r *Runner) critical(ctx context.Context, eventID, hazardID, source string, attrs map[string]any, emitted time.Time) {
	r.alertMu.Lock()
	if r.alerted == nil {
		r.alerted = map[string]bool{}
	}
	if r.alerted[hazardID] {
		r.alertMu.Unlock()
		return
	}
	r.alerted[hazardID] = true
	lat := 0.0
	if !emitted.IsZero() {
		lat = time.Since(emitted).Seconds()
	}
	a := Alert{EventID: eventID, HazardID: hazardID, Source: source, NotifiedAt: time.Now().UTC(), LatencyMs: lat * 1000, Attributes: attrs}
	r.alerts = append(r.alerts, a)
	if len(r.alerts) > 500 {
		r.alerts = r.alerts[len(r.alerts)-500:]
	}
	r.alertMu.Unlock()
	criticalAlerts.Inc(r.name, source)
	criticalLatency.Observe(lat, r.name)
	r.log.WarnContext(ctx, "critical_alert", "consumer", r.name, "hazard_id", hazardID, "event_id", eventID,
		"source", source, "latency_ms", lat*1000, "attributes", attrs)
}

// Alerts returns the most recent critical alerts (newest last).
func (r *Runner) Alerts() []Alert {
	r.alertMu.Lock()
	defer r.alertMu.Unlock()
	return append([]Alert{}, r.alerts...)
}
