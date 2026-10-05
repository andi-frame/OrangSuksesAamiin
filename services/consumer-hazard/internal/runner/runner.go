package runner

import (
	"context"
	"encoding/json"
	"log/slog"
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

	var env struct {
		EventID string `json:"event_id"`
		Kind    string `json:"kind"`
		Hazard  struct {
			HazardID string `json:"hazard_id"`
		} `json:"hazard"`
	}
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		r.log.WarnContext(ctx, "consumer_bad_message", "error", err.Error())
		return
	}
	if env.EventID == "" {
		r.log.WarnContext(ctx, "consumer_bad_message", "error", "missing event_id")
		return
	}
	if !r.dedup.Mark(env.EventID) {
		r.deduplicated.Add(1)
		return
	}
	r.processed.Add(1)
	r.lastEventID.Store(env.EventID)
	r.lastHazard.Store(env.Hazard.HazardID)
	r.lastKind.Store(env.Kind)
	r.log.InfoContext(ctx, "consumer_event",
		"consumer", r.name, "group", r.group,
		"event_id", env.EventID, "kind", env.Kind, "hazard_id", env.Hazard.HazardID)
}
