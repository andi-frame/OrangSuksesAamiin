// Package processor processes Tim Lapangan field reports exactly once
// (M2 Problem 3, criterion 6).
//
// Input:  topic "field-reports"           (produced by the Client-Facing API before its 202)
// Output: topic "field-reports.processed" (compacted, key = report_id)
//
// Exactly-once, end to end:
//
//   - Consume-transform-produce runs in Kafka transactions
//     (kgo.GroupTransactSession): the processed records and the consumer
//     offsets of the input are committed atomically. A crash anywhere before
//     the commit aborts both, the input is read again, and nothing aborted is
//     visible (all readers use read_committed). A crash after the commit has
//     nothing left to redo.
//   - The same report submitted twice (client retry with the same
//     Idempotency-Key -> same report_id) is two input records. The processor
//     keeps the set of processed report_ids and skips the second one. The set
//     is rebuilt at startup from the compacted output topic (read_committed, up
//     to the last stable offset) before the group is joined, so it survives
//     restarts without a database of its own.
//   - A zombie instance (paused, partitioned) is fenced twice: by the
//     transactional id (a new instance with the same id aborts its open
//     transaction and makes its later commits fail) and by the group
//     generation (its offset commit is rejected after a rebalance).
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// Incoming is what the Client-Facing API produced.
type Incoming struct {
	ReportID       string          `json:"report_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	ClientID       string          `json:"client_id"`
	ReceivedAt     time.Time       `json:"received_at"`
	Report         json.RawMessage `json:"report"`
}

// Record is one processed report (the value in the output topic).
type Record struct {
	ReportID       string          `json:"report_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	ClientID       string          `json:"client_id"`
	Status         string          `json:"status"` // "processed"
	ReceivedAt     time.Time       `json:"received_at"`
	ProcessedAt    time.Time       `json:"processed_at"`
	Report         json.RawMessage `json:"report"`
	Input          string          `json:"input"` // topic/partition@offset that was processed
	Processor      string          `json:"processor"`
}

type Config struct {
	Brokers       []string
	InTopic       string
	OutTopic      string
	Group         string
	TransactionID string
	Instance      string
	Delay         time.Duration // artificial processing time per report (demo: "slow downstream")
}

type Processor struct {
	cfg Config
	log *slog.Logger

	mu    sync.RWMutex
	state map[string]Record

	ready      atomic.Bool
	processed  atomic.Int64
	duplicates atomic.Int64
	invalid    atomic.Int64
	aborted    atomic.Int64
}

func New(cfg Config, log *slog.Logger) *Processor {
	return &Processor{cfg: cfg, log: log, state: map[string]Record{}}
}

var (
	mProcessed = obs.NewCounter("field_reports_processed_total",
		"Field reports processed (committed to the output topic), by outcome: processed, duplicate, invalid.", "outcome")
	mAborted  = obs.NewCounter("field_reports_transactions_aborted_total", "Transactions aborted (rebalance, broker error); the input is read again.")
	mLatency  = obs.NewHistogram("field_reports_end_to_end_seconds", "From the 202 of the Client-Facing API to the committed processing.", []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300})
	mStateLen = obs.NewGauge("field_reports_known", "report_ids already processed (dedup set).")
)

// Get returns a processed report.
func (p *Processor) Get(id string) (Record, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	r, ok := p.state[id]
	return r, ok
}

// Ready is true once the dedup set is rebuilt and the group is joined.
func (p *Processor) Ready() bool { return p.ready.Load() }

// Stats for GET /stats.
func (p *Processor) Stats() map[string]any {
	p.mu.RLock()
	n := len(p.state)
	p.mu.RUnlock()
	return map[string]any{"known_reports": n, "processed": p.processed.Load(), "duplicates_skipped": p.duplicates.Load(),
		"invalid": p.invalid.Load(), "transactions_aborted": p.aborted.Load(), "ready": p.Ready()}
}

// Rebuild reads the whole output topic (read_committed) up to its last stable
// offsets into the dedup set.
func (p *Processor) Rebuild(ctx context.Context) error {
	// KeepControlRecords: transaction markers occupy offsets too; seeing them is
	// how a partition whose last offset is a commit marker is known to be done.
	cl, err := kgo.NewClient(kgo.SeedBrokers(p.cfg.Brokers...), kgo.ConsumeTopics(p.cfg.OutTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords())
	if err != nil {
		return err
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListCommittedOffsets(ctx, p.cfg.OutTopic)
	if err != nil {
		return fmt.Errorf("list offsets of %s: %w", p.cfg.OutTopic, err)
	}
	if err := ends.Error(); err != nil {
		return fmt.Errorf("list offsets of %s: %w", p.cfg.OutTopic, err)
	}
	want := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			want[o.Partition] = o.Offset
		}
	})
	n := 0
	for len(want) > 0 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fs.EachError(func(t string, part int32, err error) {
			p.log.Warn("rebuild_fetch_error", "topic", t, "partition", part, "error", err.Error())
		})
		fs.EachRecord(func(r *kgo.Record) {
			var rec Record
			if !r.Attrs.IsControl() && json.Unmarshal(r.Value, &rec) == nil && rec.ReportID != "" {
				p.mu.Lock()
				p.state[rec.ReportID] = rec
				p.mu.Unlock()
				n++
			}
			if end, ok := want[r.Partition]; ok && r.Offset+1 >= end {
				delete(want, r.Partition)
			}
		})
	}
	p.mu.RLock()
	known := len(p.state)
	p.mu.RUnlock()
	mStateLen.Set(float64(known))
	p.log.Info("dedup_set_rebuilt", "topic", p.cfg.OutTopic, "records", n, "known_reports", known)
	return nil
}

// Run processes until ctx ends.
func (p *Processor) Run(ctx context.Context) error {
	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(p.cfg.Brokers...),
		kgo.TransactionalID(p.cfg.TransactionID),
		// the client gives up on a produce (20 s) and aborts before the broker's
		// transaction timeout (60 s) would fence it
		kgo.TransactionTimeout(60*time.Second),
		kgo.RecordDeliveryTimeout(20*time.Second),
		kgo.DefaultProduceTopic(p.cfg.OutTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ConsumerGroup(p.cfg.Group),
		kgo.ConsumeTopics(p.cfg.InTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequireStableFetchOffsets(),
	)
	if err != nil {
		return err
	}
	defer sess.Close()
	p.ready.Store(true)
	p.log.Info("processor_started", "in", p.cfg.InTopic, "out", p.cfg.OutTopic, "group", p.cfg.Group,
		"transactional_id", p.cfg.TransactionID, "delay_ms", p.cfg.Delay.Milliseconds())

	for ctx.Err() == nil {
		fs := sess.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, part int32, err error) {
			if !errors.Is(err, context.Canceled) {
				p.log.Warn("fetch_error", "topic", t, "partition", part, "error", err.Error())
			}
		})
		if fs.NumRecords() == 0 {
			continue
		}
		if err := p.batch(ctx, sess, fs); err != nil && ctx.Err() == nil {
			// The transaction was aborted, and the producer may be fenced (its
			// epoch bumped by a coordinator timeout): such a session is unusable.
			// Start over with a new session (new epoch) and a rebuilt dedup set;
			// the input is read again from the last committed offsets.
			p.ready.Store(false)
			return fmt.Errorf("batch failed, restarting the session: %w", err)
		}
	}
	return nil
}

// batch processes one fetch inside one transaction.
func (p *Processor) batch(ctx context.Context, sess *kgo.GroupTransactSession, fs kgo.Fetches) error {
	if err := sess.Begin(); err != nil {
		return err
	}
	pending := map[string]Record{}
	var outcomes []string
	var spans []*obs.Span
	var lat []float64
	var produceErr error
	var wg sync.WaitGroup
	var errMu sync.Mutex
	fs.EachRecord(func(r *kgo.Record) {
		h := map[string]string{}
		for _, rh := range r.Headers {
			h[rh.Key] = string(rh.Value)
		}
		rctx := obs.ExtractHeaders(ctx, h)
		rctx, span := obs.StartSpan(rctx, "process field-report", obs.SpanConsumer,
			"partition", int(r.Partition), "offset", r.Offset)
		spans = append(spans, span)
		var in Incoming
		if err := json.Unmarshal(r.Value, &in); err != nil || in.ReportID == "" {
			outcomes = append(outcomes, "invalid")
			p.log.WarnContext(rctx, "field_report_invalid", "partition", r.Partition, "offset", r.Offset)
			return
		}
		span.SetAttr("report_id", in.ReportID)
		if _, done := p.Get(in.ReportID); done || pending[in.ReportID].ReportID != "" {
			outcomes = append(outcomes, "duplicate")
			p.log.InfoContext(rctx, "field_report_duplicate_skipped", "report_id", in.ReportID,
				"idempotency_key", in.IdempotencyKey, "input", fmt.Sprintf("%s/%d@%d", r.Topic, r.Partition, r.Offset))
			return
		}
		if p.cfg.Delay > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(p.cfg.Delay):
			}
		}
		rec := Record{ReportID: in.ReportID, IdempotencyKey: in.IdempotencyKey, ClientID: in.ClientID, Status: "processed",
			ReceivedAt: in.ReceivedAt, ProcessedAt: time.Now().UTC(), Report: in.Report,
			Input: fmt.Sprintf("%s/%d@%d", r.Topic, r.Partition, r.Offset), Processor: p.cfg.Instance}
		val, _ := json.Marshal(rec)
		out := &kgo.Record{Key: []byte(rec.ReportID), Value: val}
		hdr := map[string]string{}
		obs.InjectHeaders(rctx, hdr)
		for k, v := range hdr {
			out.Headers = append(out.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		wg.Add(1)
		sess.Produce(ctx, out, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				errMu.Lock()
				produceErr = err
				errMu.Unlock()
			}
		})
		pending[rec.ReportID] = rec
		outcomes = append(outcomes, "processed")
		if !in.ReceivedAt.IsZero() {
			lat = append(lat, time.Since(in.ReceivedAt).Seconds())
		}
		p.log.InfoContext(rctx, "field_report_processing", "report_id", rec.ReportID, "input", rec.Input)
	})
	wg.Wait()
	try := kgo.TryCommit
	if produceErr != nil {
		try = kgo.TryAbort
	}
	endCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	committed, err := sess.End(endCtx, try)
	cancel()
	for _, s := range spans {
		if !committed {
			s.RecordError(errors.New("transaction aborted"))
		}
		s.End()
	}
	if !committed {
		p.aborted.Add(1)
		mAborted.Inc()
		p.log.Warn("transaction_aborted", "records", len(outcomes), "produce_error", fmt.Sprint(produceErr), "error", fmt.Sprint(err))
		if err == nil {
			err = fmt.Errorf("transaction aborted: %v", produceErr)
		}
		return err
	}
	p.mu.Lock()
	for id, rec := range pending {
		p.state[id] = rec
	}
	known := len(p.state)
	p.mu.Unlock()
	mStateLen.Set(float64(known))
	for _, o := range outcomes {
		mProcessed.Inc(o)
		switch o {
		case "processed":
			p.processed.Add(1)
		case "duplicate":
			p.duplicates.Add(1)
		case "invalid":
			p.invalid.Add(1)
		}
	}
	for _, l := range lat {
		mLatency.Observe(l)
	}
	for id := range pending {
		p.log.Info("field_report_processed", "report_id", id)
	}
	return nil
}

// Reset forgets the dedup set before a Rebuild (a restart of the session).
func (p *Processor) Reset() {
	p.mu.Lock()
	p.state = map[string]Record{}
	p.mu.Unlock()
}
