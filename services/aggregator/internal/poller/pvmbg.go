package poller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// PVMBGSource polls volcanic reports.
type PVMBGSource struct {
	Client    *sources.PVMBG
	Repo      ports.Repository
	Ingestor  *ingest.Ingestor
	DLQ       *dlq.Queue
	Rules     mapping.SchemaRules
	Volcanoes mapping.VolcanoTable
	Overlap   time.Duration
	Log       *slog.Logger
}

func (s *PVMBGSource) Name() string { return "PVMBG" }

func (s *PVMBGSource) PollOnce(ctx context.Context) (Stats, error) {
	var st Stats
	since, err := sinceFor(ctx, s.Repo, CursorPVMBG, s.Overlap)
	if err != nil {
		return st, err
	}
	els, err := s.Client.VolcanicReports(ctx, since)
	if err != nil {
		return st, err
	}
	st.Fetched = len(els)

	var maxReported time.Time
	for _, el := range els {
		key := recordKey(el.Raw, "report_id")
		if el.Problem != "" {
			kind := KindPVMBGReport
			if el.Problem == sources.ProblemTruncated {
				kind = KindPVMBGFragment
			}
			msg := dlq.Message{Source: "PVMBG", Kind: kind, RecordKey: key, Raw: el.Raw, Size: el.Size}
			if err := deadLetter(ctx, s.DLQ, &st, msg, elementFailure(el)); err != nil {
				return st, err
			}
			continue
		}
		msg := dlq.Message{Source: "PVMBG", Kind: KindPVMBGReport, RecordKey: key, Raw: el.Raw}
		ahead, err := s.DLQ.OpenAhead(ctx, "PVMBG", KindPVMBGReport, key)
		if err != nil {
			return st, err
		}
		if ahead != nil {
			// an older message of this record is still open: this one waits behind it
			// (or is that very message, re-fetched by the overlap window)
			if ahead.MessageID != dlq.MessageID(msg.Source, msg.Kind, msg.Raw) {
				f := dlq.Transient("ordered_after: %s (older message of report %s is %s)", ahead.MessageID, key, ahead.Status)
				if err := deadLetter(ctx, s.DLQ, &st, msg, f); err != nil {
					return st, err
				}
			}
			maxReported = later(maxReported, reportedAt(el.Raw))
			continue
		}
		out, rep, err := s.process(ctx, el.Raw)
		maxReported = later(maxReported, rep) // advances even past a parked record: it is safe in the dead-letter store
		if errors.Is(err, ports.ErrFenced) {
			return st, err
		}
		if err != nil {
			if err := deadLetter(ctx, s.DLQ, &st, msg, dlq.Classify(err)); err != nil {
				return st, err
			}
			continue
		}
		count(&st, out)
	}
	return st, advance(ctx, s.Repo, CursorPVMBG, maxReported)
}

// process reads, adapts, maps and stores one report. It is used for fresh
// messages and for dead-letter messages alike, so both follow the same rules.
func (s *PVMBGSource) process(ctx context.Context, raw []byte) (ingest.Outcome, time.Time, error) {
	m, err := decodeObject(raw)
	if err != nil {
		return "", time.Time{}, err
	}
	if applied := s.Rules.Apply("PVMBG", m); len(applied) > 0 {
		s.Log.InfoContext(ctx, "schema_rules_applied", "source", "PVMBG", "report_id", m["report_id"], "rules", applied)
	}
	rep, _ := parseTime(m["reported_at"])
	h, err := mapping.Volcanic(m, s.Volcanoes, time.Now())
	if err != nil {
		return "", rep, mappingFailure(err)
	}
	if _, unknown := h.Attributes["volcano_id"]; unknown {
		s.Log.WarnContext(ctx, "unknown_volcano_id", "volcano_id", h.Attributes["volcano_id"], "report_id", h.SourceRefID)
	}
	out, err := s.Ingestor.Upsert(ctx, h, nil)
	return out, rep, err
}

// Reprocess is the dead-letter handler for KindPVMBGReport.
func (s *PVMBGSource) Reprocess(ctx context.Context, m ports.Unprocessed) error {
	_, _, err := s.process(ctx, m.Raw)
	return err
}

func reportedAt(raw []byte) time.Time {
	m, err := decodeObject(raw)
	if err != nil {
		return time.Time{}
	}
	t, _ := parseTime(m["reported_at"])
	return t
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func count(st *Stats, o ingest.Outcome) {
	switch o {
	case ingest.Created:
		st.Created++
	case ingest.Updated:
		st.Updated++
	default:
		st.Unchanged++
	}
}
