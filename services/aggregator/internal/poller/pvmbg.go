package poller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// PVMBGSource polls volcanic reports.
type PVMBGSource struct {
	Client     *sources.PVMBG
	Repo       ports.Repository
	Ingestor   *ingest.Ingestor
	Quarantine *ingest.Quarantine
	Volcanoes  mapping.VolcanoTable
	Overlap    time.Duration
	Log        *slog.Logger
}

func (s *PVMBGSource) Name() string { return "PVMBG" }

func (s *PVMBGSource) PollOnce(ctx context.Context) (Stats, error) {
	var st Stats
	since, err := sinceFor(ctx, s.Repo, CursorPVMBG, s.Overlap)
	if err != nil {
		return st, err
	}
	raws, err := s.Client.VolcanicReports(ctx, since)
	if err != nil {
		return st, err
	}
	st.Fetched = len(raws)

	var maxReported time.Time
	for _, item := range raws {
		raw, ok := item.(map[string]any)
		if !ok {
			reject(ctx, s.Quarantine, &st, "PVMBG", "", "", "record is not a JSON object", item)
			continue
		}
		if t, ok := parseTime(raw["reported_at"]); ok && t.After(maxReported) {
			maxReported = t // the watermark advances even past a record we must reject
		}
		h, err := mapping.Volcanic(raw, s.Volcanoes, time.Now())
		var me *mapping.MappingError
		if errors.As(err, &me) {
			reject(ctx, s.Quarantine, &st, "PVMBG", mapping.RecordID(raw, "report_id"), me.Field, me.Reason, raw)
			continue
		} else if err != nil {
			return st, err
		}
		if _, unknown := h.Attributes["volcano_id"]; unknown {
			s.Log.WarnContext(ctx, "unknown_volcano_id", "volcano_id", h.Attributes["volcano_id"], "report_id", h.SourceRefID)
		}
		out, err := s.Ingestor.Upsert(ctx, h, nil)
		if err != nil {
			return st, err
		}
		count(&st, out)
	}
	return st, advance(ctx, s.Repo, CursorPVMBG, maxReported)
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
