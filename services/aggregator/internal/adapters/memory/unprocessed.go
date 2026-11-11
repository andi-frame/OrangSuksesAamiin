package memory

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func (r *Repo) RaiseFence(ctx context.Context, f ports.Fence) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	return r.checkFenceLocked(ports.WithFence(ctx, f))
}

func cloneUnprocessed(m *ports.Unprocessed) ports.Unprocessed {
	c := *m
	c.Raw = append([]byte(nil), m.Raw...)
	if m.ResolvedAt != nil {
		t := *m.ResolvedAt
		c.ResolvedAt = &t
	}
	return c
}

func (r *Repo) AddUnprocessed(ctx context.Context, m ports.Unprocessed) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return false, r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return false, err
	}
	if _, dup := r.dlq[m.MessageID]; dup {
		return false, nil
	}
	r.dlqSeq++
	m.Seq = r.dlqSeq
	m.ReceivedAt = m.ReceivedAt.UTC()
	m.NextAttemptAt = m.NextAttemptAt.UTC()
	if m.RawSize == 0 {
		m.RawSize = int64(len(m.Raw))
	}
	c := cloneUnprocessed(&m)
	r.dlq[m.MessageID] = &c
	return true, nil
}

func (r *Repo) UpdateUnprocessed(ctx context.Context, m ports.Unprocessed) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return err
	}
	cur, ok := r.dlq[m.MessageID]
	if !ok {
		return ports.ErrNotFound
	}
	cur.Reason, cur.Retryable, cur.Attempts, cur.Status = m.Reason, m.Retryable, m.Attempts, m.Status
	cur.NextAttemptAt = m.NextAttemptAt.UTC()
	cur.ResolvedAt = nil
	if m.ResolvedAt != nil {
		t := m.ResolvedAt.UTC()
		cur.ResolvedAt = &t
	}
	return nil
}

func (r *Repo) GetUnprocessed(_ context.Context, id string) (*ports.Unprocessed, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return nil, r.failing
	}
	m, ok := r.dlq[id]
	if !ok {
		return nil, ports.ErrNotFound
	}
	c := cloneUnprocessed(m)
	return &c, nil
}

func (r *Repo) ListUnprocessed(_ context.Context, f ports.UnprocessedFilter) (ports.UnprocessedPage, error) {
	f.Normalize()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return ports.UnprocessedPage{}, r.failing
	}
	var after int64
	if f.Cursor != "" {
		s, err := ports.DecodeSeqCursor(f.Cursor)
		if err != nil {
			return ports.UnprocessedPage{}, err
		}
		after = s
	}
	var all []ports.Unprocessed
	for _, m := range r.dlq {
		switch {
		case m.Seq <= after,
			f.Source != "" && m.Source != f.Source,
			f.Kind != "" && m.Kind != f.Kind,
			f.RecordKey != "" && m.RecordKey != f.RecordKey,
			!slices.Contains(f.Statuses, m.Status),
			f.DueBefore != nil && (m.Status != ports.StatusRetrying || m.NextAttemptAt.After(*f.DueBefore)):
			continue
		}
		all = append(all, cloneUnprocessed(m))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })
	page := ports.UnprocessedPage{Items: []ports.Unprocessed{}}
	if len(all) > f.Limit {
		all = all[:f.Limit]
		page.NextCursor = ports.EncodeSeqCursor(all[len(all)-1].Seq)
	}
	page.Items = append(page.Items, all...)
	return page, nil
}

func (r *Repo) RequeueUnprocessed(_ context.Context, source string, ids []string, now time.Time) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return 0, r.failing
	}
	n := 0
	for _, m := range r.dlq {
		if m.Status != ports.StatusParked || (source != "" && m.Source != source) ||
			(len(ids) > 0 && !slices.Contains(ids, m.MessageID)) {
			continue
		}
		m.Status, m.NextAttemptAt = ports.StatusRetrying, now.UTC()
		n++
	}
	return n, nil
}

func (r *Repo) CountUnprocessed(context.Context) (ports.UnprocessedCounts, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return nil, r.failing
	}
	out := ports.UnprocessedCounts{}
	for _, m := range r.dlq {
		if m.Status == ports.StatusResolved {
			continue
		}
		if out[m.Source] == nil {
			out[m.Source] = map[string]int64{}
		}
		out[m.Source][m.Status]++
	}
	return out, nil
}

type statusRow struct {
	doc []byte
	at  time.Time
}

func (r *Repo) PutSourceStatus(ctx context.Context, source string, doc []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return err
	}
	r.status[source] = statusRow{doc: append([]byte(nil), doc...), at: time.Now()}
	return nil
}

func (r *Repo) ListSourceStatus(context.Context) ([]ports.SourceStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return nil, r.failing
	}
	out := make([]ports.SourceStatus, 0, len(r.status))
	for src, row := range r.status {
		out = append(out, ports.SourceStatus{Source: src, Doc: append([]byte(nil), row.doc...), Age: time.Since(row.at)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Source < out[j].Source })
	return out, nil
}
