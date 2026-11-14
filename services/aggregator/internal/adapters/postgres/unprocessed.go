package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// inTx runs fn in one transaction, after checking the fence carried by ctx.
func (r *Repo) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after Commit
	if err := checkFence(ctx, tx); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// checkFence is the first statement of every fenced write. The upsert raises the
// stored term when ours is newer and, in every case, locks the fence row until
// the transaction ends. A new leader's RaiseFence therefore waits for an
// in-flight write of the old term to finish, and every old-term write that
// starts after it reads the new term and is rejected. The decision is made by
// the store alone: no clock is involved.
func checkFence(ctx context.Context, tx pgx.Tx) error {
	f, ok := ports.FenceFrom(ctx)
	if !ok {
		return nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fence (name, term, holder) VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE SET term = EXCLUDED.term, holder = EXCLUDED.holder, raised_at = now()
		WHERE fence.term < EXCLUDED.term`, f.Name, f.Term, f.Holder); err != nil {
		return fmt.Errorf("fence: %w", err)
	}
	var term int64
	if err := tx.QueryRow(ctx, `SELECT term FROM fence WHERE name = $1`, f.Name).Scan(&term); err != nil {
		return fmt.Errorf("fence: %w", err)
	}
	if term > f.Term {
		return ports.ErrFenced
	}
	return nil
}

func (r *Repo) RaiseFence(ctx context.Context, f ports.Fence) error {
	return r.inTx(ports.WithFence(ctx, f), func(pgx.Tx) error { return nil })
}

const unprocessedCols = `message_id, seq, source, kind, record_key, received_at, reason, retryable, attempts, status,
  next_attempt_at, resolved_at, correlation_id, raw, raw_size`

func scanUnprocessed(row pgx.Row) (*ports.Unprocessed, error) {
	var m ports.Unprocessed
	if err := row.Scan(&m.MessageID, &m.Seq, &m.Source, &m.Kind, &m.RecordKey, &m.ReceivedAt, &m.Reason, &m.Retryable,
		&m.Attempts, &m.Status, &m.NextAttemptAt, &m.ResolvedAt, &m.CorrelationID, &m.Raw, &m.RawSize); err != nil {
		return nil, err
	}
	m.ReceivedAt, m.NextAttemptAt = m.ReceivedAt.UTC(), m.NextAttemptAt.UTC()
	if m.ResolvedAt != nil {
		t := m.ResolvedAt.UTC()
		m.ResolvedAt = &t
	}
	if m.Raw == nil {
		m.Raw = []byte{}
	}
	return &m, nil
}

func (r *Repo) AddUnprocessed(ctx context.Context, m ports.Unprocessed) (bool, error) {
	if m.RawSize == 0 {
		m.RawSize = int64(len(m.Raw))
	}
	raw := m.Raw
	if raw == nil {
		raw = []byte{}
	}
	var created bool
	err := r.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO unprocessed (message_id, source, kind, record_key, received_at, reason,
			retryable, attempts, status, next_attempt_at, resolved_at, correlation_id, raw, raw_size)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT (message_id) DO NOTHING`,
			m.MessageID, m.Source, m.Kind, m.RecordKey, m.ReceivedAt.UTC(), m.Reason, m.Retryable, m.Attempts, m.Status,
			m.NextAttemptAt.UTC(), m.ResolvedAt, m.CorrelationID, raw, m.RawSize)
		created = err == nil && tag.RowsAffected() == 1
		return err
	})
	return created, err
}

func (r *Repo) UpdateUnprocessed(ctx context.Context, m ports.Unprocessed) error {
	return r.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE unprocessed SET reason=$2, retryable=$3, attempts=$4, status=$5,
			next_attempt_at=$6, resolved_at=$7 WHERE message_id=$1`,
			m.MessageID, m.Reason, m.Retryable, m.Attempts, m.Status, m.NextAttemptAt.UTC(), m.ResolvedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ports.ErrNotFound
		}
		return nil
	})
}

func (r *Repo) GetUnprocessed(ctx context.Context, id string) (*ports.Unprocessed, error) {
	m, err := scanUnprocessed(r.pool.QueryRow(ctx, `SELECT `+unprocessedCols+` FROM unprocessed WHERE message_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	return m, err
}

func (r *Repo) ListUnprocessed(ctx context.Context, f ports.UnprocessedFilter) (ports.UnprocessedPage, error) {
	f.Normalize()
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(args))))
	}
	add("status = ANY(?)", f.Statuses)
	if f.Source != "" {
		add("source = ?", f.Source)
	}
	if f.Kind != "" {
		add("kind = ?", f.Kind)
	}
	if f.RecordKey != "" {
		add("record_key = ?", f.RecordKey)
	}
	if f.DueBefore != nil {
		where = append(where, "status = 'retrying'")
		add("next_attempt_at <= ?", f.DueBefore.UTC())
	}
	if f.Cursor != "" {
		seq, err := ports.DecodeSeqCursor(f.Cursor)
		if err != nil {
			return ports.UnprocessedPage{}, err
		}
		add("seq > ?", seq)
	}
	args = append(args, f.Limit+1)
	q := fmt.Sprintf(`SELECT %s FROM unprocessed WHERE %s ORDER BY seq LIMIT $%d`,
		unprocessedCols, strings.Join(where, " AND "), len(args))
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return ports.UnprocessedPage{}, err
	}
	defer rows.Close()
	page := ports.UnprocessedPage{Items: []ports.Unprocessed{}}
	for rows.Next() {
		m, err := scanUnprocessed(rows)
		if err != nil {
			return ports.UnprocessedPage{}, err
		}
		page.Items = append(page.Items, *m)
	}
	if err := rows.Err(); err != nil {
		return ports.UnprocessedPage{}, err
	}
	if len(page.Items) > f.Limit {
		page.Items = page.Items[:f.Limit]
		page.NextCursor = ports.EncodeSeqCursor(page.Items[f.Limit-1].Seq)
	}
	return page, nil
}

func (r *Repo) RequeueUnprocessed(ctx context.Context, source string, ids []string, now time.Time) (int, error) {
	q := `UPDATE unprocessed SET status = 'retrying', next_attempt_at = $1 WHERE status = 'parked'`
	args := []any{now.UTC()}
	if source != "" {
		args = append(args, source)
		q += fmt.Sprintf(" AND source = $%d", len(args))
	}
	if len(ids) > 0 {
		args = append(args, ids)
		q += fmt.Sprintf(" AND message_id = ANY($%d)", len(args))
	}
	tag, err := r.pool.Exec(ctx, q, args...)
	return int(tag.RowsAffected()), err
}

func (r *Repo) CountUnprocessed(ctx context.Context) (ports.UnprocessedCounts, error) {
	rows, err := r.pool.Query(ctx, `SELECT source, status, count(*) FROM unprocessed
		WHERE status <> 'resolved' GROUP BY source, status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := ports.UnprocessedCounts{}
	for rows.Next() {
		var src, st string
		var n int64
		if err := rows.Scan(&src, &st, &n); err != nil {
			return nil, err
		}
		if out[src] == nil {
			out[src] = map[string]int64{}
		}
		out[src][st] = n
	}
	return out, rows.Err()
}

func (r *Repo) PutSourceStatus(ctx context.Context, source string, doc []byte) error {
	return r.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO source_status (source, doc, updated_at) VALUES ($1, $2, now())
			ON CONFLICT (source) DO UPDATE SET doc = EXCLUDED.doc, updated_at = now()`, source, doc)
		return err
	})
}

func (r *Repo) ListSourceStatus(ctx context.Context) ([]ports.SourceStatus, error) {
	rows, err := r.pool.Query(ctx, `SELECT source, doc::text, extract(epoch FROM now() - updated_at)
		FROM source_status ORDER BY source`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.SourceStatus
	for rows.Next() {
		var s ports.SourceStatus
		var doc string
		var age float64
		if err := rows.Scan(&s.Source, &doc, &age); err != nil {
			return nil, err
		}
		s.Doc, s.Age = []byte(doc), time.Duration(age*float64(time.Second))
		out = append(out, s)
	}
	return out, rows.Err()
}
