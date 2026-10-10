// Package postgres is the PostgreSQL implementation of ports.Repository.
//
// Only identity and canonical fields are columns; everything dynamic lives in
// attributes JSONB, so a new source field never needs DDL. The schema is created
// once at start (CREATE ... IF NOT EXISTS), not per source-schema change.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func init() {
	adapters.RegisterRepository("postgres", func(ctx context.Context, log *slog.Logger) (ports.Repository, error) {
		url := os.Getenv("CANONICAL_DB_URL")
		if url == "" {
			return nil, errors.New("CANONICAL_DB_URL is empty (required when REPOSITORY_BACKEND=postgres)")
		}
		r, err := New(ctx, url)
		if err != nil {
			return nil, err
		}
		log.InfoContext(ctx, "postgres_ready")
		return r, nil
	})
}

// hazard_id uses the "C" collation so that ORDER BY / keyset comparison match Go's
// bytewise string ordering (and therefore the other backends and the cursor).
// The outbox payload is `json`, not `jsonb`: the message is stored exactly as built.
const schema = `
CREATE TABLE IF NOT EXISTS hazards (
  hazard_id     text COLLATE "C" PRIMARY KEY,
  source        text NOT NULL,
  source_ref_id text NOT NULL,
  hazard_type   text NOT NULL,
  severity      text NOT NULL,
  area_name     text NOT NULL,
  latitude      double precision,
  longitude     double precision,
  occurred_at   timestamptz NOT NULL,
  ingested_at   timestamptz NOT NULL,
  updated_at    timestamptz NOT NULL,
  version       integer NOT NULL,
  attributes    jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS hazards_list_idx ON hazards (hazard_type, occurred_at DESC, hazard_id DESC);
CREATE INDEX IF NOT EXISTS hazards_time_idx ON hazards (occurred_at DESC, hazard_id DESC);

CREATE TABLE IF NOT EXISTS outbox (
  seq            bigserial PRIMARY KEY,
  event_id       text NOT NULL UNIQUE,
  hazard_id      text NOT NULL,
  version        integer NOT NULL,
  payload        json NOT NULL,
  correlation_id text NOT NULL DEFAULT '',
  created_at     timestamptz NOT NULL DEFAULT now(),
  published_at   timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (seq) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS poll_cursor (name text PRIMARY KEY, value timestamptz NOT NULL);
`

// Repo is a ports.Repository backed by a pgx connection pool.
type Repo struct{ pool *pgxpool.Pool }

// New connects, verifies the connection and creates the schema if needed.
func New(ctx context.Context, url string) (*Repo, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, err
	}
	r := &Repo{pool: pool}
	if err := r.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return r, nil
}

func (r *Repo) Close() { r.pool.Close() }

// migrate runs the DDL under an advisory lock so several Aggregator instances
// starting together (M2) do not race on CREATE ... IF NOT EXISTS.
func (r *Repo) migrate(ctx context.Context) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(4031)`); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(4031)`) //nolint:errcheck
	if _, err := conn.Exec(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

// Reset empties every table. Test helper only.
func (r *Repo) Reset(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `TRUNCATE hazards, outbox, poll_cursor RESTART IDENTITY`)
	return err
}

func (r *Repo) Ping(ctx context.Context) error {
	var one int
	return r.pool.QueryRow(ctx, `SELECT 1`).Scan(&one)
}

const hazardCols = `hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude,
  occurred_at, ingested_at, updated_at, version, attributes`

func scanHazard(row pgx.Row) (*domain.HazardEvent, error) {
	var h domain.HazardEvent
	var attrs []byte
	if err := row.Scan(&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity, &h.AreaName,
		&h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &h.UpdatedAt, &h.Version, &attrs); err != nil {
		return nil, err
	}
	h.OccurredAt, h.IngestedAt, h.UpdatedAt = h.OccurredAt.UTC(), h.IngestedAt.UTC(), h.UpdatedAt.UTC()
	if err := json.Unmarshal(attrs, &h.Attributes); err != nil { // float64 for numbers, like every backend
		return nil, fmt.Errorf("decode attributes of %s: %w", h.HazardID, err)
	}
	return &h, nil
}

func (r *Repo) Get(ctx context.Context, id string) (*domain.HazardEvent, error) {
	h, err := scanHazard(r.pool.QueryRow(ctx, `SELECT `+hazardCols+` FROM hazards WHERE hazard_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	return h, err
}

func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

// Save writes the hazard and its outbox entry in one transaction. A version
// mismatch (0 rows updated, or a duplicate key on insert) writes nothing.
func (r *Repo) Save(ctx context.Context, h domain.HazardEvent, expected int, out ports.OutboxEntry) error {
	if h.Version != expected+1 {
		return ports.ErrVersionConflict
	}
	attrs := h.Attributes
	if attrs == nil {
		attrs = map[string]any{}
	}
	attrJSON, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	if out.CreatedAt.IsZero() {
		out.CreatedAt = time.Now().UTC()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after Commit

	if expected == 0 {
		_, err = tx.Exec(ctx, `INSERT INTO hazards (`+hazardCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			h.HazardID, string(h.Source), h.SourceRefID, string(h.HazardType), string(h.Severity), h.AreaName,
			h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, h.UpdatedAt, h.Version, attrJSON)
		if isUniqueViolation(err) {
			return ports.ErrVersionConflict
		}
		if err != nil {
			return err
		}
	} else {
		tag, err := tx.Exec(ctx, `UPDATE hazards SET source=$2, source_ref_id=$3, hazard_type=$4, severity=$5, area_name=$6,
			latitude=$7, longitude=$8, occurred_at=$9, ingested_at=$10, updated_at=$11, version=$12, attributes=$13
			WHERE hazard_id=$1 AND version=$14`,
			h.HazardID, string(h.Source), h.SourceRefID, string(h.HazardType), string(h.Severity), h.AreaName,
			h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, h.UpdatedAt, h.Version, attrJSON, expected)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ports.ErrVersionConflict
		}
	}

	payload := out.Payload
	if len(payload) == 0 {
		payload = []byte("null")
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (event_id, hazard_id, version, payload, correlation_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, out.EventID, out.HazardID, out.Version, payload, out.CorrelationID, out.CreatedAt)
	if isUniqueViolation(err) {
		return ports.ErrVersionConflict
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repo) List(ctx context.Context, f ports.ListFilter) (ports.Page, error) {
	f.Normalize()
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(args))))
	}
	if f.HazardType != "" {
		add("hazard_type = ?", string(f.HazardType))
	}
	if f.Source != "" {
		add("source = ?", string(f.Source))
	}
	if f.Severity != "" {
		add("severity = ?", string(f.Severity))
	}
	if f.Since != nil {
		add("occurred_at >= ?", *f.Since)
	}
	if f.Cursor != "" {
		ct, cid, err := ports.DecodeCursor(f.Cursor)
		if err != nil {
			return ports.Page{}, err
		}
		args = append(args, ct, cid)
		where = append(where, fmt.Sprintf("(occurred_at, hazard_id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	q := `SELECT ` + hazardCols + ` FROM hazards`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1) // one extra row tells whether another page exists
	q += fmt.Sprintf(` ORDER BY occurred_at DESC, hazard_id DESC LIMIT $%d`, len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return ports.Page{}, err
	}
	defer rows.Close()
	page := ports.Page{Items: []domain.HazardEvent{}}
	for rows.Next() {
		h, err := scanHazard(rows)
		if err != nil {
			return ports.Page{}, err
		}
		page.Items = append(page.Items, *h)
	}
	if err := rows.Err(); err != nil {
		return ports.Page{}, err
	}
	if len(page.Items) > f.Limit {
		page.Items = page.Items[:f.Limit]
		last := page.Items[f.Limit-1]
		page.NextCursor = ports.EncodeCursor(last.OccurredAt, last.HazardID)
	}
	return page, nil
}

func (r *Repo) GetCursor(ctx context.Context, name string) (time.Time, bool, error) {
	var t time.Time
	err := r.pool.QueryRow(ctx, `SELECT value FROM poll_cursor WHERE name = $1`, name).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return t.UTC(), true, nil
}

func (r *Repo) SetCursor(ctx context.Context, name string, t time.Time) error {
	_, err := r.pool.Exec(ctx, `INSERT INTO poll_cursor (name, value) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value`, name, t.UTC())
	return err
}

func (r *Repo) PendingOutbox(ctx context.Context, limit int) ([]ports.OutboxEntry, error) {
	rows, err := r.pool.Query(ctx, `SELECT event_id, hazard_id, version, payload::text, correlation_id, created_at
		FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ports.OutboxEntry
	for rows.Next() {
		var e ports.OutboxEntry
		var payload string
		if err := rows.Scan(&e.EventID, &e.HazardID, &e.Version, &payload, &e.CorrelationID, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Payload, e.CreatedAt = []byte(payload), e.CreatedAt.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *Repo) MarkPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE event_id = ANY($1) AND published_at IS NULL`, ids)
	return err
}
