package postgres

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// OpenShard opens lazily so an unavailable group does not block the read
// service. Initialize its schema on its first successful primary connection.
func OpenShard(ctx context.Context, url string) (*Repo, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	var initialized atomic.Bool
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if initialized.Load() {
			return nil
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(4031)"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, schema); err != nil {
				return err
			}
			return nil
		})
		if err == nil {
			initialized.Store(true)
		}
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Repo{pool: pool}, nil
}

// OpenMember connects directly to one pod for read-only placement checks.
func OpenMember(ctx context.Context, url, host string) (*Repo, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Host = host
	cfg.ConnConfig.Fallbacks = nil
	cfg.ConnConfig.ValidateConnect = nil
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Repo{pool: pool}, nil
}

func (r *Repo) Placement(ctx context.Context, id, unit string) (ports.Location, bool, error) {
	location := ports.Location{Unit: unit, Role: "primary"}
	var standby, found bool
	err := r.pool.QueryRow(ctx, `SELECT COALESCE(current_setting('argus.node', true), ''),
		pg_is_in_recovery(), EXISTS(SELECT 1 FROM hazards WHERE hazard_id=$1 AND NOT retired)`, id).
		Scan(&location.Node, &standby, &found)
	if standby {
		location.Role = "replica"
	}
	return location, found, err
}

func (r *Repo) LoadSatellite(ctx context.Context, key string) ([]byte, error) {
	var body []byte
	err := r.pool.QueryRow(ctx, "SELECT doc FROM satellite_checkpoint WHERE name=$1", key).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return body, err
}

func (r *Repo) StoreSatellite(ctx context.Context, key string, body []byte) error {
	return r.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO satellite_checkpoint(name, doc) VALUES ($1,$2)
			ON CONFLICT (name) DO UPDATE SET doc=EXCLUDED.doc`, key, body)
		return err
	})
}

// AuditReferences scans only identities so checksum requests do not load
// hazard attributes or mix pages within a shard's statement snapshot.
func (r *Repo) AuditReferences(ctx context.Context, source domain.Source) ([]string, []string, error) {
	var args []any
	query := "SELECT source_ref_id FROM hazards WHERE NOT retired"
	if source != "" {
		query += " AND source=$1"
		args = append(args, string(source))
	}
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	refs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return refs, nil, err
}

func (r *Repo) ConfirmHazard(ctx context.Context, id string, version int) error {
	return r.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE hazards SET version=version WHERE hazard_id=$1 AND version >= $2", id, version)
		if err == nil && tag.RowsAffected() != 1 {
			return ports.ErrVersionConflict
		}
		return err
	})
}

// UnitStats reports where this database runs, whether it is a standby, and how many active hazards it holds
// (P1: data and roles per node; read by the sharded adapter for its placement metrics).
func (r *Repo) UnitStats(ctx context.Context) (node string, replica bool, rows int64, err error) {
	err = r.pool.QueryRow(ctx, `SELECT COALESCE(current_setting('argus.node', true), ''), pg_is_in_recovery(),
		(SELECT count(*) FROM hazards WHERE NOT retired)`).Scan(&node, &replica, &rows)
	return node, replica, rows, err
}
