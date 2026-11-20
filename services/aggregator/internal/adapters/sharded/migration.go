package sharded

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MigrateSingle copies a quiescent M2 PostgreSQL store to fresh shards. It
// preserves rows verbatim and verifies equality before committing each target.
// A restart can safely reuse already committed targets.
func MigrateSingle(ctx context.Context, sourceURL, shardURLs string, log *slog.Logger) error {
	opened, err := Open(ctx, shardURLs, "")
	if err != nil {
		return err
	}
	for _, s := range opened.shards {
		// Initialize every database before starting the copy transactions.
		pg := s.unit.(interface{ Close() })
		defer pg.Close()
		if err := s.unit.Ping(ctx); err != nil {
			return fmt.Errorf("%s is unreachable", s.name)
		}
	}
	source, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("open legacy canonical store: %w", err)
	}
	defer source.Close(context.WithoutCancel(ctx))
	read, err := source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer read.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck

	var targets []pgx.Tx
	for _, url := range strings.Split(shardURLs, ",") {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			return err
		}
		defer conn.Close(context.WithoutCancel(ctx))
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
		targets = append(targets, tx)
	}

	tables := []struct {
		name, key string
		spread    bool
		sequence  bool
	}{
		{"hazards", "hazard_id", true, false},
		{"outbox", "event_id", true, true},
		{"poll_cursor", "name", false, false},
		{"source_status", "source", false, false},
		{"unprocessed", "message_id", false, true},
		{"fence", "name", false, false},
		{"satellite_checkpoint", "name", false, false},
	}
	for _, table := range tables {
		var present bool
		if err := read.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table.name).Scan(&present); err != nil {
			return err
		}
		counts := make([]int, len(targets))
		if present {
			route := ""
			if table.spread {
				route = "hazard_id"
			}
			copies := targets[:1]
			if table.spread || table.name == "fence" {
				copies = targets
			}
			if err := copyTable(ctx, read, copies, table.name, table.key, route, counts); err != nil {
				return fmt.Errorf("migrate %s: %w", table.name, err)
			}
		}
		for i, tx := range targets {
			var actual int
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table.name).Scan(&actual); err != nil {
				return err
			}
			if actual != counts[i] {
				return fmt.Errorf("shard-%d %s contains %d rows, expected %d", i, table.name, actual, counts[i])
			}
			if table.sequence {
				_, err := tx.Exec(ctx, "SELECT setval(pg_get_serial_sequence($1, 'seq'), COALESCE(max(seq),1), count(*)>0) FROM "+table.name, table.name)
				if err != nil {
					return err
				}
			}
		}
		log.InfoContext(ctx, "migration_table_verified", "table", table.name, "rows_per_shard", counts)
	}
	for i, tx := range targets {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit shard-%d: %w", i, err)
		}
	}
	return read.Commit(ctx)
}

func copyTable(ctx context.Context, source pgx.Tx, targets []pgx.Tx, table, key, route string, counts []int) error {
	body := "to_jsonb(t)"
	if table == "hazards" {
		body += " || jsonb_build_object('retired', COALESCE((to_jsonb(t)->>'retired')::boolean, false))"
	}
	if table == "outbox" {
		// Store the original JSON text, including a literal null payload.
		body += " || jsonb_build_object('payload', payload::text)"
	}
	if route == "" {
		route = "''::text"
	}
	rows, err := source.Query(ctx, "SELECT "+body+", "+key+"::text, "+route+" FROM "+table+" t")
	if err != nil {
		return err
	}
	defer rows.Close()
	insert := "INSERT INTO " + table + " SELECT * FROM jsonb_populate_record(NULL::" + table + ", $1) ON CONFLICT DO NOTHING"
	verify := "SELECT (" + body + ") = $1::jsonb FROM " + table + " t WHERE " + key + "::text = $2"
	if table == "outbox" {
		insert = `INSERT INTO outbox (seq,event_id,hazard_id,version,payload,correlation_id,created_at,published_at,writer_term,writer,stored_at)
			SELECT seq,event_id,hazard_id,version,($1::jsonb->>'payload')::json,correlation_id,created_at,published_at,writer_term,writer,stored_at
			FROM jsonb_populate_record(NULL::outbox, $1::jsonb) ON CONFLICT DO NOTHING`
	}
	for rows.Next() {
		var doc []byte
		var id, routingID string
		if err := rows.Scan(&doc, &id, &routingID); err != nil {
			return err
		}
		start, stop := 0, len(targets)
		if routingID != "" {
			start = Index(routingID, len(targets))
			stop = start + 1
		}
		for i := start; i < stop; i++ {
			tag, err := targets[i].Exec(ctx, insert, doc)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				var equal bool
				if err := targets[i].QueryRow(ctx, verify, doc, id).Scan(&equal); err != nil {
					return err
				}
				if !equal {
					return fmt.Errorf("conflicting destination row for %q on shard-%d", id, i)
				}
			}
			counts[i]++
		}
	}
	return rows.Err()
}
