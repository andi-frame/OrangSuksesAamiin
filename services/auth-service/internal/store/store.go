package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS refresh_tokens (
  token_hash TEXT PRIMARY KEY,
  client_id  TEXT NOT NULL,
  role       TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  revoked    INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
)`)
	return err
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.QueryRowContext(ctx, `SELECT 1`).Scan(new(int))
}

func (s *Store) SaveRefresh(ctx context.Context, tokenHash, clientID, role string, exp time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO refresh_tokens(token_hash, client_id, role, expires_at, revoked) VALUES (?,?,?,?,0)`,
		tokenHash, clientID, role, exp.UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) ConsumeRefresh(ctx context.Context, tokenHash string) (clientID, role string, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback() }()

	var expStr string
	var revoked int
	row := tx.QueryRowContext(ctx,
		`SELECT client_id, role, expires_at, revoked FROM refresh_tokens WHERE token_hash=?`, tokenHash)
	if err := row.Scan(&clientID, &role, &expStr, &revoked); err != nil {
		return "", "", err
	}
	exp, parseErr := time.Parse(time.RFC3339Nano, expStr)
	if parseErr != nil {
		return "", "", fmt.Errorf("bad expires_at: %w", parseErr)
	}
	if revoked != 0 || time.Now().After(exp) {
		return "", "", sql.ErrNoRows
	}
	if _, err := tx.ExecContext(ctx, `UPDATE refresh_tokens SET revoked=1 WHERE token_hash=?`, tokenHash); err != nil {
		return "", "", err
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return clientID, role, nil
}
