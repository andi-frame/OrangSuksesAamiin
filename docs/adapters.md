# Panduan adapter (untuk Orang 3): PostgreSQL dan Kafka

Aggregator tidak tahu database atau broker apa yang dipakai: ia bicara ke dua interface di
`services/aggregator/internal/ports`. Tugas Orang 3 adalah **mengisi dua interface itu** dan mendaftarkannya. Tidak ada
kode Aggregator lain yang perlu diubah.

## 1. Langkah pemasangan

1. Buat paket `internal/adapters/postgres` (implementasi `ports.Repository`) dan `internal/adapters/kafka`
   (implementasi `ports.Publisher`). Masing-masing mendaftar di `init()`:
   ```go
   func init() {
       adapters.RegisterRepository("postgres", func(ctx context.Context, log *slog.Logger) (ports.Repository, error) { ... })
   }
   ```
   Baca konfigurasi dari env sendiri (`CANONICAL_DB_URL`; `KAFKA_BROKERS`, `KAFKA_TOPIC`).
2. Aktifkan **blank import** di `cmd/aggregator/backends.go` (barisnya sudah disiapkan, dikomentari).
3. Set `REPOSITORY_BACKEND=postgres` dan `PUBLISHER_BACKEND=kafka` di `.env`; tambahkan `postgres-canonical` dan
   `kafka` ke `depends_on`-nya **tidak perlu**: Aggregator mencoba ulang koneksi hingga 90 detik saat start.
4. Tambahkan dependensi ke `services/aggregator/go.mod` (`pgx/v5`, `franz-go`) dan jalankan `go mod tidy`.

## 2. `ports.Repository` harus lolos contract test

`internal/ports/repotest` adalah perilaku baku yang harus dipenuhi setiap repository. Adapter Postgres **selesai** ketika
test berikut lulus terhadap database nyata:
```go
func TestPostgresRepo(t *testing.T) {
    repotest.Run(t, func(t *testing.T) ports.Repository { return newRepoOnEmptyDatabase(t) })
}
```
Yang diuji antara lain: round-trip tipe JSON (`float64`, bersarang, `null`), presisi mikrodetik, koordinat `null`,
konkurensi optimistis, outbox atomik, urutan outbox, keyset pagination dengan tie-break, kursor polling.

## 3. Skema Postgres yang disarankan

Kolom tetap hanya untuk identitas dan field kanonik; **semua yang dinamis di `attributes JSONB`**. Munculnya
`confidence_level` (atau field apa pun) tidak membutuhkan DDL. DDL dijalankan sekali saat start
(`CREATE ... IF NOT EXISTS`), bukan sebagai langkah migrasi per perubahan skema sumber.

```sql
CREATE TABLE IF NOT EXISTS hazards (
  hazard_id     text PRIMARY KEY,
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
  attributes    jsonb   NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS hazards_list_idx ON hazards (hazard_type, occurred_at DESC, hazard_id DESC);
CREATE INDEX IF NOT EXISTS hazards_time_idx ON hazards (occurred_at DESC, hazard_id DESC);

CREATE TABLE IF NOT EXISTS outbox (
  seq            bigserial PRIMARY KEY,
  event_id       text NOT NULL UNIQUE,
  hazard_id      text NOT NULL,
  version        integer NOT NULL,
  payload        jsonb NOT NULL,
  correlation_id text NOT NULL DEFAULT '',
  created_at     timestamptz NOT NULL DEFAULT now(),
  published_at   timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (seq) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS poll_cursor (name text PRIMARY KEY, value timestamptz NOT NULL);
```

Catatan implementasi:
- **`Save` = satu transaksi**: `INSERT ... ` (expected=0) atau `UPDATE hazards SET ... WHERE hazard_id=$1 AND version=$expected`
  lalu `INSERT INTO outbox`. Jika 0 baris terpengaruh / `unique_violation` → `ports.ErrVersionConflict` dan **tidak ada** yang tertulis.
  Validasi juga `h.Version == expected+1`.
- **Presisi waktu**: simpan `timestamptz` (mikrodetik). Mapper sudah memotong ke mikrodetik, jadi round-trip stabil
  (jika tidak, `SameContent` menganggap record berubah dan versi naik terus).
- **Tipe JSON**: gunakan `encoding/json` untuk `attributes` (angka kembali sebagai `float64`); jangan `UseNumber`.
  Kunci `payload` outbox disimpan apa adanya (`[]byte` JSON).
- `PendingOutbox`: `WHERE published_at IS NULL ORDER BY seq LIMIT $1`. `MarkPublished`: `UPDATE ... SET published_at=now() WHERE event_id = ANY($1)`.
- `List`: keyset `(occurred_at, hazard_id) < ($t, $id)` dengan `ORDER BY occurred_at DESC, hazard_id DESC`; gunakan
  `ports.EncodeCursor/DecodeCursor` agar token seragam antar backend. Cursor tidak valid → error `invalid cursor`.
- `Ping`: `SELECT 1`.

## 4. `ports.Publisher` (Kafka)

- Klien: **`franz-go`** (Go murni, tanpa cgo, jadi Dockerfile tetap sederhana).
- `Publish` mengembalikan `nil` **hanya setelah broker mengakui** (`ProduceSync`, `RequiredAcks=all`, producer idempoten).
  Relay yang menjamin urutan dan retry; jangan buffer diam-diam.
- `Key = msg.Key` (hazard_id). `Headers = msg.Headers` (`correlation_id`, `event_id`). `Value = msg.Value` apa adanya.
- Log per pesan dengan `obs.ExtractHeaders(ctx, msg.Headers)` agar `cid` sama dengan siklus poll asalnya.
- Topic dibuat `kafka-init` (auto-create dimatikan). Producer **tidak** membuat topic.

## 5. Consumer (Orang 3)

- Satu **consumer group per consumer** (`dashboard-updater`, `notifier`, `pemda-portal`), sehingga masing-masing menerima
  semua event dan consumer baru cukup memakai group baru tanpa menyentuh Aggregator.
- **Idempoten**: simpan `event_id` yang sudah diproses (atau bandingkan `hazard_id:version`); abaikan yang lebih lama
  dari versi terakhir hazard itu. Semantiknya at-least-once.
- Commit offset **setelah** event diproses. Saat consumer mati, event menumpuk di topic (retensi 7 hari) dan disusul
  ketika hidup lagi. `auto.offset.reset=earliest` untuk group baru bila ingin membaca riwayat.
- Ambil `correlation_id` dari header dengan `obs.ExtractHeaders` dan log dengannya.
- Consumer berada di `broker-net` saja (tidak perlu menyentuh database).

## 6. Yang sudah disediakan Orang 1

`repotest` (contract test), adapter `memory` dan `log` (untuk test), relay outbox, kursor polling persisten,
`kafka-init`, jaringan terpisah, `pkg/obs` (log, correlation ID, header Kafka).
