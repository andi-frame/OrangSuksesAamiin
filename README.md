# BNPB Disaster Coordination Gateway — IF4031 Tugas Besar, Milestone 1

Gateway BNPB yang menyatukan data dua instansi otonom (BMKG: gempa dan tsunami; PVMBG: gunung api) ke format kanonik
`HazardEvent`, menyimpannya di Canonical Store, menyebarkan setiap perubahan lewat message broker, dan menyajikannya ke
tiga jenis client dengan hak akses berbeda. BMKG dan PVMBG adalah **mock** yang dibangun sesuai kontrak spesifikasi.

```
BMKG mock :8081 ◄─poll─┐                                   ┌─► consumer-dashboard
PVMBG mock :8082 ◄─poll─┤ AGGREGATOR ─outbox─► Kafka ─────┼─► consumer-notifier
                        │  │ (pemilik satu-satunya)        └─► consumer-pemda
                        │  └─► PostgreSQL (Canonical Store, jaringan internal)
Client ─► Client-Facing API :8080 ─► API internal Aggregator :8083     Auth Service
```
Diagram lengkap dan narasi: `docs/report/00-umum.md`. Kontrak antar-komponen: `docs/contracts.md`.

## 1. Prasyarat

- **Docker** dengan **Compose v2** (`docker compose`). Itu satu-satunya kebutuhan untuk menjalankan sistem.
- `curl` dan `bash` untuk skrip demo. `jq` opsional (bila tidak ada, skrip memakai container `ghcr.io/jqlang/jq`).
- `make` opsional (jalan pintas); **Go 1.25+** hanya bila ingin menjalankan test di luar Docker.

## 2. Menjalankan dari kondisi awal

```bash
git clone <repo> && cd <repo>
cp .env.example .env            # nilai contoh; ganti bila perlu. .env tidak pernah di-commit
docker compose up --build       # satu perintah untuk seluruh sistem   (atau: make up)
```
`make up` melakukan hal yang sama, membuat `.env` bila belum ada, dan menunggu semua service sehat.
Cek: `curl localhost:8081/health` · `curl localhost:8082/health` · `curl 127.0.0.1:8083/health` · `curl localhost:8080/health` · `curl localhost:8084/health`.
Mulai dari kondisi bersih (hapus data dan volume): `make reset`. Berhenti: `make down`.

### Port (dapat diubah lewat `.env`)

| Komponen | Port host | Keterangan |
|---|---|---|
| BMKG mock | **8081** | `BMKG_PORT` |
| PVMBG mock | **8082** | `PVMBG_PORT` |
| Client-Facing API (BNPB) | **8080** | dimiliki Orang 2 |
| Aggregator (API internal) | 127.0.0.1:**8083** | hanya loopback; klien memakai 8080 |
| Auth Service | **8084** | `AUTH_SERVICE_PORT` |
| Consumer dashboard / notifier | **8091** / **8092** | stats: `GET /stats` |
| PostgreSQL, Kafka | – | **tidak dipublikasikan** ke host (hanya jaringan internal) |

## 3. Variabel konfigurasi

Daftar lengkap beserta nilai contoh dan penjelasan per baris: **`.env.example`**. Ringkasan:

| Variabel | Default | Arti |
|---|---|---|
| `BMKG_API_KEY` | – (wajib) | kredensial BMKG (`X-BMKG-Key`); dipegang bmkg-mock dan Aggregator |
| `PVMBG_TOKEN_SECRET` | – (wajib) | rahasia penanda tangan token PVMBG; **hanya** pvmbg-mock |
| `PVMBG_GATEWAY_TOKEN` | – (wajib) | token yang dipakai Aggregator ke PVMBG (`make pvmbg-token` membuatnya) |
| `AGGREGATOR_SERVICE_KEY` | – (wajib) | kredensial API internal Aggregator (dipakai Client-Facing API) |
| `BMKG_ADMIN_TOKEN`, `PVMBG_ADMIN_TOKEN` | – (wajib) | token operator untuk `/admin/*` |
| `BMKG_DELAY_MIN_MS` / `_MAX_MS` | 50 / 150 | delay respons BMKG (spesifikasi: 50–150 ms) |
| `PVMBG_DELAY_MIN_MS` / `_MAX_MS` | 500 / 3000 | delay acak PVMBG (spesifikasi: 500 ms–3 s); juga dapat diubah saat berjalan |
| `BMKG_EVENT_INTERVAL_SEC`, `PVMBG_REPORT_INTERVAL_SEC` | 10 | satu data baru per interval (percepat untuk demo) |
| `BMKG_SEED_COUNT`, `PVMBG_SEED_COUNT` | 24 | record historis saat start (minimum 20) |
| `POLL_INTERVAL_SEC` | 3 | jeda antar siklus polling (disarankan 2–5) |
| `POLL_OVERLAP_SEC` | 5 | mundur dari watermark agar data yang terlambat tidak terlewat |
| `SOURCE_TIMEOUT_SEC` | 5 | timeout per request ke sumber |
| `BREAKER_FAILURE_THRESHOLD`, `BREAKER_OPEN_SEC` | 3, 10 | circuit breaker per sumber |
| `REPOSITORY_BACKEND` | `postgres` | `postgres` atau `memory` (tanpa database, untuk test) |
| `PUBLISHER_BACKEND` | `kafka` | `kafka` atau `log` (hanya mencatat, untuk test) |
| `CANONICAL_DB_*` | contoh | kredensial PostgreSQL (hanya Aggregator yang terhubung) |
| `KAFKA_TOPIC`, `KAFKA_PARTITIONS`, `KAFKA_RETENTION_MS` | `hazard.events`, 3, 7 hari | topic dibuat oleh `kafka-init` |
| `LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |

Client-Facing API / Auth Service / consumer: lihat bagian bertanda di `.env.example` (`AUTH_*`, `CLIENT_API_*`, `CLIENT_RATE_LIMIT_RPS/BURST`).
Load test P2 dapat diubah lewat `K6_VUS`, `K6_DURATION`, `K6_THINK_S`, `K6_SUMMARY`, `K6_CLIENT_ID/_SECRET` (lihat `scripts/loadtest/run-p2.sh`).

## 4. Cara memicu setiap skenario

Setiap demo **memverifikasi dirinya sendiri** (kode keluar ≠ 0 bila gagal) dan menyimpan transkrip ke `docs/evidence/`.
Jalankan setelah `make up` (untuk hasil yang bersih: `make reset` dulu).

| Problem | Perintah | Membuktikan |
|---|---|---|
| **P1** schema evolution | `make demo-p1` | skema awal → pemetaan benar (diverifikasi oracle independen) → `POST /admin/schema-version` saat berjalan → tetap valid, tanpa restart; field tak dikenal → `attributes`; perubahan tak didukung → karantina |
| **P2** sisi ingest | `make demo-p2-aggregator` | PVMBG 3 s tidak mengganggu BMKG; outage `error` dan `hang`; data tetap tersaji dengan `stale_since`; pulih tanpa restart |
| **P2** sisi layanan (load test) | `make demo-p2` | p95 BMKG via :8080 saat PVMBG 3 s, outage `error`/`hang` → data usang bertanda `stale`, k6 50 VU × 60 s (tiap VU login + refresh sendiri), lalu overload 150 VU → kelebihan ditolak 429 tanpa 5xx |
| **P3** kredensial mock | `make demo-mock-auth` | kredensial BMKG ditolak PVMBG dan sebaliknya; tiap container hanya memegang rahasianya sendiri |
| **P3** client downstream | `make demo-p3` | 401/403, Media hanya Ringkasan (semua bentuk permintaan Mentah → 403), token kedaluwarsa 60 s → refresh tanpa login, refresh lama ditolak (±70 detik) |
| **P4** isolasi storage | `make demo-isolation` | Canonical Store dan Kafka tidak terjangkau dari container lain |
| **P4** rebuild satu service, JSONB | `make demo-p4` | *Orang 3* |
| **P5** pub/sub | `make demo-p5` | 2 consumer groups, fan-out, catch-up after stop |
| Kredensial tidak ter-commit | `make secrets` | `.env` tidak dilacak/tidak pernah di-commit; gitleaks bersih; `.env.example` ada |
| Semua sekaligus | `make evidence` | menjalankan semua demo |

### Memicu kegagalan secara manual (contoh)

```bash
set -a; . ./.env; set +a
# PVMBG lambat 3 detik (spesifikasi P2)
curl -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -d '{"ms":3000}' localhost:8082/admin/delay
# PVMBG mati / hidup lagi, seketika (mode error = 503, mode hang = tidak menjawab)
curl -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -d '{"enabled":true,"mode":"hang"}' localhost:8082/admin/outage
curl -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -d '{"enabled":false}' localhost:8082/admin/outage
# skema PVMBG berubah saat berjalan (confidence_level muncul); field baru arbitrer
curl -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -d '{"confidence_level":true}' localhost:8082/admin/schema-version
curl -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -d '{"extra_fields":{"lava_dome_volume_m3":125000}}' localhost:8082/admin/schema-version
# percepat demo: buat data sekarang juga (BMKG: gempa tsunami; warning menyusul 2-8 detik kemudian)
curl -X POST -H "X-Admin-Token: $BMKG_ADMIN_TOKEN" -d '{"magnitude":7.6,"tsunami":true}' localhost:8081/admin/generate
curl -X POST -H "X-Admin-Token: $PVMBG_ADMIN_TOKEN" -d '{"count":3}' localhost:8082/admin/generate
# lihat hasilnya lewat API internal Aggregator
curl -H "X-Service-Key: $AGGREGATOR_SERVICE_KEY" "localhost:8083/internal/hazards?hazard_type=VOLCANIC&limit=5"
curl -H "X-Service-Key: $AGGREGATOR_SERVICE_KEY" localhost:8083/internal/sources/status
```

### Menjalankan dan mematikan service secara independen

```bash
docker compose stop pvmbg-mock                 # mati
docker compose up -d pvmbg-mock                # hidup lagi
docker compose up -d --build --no-deps aggregator   # rebuild dan jalankan ulang HANYA aggregator
```
Aggregator tidak memiliki `depends_on` ke mock atau store: ia tetap berjalan dan pulih sendiri saat salah satunya hilang.

## 5. Struktur repositori

```
docker-compose.yml   .env.example   Makefile
contracts/           openapi/*.yaml, schemas/*.json   (kontrak yang divalidasi test)
docs/                contracts.md, adapters.md, traceability.md, report/, evidence/
scripts/             lib.sh, wait-healthy.sh, check-secrets.sh, demo/*.sh
pkg/obs/             utilitas teknis bersama: log JSON, correlation ID, klien HTTP ber-latensi, health, env
services/
  bmkg-mock/  pvmbg-mock/  aggregator/        (Orang 1)
  auth-service/  client-api/                   (Orang 2)
  consumer-hazard/ (2 groups in compose)       (P5)
tools/pvmbg-token/   pembuat token PVMBG
deploy/kafka/        pembuat topic (kafka-init)
```

**Kode bersama.** Ketentuan melarang berbagi *library business-logic* lintas service. `pkg/obs` hanya berisi utilitas
teknis (logging, propagasi correlation ID, health, parsing env) tanpa logika domain; tiap service tetap memiliki
`go.mod` dan container sendiri dan hanya berkomunikasi lewat jaringan.

## 6. Pengembangan

```bash
make test        # seluruh test semua modul       make test-race   # dengan race detector
make lint        # gofmt + go vet                 make secrets     # cek kredensial ter-commit
```
CI (`.github/workflows/ci.yml`): gofmt/vet/test `-race`, gitleaks, dan smoke test `docker compose up` + demo P1.

## 7. Catatan

- **Windows + Git Bash + MSYS make**: make MSYS membuang variabel lingkungan Windows dari recipe; `scripts/winenv.sh`
  memulihkannya otomatis (tanpa efek di Linux/macOS). Bila ragu, jalankan `docker compose ...` dan `bash scripts/demo/p1.sh`
  langsung dari Git Bash.
- Port bentrok? Ubah `*_PORT` di `.env`. Mulai dari nol: `make reset`.
