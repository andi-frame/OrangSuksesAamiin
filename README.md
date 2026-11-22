# ARGUS: Aggregated Real-time Geohazard Unified System — IF4031 Tugas Besar, Milestone 1 dan 2

ARGUS adalah gateway BNPB yang menyatukan data dua instansi otonom (BMKG: gempa dan tsunami; PVMBG: gunung api) ke format kanonik
`HazardEvent`, menyimpannya di Canonical Store, menyebarkan setiap perubahan lewat message broker, dan menyajikannya ke
tiga jenis client dengan hak akses berbeda. BMKG dan PVMBG adalah **mock** yang dibangun sesuai kontrak spesifikasi.

```
BMKG mock :8081 ◄─poll─┐                                   ┌─► consumer-dashboard
PVMBG mock :8082 ◄─poll─┤ AGGREGATOR ─outbox─► Kafka ─────┼─► consumer-notifier
                        │  │ (pemilik satu-satunya)        └─► consumer baru (pemda-portal, demo P5)
                        │  └─► PostgreSQL (Canonical Store, jaringan internal)
Client ─► Client-Facing API :8080 ─► API internal Aggregator :8083     Auth Service
```
Diagram lengkap dan penjelasannya ada di laporan (`docs/IF4031_M1_OrangSuksesAamiin.pdf`). Kontrak antarkomponen: `docs/contracts.md`.

## M2 — Menjalankan di Kubernetes (demo M2)

Milestone 2 menjalankan sistem yang sama di Kubernetes (k3s, ≥ 2 node dengan kernel terpisah). `docker compose` (bagian 1–7
di bawah) tetap berfungsi untuk pengembangan satu mesin.

### M2.1 Deklarasi topologi

| Node | Peran | Jenis | vCPU / RAM node | OS / kernel | Host fisik | CPU / RAM host |
|---|---|---|---|---|---|---|
| node-a (10.77.0.11) | control plane + worker | VM VirtualBox 7.1.4 | 4 vCPU / 4,5 GiB, disk 10 GB | Ubuntu 24.04.5, kernel 6.8.0-147, k3s v1.37.2 | laptop Acer Nitro AN515-58, Windows 11 Home | Intel Core i5-12500H / 24 GB |
| node-b (10.77.0.12) | worker | VM VirtualBox 7.1.4 | 4 vCPU / 4,5 GiB, disk 10 GB | Ubuntu 24.04.5, kernel 6.8.0-147, k3s v1.37.2 | laptop yang sama | sama |

Setiap node juga punya NIC kedua di jaringan host-only VirtualBox (node-a `192.168.56.11`, node-b `192.168.56.12`). Lewat NIC ini host menjangkau NodePort langsung, misalnya k6 untuk load test atau peramban untuk dashboard. Forwarder NAT `127.0.0.1` tidak sanggup menahan 10.000 koneksi.

Kedua node berjalan di satu host fisik, dengan kernel terpisah (boot id berbeda; `docs/evidence/m2/topology.log`) dan
saling terhubung lewat VirtualBox NAT Network `10.77.0.0/24`. Cluster ini dibuat dengan `bash scripts/k8s/vbox-cluster.sh up`.

Kolom node, kernel, CPU, dan RAM tiap node dihasilkan dari cluster dengan `bash scripts/k8s/topology.sh` (memeriksa juga
bahwa setiap node punya kernel sendiri). Cara menyiapkan VM dan k3s: [`docs/k8s-cluster.md`](docs/k8s-cluster.md).

### M2.2 Satu perintah

```bash
bash scripts/k8s/vbox-cluster.sh up      # 2 VM VirtualBox (node-a, node-b; 4,5 GiB / 4 vCPU), k3s, konteks argus-k3s
cp .env.example .env                      # isi token dari asisten: AUDIT_TOKEN, SATELLITE_FEED_TOKEN
export KUBE_CONTEXT=<konteks-cluster>     # default: konteks kubectl saat ini
export K8S_SSH_NODES="ubuntu@<ip-a> ubuntu@<ip-b>"   # atau IMAGE_REGISTRY=<registry>; kind: otomatis
make k8s-up                               # build image -> Secret per komponen -> kubectl apply -k deploy/ -> Tester Agent -> tunggu siap
make k8s-status                           # pod per node, leader, endpoint Aggregator
make k8s-down                             # hapus semuanya, termasuk data
# Windows + MSYS make membuang variabel lingkungan: pilih cluster dengan `kubectl config use-context <konteks>`
# atau jalankan skripnya langsung, mis. `KUBE_CONTEXT=<konteks> bash scripts/k8s/up.sh`
```

Titik masuk (IP node mana pun): Client-Facing API `:30080`, Auth `:30084`, Aggregator `:30083`
(`/internal/*` dengan `X-Service-Key`, `/internal/ops/*` dengan `AGGREGATOR_OPS_TOKEN`, `/internal/audit/*` dengan `AUDIT_TOKEN`).
Semua komponen di namespace `sistem-informasi-kebencanaan-bnpb`, setiap pod berlabel `role`, `resources` sesuai Lampiran B,
kredensial di Secret per komponen (`scripts/k8s/secrets.sh`), konfigurasi di ConfigMap. Tester Agent dipasang dari
manifest yang dipin, tanpa modifikasi.

### M2.3 Variabel konfigurasi baru

| Variabel | Default | Arti |
|---|---|---|
| `AGGREGATOR_OPS_TOKEN` | – (wajib) | token BNPB Internal Ops: melihat dan memproses ulang pesan gagal |
| `AUDIT_TOKEN` | – (wajib) | token Audit API (Lampiran A), dari asisten |
| `SATELLITE_FEED_URL`, `SATELLITE_FEED_TOKEN` | URL asisten, – (wajib) | Feed Satelit (P4); Secret `satellite-feed-credentials` |
| `LEADER_ELECTION` | `none` (compose) / `kubernetes` | satu instance Aggregator yang ingest, dipilih lewat Lease `aggregator-ingest` |
| `LEASE_DURATION_SEC`, `LEASE_RENEW_DEADLINE_SEC`, `LEASE_RETRY_PERIOD_SEC` | 15, 10, 2 | batas waktu leader (laporan P2 §4) |
| `SOURCE_STATUS_MAX_AGE_SEC` | 30 | status sumber yang lebih tua = tidak ada yang polling (ditandai stale) |
| `MESSAGE_MAX_BYTES`, `MESSAGE_MAX_DEPTH` | 1048576, 32 | batas satu pesan; lebih dari itu disimpan sebagai *malformed* |
| `DLQ_MAX_ATTEMPTS`, `DLQ_BASE_BACKOFF_SEC`, `DLQ_MAX_BACKOFF_SEC` | 8, 2, 120 | retry pesan *retrying* |
| `SCHEMA_RULES_PATH` | `/app/rules/schema-rules.json` | aturan penyesuaian skema (ConfigMap dari `deploy/k8s/schema-rules.json`) |
| `IMAGE_TAG`, `IMAGE_REGISTRY`, `K8S_IMAGE_MODE`, `K8S_SSH_NODES` | git sha, –, kind/ssh | cara image sampai ke node (`scripts/k8s/common.sh`) |
| `PVMBG_ADMIN_OPEN`, `BMKG_ADMIN_OPEN` | `false` (compose) / `true` (k8s) | `/admin/*` mock tanpa token, karena Tester Agent memanggilnya tanpa kredensial (Service mock hanya ClusterIP) |
| `ENV_FILE` | `.env` | file kredensial yang dibaca skrip `scripts/k8s/*` dan demo M2 (mis. satu file per cluster) |
| `PANTAI_AUTH_TOKEN` | dibuat acak oleh `secrets.sh` bila kosong | token Sensor Pantai (P5); Secret `pantai-token` (sensor) + `aggregator-credentials` (Aggregator) |
| `SENSOR_POLL_INTERVAL_MS`, `SENSOR_TIMEOUT_SEC`, `SENSOR_BREAKER_OPEN_SEC` | 1000, 2, 10 | poller Sensor Pantai: interval, timeout per permintaan, lama breaker terbuka |
| `SENSOR_FREEZE_AFTER_SEC`, `SENSOR_MAX_LAG_SEC`, `SENSOR_PAGE_SIZE`, `SENSOR_MAX_PAGES` | 15, 7200, 500, 20 | `generated_at` beku = sumber basi; batas kedaluwarsa (waktu sensor); paging saat burst |
| `CLIENT_PRIORITY_RESERVE`, `CLIENT_MAX_IN_FLIGHT` | 0.3, 64 | kontrol admisi client-api: cadangan untuk Lapangan/Pusat, permintaan bersamaan per pod (Media → 429 lebih dulu) |
| `KAFKA_BROKERS` (client-api), `FIELD_REPORT_ACK_TIMEOUT_SEC`, `FIELD_REPORTS_URL` | `kafka:9092`, 5, `http://field-reports:8093` | `POST /v1/field-reports`: 202 setelah broker mengonfirmasi; status laporan dari pemroses |
| `FIELD_REPORT_PROCESS_DELAY_MS` | 0 | jeda buatan per laporan di pemroses (demo "hilir lambat") |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_SAMPLER_ARG` | `http://jaeger:4318`, 1 | ekspor trace (ConfigMap `observability-config`); kosong = trace hanya di log (`trace_id`) |
| `K8S_STORAGE` | `sharded` | `sharded`: Canonical Store = 3 shard Patroni × 2 anggota (P1); `single`: satu PostgreSQL (pengembangan) |
| `STORE_NODES` | semua node | node penyimpanan; primary pilihan shard dirotasi di node ini (ConfigMap `shard-placement`) |
| `CANONICAL_DB_ADMIN_PASSWORD`, `CANONICAL_DB_REPLICATION_PASSWORD` | – (wajib, sharded) | superuser dan replikasi Patroni; Secret `canonical-shard-credentials` |
| `BALANCE_STABLE_SEC`, `BALANCE_COOLDOWN_SEC` | 60, 300 | `balance-shard`: replica di node pilihan harus sinkron selama ini sebelum switchover; jeda antar-switchover |
| `SATELLITE_LATENESS_SEC` | 86400 | batas keterlambatan waktu deteksi (P4 kriteria 2); lebih tua dari watermark = terlambat, dihitung |
| `SATELLITE_START_FROM` | `latest` (k8s) | sesi practice tanpa checkpoint mulai dari head; profil `exam` selalu dari offset 0 |
| `SATELLITE_PROVINCES_PATH` | `/app/config/provinces.json` | tabel pusat provinsi untuk `area_name` WILDFIRE |
| `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | `admin`, dibuat acak bila kosong | admin Grafana (dashboard juga terbuka anonim sebagai Viewer) |

### M2.4 Skenario pengujian

| Problem | Perintah | Membuktikan |
|---|---|---|
| Platform | `make demo-m2-isolation` | hanya Aggregator yang bisa membuka koneksi ke Canonical Store, hanya klien Kafka ke broker; Secret per komponen |
| **P2** | `make demo-m2-p2` (`NEW_TAG=<tag>` untuk ganti versi) | ≥ 3 instance, satu leader, upstream dipoll satu instance, failover ≤ 60 s tanpa celah/duplikat, audit fencing, rolling update di bawah beban tanpa gagal |
| P2 jam bergeser | `bash scripts/k8s/practice.sh clock-skew` | skenario Tester Agent + pemeriksaan "satu penulis" (`scripts/demo/m2-single-writer.sh`) |
| P2 node freeze | `FREEZE_NODE=node-b FREEZE_CMD='…' THAW_CMD='…' bash scripts/demo/m2-freeze.sh` | leader yang dibekukan lalu bangun tidak menyimpan apa pun |
| **P3** (kriteria 1–5) | `make demo-m2-p3` | perubahan kompatibel diterima, pesan rusak tidak membuat crash, hanya Internal Ops melihat pesan gagal, perubahan tidak kompatibel di-*parked*, pulih setelah penyesuaian tanpa duplikat |
| Tester Agent | `bash scripts/k8s/practice.sh [<skenario>]` | daftar / jalankan skenario latihan, log disimpan di `docs/evidence/m2/` |
| **Observabilitas** | `make demo-m2-observability` | `/metrics` setiap service ter-scrape, satu trace lintas service (baca, tulis, ingest) dengan durasi tiap langkah, `cid` + `trace_id` di log |
| **P3 kriteria 6** | `make demo-m2-field-reports` (`N=40`) | 202 cepat walau pemroses mati, retry kunci sama → `report_id` sama, penerima dan pemroses dibunuh, setiap laporan ber-202 diproses tepat sekali |
| **P5** | `make demo-m2-p5` (`OUTAGE_S=45`, `P5_LOAD=0` tanpa k6) | image sensor tidak diubah, pelanggaran kontrak disisihkan, 0 peringatan kritis hilang dan p95 ≤ 5 s, sensor padam: BMKG tetap < 300 ms, data pesisir cepat + basi, permintaan tidak melonjak, pulih sendiri; prioritas Lapangan; SLO |
| P5 profil ujian | `bash scripts/k8s/sensor-exam.sh exam <key-file>` / `practice` | Secret `pantai-key` + `PANTAI_PROFILE=exam`, image tetap |
| **P1 kriteria 7** | `make demo-m2-analytics`; peramban `http://<node>:30080/dashboard` | angka dashboard = hitungan langsung Audit API; satu shard mati → `partial` dan banner "BELUM LENGKAP" |
| **P2 kriteria 6** | `make loadtest-m2-scale` (`K6_VUS=10000`, k6 di mesin ini; lihat `docs/report/m2/02-p2.md` §8) | 10 000 VU, ≥ 120 s, Aggregator 3 → 5 → 2, error < 1 % di luar 429, lalu pemeriksaan satu penulis |
| **P2 kriteria 7** | `make loadtest-m2-throughput` | throughput 3 instance ≥ 2× 1 instance (CPU limit sama per pod) |
| **P1 kriteria 1–2** | `make demo-m2-p1-load` (`CATALOG=<ndjson>`; CSV: `python3 tools/catalog/catalog.py convert x.csv`) | katalog dimuat, unit/peran/baris per node, tidak ada unit memuat seluruh data, beban tulis per node saat muat dan saat lonjakan satu wilayah, perbandingan dua alternatif penempatan |
| **P1 kriteria 3** | `bash scripts/demo/m2-p1.sh snapshot` | 50 event terbaru lintas shard, urutan keyset benar, `next_cursor` |
| **P1 kriteria 4–6** | `make demo-m2-p1-recovery` (`SCENARIO=wipe-node`) | worker dimatikan dan datanya dihapus saat dibaca terus: semua 200 (lengkap/partial bertanda), pulih sendiri, primary kembali ke node yang kembali, 0 HazardEvent hilang, tahan kehilangan berikutnya |
| **P4** | `bash scripts/demo/m2-p4.sh` (`BURST_END_AT`, `BURST_END_OFFSET`); `python3 tools/satellite/probe.py` | lag dan p95 masuk → tersimpan, catch-up, checksum SATELIT; distribusi keterlambatan feed |
| **P5 game day** | `make demo-m2-gameday` (`SENSOR_EXAM_KEY=<file>` untuk profil ujian) | kombinasi kegagalan tanpa intervensi, pulih sendiri, SLO dan error budget terpakai |
| Bonus HPA | `make demo-m2-hpa` (`bash scripts/k8s/hpa.sh on/off`) | instance bertambah saat beban naik dan berkurang saat turun; matikan sebelum load test P2 |

UI di node mana pun: **Grafana** `http://<node>:30030` (dashboard "Argus / Observabilitas"), **Jaeger** `http://<node>:30686`,
**Prometheus** `http://<node>:30090`, **dashboard analitik** `http://<node>:30080/dashboard`. Di compose:
`docker compose --profile obs up -d` (Grafana `:3000`, Jaeger `:16686`, Prometheus `:9090`, Sensor Pantai `:8094`).

Memicu manual di cluster (lewat `kubectl port-forward svc/pvmbg-mock 18082:8082`):

```bash
curl -X POST -d '{"mode":"incompatible"}' localhost:18082/admin/schema-version     # rename + type change + field wajib hilang
curl -X POST -d '{"version":1}' localhost:18082/admin/schema-version               # kembali ke skema asli
curl -X POST -d '{"kind":"truncated"}' localhost:18082/admin/malformed             # juga: deep, invalid_utf8, oversize
curl -H "Authorization: Bearer $AGGREGATOR_OPS_TOKEN" "localhost:18083/internal/ops/unprocessed?status=all"
curl -X POST -H "Authorization: Bearer $AGGREGATOR_OPS_TOKEN" -d '{"source":"PVMBG"}' localhost:18083/internal/ops/unprocessed/reprocess
```

Penyesuaian skema permanen: ubah `deploy/k8s/schema-rules.json` (contoh `deploy/schema-rules/pvmbg-v3.json`), jalankan
`make k8s-up` (ConfigMap berganti nama → rolling update), lalu *reprocess* setelah rollout selesai.

### M2.5 Canonical Store terdistribusi (P1) dan Feed Satelit (P4)

`make k8s-up` memasang Canonical Store sebagai **3 shard Patroni**, masing-masing 2 anggota PostgreSQL di node berbeda
(pod anti-affinity, disk lokal `local-path`). HazardEvent diarahkan ke shard dengan FNV-1a 64 atas `hazard_id` mod 3.
Primary pilihan tiap shard dirotasi di node penyimpanan (2 node: shard-0 → node-a, shard-1 → node-b, shard-2 → node-a),
sehingga beban tulis ada di lebih dari satu node. `balance-shard` di setiap anggota mengembalikan primary ke node
pilihannya lewat switchover terencana setelah replica di sana sinkron (node yang kembali dengan disk kosong dibangun
ulang otomatis oleh Patroni dengan base backup, lalu ikut menanggung tulisan lagi). Laporan: [P1](docs/report/m2/01-p1.md).

Cluster yang masih memakai store tunggal: `MIGRATE_CANONICAL=1 make k8s-up` menyalin, memverifikasi, lalu berpindah ke
sharded (PVC lama dipertahankan). Cluster QEMU 3 node di host Linux: `bash scripts/k8s/qemu-cluster.sh up && … deploy`
([panduan](docs/k8s-cluster.md#menjalankan-demo-dari-host-linux)).

Katalog gempa historis: satu objek BMKG `SeismicEvent` per baris (`contracts/schemas/seismic-event.json`).

```bash
python3 tools/catalog/catalog.py convert katalog-asisten.csv > catalog.ndjson   # atau: generate --count 5000
CATALOG=catalog.ndjson make demo-m2-p1-load
```

Feed Satelit dibaca oleh instance Aggregator yang memegang lease ingest (SSE, resume dari offset tersimpan); token di
Secret `satellite-feed-credentials`. Laporan: [P4](docs/report/m2/04-p4.md).

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
| Client-Facing API (BNPB) | **8080** | `CLIENT_API_PORT` |
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
| **P4** rebuild satu service, JSONB | `make demo-p4` | `pvmbg-mock` dihentikan, di-rebuild, dan dijalankan lagi tanpa menyentuh container lain; field baru masuk ke JSONB tanpa migrasi dan record lama tetap terbaca; Canonical Store hanya terjangkau Aggregator |
| **P5** pub/sub | `make demo-p5` | event sampai ke dua consumer group, satu consumer dimatikan lalu menyusul event yang terlewat, dan group baru ditambahkan tanpa mengubah producer |
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
docs/                contracts.md, adapters.md, traceability.md, report/ (sumber laporan), evidence/, laporan PDF
scripts/             lib.sh, wait-healthy.sh, check-secrets.sh, demo/*.sh, k8s/*.sh, loadtest/ (k6 M1 + M2)
pkg/obs/             utilitas teknis bersama: log JSON, correlation ID, klien HTTP ber-latensi, health, env,
                     metrik Prometheus (/metrics) dan tracing W3C/OTLP - tanpa dependensi pihak ketiga
services/
  bmkg-mock/  pvmbg-mock/  aggregator/
  auth-service/  client-api/   (client-api: juga POST /v1/field-reports, admisi berprioritas, /dashboard)
  consumer-hazard/   (dijalankan dua kali: dashboard dan notifier; notifier = peringatan kritis Tim Lapangan)
  field-reports/     pemroses laporan Tim Lapangan, exactly-once dengan transaksi Kafka (M2 P3 kriteria 6)
tools/pvmbg-token/   pembuat token PVMBG
deploy/kafka/        pembuat topic (kafka-init): hazard.events, field-reports, field-reports.processed
deploy/k8s/          manifest M2 (+ sensor-pantai.yaml, field-reports.yaml, observability.yaml)
deploy/grafana/      dashboard metrik (gen-dashboard.py -> argus-overview.json)
deploy/hpa/          bonus autoscaling (tidak termasuk kustomization; scripts/k8s/hpa.sh)
deploy/compose/      konfigurasi Prometheus/Grafana untuk docker compose --profile obs
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
