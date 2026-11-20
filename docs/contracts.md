# Kontrak antar-komponen (dibekukan)

Dokumen ini adalah sumber kebenaran untuk hal-hal yang dipakai lebih dari satu orang. **Perubahan hanya lewat PR
ke dokumen ini (dan file di `contracts/`) dan harus diumumkan ke grup.** Kontrak yang bisa diuji mesin ada di
`contracts/schemas/*.json` (JSON Schema) dan `contracts/openapi/*.yaml`; test tiap service memvalidasi terhadapnya.

## 1. Port dan jaringan

| Komponen | Port container | Port host | Jaringan |
|---|---|---|---|
| bmkg-mock | 8081 | 8081 | `app-net` |
| pvmbg-mock | 8082 | 8082 | `app-net` |
| aggregator (API internal) | 8083 | **127.0.0.1**:8083 | `app-net`, `canonical-net`, `broker-net` |
| client-api (Orang 2) | 8080 | 8080 | `app-net` |
| auth-service (Orang 2) | (bebas) | – | `app-net` (+ storage sendiri, jaringan sendiri) |
| postgres-canonical | 5432 | – (tidak dipublikasikan) | `canonical-net` (internal) |
| kafka | 9092 | – (tidak dipublikasikan) | `broker-net` (internal) |
| consumer-* (Orang 3) | 8091 / 8092 (`/health`, `/stats`) | 8091 / 8092 | `broker-net` + `app-net` (`broker-net` bersifat `internal`, port host hanya dapat dipublikasikan lewat `app-net`) |

Aturan: **hanya Aggregator** berada di `canonical-net`. Komponen lain membaca data lewat API Aggregator.
Port dapat diubah lewat `.env` (`*_PORT`) dan tercatat di README.

## 2. Kredensial (masing-masing domain independen)

| Domain | Header | Pemegang | Catatan |
|---|---|---|---|
| BMKG | `X-BMKG-Key: <BMKG_API_KEY>` | bmkg-mock (memvalidasi), aggregator (memakai) | kunci opak |
| PVMBG | `Authorization: Bearer <token>` | pvmbg-mock (secret penanda tangan), aggregator (hanya token) | token `pvmbg.v1.<claims>.<hmac-sha256>`, klaim `iss=pvmbg-geologi`, `aud=pvmbg-api`, `exp`. Dibuat dengan `make pvmbg-token` |
| API internal Aggregator | `X-Service-Key: <AGGREGATOR_SERVICE_KEY>` | aggregator (memvalidasi), client-api (memakai) | |
| Operator mock | `X-Admin-Token` | BMKG dan PVMBG masing-masing punya token sendiri | untuk `/admin/*` |

Kode status: kredensial tidak ada → **401**; salah / milik instansi lain → **403**; token PVMBG kedaluwarsa → 401.
### Auth Service (Orang 2)

| Item | Nilai |
|---|---|
| Port | **8084** (`AUTH_SERVICE_PORT`) |
| Jaringan | `app-net` (client-api), `auth-net` (internal; SQLite volume) |
| Issue token | `POST /v1/token` JSON `{client_id, client_secret}` → `{access_token, refresh_token, token_type, expires_in, role}` |
| Refresh | `POST /v1/refresh` JSON `{refresh_token}` → pasangan token baru; refresh lama one-time (rotate) |
| Access token | JWT HS256, claim `role` ∈ `media` \| `lapangan` \| `pusat`, TTL default **60 s** (`AUTH_ACCESS_TTL_SEC`) |
| Verifikasi | Client-Facing API memakai `AUTH_JWT_SECRET` yang sama (tanpa hardcode) |
| Kredensial klien | `AUTH_*_CLIENT_ID` + `AUTH_*_CLIENT_SECRET` per role; dari env |

### Client-Facing API (Orang 2)

OpenAPI: `contracts/openapi/client-api.yaml`.

| Item | Nilai |
|---|---|
| Port | **8080** (`CLIENT_API_PORT`) |
| Jaringan | `app-net` saja; **tidak** akses `canonical-net` / broker |
| Upstream | `GET {AGGREGATOR_URL}/internal/*` dengan `X-Service-Key` |
| Auth client | `Authorization: Bearer <JWT>` dari Auth Service; tanpa header, token rusak, salah tanda tangan, atau kedaluwarsa → **401** (dengan `WWW-Authenticate: Bearer`); token sah tetapi peran tidak berhak, atau Media meminta field Mentah → **403** |
| Endpoints | `GET /v1/hazards`, `GET /v1/hazards/{id}`, `GET /v1/sources/status` (lapangan + pusat saja) |
| Field policy | **Media**: hanya Ringkasan (+ `stale`); `fields` hanya boleh berisi field Ringkasan dan `include` hanya kosong/`summary`; selain itu (Mentah, nama tak dikenal, huruf besar, `fields[]`, parameter berulang) → **403**. **Lapangan/Pusat**: HazardEvent penuh + `stale` |
| Stale | `stale: true` per item bila `freshness[source].stale_since` (list) atau status sumber (get by id) |
| Rate limit | `CLIENT_RATE_LIMIT_RPS/BURST` (default 500/1000, satu token bucket global; `/health` dan `/ready` dikecualikan); HTTP **429** `too_many_requests` + `Retry-After` |

## 3. Keputusan yang dikunci

1. **`since` untuk `/tsunami-warnings`** memakai `issued_at` (field tambahan pada TsunamiWarning, diizinkan spesifikasi).
   `/seismic-events` memakai `occurred_at`; `/volcanic-reports` memakai `reported_at`. Semua **strict `>`**.
   Respons mock berupa **JSON array** (bukan envelope).
2. **`hazard_id`** = `bmkg-<event_id>` / `pvmbg-<report_id>` (deterministik → ingest idempoten).
3. **Outage PVMBG**: `POST /admin/outage {"enabled":true,"mode":"error"|"hang"}`. `error` = 503 seketika; `hang` =
   tidak menjawab sampai klien menyerah (dibatasi `PVMBG_HANG_MAX_SEC`). `/health` tetap menjawab dan melaporkan `outage`.
4. **Format token PVMBG**: lihat tabel di atas. Token BMKG bukan token PVMBG dan sebaliknya (struktur berbeda).
5. **Kebaruan data**: `GET /internal/hazards` menyertakan `freshness.{BMKG,PVMBG}` = `{state, last_success_at, stale_since}`.
   `stale_since` non-null selama sumber tidak `up` (nilainya waktu sinkronisasi sukses terakhir). Client-Facing API
   memakainya untuk menandai data usang saat outage. `GET /internal/sources/status` memberi detail lengkap.
6. **Envelope Kafka**: lihat §6.
7. **Format error JSON** seragam: `{"error":{"code":"...","message":"..."}}`.
8. **Waktu**: ISO 8601 UTC (RFC 3339, presisi hingga mikrodetik pada HazardEvent).
9. **Tambahan di atas kontrak M1** (diizinkan, tidak mengubah yang sudah ditetapkan): `issued_at` pada warning;
   `version` dan `updated_at` pada HazardEvent; `/admin/generate`, `/admin/delay`, `/admin/state`,
   `extra_fields` dan `override` pada mock (untuk demo); `/ready` pada semua service.

## 4. HazardEvent dan klasifikasi field

Skema: `contracts/schemas/hazard-event.json`. Klasifikasi (ditegakkan Client-Facing API, bukan klien):

| Klasifikasi | Field |
|---|---|
| **Ringkasan** | `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at`, `version`, `updated_at` |
| **Mentah** | `source_ref_id`, `latitude`, `longitude`, `attributes` |

`version` dan `updated_at` (tambahan) diklasifikasikan Ringkasan. `version` naik saat isi hazard yang sama berubah
(mis. warning tsunami terlambat menaikkan severity) dan menjadi bagian kunci idempotensi (`hazard_id:version`).

Aturan pemetaan dan kebijakan field tak dikenal: lihat `services/aggregator/internal/mapping` (komentar paket) dan
`docs/report/01-p1.md`. Ringkas: field tak dikenal → `attributes`; perubahan yang tidak didukung → karantina.
Pada update record yang sama, `attributes` **digabung aditif** (field yang berhenti dikirim sumber tidak dihapus).

## 5. API internal Aggregator

`contracts/openapi/aggregator-internal.yaml`. Ringkas:
`GET /internal/hazards?hazard_type=&source=&severity=&since=&limit=&cursor=` (terbaru dulu, keyset pagination,
`limit` maks 500), `GET /internal/hazards/{id}`, `GET /internal/sources/status`, `GET /internal/leader` (M2),
`/health`, `/ready`. Selalu mengembalikan HazardEvent **lengkap**; pembatasan field per klien adalah tugas Client-Facing API.
M2 menambah `/internal/ops/*` (BNPB Internal Ops) dan `/internal/audit/*` (Lampiran A); lihat §9. `GET /internal/quarantine`
(M1) dihapus: pesan gagal kini hanya boleh dilihat Internal Ops.

## 6. Kafka

| Item | Nilai |
|---|---|
| Topic | `hazard.events` (`KAFKA_TOPIC`), 3 partisi, replikasi 1, retensi 7 hari (dibuat oleh `kafka-init`; auto-create dimatikan) |
| Key | `hazard_id` (urutan per-hazard terjaga dalam satu partisi) |
| Value | JSON `contracts/schemas/kafka-envelope.json`: `{event_id, kind: created\|updated, emitted_at, hazard, schema}` |
| Header | `correlation_id` (sama dengan siklus poll yang menghasilkan perubahan), `event_id` |
| Kunci idempotensi | `event_id` = `<hazard_id>:<version>` |
| Semantik | **at-least-once**: producer menunggu ack (`acks=all`, idempotent); consumer wajib idempoten dengan dedup `event_id` |
| Consumer group | satu group per consumer: `dashboard-updater`, `notifier`, `pemda-portal` (consumer baru = group baru, tanpa mengubah producer) |

Producer adalah Aggregator lewat **transactional outbox** (hazard + entri outbox ditulis dalam satu transaksi; relay
mengirim ke Kafka). Kafka mati tidak menghentikan ingest dan tidak menghilangkan event.

## 7. Log terstruktur

JSON satu baris. Field tetap: `ts`, `level`, `service`, `msg`, `cid` (correlation ID). Header HTTP: `X-Correlation-ID`
(dibuat bila tidak ada, diteruskan ke semua panggilan keluar, dikembalikan pada respons). Header pesan Kafka:
`correlation_id`. Pesan log baku: `request` (`method,path,status,duration_ms`), `outbound_call`
(`target,method,url,latency_ms,status|error`), `poll_ok|poll_failed`, `hazard_stored`, `mapping_rejected`,
`outbox_published`, `auth_rejected`. Kode: `pkg/obs` (utilitas teknis, tanpa business-logic).

## 8. Variabel lingkungan

Daftar lengkap dan nilai contoh: `.env.example`. Kredensial **tidak** punya nilai default di kode; service berhenti dengan
pesan jelas bila variabelnya kosong. Orang 2 dan 3 menambahkan variabel milik mereka ke `.env.example` dan `docker-compose.yml`.

## 9. Milestone 2 (Orang 1: platform, P2, P3 sisi ingest)

### 9.1 Kubernetes

| Item | Nilai |
|---|---|
| Namespace | `sistem-informasi-kebencanaan-bnpb` (semua komponen aplikasi) |
| Manifest | `deploy/kustomization.yaml` + `deploy/k8s/*.yaml`; satu perintah: `make k8s-up` (`scripts/k8s/up.sh`) |
| Label wajib | `role` ∈ `aggregator`, `store`, `broker`, `consumer`, `api`, `mock`, `sensor`, `other` (Tester Agent mencari sasaran lewat label ini) + `app` |
| Sumber daya | sesuai Lampiran B (aggregator 256Mi/512Mi, store dan broker 512Mi/1Gi, consumer/api/other 128Mi/256Mi, mock 64Mi/128Mi) |
| Secret | satu per komponen, dibuat `scripts/k8s/secrets.sh` dari `.env`: `bmkg-mock-credentials`, `pvmbg-mock-credentials`, `canonical-db-credentials`, `aggregator-credentials`, `auth-service-credentials`, `client-api-credentials`, `satellite-feed-credentials` (`SATELLITE_FEED_TOKEN`, `SATELLITE_FEED_URL`; dipakai komponen P4 lewat `secretRef`). Komponen baru menambah Secret-nya sendiri di skrip ini |
| ConfigMap | `<komponen>-config` (non-rahasia), `aggregator-schema-rules` (dari `deploy/k8s/schema-rules.json`, nama ber-hash → ubah = rolling update) |
| Isolasi storage | NetworkPolicy: `postgres-canonical` hanya menerima pod `app=aggregator,bnpb.io/canonical-client=true`; `kafka` hanya pod `bnpb.io/kafka-client=true`. **Komponen baru yang perlu Kafka wajib memberi label itu** |
| Data store | PVC `ReadWriteOnce` pada StorageClass bawaan (k3s: `local-path` = disk node), bukan penyimpanan jaringan |
| Titik masuk | NodePort `30080` (client-api), `30084` (auth), `30083` (aggregator: ops/audit/internal) |
| Tester Agent | dipasang `up.sh` dari URL manifest yang dipin, tanpa modifikasi; tidak ada NetworkPolicy/RBAC/taint yang membatasinya |

### 9.2 Leader election dan fencing (P2)

- Lease `coordination.k8s.io/v1` bernama `aggregator-ingest` (RBAC: SA `aggregator`, verb get/create/update). Pemegang =
  nama pod. **Term** = `leaseTransitions`, naik setiap pergantian pemegang.
- Hanya leader yang polling, menjalankan relay outbox, dan retry dead letter. Semua instance melayani baca.
- **Kontrak fencing untuk setiap implementasi `ports.Repository` (termasuk store ter-shard Orang 3):** setiap tulis yang
  mengubah state ingest (`Save`, `SetCursor`, `AddUnprocessed`, `UpdateUnprocessed`, `PutSourceStatus`) membaca
  `ports.FenceFrom(ctx)` dan, **dalam transaksi yang sama dengan tulisnya, di unit penyimpanan yang ditulis**, menolak
  (`ports.ErrFenced`, tanpa menulis apa pun) term yang lebih kecil dari term terbesar yang pernah dilihat unit itu, serta
  menaikkan term tersimpan bila term tulis lebih besar. `RaiseFence` dipanggil leader baru sebelum tulis pertamanya dan
  **harus menaikkan fence di semua unit** (store ter-shard: setiap shard dan replikanya). Diuji oleh `repotest` (`runFence`).
- Status sumber dibagi lewat store (`PutSourceStatus`/`ListSourceStatus`, umur diukur dengan jam store), bukan memori
  instance; `/internal/hazards` dari instance mana pun memberi `freshness` yang sama.
- `/ready` = `Repo.Ping`. Store ter-shard sebaiknya mengartikannya "masih bisa melayani (mungkin sebagian)", agar instance
  tidak dikeluarkan dari Service hanya karena satu shard hilang.

### 9.3 Dead-letter store (P3)

- Tabel `unprocessed` di Canonical Store (`ports.Unprocessed`): `message_id` deterministik (sha256 source+kind+bytes),
  `raw` = **byte persis** yang diterima (pesan > `MESSAGE_MAX_BYTES` disimpan 1 MiB pertama + `raw_size` asli), `reason`,
  `retryable`, `attempts`, `status` ∈ `retrying` | `parked` | `resolved`, `kind`, `record_key`.
- Kebijakan skema: lihat komentar paket `services/aggregator/internal/mapping/raw.go`. Penyesuaian sistem = aturan di
  `deploy/k8s/schema-rules.json` (`rename`, `coerce`); contoh untuk PVMBG v3: `deploy/schema-rules/pvmbg-v3.json`.
- Kredensial: `AGGREGATOR_OPS_TOKEN` (Internal Ops, `/internal/ops/*`), `AUDIT_TOKEN` (auditor, `/internal/audit/*`).
  `X-Service-Key` milik Client-Facing API **tidak** membuka keduanya.
- Orang 3: endpoint Audit API lain (`events`, `checksum`, `location`) dipasang di `internal/api` memakai middleware
  `s.bearer("audit", s.AuditToken, ...)` yang sama agar format error dan aturan 401/403 seragam.

### 9.4 PVMBG mock (tambahan, kontrak lama tidak berubah)

`POST /admin/schema-version`: `{"version":3}` / `{"mode":"incompatible"}` / `{"incompatible":["rename","type_change","drop_required"]}`;
`{"version":1}` kembali ke skema asli. `POST /admin/malformed {"kind":"truncated|deep|invalid_utf8|oversize"}`.
Rincian: `contracts/openapi/pvmbg.yaml`.

### M2 P1 Audit API (Lampiran A)

All routes use the existing audit bearer principal. Missing or invalid bearer tokens
return 401. Another known principal returns 403. Responses use UTC and JSON.

| Route | Query | Response |
|---|---|---|
| `GET /internal/audit/events` | Optional `source`, `since`, `limit`, `cursor` | Active canonical events sorted by `ingested_at`, then `hazard_id`, ascending. `since` is inclusive and uses ingestion time. Default page size 100, maximum 1000. The last `next_cursor` is JSON null. |
| `GET /internal/audit/checksum` | Optional `source` | `count`, `checksum`, `source`, `algorithm`, `as_of` and coverage. Sort the `source_ref_id` values by byte order, join with LF and hash using SHA-256. No trailing LF. An omitted source returns `source: ""` and includes every source. |
| `GET /internal/audit/location` | Required `hazard_id` | `hazard_id`, `locations`, and `partial`. Each verified location has `node`, `unit` and `role`. A 404 means the authoritative shard confirmed absence. |
| `GET /internal/audit/unprocessed` | Optional `source`, `limit`, `cursor` | Existing open DLQ projection. See earlier section. |

Event, checksum, and both hazard list APIs report `partial` with `unavailable_units`.
An unavailable replica can make coverage partial while its primary still supplies all
logical rows. An unavailable logical shard can omit rows. The API does not hold a global
snapshot between pages. Restart a partial scan after recovery. A checksum uses a
statement snapshot on each reachable primary and can be compared reliably with an
independent export while input is quiescent.

### M2 P4 Feed Satelit

`SATELIT/WILDFIRE` IDs begin with `satelit-`. The `detection_keys` attributes retain
the lexical values received from the feed. An extinguished/retracted component has
a versioned `retired` envelope, with `hazard.retired: true`. Ordinary reads and Audit
exclude retired records. Existing created/updated envelopes keep their field names.
The Aggregator's `GET /internal/satellite/status` uses the service key and exposes
its last applied offset, feed head, lag, singleton and late counts, and status age.
Structured `satellite_hazard_applied` logs carry receive-to-store latency.

## 10. Milestone 2 (Orang 2: observabilitas, P3 kriteria 6, P5, P1 kriteria 7, load test)

Semua penambahan bersifat aditif; kontrak di atas tidak berubah.

### 10.1 Observabilitas (setiap service, `pkg/obs`)

| Aspek | Kontrak |
|---|---|
| Metrik | `GET /metrics` di port HTTP setiap service (format teks Prometheus 0.0.4), dilayani oleh `obs.Middleware`. Standar: `http_server_requests_total{service,method,route,code}`, `http_server_request_duration_seconds{service,method,route}` (histogram), `http_server_in_flight_requests`, `http_client_requests_total{service,target,code}`, `http_client_request_duration_seconds{service,target}`. `route` = pola ServeMux (`/v1/hazards/{id}`), bukan path mentah. Metrik khusus per service: `aggregator_*`, `sensor_pantai_*`, `consumer_*`, `client_api_*`, `field_reports_*`. |
| Penemuan target | Prometheus menemukan sendiri setiap pod di namespace yang punya container port bernama `http`. Label hasil: `app`, `pod`, `node`, `role`. |
| Trace | W3C `traceparent` dipropagasikan lewat header HTTP dan header record Kafka (`traceparent`, di samping `correlation_id`). Setiap respons HTTP membawa `traceparent` sehingga pemanggil dapat mencari trace-nya. Trace yang dimulai tanpa `traceparent` mendapat `trace_id = sha256("cid:"+cid)[:16]`, sehingga relay outbox (yang hanya tahu `cid`) masuk ke trace yang sama. Ekspor: OTLP/HTTP JSON ke `$OTEL_EXPORTER_OTLP_ENDPOINT/v1/traces` (Jaeger `:4318`). Sampling: `OTEL_TRACES_SAMPLER_ARG` (0..1, deterministik per trace). Ekspor tidak pernah memblok: antrean 4096 span, sisanya dibuang dan dihitung (`obs_trace_spans_dropped_total`). |
| Log | Baris JSON yang sudah ada mendapat `trace_id` di samping `cid` (`cid` tetap = `X-Correlation-ID`). |
| UI (NodePort) | Grafana `30030` (dashboard "Argus / Observabilitas", anonim Viewer), Jaeger `30686`, Prometheus `30090`. Compose: `docker compose --profile obs up -d` (`3000`, `16686`, `9090` di loopback). |

### 10.2 `POST /v1/field-reports` (P3 kriteria 6)

| Aspek | Kontrak |
|---|---|
| Siapa | Hanya peran `lapangan` (selain itu 403). `GET /v1/field-reports/{id}`: `lapangan` (laporan sendiri) dan `pusat` (semua). |
| Idempotensi | Header `Idempotency-Key` wajib (`[A-Za-z0-9._:-]{1,128}`). `report_id = "fr-" + hex(sha256(client_id + "\0" + key))[:20]`: kunci yang sama dari klien yang sama selalu menghasilkan `report_id` yang sama. |
| 202 | Dikirim **setelah** record berada di topic Kafka `field-reports` (key = `report_id`, `acks=all`, producer idempoten). Bila broker tidak mengonfirmasi dalam `FIELD_REPORT_ACK_TIMEOUT_SEC` (5 s) → **503** + `Retry-After`; klien mengulang dengan kunci yang sama. Pemroses hilir tidak ada di jalur 202. |
| Record `field-reports` | `{report_id, idempotency_key, client_id, received_at, report}`, header `correlation_id`, `traceparent`, `report_id`. |
| Pemroses | `services/field-reports` (consumer group `field-report-processor`, transactional id `field-report-processor`): consume → produce `field-reports.processed` (compacted, key = `report_id`) → commit offset input dalam **satu transaksi Kafka**. `report_id` yang sudah ada di set dedup dilewati. Set dedup dibangun ulang dari `field-reports.processed` (read_committed) saat start. |
| Jaminan | At-least-once ke broker (klien mengulang sampai 202) + pemrosesan transaksional dan idempoten = **effectively exactly-once** per `report_id`. |

### 10.3 Kontrol admisi Client-Facing API (P5 kriteria 5)

Setelah autentikasi, setiap permintaan melewati `limit.Priority`: satu token bucket (`CLIENT_RATE_LIMIT_RPS/BURST` per pod)
dan batas permintaan bersamaan (`CLIENT_MAX_IN_FLIGHT`, default 64 per pod). `lapangan` dan `pusat` memiliki cadangan
`CLIENT_PRIORITY_RESERVE` (0.3) dari keduanya. Media ditolak lebih dulu: **429** + `Retry-After: 1` (penolakan terkontrol).
Prioritas tinggi ditolak hanya ketika kapasitas benar-benar habis, juga dengan **429** (kontrak M1 API ini). Metrik
`client_api_admission_total{role,outcome}` membedakan `shed` (prioritas rendah) dari `rejected` (kapasitas habis).

### 10.4 Sensor Pantai (P5)

| Aspek | Kontrak |
|---|---|
| Deployment | Image asisten dipin dengan digest, manifest contoh asisten (`deploy/k8s/sensor-pantai.yaml`). Ditambahkan hanya `resources`, Service `sensor-pantai:8080`, Secret `pantai-token` (key `token`). Profil ujian: `bash scripts/k8s/sensor-exam.sh exam <key-file>`. |
| Pengambilan | Aggregator (leader) memanggil `GET /v1/observations?cursor=&limit=500` setiap `SENSOR_POLL_INTERVAL_MS` (1 s) dengan timeout `SENSOR_TIMEOUT_SEC` (2 s), hingga 20 halaman per siklus. Klien HTTP dan circuit breaker sendiri (`SENSOR_BREAKER_OPEN_SEC` 10 s). Cursor disimpan sebagai dokumen checkpoint ber-fence `sensor-pantai.cursor` dan baru disimpan setelah semua observasi yang diambil tersimpan atau disisihkan. |
| Validasi kontrak | `observation_id` string tak kosong; `station_id` ada di `GET /v1/stations`; `observed_at` RFC 3339 dan tidak melewati `generated_at` + 1 menit; `sea_level_anomaly_m` angka hingga ±20; `wave_height_m` angka 0..30; `quality` = `"ok"`; `critical` boolean; observasi tidak lebih tua dari `generated_at` − (`SENSOR_MAX_LAG_SEC` + waktu sensor yang lewat selama kita tidak membaca). Pelanggaran → dead-letter `sensor-pantai.observation` (stasiun tak dikenal: retrying; lainnya: parked). Halaman yang tidak dapat di-parse disimpan utuh sebagai `sensor-pantai.response` (parked), item yang masih terbaca diproses, dan aliran berlanjut dari `next_cursor` yang masih terbaca. ID yang sama dengan nilai berbeda → `conflicting_duplicate` (parked); ID yang sama dengan nilai sama → diabaikan. |
| Kesehatan dari data | `/healthz` tidak dipercaya. `generated_at` yang tidak maju selama `SENSOR_FREEZE_AFTER_SEC` (15 s) atau mundur dihitung sebagai kegagalan sumber (breaker, `stale_since`). Bila mundur, cursor di-reset (sensor restart). |
| Pemetaan | `HazardEvent{source: SENSOR_PANTAI, hazard_type: COASTAL, hazard_id: "sensor-pantai-"+observation_id, source_ref_id: observation_id, area_name: province stasiun, lat/lon stasiun, occurred_at: observed_at}`; severity: `critical` → AWAS; \|anomali\| ≥ 1.0 m atau gelombang ≥ 4.0 m → SIAGA; ≥ 0.5 m atau ≥ 2.5 m → WASPADA; selain itu NORMAL. `attributes`: `station_id, station_name, sea_level_anomaly_m, wave_height_m, quality, critical`. Observasi kritis disimpan lebih dulu dalam setiap siklus. |
| Peringatan | `consumer-notifier` mengirim peringatan untuk setiap event dengan `attributes.critical = true` (sekali per `hazard_id`): baris log `critical_alert`, `GET /alerts`, dan metrik `consumer_critical_alert_seconds` (simpan → notifier). Offset di-commit hanya setelah peringatan dikirim. |

### 10.5 Analitik (P1 kriteria 7)

`GET /internal/analytics/summary` (Aggregator, `X-Service-Key`) dan `GET /v1/analytics/summary` (client-api, semua peran)
memiliki parameter `from`, `to` (atas `occurred_at`), `bucket`, dan `ingested_before`. Respons: `by_category` (TSUNAMI =
SEISMIC dengan `attributes.warning_id`), `by_severity`, `by_area`, `by_source`, `series`, `partial`, dan `unavailable_units`.
Angka dihitung dari satu scan penuh berurutan audit yang memakai `Repository.List` yang sama dengan Audit API, dengan cache 5 s.
Halaman: `GET /dashboard`.

### 10.6 Topic Kafka tambahan

| Topic | Partisi | Konfigurasi | Penulis → pembaca |
|---|---|---|---|
| `field-reports` | 3 | retention 7 hari | client-api → field-reports |
| `field-reports.processed` | 3 | `cleanup.policy=compact` | field-reports → field-reports (rebuild dedup) |

NetworkPolicy broker: selain klien berlabel `bnpb.io/kafka-client: "true"`, pod Kafka sendiri juga diizinkan, karena transaksi
membuat broker mengirim `AddPartitionsToTxn` ke dirinya sendiri lewat `kafka:9092`.
