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
`limit` maks 500), `GET /internal/hazards/{id}`, `GET /internal/sources/status`, `GET /internal/quarantine`,
`/health`, `/ready`. Selalu mengembalikan HazardEvent **lengkap**; pembatasan field per klien adalah tugas Client-Facing API.

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
