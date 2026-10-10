# Matriks penelusuran: kriteria → demo → bukti → kode

Laporan mensyaratkan setiap klaim dapat ditelusuri ke bukti atau kode. Tiap kriteria di spesifikasi dipetakan di sini.
Kolom **Status** diisi pemiliknya. `make demo-*` menyimpan transkrip ke `docs/evidence/`.

Legenda: ✅ terbukti dan lulus · 🟡 sebagian · ⬜ belum (pemilik: nama di kolom Owner).

## Ketentuan implementasi (lintas problem)

| Ketentuan | Demo / cara memeriksa | Bukti | Kode | Owner | Status |
|---|---|---|---|---|---|
| Komunikasi lewat jaringan, tanpa fungsi langsung / library business-logic bersama | `pkg/obs` hanya utilitas teknis; tiap service `go.mod` sendiri | README §Kode bersama | `pkg/obs`, `services/*/go.mod` | 1 | ✅ |
| Satu storage satu pemilik; Canonical Store hanya diakses Aggregator | `make demo-isolation` | `docs/evidence/p4/isolation.log` | `docker-compose.yml` (network), `services/aggregator/internal/ports` | 1 (topologi) / 3 (P4) | ✅ topologi |
| Satu container per service, `docker compose up` | `make reset` | – | `services/*/Dockerfile`, `docker-compose.yml` | 1 | ✅ |
| Kredensial tidak lintas instansi | `make demo-mock-auth` | `docs/evidence/p3/mock-auth.log` | `services/*-mock/internal/*/server.go`, `token.go` | 1 | ✅ |
| Tidak ada kredensial di kode/commit; `.env.example` | `make secrets` | output `check-secrets.sh` | `.env.example`, `.gitignore`, `.gitleaks.toml`, `.github/workflows/ci.yml` | 1 | ✅ |
| `/health`, log terstruktur + correlation ID, latensi panggilan keluar | P1 langkah 8 | `docs/evidence/p1/latest.log` | `pkg/obs` | 1 | ✅ |
| Polling dari BNPB (mock pasif), interval configurable, ≥1 event/10 s, seed ≥20 | test mock + P1 | `docs/evidence/p1/latest.log` | `services/*-mock/internal/*/generator.go` | 1 | ✅ |
| Kegagalan benar-benar dipicu saat demo | `make demo-p2-aggregator` | `docs/evidence/p2/aggregator-latest.log` | `POST /admin/outage`, `/admin/delay` | 1 | ✅ |

## Problem 1 — schema evolution (Owner: 1)

| Kriteria | Demo | Bukti | Kode |
|---|---|---|---|
| Skema awal → HazardEvent sesuai aturan pemetaan (kedua sumber) | `make demo-p1` langkah 2 (oracle independen) | `docs/evidence/p1/latest.log` | `services/aggregator/internal/mapping`; `mapping_test.go` |
| Perubahan skema lewat `POST /admin/schema-version` tanpa restart/redeploy | langkah 3–5 (`StartedAt` tidak berubah) | idem | `services/pvmbg-mock/internal/pvmbg/server.go` (`adminSchema`) |
| Aggregator tetap valid setelah perubahan, tanpa crash / intervensi kode | langkah 5–6 | idem | `poller/pvmbg.go`, `mapping/volcanic.go` |
| Kebijakan field tak dikenal dideklarasikan dan konsisten | langkah 5 (`lava_dome_volume_m3`), 7 (karantina); `TestUnknownFieldPolicyIsConsistent` | idem; `docs/report/01-p1.md` | `mapping/raw.go` (komentar paket) |
| Laporan: mekanisme + jenis perubahan didukung/tidak didukung | – | `docs/report/01-p1.md` | – |

## Problem 2 — concurrency, availability, degradation (Owner: 2; sisi ingest: 1)

| Kriteria | Demo | Bukti | Kode | Owner | Status |
|---|---|---|---|---|---|
| Tak ada head-of-line blocking di sisi ingest (loop per sumber) | `make demo-p2-aggregator` A; `TestSlowSourceDoesNotBlockFastSourceAndCyclesNeverOverlap` | `docs/evidence/p2/aggregator-latest.log` | `aggregator/internal/poller/loop.go` | 1 | ✅ |
| p95 BMKG-only < 300 ms saat PVMBG delay 3 s, bersamaan dengan pembaca data vulkanik (lewat Client-Facing API) | `make demo-p2` langkah A | `docs/evidence/p2/latest.log` | `scripts/demo/p2.sh`, `services/client-api` | 2 | ✅ |
| Load test ≥50 koneksi ≥60 s, throughput/p50/p95/p99/error rate (tiap VU login + refresh sendiri; 429 dilaporkan terpisah) | `make demo-p2` langkah E | `docs/evidence/p2/k6-summary.json` | `scripts/loadtest/p2.js` | 2 | ✅ |
| Kelebihan beban ditolak terkendali (429), tanpa 5xx, layanan tetap hidup | `make demo-p2` langkah F | `docs/evidence/p2/k6-overload-summary.json` | `client-api/internal/limit`, `limit_test.go` | 2 | ✅ |
| Outage: seismik normal; vulkanik dari store + `stale_since` / status eksplisit; pulih tanpa restart | sisi Aggregator: `make demo-p2-aggregator` B–D; sisi Client-API: `make demo-p2` B–D; `TestStaleMarkerWhenSourceIsDown` | `aggregator-latest.log`, `latest.log`, `cid-trace.log` | `resilience/`, `client-api/internal/api` | 1 / 2 | ✅ |
| Laporan: model konkurensi, alternatif, batasan, konfigurasi dan tooling load test | – | `docs/report/02-p2.md` | – | 2 | ✅ |

## Problem 3 — auth (Owner: 2; sisi mock: 1)

| Kriteria | Demo | Bukti | Owner | Status |
|---|---|---|---|---|
| Kredensial BMKG ditolak PVMBG dan sebaliknya (401/403) | `make demo-mock-auth` | `docs/evidence/p3/mock-auth.log` | 1 | ✅ |
| Media hanya field Ringkasan; **semua bentuk** permintaan field mentah ditolak server (403) | `make demo-p3` B–C; `TestMediaRejectsEveryWayOfAskingForMentah`, `TestMediaNeverReceivesMentahOnListOrGet` | `docs/evidence/p3/latest.log` | 2 | ✅ |
| Token Lapangan kedaluwarsa (60 s) → refresh tanpa login; token lama ditolak | `make demo-p3` | idem | 2 | ✅ |
| Tidak ada kredensial ter-hardcode/ter-commit; `.env.example` | `make secrets` | output skrip | 1 | ✅ |
| Laporan: mekanisme, alternatif, batasan, mengapa tidak membuka celah baru | – | `docs/report/03-p3.md` | 2 | ✅ |

## Problem 4 — microservice + storage (Owner: 3; topologi: 1)

| Kriteria | Demo | Bukti | Owner | Status |
|---|---|---|---|---|
| Tiap komponen container terpisah; satu dapat di-stop/rebuild/start tanpa merestart yang lain | `make demo-p4` langkah A (`pvmbg-mock` di-stop, di-build, dijalankan lagi; ID dan `StartedAt` container lain tetap) | `docs/evidence/p4/latest.log` | 3 | ✅ |
| HazardEvent di penyimpanan tanpa migrasi saat field baru muncul; record lama dan baru berdampingan | `make demo-p4` langkah B–D (Postgres JSONB, sidik kolom tabel tetap), `repotest` | `docs/evidence/p4/latest.log` | 3 | ✅ |
| Canonical Store hanya dapat diakses Aggregator | `make demo-isolation`; juga langkah E `make demo-p4` | `docs/evidence/p4/isolation.log`, `docs/evidence/p4/latest.log` | 1 | ✅ |

## Problem 5 — pub/sub (Owner: 3; producer: 1)

| Kriteria | Demo | Bukti | Owner | Status |
|---|---|---|---|---|
| Event dipublikasikan ke broker, bukan ke consumer langsung | outbox + relay; `TestBrokerOutageDoesNotStopIngestionOrLoseEvents` | `aggregator/internal/relay` | 1 | ✅ (sisi producer) |
| ≥2 consumer independen; consumer mati → producer tak tertahan; catch-up; consumer baru tanpa ubah producer | `make demo-p5` | `docs/evidence/p5/latest.log` | 3 | ✅ |
| Semantik pengiriman dan idempotensi | `docs/contracts.md` §6, `docs/report/05-p5.md` | laporan | 3 | ✅ |
| Group baru membaca riwayat topic (pengukuran terpisah dari demo) | perintah manual yang tercatat di berkasnya | `docs/evidence/p5/new-group-replay.log` | 1 | ✅ terukur |
