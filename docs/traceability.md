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

---

# Milestone 2

Pembagian kerja: **Orang 1** platform Kubernetes, P2, P3 sisi ingest (kriteria 1–5); **Orang 2** observabilitas, P5,
P3 kriteria 6 (`POST /field-reports`), load test, dashboard analitik P1; **Orang 3** P1 (storage terdistribusi, Audit API
`events`/`checksum`/`location`), P4. Bukti M2 di `docs/evidence/m2/`. "k3s" = lulus di cluster demo k3s 2 VM (README
§M2.1, `docs/evidence/m2/topology.log`), dijalankan 14 Nov 2026.

## Ketentuan implementasi M2

| Ketentuan | Demo / cara memeriksa | Bukti | Kode | Owner | Status |
|---|---|---|---|---|---|
| Semua komponen di Kubernetes, satu perintah deploy | `make k8s-up` | output `scripts/k8s/up.sh` | `deploy/kustomization.yaml`, `deploy/k8s/*`, `scripts/k8s/*` | 1 | ✅ k3s |
| Cluster ≥ 2 node kernel terpisah; deklarasi topologi | README §M2.1; `bash scripts/k8s/topology.sh` | `docs/evidence/m2/topology.log` | `scripts/k8s/vbox-cluster.sh` | 1 (semua) | ✅ k3s (2 VM VirtualBox) |
| `resources.requests/limits` sesuai Lampiran B, label `role` | `kubectl get pods -L role`; manifest | `deploy/k8s/*.yaml` | idem | 1 | ✅ |
| Secret per komponen, ConfigMap, `.env.example` | `make demo-m2-isolation` langkah 4 | `docs/evidence/m2/isolation-latest.log` | `scripts/k8s/secrets.sh` | 1 | ✅ k3s |
| "Satu storage satu pemilik" ditegakkan platform | `make demo-m2-isolation` | idem | `deploy/k8s/networkpolicy.yaml` | 1 | ✅ k3s |
| Data `role=store` di disk node (local PV) | `kubectl get pv` (StorageClass `local-path`) | – | `deploy/k8s/postgres-canonical.yaml` | 1 / 3 | ✅ |
| Tester Agent terpasang tanpa modifikasi, tidak dihalangi; `schema` sampai ke PVMBG (HTTP 200) | `make k8s-up`; `bash scripts/k8s/practice.sh schema` | `docs/evidence/m2/tester-*.log` (schema, kill-leader, clock-skew, partition) | `scripts/k8s/common.sh` (URL dipin), `deploy/k8s/*-mock.yaml` (`*_ADMIN_OPEN`, Service bmkg tanpa label role), `vbox-cluster.sh` (symlink iptables agar `partition` jalan) | 1 | ✅ k3s |
| Observabilitas (dashboard metrik, trace, log `cid`) | – | – | – | 2 | ⬜ |
| Audit API `unprocessed` | `make demo-m2-p3` langkah 3; `TestAuditUnprocessedMatchesLampiranA` | `docs/evidence/m2/p3-latest.log` | `aggregator/internal/api/ops.go` | 1 | ✅ |
| Audit API `events`, `checksum`, `location` | `TestAuditRoutesMatchAppendixA` | `docs/evidence/m2/p1-kind-audit.log` | `aggregator/internal/api/audit.go` | 3 | 🟡 kind dan unit test |

## Problem 2 — Dua Nakhoda, Satu Kapal (Owner: 1; load test: 2)

| Kriteria | Demo | Bukti | Kode | Owner | Status |
|---|---|---|---|---|---|
| 1. ≥ 3 instance di node berbeda, satu titik masuk yang hanya mengarah ke instance sehat | `make demo-m2-p2` langkah 1 | `docs/evidence/m2/p2-latest.log` | `deploy/k8s/aggregator.yaml` (topologySpread, readinessProbe) | 1 | ✅ k3s (3 instance di 2 node) |
| 2. Tiap sumber diambil tepat sekali; tidak ada duplikat; upstream tidak menerima permintaan berlipat | `make demo-m2-p2` langkah 2–4 (log mock per `caller`) | idem | `internal/leader`, `poller/loop.go` (Gate), header `X-Caller` | 1 | ✅ k3s |
| 3. Instance yang mengambil data mati → diambil alih ≤ 60 s, tanpa celah/duplikat | `make demo-m2-p2` langkah 5–6; `practice kill-leader` | idem, `tester-kill-leader.log` | `internal/leader/elector.go` | 1 | ✅ k3s (13 s) |
| 4. Jam bergeser (+45 s / −30 s) → tidak ada dua instance menyimpan pada periode sama | `practice clock-skew` lalu `bash scripts/demo/m2-single-writer.sh`; `TestSkewedHolderClockDoesNotCauseTakeover`, `TestClockJumpOnLeaderNodeKeepsSingleLeader` | `docs/evidence/m2/tester-clock-skew.log` (latihan: +300 s selama 30 s) | `leader/elector.go`, fencing di `adapters/postgres/unprocessed.go` (`checkFence`) | 1 | ✅ k3s |
| 5. Node leader freeze lalu bangun → tidak menyimpan apa pun sampai statusnya diperbarui | `bash scripts/demo/m2-freeze.sh` (VM node-b di-*pause* VirtualBox, `FREEZE_CMD`/`THAW_CMD`); `TestFrozenLeaderIsFencedAfterWakingUp` | `docs/evidence/m2/freeze.log` (beku 48 s, term 33 → 34, 0 baris dari term 33 setelah freeze) | `ports/fence.go`, `leader.Confirm` | 1 | ✅ k3s |
| 6. Load test ≥ 10.000 VU ≥ 120 s, skala 3 → 5 → 2, error < 1 % | – | – | `scripts/loadtest/` | 2 | ⬜ |
| 7. Throughput 3 instance ≥ 2× 1 instance | – | – | – | 2 | ⬜ |
| 8. Ganti versi Aggregator saat beban tanpa permintaan gagal | `make demo-m2-p2` langkah 8 (`NEW_TAG=`) | `docs/evidence/m2/p2-latest.log` (k3s2 → k3s1, 1 955 permintaan, 0 gagal) | `deploy/k8s/aggregator.yaml` (maxUnavailable 0, preStop), `cmd/aggregator/main.go` (drain, lease release) | 1 | ✅ k3s |
| Laporan: persoalan, mekanisme, sumber waktu, batas waktu dan trade-off | – | `docs/report/m2/02-p2.md` | – | 1 | ✅ draf |

## Problem 3 — Surat dari Tetangga (Owner: 1; kriteria 6: 2)

| Kriteria | Demo | Bukti | Kode | Owner | Status |
|---|---|---|---|---|---|
| Mock: mode baru `/admin/schema-version` (rename, type change, field wajib hilang) | `make demo-m2-p3` langkah 4; `TestIncompatibleModeChangesNewReportsOnly` | `docs/evidence/m2/p3-latest.log` | `pvmbg-mock/internal/pvmbg/{server,store}.go` | 1 | ✅ |
| 1. Deklarasi perubahan yang diterima otomatis / tidak; perilaku konsisten | `make demo-m2-p3` langkah 1, 4 | idem | `mapping/raw.go` (komentar paket) | 1 | ✅ k3s |
| 2. Pesan gagal tersimpan utuh + alasan, sumber, waktu; retryable vs parked diperlakukan beda | `make demo-m2-p3` langkah 2; `dead_letter_*` (repotest) | idem | `internal/dlq`, tabel `unprocessed` | 1 | ✅ k3s |
| 3. Pesan rusak (terpotong, dalam, UTF-8 invalid, besar) tidak membuat crash; yang lain tetap diproses | `make demo-m2-p3` langkah 2; `TestSplitIsolatesEveryKindOfBrokenElement`, `TestMalformedMessagesAreParkedAndOthersProcessed` | idem | `sources/split.go` | 1 | ✅ k3s |
| 4. Setelah penyesuaian, pesan gagal diproses ulang: pulih semua, tanpa duplikat, urut per `source_ref_id` | `make demo-m2-p3` langkah 5; `TestIncompatibleChangeRecoversAfterAdjustmentInOrderWithoutDuplicates` | idem | `mapping/rules.go`, `dlq.Worker`, `deploy/schema-rules/pvmbg-v3.json` | 1 | ✅ k3s |
| 5. Pesan gagal hanya dapat diperiksa BNPB Internal Ops | `make demo-m2-p3` langkah 3; `TestDeadLettersOnlyForTheirPrincipals` | idem | `api/internal.go` (`bearer`), `api/ops.go` | 1 | ✅ |
| 6. `POST /field-reports` 202 cepat; tidak hilang, tidak ganda | – | – | – | 2 | ⬜ |
| Laporan: persoalan, kebijakan skema, mekanisme DLQ, jaminan pengiriman | – | `docs/report/m2/03-p3.md` (kriteria 1–5) | – | 1 / 2 | 🟡 kriteria 6 milik Orang 2 |

## Problem 1 — Canonical Store terdistribusi

| Kriteria | Demo / bukti | Owner | Status |
|---|---|---|---|
| 1. Katalog dan sebaran fisik | `aggregator import-catalog`, `m2-p1.sh baseline`, `docs/evidence/m2/p1-kind-audit.log` | 3 | 🟡 katalog sintetis |
| 2. Lonjakan satu wilayah | Impor fixture regional, bandingkan snapshot SQL dari `m2-p1.sh` | 3 | ⬜ pengukuran k3s |
| 3. Urutan dan pagination global | `TestShardLossStillServesAndFenceCoversEveryShard`, `p1-kind-audit.log` | 3 | 🟡 kind dan unit test |
| 4. Worker hilang, cek checksum dan cakupan parsial | `p1-kind-failover.log`, `practice.sh kill-store`, `practice.sh wipe-node` | 3 | 🟡 freeze kind, Tester k3s tertunda |
| 5. Rebuild disk dan kembalikan beban | `scripts/k8s/restore-shard.sh --confirmed-wipe ... --promote`, `p1-kind-restore.log` | 3 | 🟡 kind |
| 6. Tahan gagal lagi setelah sinkronisasi | `p1-kind-restore.log`, kehilangan worker berikutnya setelah standby sinkron | 3 | 🟡 kind, 64/64 record |
| 7. Dashboard analitik | Gunakan angka logis dan metadata cakupan dari API | 2 | ⬜ |
| Laporan | `docs/report/m2/01-p1.md` | 3 | 🟡 draf sebelum demo k3s |

## Problem 4 — Feed Satelit

| Kriteria | Demo / bukti | Owner | Status |
|---|---|---|---|
| 1. WILDFIRE lintas satelit | `TestInclusiveResumePreservesOffsetsAndRetiresOriginalHazard`, `p4-practice-dev.log` | 3 | 🟡 fixture dan sampel practice |
| 2. Waktu akuisisi, keterlambatan, singleton | `satellite_checkpoint` dan `/internal/satellite/status` | 3 | 🟡 implementasi |
| 3. Update/retract menjaga ID kebakaran | `TestInclusiveResumePreservesOffsetsAndRetiresOriginalHazard` | 3 | ✅ fixture |
| 4. 60×, catch-up ≤60 s, p95 | `scripts/demo/m2-p4.sh` dengan akhir burst, `p4-practice-dev.log` | 3 | 🟡 p95 replay 597 ms di kind, target k3s tertunda |
| 5. Restart selama pemrosesan | `TestPendingChangesResumeAfterPartialApply` | 3 | ✅ fixture |
| 6. Perbandingan hasil referensi | Audit API `events` dan `checksum?source=SATELIT` | 3 | ⬜ menunggu referensi |
| 7. Live feed selama demo | `p4-practice-dev.log` membuktikan wire contract | 3 | 🟡 rekaman cluster tertunda |
| 8. Resume tanpa gap atau duplikasi | SSE `from` inklusif, tes reconnect dan replay | 3 | ✅ fixture |
| Laporan | `docs/report/m2/04-p4.md` | 3 | 🟡 draf sebelum demo k3s |

## Problem 5 dan bonus

| Bagian | Owner | Status |
|---|---|---|
| P5 Sensor Pantai, SLO, game day | 2 | ⬜ |
| Bonus host fisik berbeda / AI-native / HPA | 1 / 3 / 2 | ⬜ opsional |
