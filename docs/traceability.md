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

Semua bukti M2 diambil pada 22 Nov 2026 di cluster demo k3s (2 VM VirtualBox, README §M2.1) dengan storage sharded,
kecuali yang ditandai lain. ✅ = lulus di cluster demo, 🟡 = sebagian (alasan di kolom Status), ❌ = belum tercapai,
➡️ = membutuhkan bahan asisten yang baru diberikan setelah tenggat (ditunjukkan di video).

| Ketentuan | Demo / cara memeriksa | Bukti | Kode | Owner | Status |
|---|---|---|---|---|---|
| Semua komponen di Kubernetes, satu perintah deploy | `make k8s-up` | output `scripts/k8s/up.sh` | `deploy/`, `overlays/sharded/`, `scripts/k8s/*` | 1 | ✅ |
| Cluster ≥ 2 node kernel terpisah; deklarasi topologi | README §M2.1; `bash scripts/k8s/topology.sh` | `docs/evidence/m2/topology.log` | `scripts/k8s/vbox-cluster.sh` | 1 | ✅ |
| `resources.requests/limits` sesuai Lampiran B, label `role` | `kubectl get pods -L role`; manifest | `deploy/k8s/*.yaml`, `overlays/sharded/postgresql.yaml` | idem | 1 | ✅ |
| Secret per komponen, ConfigMap, `.env.example` | `make demo-m2-isolation` langkah 4 | `docs/evidence/m2/isolation-latest.log` | `scripts/k8s/secrets.sh` | 1 | ✅ |
| "Satu storage satu pemilik" ditegakkan platform | `make demo-m2-isolation` | idem (hanya Aggregator ke ketiga shard; hanya klien Kafka ke broker) | `deploy/k8s/networkpolicy.yaml`, `overlays/sharded/postgresql.yaml` | 1 | ✅ |
| Data `role=store` di disk node (local PV) | `kubectl get pv` (StorageClass `local-path`) | `docs/evidence/m2/p1-recovery-observed.log` (direktori PV di node-b) | `overlays/sharded/postgresql.yaml` | 1 / 3 | ✅ |
| Tester Agent terpasang tanpa modifikasi, tidak dihalangi | `make k8s-up`; `bash scripts/k8s/practice.sh` | `docs/evidence/m2/tester-list.log`, `tester-*.log` | `scripts/k8s/common.sh` (manifest dipin) | 1 | ✅ |
| Observabilitas (dashboard metrik, trace, log `cid`) | `make demo-m2-observability`; Grafana `:30030`, Jaeger `:30686` | `docs/evidence/m2/observability-latest.log` | `pkg/obs`, `deploy/k8s/observability.yaml`, `deploy/grafana/` | 2 | 🟡 lulus di kind (`observability-kind.log`); dashboard P1/P4 baru dan Grafana berjalan di k3s, demo otomatis tidak diulang karena waktu |
| Audit API `unprocessed` | `make demo-m2-p3` langkah 3 | `docs/evidence/m2/p3-latest.log` | `aggregator/internal/api/ops.go` | 1 | ✅ |
| Audit API `events`, `checksum`, `location` | `make demo-m2-analytics`, `m2-p1.sh`, `m2-p1-recovery.sh` | `p1-analytics-latest.log`, `p1-snapshot.log`, `p1-recovery-observed.log` | `aggregator/internal/api/audit.go`, `adapters/sharded/location.go` | 3 | ✅ dengan token pengembangan; token asisten ➡️ |

## Problem 1 — Yang Pulang Membawa Laptopnya (Owner: 3; dashboard: 2; integrasi 2 node: 1)

| Kriteria | Demo | Bukti | Kode | Status |
|---|---|---|---|---|
| 1. Data dipartisi, tidak ada unit memuat semua, primary di > 1 node, sebaran dilaporkan | `make demo-m2-p1-load` | `p1-load-latest.log`, Grafana baris P1 | `adapters/sharded`, `overlays/sharded`, `services/canonical-shard/balance.py` | ✅ (katalog sintetis; katalog asisten ➡️) |
| 2. Ketidakmerataan alami, beban per node saat muat dan lonjakan | idem | idem §2–3 dan perbandingan alternatif §5 | `tools/catalog/catalog.py` | ✅ |
| 3. Baca lintas lokasi, 50 terbaru, pagination, urutan benar | `bash scripts/demo/m2-p1.sh snapshot` | `p1-snapshot.log` | `adapters/sharded/sharded.go` (`List`) | ✅ |
| 4. Worker mati + data dihapus: tidak ada yang hilang; jawaban lengkap/parsial, bukan 500 | Tester `wipe-node` | `p1-recovery-observed.log` (failover, jumlah per sumber), `p1-analytics-latest.log` (unit hilang → `partial`) | Patroni, `adapters/sharded` | 🟡 failover dan jawaban parsial terbukti; pembandingan per record selama skenario tidak terekam |
| 5. Node kembali dengan disk kosong ikut menanggung data dan beban | idem | `p1-recovery-observed.log`: rebuild otomatis, self-heal replica rusak, switchover primary shard-1 kembali ke node-b | `balance.py` | ✅ |
| 6. Kembali tahan kehilangan satu node berikutnya | idem | setiap shard kembali punya replica sinkron di node lain | idem | 🟡 redundansi pulih; kehilangan kedua tidak diulang |
| 7. Dashboard analitik = Audit API; tanda tidak lengkap | `make demo-m2-analytics` | `p1-analytics-latest.log` | `client-api` dashboard, `api/analytics.go` | ✅ |
| Laporan | – | `docs/report/m2/01-p1.md` | – | ✅ |

## Problem 2 — Dua Nakhoda, Satu Kapal (Owner: 1; load test: 2)

| Kriteria | Demo | Bukti | Kode | Status |
|---|---|---|---|---|
| 1. ≥ 3 instance di node berbeda, satu titik masuk yang hanya ke instance sehat | `make demo-m2-p2` | `p2-latest.log` | `deploy/k8s/aggregator.yaml` | ✅ |
| 2. Tiap sumber diambil tepat sekali, tanpa duplikat, upstream tidak menerima permintaan berlipat | idem | idem (log mock per `caller`) | `internal/leader`, `poller` | ✅ |
| 3. Pengambil data mati → dilanjutkan ≤ 60 s tanpa celah/duplikat | idem | idem (31 s, 0 hilang, 0 ganda) | `leader/elector.go` | ✅ |
| 4. Jam bergeser → tidak ada dua instance menyimpan pada periode sama | `practice clock-skew` + `m2-single-writer.sh` | `tester-clock-skew.log` (14 Nov, store tunggal) | `leader`, fencing | ✅ (bukti 14 Nov; fencing per shard diuji di `repotest`) |
| 5. Node leader freeze lalu bangun → tidak menyimpan apa pun | `bash scripts/demo/m2-freeze.sh` | `freeze.log` (22 Nov, sharded) | `ports/fence.go`, `leader.Confirm` | ✅ |
| 6. Load test ≥ 10.000 VU ≥ 120 s, skala 3 → 5 → 2, error < 1 % | `scripts/loadtest/m2-scale.sh` | `p2-loadtest-scale-10k-attempt.log` | `scripts/loadtest/*` | ❌ cluster satu laptop jenuh (laporan P2 §6) |
| 7. Throughput 3 instance ≥ 2× 1 instance | `scripts/loadtest/m2-throughput.sh` | – | idem | ❌ tidak diukur (alasan sama) |
| 8. Ganti versi saat beban tanpa permintaan gagal | `make demo-m2-p2` (`NEW_TAG`) | `p2-latest.log` (22c2a80 → b500253, 204 permintaan, 0 gagal) | `aggregator.yaml`, `cmd/aggregator/main.go` | ✅ |
| Laporan | – | `docs/report/m2/02-p2.md` | – | ✅ |

## Problem 3 — Surat dari Tetangga (Owner: 1; kriteria 6: 2)

| Kriteria | Demo | Bukti | Kode | Status |
|---|---|---|---|---|
| Mock: mode `/admin/schema-version` tidak kompatibel | `make demo-m2-p3` | `p3-sharded-20261122.log`, `p3-latest.log` | `pvmbg-mock/internal/pvmbg` | ✅ |
| 1. Deklarasi perubahan yang diterima / tidak; perilaku konsisten | idem | `p3-sharded-20261122.log` (22 Nov, sharded) | `mapping/raw.go` | ✅ |
| 2. Pesan gagal tersimpan utuh; retryable vs parked | idem | `p3-sharded-20261122.log` (22 Nov, sharded) | `internal/dlq` | ✅ |
| 3. Pesan rusak tidak membuat crash | idem | `p3-sharded-20261122.log` (22 Nov, sharded) | `sources/split.go` | ✅ |
| 4. Diproses ulang: pulih semua, tanpa duplikat, urut per `source_ref_id` | `make demo-m2-p3` langkah 5 | `p3-latest.log` (k3s 14 Nov) | `mapping/rules.go`, `dlq.Worker` | ✅ (14 Nov); ulangan 22 Nov terhenti di rolling update karena NetworkPolicy node-a, lihat laporan P3 §6 |
| 5. Pesan gagal hanya untuk BNPB Internal Ops | idem | `p3-sharded-20261122.log` (22 Nov, sharded) | `api/ops.go` | ✅ |
| 6. `POST /field-reports` 202 cepat; tidak hilang, tidak ganda | `make demo-m2-field-reports` | `p3-field-reports-kind.log` (kind) | `client-api/internal/reports`, `services/field-reports` | 🟡 lulus di kind; tidak diulang di k3s karena waktu |
| Laporan | – | `docs/report/m2/03-p3.md` | – | ✅ |

## Problem 4 — Atap Seng di Siang Bolong (Owner: 3; metrik dan ketahanan stream: 1)

| Kriteria | Demo / bukti | Status |
|---|---|---|
| 1. WILDFIRE lintas satelit, satu kebakaran satu HazardEvent | `TestInclusiveResumePreservesOffsetsAndRetiresOriginalHazard`; feed practice live di cluster (Audit SATELIT 400 event) | ✅ |
| 2. Waktu deteksi, batas keterlambatan, deteksi terlambat dilaporkan | `tools/satellite/probe.py` → `p4-lateness-probe.log`; metrik `satellite_late_detections` | ✅ |
| 3. Update/retract memperbarui event yang sama | `TestInclusiveResumePreservesOffsetsAndRetiresOriginalHazard` | ✅ (uji) |
| 4. 60×, tidak hilang, mutakhir ≤ 60 s, p95, ketertinggalan di dashboard | practice berjalan 60× (`speed=60`); panel Grafana P4 (lag, p95) | 🟡 berjalan live; angka catch-up/p95 dari sesi ujian ➡️ |
| 5. Pemroses mati di tengah pengenalan → hasil sama | `TestPendingChangesResumeAfterPartialApply`; failover leader P2 selama feed berjalan | ✅ (uji) |
| 6. Perbandingan dengan referensi asisten | metode di laporan P4 §5 | ➡️ |
| 7. Feed live beberapa menit saat demo | consumer di leader membaca practice live (SATELIT 8 → 400 event selama gladi) | ✅ |
| 8. Putus dan menyambung tanpa hilang/ganda | resume inklusif dari offset; `TestBrokenEventsAreSkippedWithoutStallingTheFeed` | ✅ (uji) |
| Laporan | `docs/report/m2/04-p4.md` | ✅ |

## Problem 5 — Pelampung yang Berbohong (Owner: 2)

| Kriteria | Demo / test | Bukti | Status |
|---|---|---|---|
| 1. 0 event kritis hilang, p95 ≤ 5 s | `make demo-m2-p5` | `p5-k3s-20261122.log` | 🟡 compose 12,8/992,8 ms dan kind 3/3 terkirim (`p5-compose-local.log`); di k3s 22 Nov langkah ini berhenti karena topik Kafka kosong setelah Kafka crash-loop (`p5-k3s-20261122.log`) |
| 2. Pelanggaran kontrak disisihkan | idem | idem | ✅ k3s 22 Nov (`p5-k3s-20261122.log`: 59+ observasi `quality "suspect"` disisihkan, tidak ada yang menjadi HazardEvent) |
| 3. Sumber lain tetap latensi M1 | idem | idem | 🟡 compose (BMKG-only p95 23,7 ms saat sensor padam); tidak diulang di k3s |
| 4. Tidak melonjak, jawab cepat, pulih sendiri | idem | idem | 🟡 compose (10 permintaan/45 s, p95 11 ms, pulih 4 s) + uji unit; tidak diulang di k3s |
| 5. Lapangan dilayani, Media ditolak terkontrol | idem langkah 5 | idem | 🟡 compose k6 300 VU (Lapangan 0 ditolak, Media 26.730×429) + uji unit |
| 6. Kombinasi kegagalan tanpa intervensi (game day) | `make demo-m2-gameday` | – | ➡️ skenario ujian dijalankan saat video; skrip siap |
| 7. Jelaskan kegagalan dari dashboard/metrik/trace | Grafana baris P5; Jaeger | `observability-latest.log` | ✅ |
| 8. SLO + error budget | `make demo-m2-p5` langkah 6; `make demo-m2-gameday` | `p5-k3s-20261122.log` | 🟡 aturan Prometheus dan panel siap; angka game day ➡️ video |
| Laporan | – | `docs/report/m2/05-p5.md` | ✅ |

## Bonus

| Bagian | Status |
|---|---|
| Host fisik berbeda / AI-native | tidak diambil |
| Kapasitas yang menyesuaikan diri (HPA) | 🟡 manifest dan demo siap (`deploy/hpa/hpa.yaml`, `make demo-m2-hpa`); tidak dijalankan di cluster demo karena kapasitas (lihat P2 kriteria 6) |
