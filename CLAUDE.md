# CLAUDE.md — konvensi proyek (IF4031 M1: BNPB gateway)

Konteks: tugas besar Arsitektur Aplikasi Terdistribusi. Nilai ditentukan oleh **bukti yang dapat didemokan** (laporan 9 +
implementasi 9 poin per problem). Baca `README.md`, `docs/contracts.md`, `docs/traceability.md` sebelum mengubah apa pun.

## Aturan yang tidak boleh dilanggar (dari spesifikasi)
- Komunikasi antar-service hanya lewat jaringan. **Tidak boleh** berbagi library business-logic; `pkg/obs` hanya utilitas teknis.
- Satu storage satu pemilik. Canonical Store hanya diakses **Aggregator**; komponen lain lewat `/internal/*`.
- Kredensial BMKG, PVMBG, internal, dan client independen. Tidak ada kredensial di kode atau repo; baca dari env;
  `.env.example` berisi nilai contoh. Service berhenti dengan pesan jelas bila kredensial kosong (`obs.MustEnv`).
- Setiap service: Dockerfile sendiri, `/health`, log JSON dengan `cid` yang diteruskan, latensi panggilan keluar dicatat.
- Kontrak mock (endpoint, skema, delay, port) tidak boleh diubah; hanya boleh **ditambah**.

## Konvensi kode
- Go 1.25, `net/http` mux, `log/slog` lewat `pkg/obs` (`logger.InfoContext(ctx, ...)` agar `cid` ikut). Komentar dan
  identifier dalam bahasa Inggris; dokumen untuk kelompok dalam bahasa Indonesia.
- Satu `go.mod` per service dengan `replace` ke `pkg/obs`; Dockerfile dibangun dengan konteks root repo.
- Perubahan kontrak = ubah `docs/contracts.md` **dan** `contracts/` (JSON Schema/OpenAPI); test kontrak harus tetap hijau.
- Adapter backend Aggregator didaftarkan lewat `internal/adapters` (lihat `docs/adapters.md`); `ports.Repository` wajib
  lulus `internal/ports/repotest`.
- Field tak dikenal dari sumber → `attributes` (jangan buang); perubahan tak didukung → karantina, bukan tebakan.

## Cara memeriksa pekerjaan
`make lint && make test-race` lalu `make reset && make demo-p1 demo-p2-aggregator demo-mock-auth demo-isolation`.
Sebelum tag `milestone-1`: `make secrets`, README bisa diikuti dari clone bersih, `docs/traceability.md` tidak punya baris kosong.
