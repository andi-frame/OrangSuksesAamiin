# Lampiran

## A. Perintah pemeriksaan penuh (dari clone bersih)

```bash
bash scripts/k8s/vbox-cluster.sh up          # 2 VM, k3s; konteks kubectl argus-k3s
cp .env.example .env                         # isi AUDIT_TOKEN, SATELLITE_FEED_TOKEN, CANONICAL_DB_*_PASSWORD
make lint && make test-race                  # semua modul Go
make k8s-up                                  # satu perintah: image, Secret, manifest, Tester Agent
make demo-m2-isolation demo-m2-observability
make demo-m2-p1-load demo-m2-analytics demo-m2-p1-recovery          # P1
make demo-m2-p2 && bash scripts/demo/m2-freeze.sh                   # P2 (FREEZE_CMD/THAW_CMD, README §M2.4)
make loadtest-m2-scale loadtest-m2-throughput                       # P2 kriteria 6-7
make demo-m2-p3 demo-m2-field-reports                               # P3
bash scripts/demo/m2-p4.sh                                          # P4
make demo-m2-p5 demo-m2-gameday                                     # P5
make demo-m2-hpa                                                    # bonus HPA
python3 tools/report/build.py                                       # laporan ini
```

Setiap demo memeriksa hasilnya sendiri (PASS/FAIL) dan menyimpan transkrip di `docs/evidence/m2/`. Matriks lengkap
kriteria → perintah → bukti → kode ada di `docs/traceability.md`.

## B. Form Penggunaan AI

Kelompok memakai asisten AI (Claude, lewat Claude Code dan oh-my-pi) sebagai alat bantu pemrograman dan penulisan.
Penggunaannya meliputi:
- menyusun dan merevisi kode;
- menjalankan perintah pengujian dan demo di cluster kelompok;
- mencari penyebab kegagalan;
- menyusun draf dokumen.

Setiap keputusan desain, setiap angka di laporan, dan setiap klaim "lulus" dicek terhadap keluaran program yang
sebenarnya (transkrip di `docs/evidence/`). Surat pernyataan penggunaan AI yang ditandatangani ketiga anggota
dilampirkan pada halaman berikut (`IF4031_Surat-Pernyataan-AI_M2_OrangSuksesAamiin.pdf`).
