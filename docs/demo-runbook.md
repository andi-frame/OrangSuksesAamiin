# Runbook demo (bagian Foundation / Aggregator / P1)

Demo sinkron akan dinilai 10 poin, ditambah bukti implementasi per problem. Urutan di bawah dirancang agar semua
kriteria yang menjadi bagian Orang 1 tampil dalam ±6 menit, dengan kegagalan yang **benar-benar dipicu**.

## Sebelum mulai (sekali)
```bash
make reset          # kondisi pristine: container dan volume baru, semua service sehat
```
Buka dua terminal: satu untuk perintah, satu untuk `docker compose logs -f aggregator` (log JSON ber-`cid`).

## Alur

| # | Perintah | Yang ditunjukkan | Kalimat kunci |
|---|---|---|---|
| 1 | `docker compose ps` lalu `make demo-isolation` | 1 perintah menyalakan semua; tiap service container sendiri; DB dan Kafka tidak terjangkau service lain | "Storage dimiliki satu service; jaringan yang membuktikannya, bukan janji" |
| 2 | `make demo-mock-auth` | 401/403 silang BMKG↔PVMBG; token PVMBG berstruktur sendiri; setiap container hanya memegang rahasianya | "Kompromi satu domain tidak berdampak ke domain lain" |
| 3 | `make demo-p1` | sistem naik → skema lama benar (oracle independen) → skema berubah tanpa restart → tetap valid; field tak dikenal → `attributes`; enum baru → karantina | "Sumber otonom; BNPB harus menerima perubahan aditif tanpa redeploy" |
| 4 | `make demo-p2-aggregator` | PVMBG 3 s tidak mengganggu BMKG; outage `error`/`hang`; data tetap tersaji + `stale_since`; pulih tanpa restart | "Polling terpisah dari layanan: tidak ada head-of-line blocking" |
| 5 | `docker compose up -d --build --no-deps aggregator` | rebuild satu service, yang lain tak tersentuh | "Deploy independen" |

Tips: skrip dapat diulang; untuk hasil paling bersih ulangi `make reset` di antara sesi. Jika waktu terbatas, jalankan
3 (P1) dan 4 (P2 ingest) saja, karena keduanya menyentuh kriteria berbobot terbesar milik Orang 1.

## Kemungkinan pertanyaan penilai
- *Mengapa polling dan bukan push?* Spesifikasi: mock pasif. Polling per sumber dengan `since` + overlap + watermark persisten.
- *Bagaimana jika record yang sama datang dua kali?* `hazard_id` deterministik + perbandingan isi: tidak menaikkan versi.
- *Bagaimana warning tsunami yang terlambat?* Dikorelasikan lewat `related_event_id`, severity dihitung ulang, versi baru
  (`updated`); consumer memakai `hazard_id:version` untuk idempotensi.
- *Apa yang terjadi jika Kafka mati?* Ingest berjalan; event menunggu di outbox (transaksional) dan terkirim berurutan setelahnya.
- *Mengapa tidak schema registry?* PVMBG mengubah skema tanpa memberi tahu siapa pun; registry butuh kerja sama sumber.
