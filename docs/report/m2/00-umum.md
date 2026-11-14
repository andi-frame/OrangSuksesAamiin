# Bagian umum M2 (Orang 1: deployment, perubahan dari M1, topologi)

> Draf. Bagian lain bagian umum (sampul, kontribusi, teknologi, asumsi, Form Penggunaan AI) mengikuti format M1.

## Diagram deployment

```mermaid
flowchart TB
  subgraph A["node-a (VM) — control plane + worker"]
    API[kube-apiserver + etcd<br/>Lease aggregator-ingest]
    AG1[aggregator]
    PG[(postgres-canonical<br/>local-path PV)]
    TA1[tester-agent]
  end
  subgraph B["node-b (VM) — worker"]
    AG2[aggregator]
    CA1[client-api]
    KF[(kafka<br/>local-path PV)]
    TA2[tester-agent]
  end
  subgraph C["node-c (VM) — worker"]
    AG3[aggregator]
    CA2[client-api]
    MOCK[bmkg-mock / pvmbg-mock]
    CON[consumer-dashboard / notifier]
    TA3[tester-agent]
  end
  CLIENT([Media / Tim Lapangan / Pusat]) -->|NodePort 30080| CA1 & CA2
  CA1 & CA2 -->|Service aggregator:8083| AG1 & AG2 & AG3
  AG1 & AG2 & AG3 -->|baca, tulis ber-fencing| PG
  AG1 & AG2 & AG3 -.->|Lease: get/renew| API
  AG2 -->|"poll (hanya leader)"| MOCK
  AG2 -->|outbox relay (hanya leader)| KF
  KF --> CON
  SAT([Feed Satelit asisten]) -.->|SSE, P4| A
```

Penempatan nyata bergantung pada scheduler; yang dijamin manifest: Aggregator tersebar maksimal satu selisih per node
(`topologySpreadConstraints`), data `role=store` dan `role=broker` di disk node tempat pod berjalan, satu Tester Agent per
node. `make k8s-status` menunjukkan penempatan aktual. Jalur data antar-node: Service (kube-proxy) dan DNS cluster; Canonical
Store dan Kafka hanya menerima koneksi dari pod yang diizinkan NetworkPolicy.

## Perubahan dari M1

| Keputusan M1 | M2 | Alasan |
|---|---|---|
| `docker compose`, satu host | **Diubah**: Kubernetes (k3s), manifest `deploy/`, satu perintah `make k8s-up`; compose tetap untuk pengembangan | Ketentuan M2; banyak node, rolling update, self-healing |
| Isolasi storage lewat jaringan Docker internal | **Diubah**: NetworkPolicy + Secret per komponen | Sama tujuannya, ditegakkan platform di cluster |
| Satu Aggregator yang polling | **Diubah**: ≥ 3 instance; satu leader (Lease) yang ingest, semua melayani baca; fencing token di store | P2: tanpa ini tiap kejadian diambil tiga kali |
| Status sumber di memori Aggregator | **Diubah**: disimpan leader di store, dibaca semua instance | Instance yang tidak polling harus memberi freshness yang sama |
| Karantina di memori, `GET /internal/quarantine` dengan service key | **Diganti**: dead-letter store persisten (raw bytes, retrying/parked), hanya untuk Internal Ops | P3: pesan gagal harus tersimpan utuh dan dapat diproses ulang; hanya Ops yang boleh melihat |
| Parser JSON standar untuk seluruh respons | **Diganti**: pemecah array streaming per elemen dengan batas ukuran/kedalaman | P3: satu pesan rusak tidak boleh menjatuhkan batch atau proses |
| Kebijakan skema additive-only, perubahan tak didukung dikarantina | **Dipertahankan**, ditambah aturan penyesuaian deklaratif (`rename`, `coerce`) | Tetap tidak menebak; manusia memutuskan, sistem memproses ulang |
| Outbox transaksional + relay, Kafka at-least-once | **Dipertahankan**; relay hanya di leader; outbox mencatat `writer_term` | Audit siapa menulis apa per term |
| Circuit breaker dan polling per sumber | **Dipertahankan** (dijalankan per masa kepemimpinan) | |
| Admin mock wajib `X-Admin-Token` | **Diubah di k8s**: boleh tanpa token (`*_ADMIN_OPEN`), token salah tetap ditolak; Service mock hanya ClusterIP | Tester Agent memanggil `/admin/*` tanpa kredensial kelompok |
| Kredensial independen per instansi | **Dipertahankan**; ditambah `AGGREGATOR_OPS_TOKEN`, `AUDIT_TOKEN`, `SATELLITE_FEED_TOKEN` | |

## Deklarasi topologi

Lihat README §M2.1 (diisi dari cluster demo dengan `bash scripts/k8s/topology.sh` ditambah host fisik dan spesifikasinya).
