# Menyiapkan cluster demo M2 (k3s, ≥ 2 node dengan kernel terpisah)

Spesifikasi M2: minimal 2 node dengan kernel terpisah (VM atau mesin Linux), Node A = control plane + worker,
Node B = worker; skenario yang mematikan node hanya dikenakan pada worker. Cluster berbasis container dengan kernel
bersama (kind, k3d, minikube multi-node) **hanya untuk pengembangan** — `scripts/k8s/topology.sh` memperingatkan bila
node memakai kernel yang sama.

**Rekomendasi: 3 VM** (A control plane + worker, B dan C worker). Dengan 3 node, Aggregator tersebar ke 3 node dan
store terdistribusi (P1) masih punya mayoritas ketika satu worker mati. 2 node adalah batas minimum spesifikasi.

## Menjalankan demo dari host Linux

Pada Linux x86-64, akses baca/tulis ke `/dev/kvm` dan Docker diperlukan. Skrip
di bawah menjalankan QEMU dalam container dan menjalankan K3s di tiga VM Ubuntu,
masing-masing dengan kernel dan disk sendiri. QEMU mengembalikan halaman memori
guest yang bebas ke host melalui virtio-balloon. State VM, kunci SSH, dan
kubeconfig disimpan di `.argus-k3s-linux/` yang diabaikan Git.

```bash
bash scripts/k8s/qemu-cluster.sh up
bash scripts/k8s/qemu-cluster.sh deploy
bash scripts/k8s/qemu-cluster.sh status
```

Alokasi RAM awal 2304 MiB untuk node-a dan 1792 MiB untuk tiap worker. Untuk host
dengan RAM lebih banyak, atur `ARGUS_QEMU_SERVER_RAM` dan `ARGUS_QEMU_WORKER_RAM`
saat membuat VM. CPU virtual masing-masing berjumlah dua. Alokasi melekat pada
container VM saat dibuat. Endpoint host Linux di `127.0.0.1` menggunakan port
30080 untuk Client API, 30083 untuk Aggregator, dan 30084 untuk Auth. Kubernetes
memakai port 26443 dan konteks `argus-k3s-linux`.

```bash
bash scripts/k8s/qemu-cluster.sh kubectl get nodes -o wide
bash scripts/k8s/qemu-cluster.sh exec bash scripts/k8s/practice.sh
bash scripts/k8s/qemu-cluster.sh pause node-b
bash scripts/k8s/qemu-cluster.sh resume node-b
bash scripts/k8s/qemu-cluster.sh stop
```

`stop` mematikan VM melalui ACPI dengan disk tetap tersimpan. Jalankan `up`
lagi untuk melanjutkan dari disk yang sama.

Host Linux yang diuji memakai Intel Core i5-12500H dengan 16 CPU logis dan
14,8 GiB RAM. K3s v1.37.2+k3s1 berjalan pada tiga VM Ubuntu 24.04.5 dengan
dua vCPU per VM. Node-a bertindak sebagai control plane dan worker.
Output topologi tersedia di [topology-linux.log](evidence/m2/topology-linux.log).

## 1. VM

**Otomatis di satu laptop (VirtualBox, tanpa hak admin):** `bash scripts/k8s/vbox-cluster.sh up` membuat VM Ubuntu 24.04
(`VBOX_NODES="node-a node-b"`, default 3 GiB RAM dan 2 vCPU per VM), memasang k3s lewat cloud-init, lalu menambah konteks
kubectl `argus-k3s`. Perintah deploy dan perintah freeze untuk node-b dicetak di akhir. Butuh RAM bebas sekitar 7 GB dan
disk sekitar 15 GB. `bash scripts/k8s/vbox-cluster.sh delete` meminta konfirmasi sebelum menghapus VM.

Contoh dengan VirtualBox (Windows 11 Home tidak punya Hyper-V) atau Multipass. Setiap VM: Ubuntu Server 24.04,
2 vCPU, 4 GiB RAM, disk 20 GiB, **jaringan bridged atau host-only** sehingga VM saling menjangkau dengan IP tetap.

```bash
# Multipass (backend VirtualBox di Windows Home)
multipass launch 24.04 --name node-a --cpus 2 --memory 4G --disk 20G
multipass launch 24.04 --name node-b --cpus 2 --memory 4G --disk 20G
multipass launch 24.04 --name node-c --cpus 2 --memory 4G --disk 20G
multipass list            # catat IP tiap node
```

Matikan swap dan pastikan jam disinkronkan (`timedatectl`), kecuali saat skenario clock skew sengaja dijalankan.

## 2. k3s

```bash
# node-a: control plane + worker. Traefik dan servicelb tidak dipakai (titik masuk = NodePort).
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --disable traefik --disable servicelb --node-name node-a --write-kubeconfig-mode 644" sh -
sudo cat /var/lib/rancher/k3s/server/node-token            # TOKEN

# node-b, node-c: worker
curl -sfL https://get.k3s.io | K3S_URL=https://<IP-node-a>:6443 K3S_TOKEN=<TOKEN> INSTALL_K3S_EXEC="agent --node-name node-b" sh -
```

k3s sudah membawa StorageClass `local-path` (data PVC di disk node itu sendiri, sesuai ketentuan `role=store`) dan
penegak NetworkPolicy (kube-router). Kubeconfig untuk laptop: salin `/etc/rancher/k3s/k3s.yaml` dari node-a, ganti
`127.0.0.1` dengan IP node-a, simpan sebagai konteks baru (`KUBE_CONTEXT`).

## 3. Image

Tanpa registry, `make k8s-up` membangun image di laptop lalu mengirimnya ke setiap node:

```bash
export KUBE_CONTEXT=<konteks-k3s> K8S_SSH_NODES="ubuntu@<IP-a> ubuntu@<IP-b> ubuntu@<IP-c>"
make k8s-up          # = bash scripts/k8s/up.sh
```

(`docker save | ssh node 'sudo k3s ctr images import -'`; butuh SSH tanpa password dan sudo tanpa password.) Dengan
registry: `IMAGE_REGISTRY=ghcr.io/<akun>` (image di-push dan ditarik node).

## 4. Pemeriksaan

```bash
bash scripts/k8s/topology.sh      # node, kernel, boot id: harus "N distinct kernels on N nodes"
make k8s-status                   # pod per node, leader, endpoint Aggregator
kubectl -n tester-agent get pods -o wide   # satu Tester Agent per node
```

## 5. Membekukan node (P2 kriteria 5)

```bash
# VirtualBox
VB='"/c/Program Files/Oracle/VirtualBox/VBoxManage.exe"'
FREEZE_NODE=node-b FREEZE_CMD="$VB controlvm node-b pause" THAW_CMD="$VB controlvm node-b resume" bash scripts/demo/m2-freeze.sh
# Multipass
FREEZE_CMD='multipass suspend node-b' THAW_CMD='multipass start node-b' bash scripts/demo/m2-freeze.sh
```

Skrip memindahkan kepemimpinan ke pod di worker lebih dulu, lalu memeriksa bahwa pod yang bangun tidak menyimpan apa pun.
Skrip selalu mencairkan node saat keluar, termasuk bila gagal di tengah jalan. Selama node-b dibekukan, API k3s di laptop
ini sempat lambat menjawab (VirtualBox di atas Hyper-V), sehingga pengambilalihan bisa lebih lama dari durasi lease 15 s.
Karena itu skrip menunggu sampai ada pemegang lease baru, paling lama 40 + 150 s. Jam VM yang dibekukan tertinggal
selama durasi pause. Sinkronkan lagi dengan `ssh -F ~/.ssh/argus-k3s.config node-b sudo systemctl restart systemd-timesyncd`.

## 6. Catatan cluster VirtualBox di satu laptop

- **RAM.** Dua VM 3 GiB ditambah Docker Desktop membuat host kehabisan RAM: node-b sempat *NotReady* saat rolling
  update di bawah beban. Setelah image dimuat ke node (`scripts/k8s/images.sh`), hentikan Docker Desktop selama demo
  (`docker desktop stop`; image, container, dan volume tetap ada). Demo cukup memakai `kubectl`. Bila `jq` tidak ada di
  host, letakkan `jq.exe` di `PATH`; `k8s-lib.sh` membuang CR dari keluarannya.
- **iptables untuk skenario `partition`.** Tester Agent mencari `iptables` di `/host/usr/sbin`. Symlink bawaan Ubuntu
  (`/etc/alternatives/...`, absolut) tidak bisa di-resolve dari mount itu. Cloud-init `vbox-cluster.sh` sudah mengarahkan
  `/usr/sbin/iptables` langsung ke `xtables-nft-multi`.
- **Jangan mengedit `vbox-cluster.sh` selagi skrip itu berjalan.** Bash membaca skrip sedikit demi sedikit, jadi
  pembacaannya bisa melompat ke cabang lain. `delete` kini hanya berjalan dari terminal interaktif dengan jawaban `yes`.
