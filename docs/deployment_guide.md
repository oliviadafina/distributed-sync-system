# 🚀 Deployment Guide — Distributed Synchronization System

Panduan lengkap untuk menjalankan, mengkonfigurasi, dan melakukan troubleshooting sistem.

---

## 📋 Daftar Isi

- [Prasyarat](#-prasyarat)
- [Cara Menjalankan (Docker)](#-cara-menjalankan-docker---rekomendasi)
- [Cara Menjalankan (Lokal tanpa Docker)](#-cara-menjalankan-lokal-tanpa-docker)
- [Verifikasi Sistem](#-verifikasi-sistem)
- [Konfigurasi Environment](#-konfigurasi-environment)
- [Menjalankan Unit Tests](#-menjalankan-unit-tests)
- [Menjalankan Load Test (Locust)](#-menjalankan-load-test-locust)
- [Troubleshooting](#-troubleshooting)

---

## ✅ Prasyarat

| Software | Versi Minimum | Cek Instalasi |
|----------|---------------|---------------|
| Docker Desktop | 20.0+ | `docker --version` |
| Docker Compose | 2.0+ (termasuk di Docker Desktop) | `docker compose version` |
| Python | 3.8+ (hanya untuk test lokal) | `python --version` |
| Git | — | `git --version` |

> **Windows:** Pastikan Docker Desktop sudah berjalan (icon di system tray) sebelum menjalankan perintah apapun.

---

## 🐳 Cara Menjalankan (Docker) — Rekomendasi

### Langkah 1: Clone Repository

```bash
git clone https://github.com/oliviadafina/distributed-sync-system.git
cd distributed-sync-system
```

### Langkah 2: Build & Jalankan Semua Container

```bash
docker-compose up --build
```

Perintah ini akan:
1. Build Docker image untuk node server (Python + FastAPI)
2. Pull image Redis 7 (Alpine)
3. Menjalankan **3 Node server** (port 8001, 8002, 8003) + **1 Redis** (port 6379)
4. Menunggu Redis healthy sebelum node dinyalakan (health check)

**Output yang diharapkan (sekitar 15-30 detik setelah start):**
```
node1  | INFO:     Uvicorn running on http://0.0.0.0:8000
node2  | INFO:     Uvicorn running on http://0.0.0.0:8000
node3  | INFO:     Uvicorn running on http://0.0.0.0:8000
node1  | [INFO] Raft-node_1 - Starting election for term 1
node3  | [INFO] Raft-node_3 - Node node_3 became LEADER for term 1
```

> Log Raft election adalah **normal** — sistem memilih satu Leader secara otomatis.

### Langkah 3: Verifikasi (lihat bagian [Verifikasi Sistem](#-verifikasi-sistem))

### Menghentikan Sistem

```bash
# Hentikan (container tetap ada, bisa di-restart)
docker-compose stop

# Hentikan & hapus container
docker-compose down

# Hentikan, hapus container, dan hapus data Redis
docker-compose down -v
```

### Restart Tanpa Rebuild

```bash
docker-compose up
```

### Rebuild Setelah Perubahan Kode

```bash
docker-compose up --build
```

---

## 🐍 Cara Menjalankan (Lokal tanpa Docker)

> Hanya untuk development/testing. Membutuhkan Redis yang sudah berjalan di lokal.

### Langkah 1: Install Redis Lokal

**Windows (via WSL atau Docker):**
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

**macOS:**
```bash
brew install redis && brew services start redis
```

### Langkah 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Langkah 3: Buat File `.env` untuk Setiap Node

**`.env.node1`:**
```env
NODE_ID=node_1
NODE_HOST=0.0.0.0
NODE_PORT=8001
PEERS=http://localhost:8002,http://localhost:8003
REDIS_HOST=localhost
REDIS_PORT=6379
ELECTION_TIMEOUT_MIN=1500
ELECTION_TIMEOUT_MAX=3000
HEARTBEAT_INTERVAL=500
```

**`.env.node2`** dan **`.env.node3`**: sama, sesuaikan `NODE_ID` dan `NODE_PORT`-nya.

### Langkah 4: Jalankan Setiap Node di Terminal Berbeda

```bash
# Terminal 1 — Node 1
set NODE_ID=node_1 && set NODE_PORT=8001 && set PEERS=http://localhost:8002,http://localhost:8003 && set REDIS_HOST=localhost && uvicorn src.nodes.base_node:app --host 0.0.0.0 --port 8001

# Terminal 2 — Node 2
set NODE_ID=node_2 && set NODE_PORT=8002 && set PEERS=http://localhost:8001,http://localhost:8003 && set REDIS_HOST=localhost && uvicorn src.nodes.base_node:app --host 0.0.0.0 --port 8002

# Terminal 3 — Node 3
set NODE_ID=node_3 && set NODE_PORT=8003 && set PEERS=http://localhost:8001,http://localhost:8002 && set REDIS_HOST=localhost && uvicorn src.nodes.base_node:app --host 0.0.0.0 --port 8003
```

---

## ✔️ Verifikasi Sistem

### 1. Cek Health Status

```bash
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

**Contoh response (node yang menjadi Leader):**
```json
{
  "status": "ok",
  "node_id": "node_3",
  "raft_state": "leader",
  "current_term": 1,
  "voted_for": "node_3",
  "commit_index": 0,
  "peers": ["http://node1:8000", "http://node2:8000"]
}
```

Cari node dengan `"raft_state": "leader"` — itulah node yang harus menerima Lock requests.

### 2. Buka Swagger UI

| Node | URL |
|------|-----|
| Node 1 | http://localhost:8001/docs |
| Node 2 | http://localhost:8002/docs |
| Node 3 | http://localhost:8003/docs |

### 3. Setup Autentikasi di Swagger

Klik tombol **Authorize** (ikon gembok hijau) → isi `admin-secret-key-123` → **Authorize** → **Close**.

Lakukan di ketiga tab!

---

## ⚙️ Konfigurasi Environment

Semua konfigurasi diatur via environment variables (di-load oleh Pydantic Settings):

| Variable | Default | Keterangan |
|----------|---------|------------|
| `NODE_ID` | `node_1` | Identifier unik untuk node ini |
| `NODE_HOST` | `0.0.0.0` | Bind address server |
| `NODE_PORT` | `8000` | Port internal (di dalam container) |
| `PEERS` | _(kosong)_ | URL peer dipisah koma, contoh: `http://node2:8000,http://node3:8000` |
| `REDIS_HOST` | `redis` | Hostname Redis (gunakan `localhost` jika tanpa Docker) |
| `REDIS_PORT` | `6379` | Port Redis |
| `ELECTION_TIMEOUT_MIN` | `1500` | Minimum timeout sebelum Raft election dimulai (ms) |
| `ELECTION_TIMEOUT_MAX` | `3000` | Maximum timeout sebelum Raft election dimulai (ms) |
| `HEARTBEAT_INTERVAL` | `500` | Interval heartbeat Leader ke Follower (ms) |

> **Tip:** Semakin kecil `ELECTION_TIMEOUT_MIN/MAX`, semakin cepat pemilihan Leader baru saat node mati. Namun terlalu kecil bisa menyebabkan split-vote.

---

## 🧪 Menjalankan Unit Tests

Unit tests dapat dijalankan **tanpa Docker** (tidak butuh Redis atau container apapun).

### Setup

```bash
pip install -r requirements.txt
```

### Jalankan Semua Unit Tests

```bash
python -m pytest tests/unit/ -v
```

### Jalankan Test per Modul

```bash
# Hanya test Raft
python -m pytest tests/unit/test_raft_node.py -v

# Hanya test Lock
python -m pytest tests/unit/test_lock_state.py -v

# Hanya test Cache MESI
python -m pytest tests/unit/test_cache_manager.py -v

# Hanya test Security
python -m pytest tests/unit/test_security.py -v
```

**Hasil yang diharapkan:** `62 passed` dalam ~5 detik.

---

## 📊 Menjalankan Load Test (Locust)

> Pastikan sistem sudah berjalan via `docker-compose up` sebelum menjalankan Locust.

### Jalankan Locust

```bash
locust -f benchmarks/load_test_scenarios.py
```

Buka UI Locust di `http://localhost:8089`, lalu konfigurasi:

| Parameter | Nilai Rekomendasi |
|-----------|-------------------|
| Number of users | 100 |
| Ramp up (users/sec) | 10 |
| Host | `http://localhost:8001` (atau port Leader) |

Klik **Start Swarming**, lalu pantau tab **Charts** selama ±30 detik.

---

## 🔧 Troubleshooting

### ❌ Port sudah dipakai

**Error:** `Bind for 0.0.0.0:8001 failed: port is already allocated`

**Solusi:**
```bash
# Cari proses yang menggunakan port
netstat -ano | findstr :8001      # Windows
lsof -i :8001                    # macOS/Linux

# Atau langsung stop semua container yang mungkin masih jalan
docker-compose down
docker ps -a   # cek apakah masih ada container lama
docker rm -f $(docker ps -aq)    # hapus semua container
```

---

### ❌ Redis tidak bisa diakses

**Error:** `redis.exceptions.ConnectionError: Error connecting to localhost:6379`

**Solusi:**
- Pastikan nama service di `docker-compose.yml` adalah `redis` — bukan `localhost`
- Pastikan `REDIS_HOST=redis` di environment variable node (bukan `localhost`)
- Cek apakah Redis container healthy: `docker-compose ps`

---

### ❌ Tidak ada Leader terpilih

**Gejala:** Semua node menampilkan `"raft_state": "follower"` terus-menerus.

**Penyebab:** Node tidak bisa saling berkomunikasi (PEERS salah konfigurasi).

**Solusi:**
```bash
# Cek log untuk pesan error komunikasi
docker-compose logs node1 | grep "Failed"

# Verifikasi PEERS sudah benar di docker-compose.yml
# node1 harus punya: PEERS=http://node2:8000,http://node3:8000
```

---

### ❌ Lock request ditolak dengan "Not leader"

**Penyebab:** Request dikirim ke node Follower. Lock `acquire` dan `release` **harus** ke Leader.

**Solusi:**
1. Cek `/health` di ketiga node untuk menemukan siapa Leader
2. Kirim request ke port Leader (8001, 8002, atau 8003)

---

### ❌ `ModuleNotFoundError` saat menjalankan tests

**Error:** `No module named 'pydantic_settings'` atau `No module named 'redis'`

**Solusi:**
```bash
pip install -r requirements.txt
# Jika masih error, coba install manual:
pip install pydantic-settings redis fastapi httpx pytest pytest-asyncio
```

---

### ❌ Docker build gagal / lambat

**Solusi:**
```bash
# Hapus cache build dan rebuild dari awal
docker-compose build --no-cache

# Atau hapus semua image lama
docker system prune -f
docker-compose up --build
```

---

## 📌 Port Reference

| Service | Port Host | Port Container | Keterangan |
|---------|-----------|----------------|------------|
| Node 1 | 8001 | 8000 | Swagger: http://localhost:8001/docs |
| Node 2 | 8002 | 8000 | Swagger: http://localhost:8002/docs |
| Node 3 | 8003 | 8000 | Swagger: http://localhost:8003/docs |
| Redis | 6379 | 6379 | Redis CLI: `redis-cli -p 6379` |
| Locust UI | 8089 | — | http://localhost:8089 (jalankan manual) |
