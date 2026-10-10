# Laporan Audit Kode: autotradesniper

**Cakupan:** seluruh `src/`, `tests/`, `benches/`. Baseline commit `1850d3f`, perbaikan di branch `claude/magical-keller-cxqchk`.
**Metode:** membaca seluruh kode baris per baris, build dan test (`cargo test`, `cargo clippy`), PoC terhadap kode baseline (debug dan release), dan pengukuran latensi di mode `--release`.
**Nomor baris** di bawah merujuk ke file pada baseline `1850d3f`, kecuali disebut lain.

---

## 0. Ringkasan Eksekutif

| Severity | Jumlah | Diperbaiki | Dimitigasi / butuh desain |
|---|---|---|---|
| Critical | 6 | 5 | 1 (C5: executor exit dan price feed masih simulasi) |
| High | 9 | 9 | 0 |
| Medium | 13 | 12 | 1 (M6: input inflow L2 belum ada) |
| Low | 10 | 6 | 4 (rekomendasi) |

Lima hal yang paling penting:

1. **Siapa pun bisa mengendalikan bot lewat Telegram.** Tidak ada pengecekan `chat_id`. Orang asing bisa mengganti mnemonic, menambah watchlist, dan menyalakan engine.
2. **Bot bisa mengirim ETH ke alamat penyerang.** Ada dua jalur: transfer ETH biasa dari wallet yang dipantau ikut "di-copy", dan Prysm Pola A bisa dipicu dengan tx bernilai 0 ke kontrak mana pun. Keduanya terbukti dengan PoC.
3. **Copy-trade membeli token untuk wallet yang di-copy, bukan untuk kita.** Calldata di-replay apa adanya, termasuk argumen `to` (penerima).
4. **Pembelian terjadi sungguhan, tapi penjualan tidak pernah terjadi.** Risk engine memakai harga acak (`rand::random`). `execute_liquidation` tidak mengirim transaksi apa pun dan membuat hash palsu `0xexit_…`. Encoder calldata exit juga salah ABI.
5. **Parser L2 bisa panic di mode release** karena prefix panjang dari delayed inbox yang bisa dikendalikan penyerang. Task feed lalu mati diam-diam karena tidak disupervisi, dan bot menjadi buta sementara Telegram tetap terlihat normal.

Status verifikasi setelah perbaikan: **36 test lulus** (21 lama + 14 regresi baru di `tests/audit_regressions.rs` + 1 unit test di `stream.rs`). `cargo build --release` dan `cargo check --all-targets` bersih.

---

## 1. Daftar Temuan

### CRITICAL

| ID | Temuan | Lokasi (baseline) | Status |
|---|---|---|---|
| C1 | Bot Telegram tanpa otorisasi. Siapa pun bisa `tr_new_wallet`/`tr_import_wallet` (mengganti wallet induk), menambah watchlist, toggle engine/Prysm, `/panic` | `telegram.rs:368`, `telegram.rs:527` | ✅ Diperbaiki |
| C2 | Transfer ETH biasa dari wallet watchlist (mis. deposit ke CEX) dianggap sinyal: kondisi `!is_swap && value == 0` meloloskan semua tx bernilai > 0. Bot lalu mengirim `default_trade_eth` ke penerimanya. **PoC terkonfirmasi.** | `stream.rs:455-458` | ✅ Diperbaiki |
| C3 | Calldata copy-trade di-replay verbatim. Pada `swapExactETHForTokens(amountOutMin, path, to, deadline)`, argumen `to` adalah wallet yang di-copy, jadi kita yang membayar dan mereka yang menerima token. Untuk `exactInputSingle`/Universal Router/`swapExactTokensForTokens`, `amountIn` di calldata tidak cocok dengan `msg.value` kita (revert atau perilaku tak terduga). | `dispatcher.rs:138-141`, `payload.rs:22-43` | ✅ Diperbaiki |
| C4 | Spoof Prysm Pola A/B. Tx bernilai **0** ke **alamat mana pun** dengan string `prysmcloud/token-images` di calldata tetap lolos semua guardrail: fallback dev-buy hard-coded `0.021` ETH, tax hard-coded 200 bps, exempt hard-coded `[from]`, dan `token = pool = to`. Tiga blok kemudian bot mengirim ETH ke alamat penyerang. **PoC terkonfirmasi.** Selain itu, untuk launch yang asli, `to` adalah kontrak launchpad, sehingga replay calldata launch berarti **membuat token duplikat**, bukan membeli. | `prysm.rs:305-386` (khususnya `355-361`, `364-371`) | ✅ Diperbaiki (allowlist, tanpa fallback, guard token unresolved) |
| C5 | Risk engine 6 lapis berjalan di atas data simulasi. Harga acak (`rand::random::<f64>() * 0.40`), kapasitas kurva sintetis `elapsed*2`, block number hard-coded. `execute_liquidation` tidak mengirim tx dan membuat hash `0xexit_…`. `/sweep` hanya mencetak daftar. `build_fast_exit_payload` salah ABI (lihat §6, C5). **Pembelian nyata, penjualan tidak pernah terjadi.** | `risk/mod.rs:161-201`, `257-270`, `367-375`; `payload.rs:75-89` | ⚠️ Dimitigasi: label "SIMULATED" tanpa hash palsu, encoder ABI yang benar, teks Telegram jujur. **Executor exit dan price feed nyata masih harus dibangun** (§4) |
| C6 | Jalur kehilangan mnemonic. State (berisi mnemonic plaintext) ditulis non-atomik dengan izin default 0644. Jika file korup/terpotong, error parse **diabaikan** (`let _ =`), `WalletManager` membuat mnemonic **baru**, lalu menimpa file lama, sehingga akses ke dana di wallet lama hilang. | `config.rs:235`, `config.rs:333`, `config.rs:354`, `wallet.rs:343-352` | ✅ Diperbaiki |

### HIGH

| ID | Temuan | Lokasi | Status |
|---|---|---|---|
| H1 | `extract_l2_txs`: `i + n` dari u64 tak tepercaya overflow. Di release terjadi `slice index starts at 9 but ends at 8` (panic, **PoC di release**). Pesan delayed-inbox (`sendL2Message` di L1) bisa dibuat siapa pun dan diteruskan feed apa adanya. Task stream tidak disupervisi, jadi feed berhenti permanen. | `stream.rs:146-152`, `main.rs:186-190` | ✅ Diperbaiki |
| H2 | Celah nonce permanen. `fetch_add` sebelum broadcast tanpa rollback/resync saat gagal, sehingga semua tx berikutnya dari akun itu tertahan "nonce too high". Sinkronisasi awal berjalan di background (trade pertama bisa memakai nonce 0), memakai tag `latest`, dan `store()` bisa memundurkan cursor. | `dispatcher.rs:163`, `212`; `wallet.rs:363-367`, `424-426` | ✅ Diperbaiki |
| H3 | Sukses palsu. Broadcast gagal tetap menghasilkan hash `0xsim_…` dan `Ok(report)`. `main.rs` lalu mendaftarkan posisi fantom, dan Telegram melaporkan "DISPATCH BERHASIL". | `dispatcher.rs:191-196`, `265-271`; `main.rs:124-137` | ✅ Diperbaiki |
| H4 | Ukuran posisi Prysm diabaikan. Dispatcher selalu memakai `default_trade_eth` (0.05), dan `main.rs` mengecek tag `"PrysmSeries"` yang tidak pernah dipancarkan (tag aslinya `"Prysm: Pola …"`), sehingga modal 5× lebih besar dari setting 0.01. | `dispatcher.rs:134`, `153`; `main.rs:126` | ✅ Diperbaiki |
| H5 | WebSocket feed: (a) tanpa idle timeout, sehingga koneksi half-open (Cloudflare/NAT) menggantung selamanya; (b) backoff tidak pernah di-reset, sehingga setelah beberapa putus selalu menunggu 30 dtk; (c) satu kegagalan handshake (429, 403, DNS) menjatuhkan bot ke HTTP poller **selamanya** karena `listen_http_poll` tidak pernah return; (d) tanpa Retry-After/jitter. | `stream.rs:225-250`, `261` | ✅ Diperbaiki |
| H6 | HTTP poller tidak memproses transaksi apa pun: `get_block_by_number` default **hashes**, sehingga `BlockTransactions::Full` tidak pernah match. Juga memulai dari blok 0 jika `eth_blockNumber` awal gagal (replay seluruh chain), dan melompati blok saat fetch gagal. | `stream.rs:369`, `385-386`, `417` | ✅ Diperbaiki |
| H7 | PBKDF2-HMAC-SHA512 (2048 ronde, BIP-39 `to_seed`) dijalankan **di setiap dispatch, per akun**, di thread worker Tokio. Terukur **7,2 ms** untuk 3 akun (release). | `wallet.rs:440-498`, dipanggil `dispatcher.rs:161`, `202` | ✅ Diperbaiki (cache) |
| H8 | Ukuran tx dan gas. Padding 94.500 + calldata + envelope melewati batas default Nitro `max-tx-data-size` 95.000 byte untuk calldata > ~240 byte, sehingga ditolak "oversized data". Gas statis 600k dengan intrinsic ~400k (94.500 × 4 gas) hanya menyisakan ~200k untuk eksekusi (risiko OOG). `GAS_LIMIT` dan `MAX_FEE_GWEI` di config diabaikan. | `dispatcher.rs:145`, `168`, `227`; `payload.rs:143-156` | ✅ Diperbaiki |
| H9 | `/panic` memanggil `toggle_engine()`. Jika engine sedang di-pause, `/panic` justru **menyalakan kembali** intake sinyal di tengah keadaan darurat. | `risk/mod.rs:319` | ✅ Diperbaiki |

### MEDIUM

| ID | Temuan | Lokasi | Status |
|---|---|---|---|
| M1 | Rekursi parser L2 tanpa batas kedalaman. Nitro membatasi 16 dan membatalkan seluruh pesan. Di release, 29k level masih lolos pada stack 2 MiB, tapi build debug atau platform lain berisiko stack overflow (abort proses, tidak bisa ditangkap). | `stream.rs:137-155` | ✅ |
| M2 | Pesan feed non-L2 (header.kind ≠ 3: deposit, batch posting report) ikut diurai sebagai L2. `sequenceNumber` dipakai sebagai nomor blok tanpa offset genesis, sehingga berbeda dengan nomor blok dari HTTP poller (guard +3 blok Prysm kacau saat failover). | `stream.rs:279-282`, `317-321` | ✅ (`FEED_GENESIS_BLOCK`) |
| M3 | BroadcasterPool: future di `FuturesUnordered` di-drop saat sukses pertama. Request in-flight dibatalkan dan koneksinya dibuang, jadi broadcast berikutnya kena handshake TLS dingin. `serde_json::Value` 190 KB di-clone dan diserialisasi 5× (1,26 ms). `pool_idle_timeout(90s)` membuat koneksi dingin setelah idle. HTTP/2 tidak aktif (`default-features=false`). | `dispatcher.rs:26-96` | ✅ |
| M4 | Fee: `MAX_FEE_GWEI` tidak diterapkan. Priority fee bisa melebihi max fee jika EMA anjlok. | `payload.rs:143-156` | ✅ |
| M5 | Guardrail Prysm: `as_secs()` memotong (15,9 dtk lolos limit 15 dtk). Order antre dilepas tanpa cek ulang MCap. Tidak ada dedup, sehingga Pola C memicu entry berulang untuk token yang sama. Read-lock `recent_launches` ditahan melewati `.await`. | `prysm.rs:275-281`, `409-433`, `560-593` | ✅ |
| M6 | Mode Target Prysm memakai literal 10000/-30 dan mengabaikan config. Flow rules tetap berlaku pada mode Target. Karena `on_inflow_event` **tidak pernah dipanggil**, Inactivity Decay selalu terpicu tepat di 25 dtk untuk **setiap** posisi (mode System juga). | `price_rules.rs:39`, `55`; `flow_rules.rs:174-188`; `risk/mod.rs:127` | ⚠️ Target diperbaiki; input inflow (System) masih terbuka (§4) |
| M7 | HTML Telegram: `ExitReason` berisi `<` dan `>=` ("< 0.1 ETH", ">= 90%") tanpa escape, sehingga Telegram menolak pesan (alert exit hilang). URL dari calldata (dikendalikan penyerang) masuk ke tampilan statistik tanpa escape. | `telegram.rs:209-214`, `805`, `968` | ✅ |
| M8 | Task tidak disupervisi (panic berarti mati diam-diam). Watchdog di-spawn tanpa handle (bocor). Tidak ada shutdown terkoordinasi. | `main.rs:114-190`, `risk/mod.rs:212` | ✅ |
| M9 | `std::fs::write` (blocking) di dalam fungsi async. | `config.rs:354` | ✅ |
| M10 | Rebalance: error RPC `get_balance` dianggap saldo 0 (dana terkirim tanpa perlu). Kegagalan connect menghasilkan hasil "sukses" simulasi. Tx gagal tidak me-resync nonce. | `wallet.rs:549-567`, `600-608` | ✅ |
| M11 | Callback data Telegram tidak dibatasi: `tr_sub:1000000` menurunkan sejuta akun (DoS CPU), `cfg_tp:NaN` diterima. | `telegram.rs:673-684`, `743-768` | ✅ |
| M12 | Kebocoran memori: tracker posisi tertutup tidak pernah dihapus (diiterasi tiap 100 ms selamanya), vektor posisi tumbuh tanpa batas, ID acak `u32` (birthday collision ~65k menimpa tracker hidup). | `risk/mod.rs:85`, `121` | ✅ |
| M13 | `tokio::time::interval` default `MissedTickBehavior::Burst`: setelah stall, evaluasi beruntun pada state basi. | `risk/mod.rs:213`, `238` | ✅ |

### LOW

| ID | Temuan | Lokasi | Status |
|---|---|---|---|
| L1 | `build_calibrated_payload` melakukan memset dua kali (`resize` lalu `write_bytes` di blok `unsafe` yang redundan). `write_into_scratch` salah memakai `reserve` (relatif terhadap len, bukan capacity). | `payload.rs:31-40`, `55-57` | ✅ |
| L2 | `detected_at` diambil setelah pemrosesan, bukan saat frame diterima. | `stream.rs:477` | ✅ |
| L3 | `signal_tx.send().await` (backpressure) memblokir pembaca feed saat antrean penuh. | `stream.rs:482`, `prysm.rs:591`, `628` | ✅ (`try_send`) |
| L4 | Guard +3 blok: order dilepas saat blok L+3 **teramati**, jadi tx masuk paling cepat di **L+4**. Perbandingannya tidak off-by-one, tapi 1 blok lebih lambat dari bunyi README ("disiarkan mulai Blok +3"). Jika jendela tax adalah [L, L+2], memicu pada L+2 menghemat 1 blok (~250 ms). Dibiarkan (pilihan aman), didokumentasikan di kode. | `prysm.rs:503` | ℹ️ |
| L5 | Di Arbitrum, sequencer FCFS dan tip/priority fee tidak dipakai untuk ordering (tip di-drop). Logika EMA priority hanya menaikkan `max_fee` (dan saldo minimum yang dibutuhkan). RMW load/store EMA tidak atomik (aman hanya karena writer tunggal). | `payload.rs:126-156` | ℹ️ Rekomendasi |
| L6 | `decode_2718` menerima byte sisa; Nitro menolak tx seperti itu. | `stream.rs:329` | ✅ (`decode_2718_exact`) |
| L7 | `tr_new_wallet` mengganti wallet tanpa konfirmasi dan mengirim mnemonic lewat chat Telegram (tersimpan di server Telegram). | `telegram.rs:648-664` | ℹ️ Rekomendasi |
| L8 | Pengaturan Telegram `target_profit_percent`, `time_decay_exit_seconds`, dan `slippage_percent` tidak dibaca oleh komponen mana pun (tombol tanpa efek). | `telegram.rs:288-298`, `743-768` | ℹ️ Rekomendasi |
| L9 | `test_l2_message_unpacking_kind3_and_kind4` menguji **salinan** fungsi, bukan fungsi produksi, sehingga bug H1 tidak terdeteksi. | `tests/prysm_tests.rs:492` | ✅ |
| L10 | `extract_target_token_fast` (kata terakhir calldata) hanya benar untuk path V2. Kini aman karena copy dibatasi ke V2. | `stream.rs:124-132` | ✅ |

---

## 2. Jawaban per Sasaran Audit

### A. Konkurensi dan keamanan thread
- **Deadlock:** tidak ditemukan. Satu-satunya penguncian bersarang (`trackers.write` lalu `positions.write` di loop 100 ms) memakai urutan yang konsisten; tidak ada jalur dengan urutan terbalik. Read-guard yang ditahan melewati `.await` (`prysm.rs:409-433`) sudah dilepas sebelum await.
- **Race nonce:** `store(on_chain)` saat sync bisa memundurkan cursor yang sudah dipakai `fetch_add` (nonce ganda). Kini memakai `fetch_max` saat startup, `store(pending)` hanya pada resync setelah broadcast gagal, dan sync **ditunggu** sebelum trading.
- **Blocking di worker Tokio:** PBKDF2 7,2 ms per dispatch (diperbaiki: cache + `spawn_blocking`), `std::fs::write` (diperbaiki), dan `ecrecover` **269 µs/tx** pada backend pure-Rust k256 (dikurangi dengan gating; lihat §3.2).
- **Task leak:** watchdog (kini `AbortOnDrop`), fee refresher (guard agar tidak dobel saat supervisor me-restart), keep-warm (di-abort saat shutdown). Task per-endpoint dibatasi timeout 2,5 dtk.

### B. Ketahanan jaringan
- **Ping/Pong:** tungstenite mengantrekan Pong dan mem-flush-nya saat `read` berikutnya (`tungstenite/src/protocol/mod.rs:457`), jadi aman. Masalah sebenarnya adalah tidak adanya **deteksi diam**. Kini ada idle timeout 20 dtk (sama dengan klien feed Nitro; broadcaster mem-ping ~5 dtk).
- **429/Cloudflare:** `Retry-After` dihormati (429/503), backoff eksponensial dengan jitter ±20 % di-reset setelah sesi sehat ≥ 60 dtk. HTTP poller hanya menjembatani jendela backoff, lalu feed WS dicoba lagi.
- **Reqwest:** keep-alive TCP 15 dtk, pool tanpa idle-expiry plus ping `eth_chainId` tiap 20 dtk, HTTP/2 via ALPN, `connect_timeout` 1,5 dtk. Request per endpoint tidak lagi dibatalkan saat endpoint lain sukses lebih dulu, sehingga koneksi tetap hangat dan tx tetap tersebar redundan.

### C. Integritas parsing byte
- Semua slicing di `extract_l2_txs` kini memakai `split_first_chunk`/`split_at_checked` (tanpa aritmetika yang bisa overflow). Segmen terpotong diperlakukan sebagai akhir batch, dan kedalaman > 16 membatalkan seluruh pesan, sama persis dengan `parseL2Message` di Nitro.
- `TxEnvelope::decode_2718_exact` menangani Legacy, 2930, 1559, 4844, dan 7702. Tipe internal Arbitrum (0x64–0x6A) tidak pernah muncul di pesan L2 bertanda tangan. Biaya decode **354 ns**; yang mahal adalah `recover_signer` (**269 µs**), kini hanya dijalankan untuk tx yang relevan.

### D. Presisi logika bisnis
- **Pembagian nol:** `entry_price <= 0` sudah dijaga, dan `peak` hanya dipakai setelah aktivasi (> 0). Tambahan: guard `is_finite()` agar harga NaN/inf tidak diam-diam mematikan semua perbandingan.
- **`Instant` vs `SystemTime`:** durasi memakai `Instant` (benar). `SystemTime` hanya untuk tampilan. `duration_since` kini `saturating_duration_since`.
- **+3 blok:** perbandingan `current < launch + delay` benar (kini `saturating_add`). Lihat L4 untuk semantik inklusi.

---

## 3. Rekomendasi Optimasi Latensi (terukur, `--release`)

| # | Item | Sebelum | Sesudah | Status |
|---|---|---|---|---|
| 3.1 | Derivasi akun per dispatch (PBKDF2 + BIP-32, 3 akun) | 7,2 ms | ~clone `Arc` | ✅ |
| 3.2 | `ecrecover` untuk setiap tx feed | 269 µs/tx (50 tx/blok ≈ 13 ms CPU) | hanya swap V2 yang bisa di-copy (atau semua jika Prysm aktif) | ✅ sebagian |
| 3.3 | Body broadcast: clone `Value` + serialisasi ×5 endpoint | 1,26 ms | 0,48 ms (sekali, `Bytes` refcount) | ✅ |
| 3.4 | Prefilter Prysm: `from_utf8_lossy` + `contains` per tx | 1,6 µs | 0,17 µs (`memchr::memmem`) | ✅ |
| 3.5 | Parsing frame feed: pohon `serde_json::Value` | alokasi per node | struct ter-borrow (`Cow<str>`), buffer base64 dipakai ulang | ✅ |
| 3.6 | Alert Telegram (format string) sebelum dispatch | di jalur kritis | setelah dispatch | ✅ |

**Rekomendasi lanjutan (belum diterapkan):**
1. **Padding 94,5 KB adalah biaya latensi terbesar yang tersisa.** Per akun: sign + keccak 0,8 ms, hex + JSON 0,48 ms, dan **190 KB upload per endpoint** (5 endpoint × N akun; ~15 ms di kabel per request pada 100 Mbit/s). Sequencer Arbitrum memproses FCFS, dan saya tidak menemukan mekanisme yang membuat tx besar diurutkan lebih awal. **Saran: `BUFFER_SIZE=0`**, kecuali Anda punya pengukuran yang menunjukkan manfaatnya.
2. **Backend `secp256k1` (libsecp256k1) untuk alloy** (`alloy = { features = [..., "secp256k1"] }`) biasanya beberapa kali lebih cepat dari k256 untuk `ecrecover`. Ukur di mesin Anda. Belum saya aktifkan karena menambah build C (perlu verifikasi di toolchain MinGW Anda).
3. **Prefilter Prysm sebelum `ecrecover`:** recover hanya jika `to ∈ launchpads ∪ recent_launches` atau calldata lolos `memmem`. Pola C butuh `from`, jadi cek `to` dulu.
4. **Watchlist lookup** memakai `tokio::RwLock` async per tx. Ganti dengan `arc-swap` atau `std::sync::RwLock` (read tanpa kontensi ~20 ns, tanpa poll future).
5. **Head-of-line blocking:** Task A memproses sinyal satu per satu (menunggu broadcast). Pertimbangkan spawn per sinyal dengan lock per akun untuk urutan nonce, dan buang sinyal yang `detected_at` sudah terlalu tua.
6. **`MAX_PRIORITY_FEE_GWEI=0`** di Arbitrum: tip tidak memengaruhi urutan dan hanya menaikkan `max_fee × gas_limit` (saldo minimum).
7. Runtime khusus (current-thread, core pinned) untuk feed + dispatch, dan `mimalloc` sebagai global allocator.

---

## 4. Item Terbuka (butuh desain atau info ABI, tidak bisa ditebak dengan aman)

1. **Executor exit nyata (C5):** `balanceOf` untuk `amountIn`, `approve` (sekali per token), sell via fungsi kurva launchpad atau router, `amountOutMin` dari quote, deadline, broadcast lewat `BroadcasterPool`, lalu tunggu receipt. Encoder `build_fast_exit_payload` yang ABI-nya benar sudah tersedia dan diuji.
2. **Price feed nyata untuk risk engine:** reserve kurva atau quoter pool, menggantikan `rand::random` di `risk/mod.rs` (ditandai `SIMULATED` di kode).
3. **Resolusi token Prysm:** ambil alamat token dari event launch (receipt) atau prediksi CREATE2, lalu susun calldata `buy` launchpad. Sampai ini ada, dispatcher **menolak** entry Prysm (`target_token == pool_or_router`), sehingga deteksi dan statistik tetap jalan tapi tidak ada ETH yang terkirim.
4. **Input lapis 2/3/4:** `on_inflow_event` dan `on_approval_event` tidak pernah dipanggil, dan `is_migrated_to_pool` tidak pernah di-set. Akibatnya L2 selalu exit di 25/30 dtk, sedangkan L3 pool-trailing dan L4 tidak pernah aktif.
5. **`amountOutMin` copy-trade:** saat ini diwarisi dari tx yang di-copy. Hitung dari quote untuk ukuran kita (`SLIPPAGE_PERCENT` saat ini tidak dipakai).
6. **Telegram:** untuk grup, otorisasi per `user_id` (bukan `chat_id`), konfirmasi dua langkah sebelum mengganti wallet, dan jangan kirim mnemonic lewat chat (L7). Tombol tanpa efek perlu disambungkan atau dihapus (L8).

---

## 5. Perubahan Perilaku dan Konfigurasi Baru

| Variabel / perilaku | Keterangan |
|---|---|
| `TELEGRAM_AUTHORIZED_CHAT_ID` | **Wajib.** 0 atau tidak di-set berarti bot mengabaikan semua chat (ada warning saat startup). |
| `PRYSM_LAUNCHPAD_FACTORIES` | Daftar kontrak launchpad (dipisah koma). Kosong berarti Pola A/B nonaktif (fail-closed). |
| `FEED_GENESIS_BLOCK` | Nomor blok L2 untuk pesan feed #0 (default 0). |
| Copy-trade | Hanya `swapExactETHForTokens` / `…SupportingFeeOnTransferTokens`, dengan penerima ditulis ulang ke akun kita. Selector lain ditolak. |
| Gas limit | `max(GAS_LIMIT, intrinsic + 300k)`. Dengan padding default sekitar ~700k (saldo minimum naik). |
| Padding | Dipotong otomatis agar tx ≤ 95.000 byte. |
| `runtime_config.json` | Ditulis atomik dengan izin 0600. Startup **gagal** jika file ada tapi korup (tidak lagi membuat mnemonic baru diam-diam). |
| Posisi | Hanya didaftarkan jika tx master diterima endpoint, dengan ukuran dari ETH yang benar-benar dikirim. |

---

## 6. Cuplikan Perbaikan (Code Diffs)

Diff lengkap ada di commit `8c37ede` pada branch `claude/magical-keller-cxqchk`. Bagian-bagian intinya:

### C1: Otorisasi Telegram (`src/telegram.rs`)
```diff
+    fn is_authorized(&self, chat_id: i64) -> bool {
+        let authorized = self.config.telegram_authorized_chat_id;
+        authorized != 0 && chat_id == authorized
+    }
     pub async fn handle_message(&self, msg: Message) -> Result<()> {
         let chat_id = msg.chat.id.0;
+        if !self.is_authorized(chat_id) {
+            warn!("Ignoring Telegram message from unauthorized chat {}", chat_id);
+            return Ok(());
+        }
 ...
             UserState::AwaitingMnemonic => {
+                // Do not leave the seed phrase sitting in the chat history.
+                if let Err(e) = self.bot.delete_message(msg.chat.id, msg.id).await { ... }
```
(Guard yang sama ada di `handle_callback`. `tr_sub` dibatasi 1..=10, `cfg_tp` harus finite dan dalam (0, 1000].)

### C2/C3: Hanya swap V2, penerima ditulis ulang (`src/stream.rs`, `src/payload.rs`, `src/dispatcher.rs`)
```diff
-        let is_swap = fast_selectors::is_swap_selector(input);
-        if !is_swap && value == U256::ZERO {
-            return;
-        }
+        if !fast_selectors::is_copyable_eth_swap(input) {
+            return;
+        }
```
```rust
pub fn retarget_swap_recipient(calldata: &[u8], recipient: Address) -> Option<Vec<u8>> {
    // (uint256 amountOutMin, address[] path, address to, uint256 deadline)
    const TO_WORD: usize = 4 + 32 * 2;
    if calldata.len() < 4 + 32 * 4 || !is_copyable_eth_swap(calldata) { return None; }
    let mut out = calldata.to_vec();
    out[TO_WORD..TO_WORD + 12].fill(0);
    out[TO_WORD + 12..TO_WORD + 32].copy_from_slice(recipient.as_slice());
    Some(out)
}
```

### C4: Prysm fail-closed (`src/models/prysm.rs`, `src/dispatcher.rs`)
```diff
-        let dev_buy_eth = if dev_buy_from_value > 0.001 { dev_buy_from_value } else { 0.021 };
+        let dev_buy_eth = f64::from(value_wei) / 1e18;
 ...
+        if !self.config.prysm_launchpads.contains(&to_addr) {
+            // warn once when the allowlist is empty
+            return;
+        }
```
```rust
// dispatcher: never replay a launch tx into the launchpad
if is_strategy && signal.target_token == signal.pool_or_router {
    anyhow::bail!("Refusing {:?} entry: token address unresolved ...", signal.strategy_tag);
}
```

### C6: State atomik 0600 dan gagal saat korup (`src/config.rs`)
```diff
-        let _ = config.load_persisted_state();
+        config.load_persisted_state()
+            .with_context(|| format!("Refusing to start: '{}' exists but is unreadable", config.state_path))?;
 ...
-        std::fs::write(&self.state_path, json)?;
+        let _guard = self.save_lock.lock().await;
+        tokio::task::spawn_blocking(move || write_private_atomic(&path, &json)).await??;
```
`write_private_atomic` membuka `*.json.tmp` dengan `mode(0o600)`, lalu `write_all`, `sync_all`, dan `rename`.

### H1/M1: Parser L2 tahan input jahat (`src/stream.rs`)
```rust
fn extract_l2_txs_at_depth<'a>(bytes: &'a [u8], out: &mut Vec<&'a [u8]>, depth: usize) -> bool {
    match bytes.split_first() {
        Some((&L2_KIND_SIGNED_TX, tx)) => out.push(tx),
        Some((&L2_KIND_BATCH, mut rest)) => {
            if depth >= MAX_L2_BATCH_DEPTH { return false; }          // Nitro: max depth 16
            while let Some((len_bytes, tail)) = rest.split_first_chunk::<8>() {
                let Ok(n) = usize::try_from(u64::from_be_bytes(*len_bytes)) else { break };
                let Some((segment, next)) = tail.split_at_checked(n) else { break };
                if !extract_l2_txs_at_depth(segment, out, depth + 1) { return false; }
                rest = next;
            }
        }
        _ => {}
    }
    true
}
```

### H2/H3: Tanpa hash palsu, resync nonce (`src/dispatcher.rs`, `src/wallet.rs`)
```diff
-                        let sim_hash = format!("0xsim_{:x}", rand::random::<u64>());
-                        tx_hashes.push(sim_hash);
+                    match tokio::time::timeout(Duration::from_secs(2),
+                        self.wallet_manager.resync_nonce(address)).await { ... }
+                    failures.push(format!("[{}] {}", role, err_msg));
 ...
+        anyhow::ensure!(master_accepted, "Master entry was not accepted by any endpoint: {}", ...);
```
```diff
-            if let Ok(on_chain_nonce) = provider.get_transaction_count(acc.address).await {
-                cursor.store(on_chain_nonce, Ordering::SeqCst);
+            match provider.get_transaction_count(acc.address).pending().await {
+                Ok(on_chain_nonce) => { acc.nonce_cursor.fetch_max(on_chain_nonce, Ordering::SeqCst); }
```

### H5: WS idle timeout dan backoff (`src/stream.rs`)
```rust
let msg = match tokio::time::timeout(FEED_IDLE_TIMEOUT, ws_stream.next()).await {
    Ok(Some(Ok(m))) => m,
    Ok(Some(Err(e))) => return Err(e).context("Sequencer feed read error"),
    Ok(None) => return Ok(()),
    Err(_) => anyhow::bail!("Sequencer feed silent for {:?}", FEED_IDLE_TIMEOUT),
};
// run(): reset backoff after a healthy session, honour Retry-After, jitter,
// and only bridge the backoff window with the HTTP poller:
match tokio::time::timeout(wait, self.listen_http_poll(&http_url)).await { ... }
```

### H6: HTTP poller (`src/stream.rs`)
```diff
-        let mut last_block = provider.get_block_number().await.unwrap_or(0);
+        let mut last_block = provider.get_block_number().await
+            .context("HTTP poller: initial eth_blockNumber failed")?;
 ...
-                            if let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).await {
+                        let block = match provider.get_block_by_number(block_num.into()).full().await {
+                            Ok(Some(b)) => b,
+                            Ok(None) | Err(_) => break, // retry next tick, never skip
+                        };
```

### H7: Cache akun (`src/wallet.rs`)
```rust
pub async fn get_all_accounts_shared(&self) -> Result<Arc<Vec<TreasuryAccount>>> {
    let count = *self.config.sub_account_count.read().await;
    if let Some(c) = self.accounts_cache.read().await.as_ref().filter(|c| c.len() == count + 1) {
        return Ok(c.clone());
    }
    // ... derive once (seed computed once) inside spawn_blocking, store in cache
}
```

### H8: Batas ukuran tx dan gas (`src/payload.rs`, `src/dispatcher.rs`)
```rust
let padding = CalldataPayloadBuilder::clamp_padding(swap.len(), buffer_size); // <= 95,000 B total
let gas_limit = configured_gas_limit
    .max(CalldataPayloadBuilder::intrinsic_gas(&swap, padding) + EXECUTION_GAS_RESERVE);
let max_fee_wei = if cap_wei > 0 { ema_max_fee_wei.min(cap_wei) } else { ema_max_fee_wei };
let priority_fee_wei = ema_priority_wei.min(max_fee_wei);
```

### H9: `/panic` idempoten (`src/risk/mod.rs`)
```diff
-        let _ = self.config.toggle_engine().await;
+        self.config.set_engine_running(false).await;
```

### C5 (mitigasi): exit jujur dan ABI exit yang benar
```diff
-        let exit_tx_hash = format!("0xexit_{:x}", rand::random::<u64>());
+        error!("No on-chain exit executor: position {} closed in memory only, tokens remain in the wallet", ...);
+        let exit_reason = format!("[SIMULATED, no sell tx] {}", signal.reason);
+        tracker.position.exit_tx_hash = None;
```
`build_fast_exit_payload(amount_in, amount_out_min, path, recipient, deadline)` kini menghasilkan ABI `swapExactTokensForETH` yang valid: offset `path` = 0xa0, lalu tail `[len, hops…]`. Ini diverifikasi dengan `alloy::sol!` decode di `tests/audit_regressions.rs`. Versi lama menaruh alamat token di `amountIn`, router di `amountOutMin`, dan `path`/`to`/`deadline` = 0.

---

## 7. Bukti (PoC baseline dan test regresi)

PoC terhadap baseline `1850d3f` (`cargo test --release`):
```
thread 'poc_l2_oversized_length_panics' panicked at src/stream.rs:150:38:
slice index starts at 9 but ends at 8
spoof -> pool_or_router=0x…deadbeef target_token=0x…deadbeef tag=Some("Prysm: Pola A (Prysmcloud Image Host)")
plain transfer copied -> router=0x…0c0ffe
fast exit: amountIn=0x1111…(token) minOut=0x2222…(router) pathOffset=0 to=0x0 deadline=0
```

Setelah perbaikan: `tests/audit_regressions.rs` (14 test) mengunci perilaku yang benar untuk H1, M1, C4 (2 varian), M5 (dedup + presisi detik), C2, C3, ABI exit (C5), H8, M6, M7, dan tag strategi. Ditambah unit test decoder frame feed. **Total 36 test lulus.**
