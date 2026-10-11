//! Every text and keyboard the bot shows, as pure functions of engine state.
//!
//! Rules: every value that did not come from a literal in this file (token symbols, exit reasons,
//! role names, feed labels, error texts, tx hashes) goes through [`html::escape`]; literals never
//! contain a bare `&`, `<` or `>`. The regression tests validate every screen with hostile inputs
//! against [`html::validate`], and the send path cuts anything too long with [`html::fit`].

use crate::models::prysm::PrysmMatchReport;
pub use crate::telegram::control::PrysmCounts;
use crate::telegram::control::{
    ApplyReport, BreakerOutcome, FeePreset, PositionView, PrysmView, SettingsView, SystemSnapshot, CARRIER_CHOICES, TRAILING_CHOICES,
};
use crate::telegram::html::escape as e;
use crate::types::{DispatchReport, ExecutionMode, GasRebalanceResult, Position, TradeSignal};
use alloy::primitives::Address;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

pub const RULE: &str = "═══════════════════════════";

fn short(addr: &Address) -> String {
    let s = format!("{:?}", addr);
    format!("{}…{}", &s[..6], &s[s.len() - 4..])
}

fn eth(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("{:.4} ETH", v),
        None => "? ETH".to_string(),
    }
}

fn signed_pct(v: f64) -> String {
    if v >= 0.0 { format!("+{:.1}%", v) } else { format!("{:.1}%", v) }
}

fn ms(us: Option<u64>) -> String {
    match us {
        Some(us) => format!("{:.1} ms", us as f64 / 1_000.0),
        None => "–".to_string(),
    }
}

fn kb(bytes: usize) -> String {
    if bytes == 0 { "0 KB".to_string() } else { format!("{:.1} KB", bytes as f64 / 1_000.0) }
}

fn role_label(index: usize) -> String {
    if index == 0 { "Ayah (master)".to_string() } else { format!("Anak #{}", index) }
}

fn back(to: &str, label: &str) -> Vec<InlineKeyboardButton> {
    vec![InlineKeyboardButton::callback(label.to_string(), to.to_string())]
}

// ───────────────────────────── keyboards ─────────────────────────────

/// Main menu. The intake button carries its target state (`intake:on` / `intake:off`), so a
/// stale button pressed later can never flip the engine the other way; while the circuit breaker
/// is latched it is replaced by the explicit release.
pub fn main_menu_keyboard(is_running: bool, breaker: Option<u64>) -> InlineKeyboardMarkup {
    let intake = if let Some(epoch) = breaker {
        // Names its breaker: pressed after a newer one fired, it is refused.
        InlineKeyboardButton::callback("🔓 Lepas Breaker + Intake ON", format!("breaker_release:{}", epoch))
    } else if is_running {
        InlineKeyboardButton::callback("⏸ Intake ON 🟢 → Pause", "intake:off")
    } else {
        InlineKeyboardButton::callback("▶️ Intake OFF 🔴 → Aktifkan", "intake:on")
    };
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("🎯 Signal Accounts", "menu_signals"),
            InlineKeyboardButton::callback("💼 Treasury & Wallets", "menu_treasury"),
        ],
        vec![InlineKeyboardButton::callback("⚙️ Execution Settings", "menu_settings"), intake],
        vec![
            InlineKeyboardButton::callback("📈 Portfolio & PnL", "menu_portfolio"),
            InlineKeyboardButton::callback("📡 Telemetri", "menu_telemetry"),
        ],
        vec![
            InlineKeyboardButton::callback("🔬 Model Strategi", "menu_models"),
            InlineKeyboardButton::callback("🔄 Refresh", "menu_main"),
        ],
        vec![
            InlineKeyboardButton::callback("🚨 PANIC SELL ALL", "op_panic"),
            InlineKeyboardButton::callback("🧹 Sweep Assets", "op_sweep"),
        ],
    ])
}

pub fn settings_keyboard(view: &SettingsView) -> InlineKeyboardMarkup {
    settings_keyboard_parts(view.mode, view.carriers, view.preset, view.padding_on, Some(view.trailing_stop_percent))
}

/// `trailing`: the current trailing-stop distance (marks its button), `None` when unknown.
pub fn settings_keyboard_parts(
    mode: ExecutionMode,
    carriers: usize,
    preset: Option<FeePreset>,
    padding_on: bool,
    trailing: Option<f64>,
) -> InlineKeyboardMarkup {
    let mark = |on: bool, label: String| if on { format!("{} ✅", label) } else { label };
    // The target mode is in the callback: a repeated or stale press cannot flip it back.
    let mode_btn = match mode {
        ExecutionMode::Single => InlineKeyboardButton::callback("🔄 Mode: Single [Ganti ke Multi]", "cfg_mode:multi"),
        ExecutionMode::MultiAccount => InlineKeyboardButton::callback("🔄 Mode: Multi [Ganti ke Single]", "cfg_mode:single"),
    };
    let carriers_row = CARRIER_CHOICES
        .iter()
        .map(|n| InlineKeyboardButton::callback(mark(carriers == *n, format!("👥 {} Anak", n)), format!("cfg_sub:{}", n)))
        .collect();
    let fee_row = FeePreset::ALL
        .iter()
        .map(|p| {
            let icon = match p {
                FeePreset::Normal => "⚡",
                FeePreset::Optimal => "🚀",
                FeePreset::Race => "🔥",
            };
            InlineKeyboardButton::callback(mark(preset == Some(*p), format!("{} {} {:.1}G", icon, p.label(), p.gwei().0)), p.callback())
        })
        .collect();
    // The target distance is in the callback: a repeated or stale press cannot change it twice.
    let mut trailing_row: Vec<InlineKeyboardButton> = TRAILING_CHOICES
        .iter()
        .map(|p| {
            let on = trailing.is_some_and(|t| (t - f64::from(*p)).abs() < 1e-9);
            InlineKeyboardButton::callback(mark(on, format!("📉 Trail {}%", p)), format!("cfg_trail:{}", p))
        })
        .collect();
    trailing_row.push(InlineKeyboardButton::callback("↺ .env", "cfg_trail:env"));
    InlineKeyboardMarkup::new(vec![
        vec![mode_btn],
        carriers_row,
        fee_row,
        vec![
            InlineKeyboardButton::callback(mark(padding_on, "📦 Buffer 95 KB".into()), "cfg_buffer:94500"),
            InlineKeyboardButton::callback(mark(!padding_on, "📦 Buffer 0 KB".into()), "cfg_buffer:0"),
        ],
        vec![
            InlineKeyboardButton::callback("🎯 TP: +25%", "cfg_tp:25.0"),
            InlineKeyboardButton::callback("🎯 TP: +35%", "cfg_tp:35.0"),
            InlineKeyboardButton::callback("🎯 TP: +50%", "cfg_tp:50.0"),
        ],
        vec![
            InlineKeyboardButton::callback("⏱ Exit: 30s", "cfg_exit:30"),
            InlineKeyboardButton::callback("⏱ Exit: 45s", "cfg_exit:45"),
            InlineKeyboardButton::callback("⏱ Exit: 60s", "cfg_exit:60"),
        ],
        trailing_row,
        back("menu_main", "🔙 Kembali ke Menu Utama"),
    ])
}

pub fn treasury_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("🔑 Buat Akun Induk Baru", "tr_new_wallet"),
            InlineKeyboardButton::callback("📥 Impor Mnemonic", "tr_import_wallet"),
        ],
        CARRIER_CHOICES
            .iter()
            .map(|n| InlineKeyboardButton::callback(format!("👥 {} Anak", n), format!("tr_sub:{}", n)))
            .collect(),
        vec![InlineKeyboardButton::callback("⛽ Rebalance Saldo Operasional", "tr_rebalance")],
        vec![InlineKeyboardButton::callback("🔄 Refresh Saldo", "menu_treasury")],
        back("menu_main", "🔙 Kembali ke Menu Utama"),
    ])
}

pub fn new_wallet_confirm_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("⚠️ Ya, ganti akun induk", "tr_new_wallet_confirm")],
        back("menu_treasury", "🔙 Batal"),
    ])
}

pub fn signals_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("➕ Tambah Alamat", "sig_add"),
            InlineKeyboardButton::callback("➖ Hapus Alamat", "sig_del_menu"),
        ],
        vec![InlineKeyboardButton::callback("📋 Lihat Daftar Pantauan", "sig_list")],
        back("menu_main", "🔙 Kembali ke Menu Utama"),
    ])
}

pub fn models_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("⚡ Seri Prysm (Fast-Path Pattern)", "menu_prysm")],
        back("menu_main", "🔙 Kembali ke Menu Utama"),
    ])
}

pub fn prysm_keyboard(is_enabled: bool) -> InlineKeyboardMarkup {
    let toggle = if is_enabled { "🔴 NONAKTIFKAN MODEL" } else { "🟢 AKTIFKAN MODEL" };
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(toggle, "prysm_toggle")],
        vec![
            InlineKeyboardButton::callback("✏️ Ubah Modal (ETH)", "prysm_set_size"),
            InlineKeyboardButton::callback("🔄 Ganti Mode Exit", "prysm_toggle_exit"),
        ],
        vec![InlineKeyboardButton::callback("📊 Statistik & Riwayat Match", "prysm_stats")],
        back("menu_main", "🔙 Kembali ke Menu Utama"),
    ])
}

pub fn back_keyboard(to: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![back(to, "🔙 Kembali")])
}

pub fn telemetry_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("🔄 Refresh", "menu_telemetry")],
        back("menu_main", "🔙 Kembali ke Menu Utama"),
    ])
}

// ───────────────────────────── screens ─────────────────────────────

fn execution_line(v: &SettingsView) -> String {
    let fee = match v.preset {
        Some(p) => format!("{} ({:.1}G / {:.1}G)", p.label(), v.priority_gwei, v.max_fee_gwei),
        None => format!("Kustom ({:.1}G / {:.1}G)", v.priority_gwei, v.max_fee_gwei),
    };
    format!(
        "{} · {} carrier aktif · padding {} · fee {}",
        v.mode,
        v.active_carriers(),
        kb(v.master_padding_bytes().max(v.carrier_padding_bytes())),
        e(&fee)
    )
}

fn mirror_line(s: &SystemSnapshot) -> String {
    match &s.mirror {
        None => "🧮 <b>Curve mirror:</b> tidak terpasang (valuasi via RPC)".to_string(),
        Some(m) if m.disabled => "🧮 <b>Curve mirror:</b> 🔴 NONAKTIF (fallback RPC penuh)".to_string(),
        Some(m) => {
            let fresh = m.curves.iter().filter(|c| matches!(c.read, crate::risk::curve_mirror::MirrorRead::Fresh(_))).count();
            let total = m.curves.len();
            let mode = if total == 0 {
                "🟢 siap (belum ada kurva dilacak)".to_string()
            } else if fresh == total {
                format!("🟢 ZERO-RPC ({}/{} kurva lokal)", fresh, total)
            } else if fresh == 0 {
                format!("🟠 FALLBACK RPC ({}/{} kurva lokal)", fresh, total)
            } else {
                format!("🟡 CAMPURAN ({}/{} kurva lokal, sisanya RPC)", fresh, total)
            };
            format!(
                "🧮 <b>Curve mirror:</b> {}\n   head <code>#{}</code> · penomoran {}",
                mode,
                m.head,
                if m.numbering_verified { "terverifikasi" } else { "belum terverifikasi" }
            )
        }
    }
}

fn feeds_block(s: &SystemSnapshot) -> String {
    if s.feeds.is_empty() {
        return "📡 <b>Feed racing:</b> <i>belum ada data feed</i>\n".to_string();
    }
    let mut out = format!(
        "📡 <b>Feed racing:</b> {}/{} live · gap {} lewat / {} pulih\n",
        s.live_feeds,
        s.feeds.len(),
        s.gaps.0,
        s.gaps.1
    );
    for f in &s.feeds {
        out.push_str(&format!(
            "• {} <code>{}</code> menang {:.1}% · RTT {} · lag {}\n",
            if f.stats.connected { "🟢" } else { "🔴" },
            e(&f.stats.label),
            f.win_pct,
            ms(f.stats.rtt_us),
            ms(f.stats.avg_lag_us)
        ));
    }
    out
}

fn position_line(p: &PositionView) -> String {
    let src = match p.in_memory {
        Some(true) => "🧮 lokal",
        Some(false) => "🌐 RPC",
        None => "🌐 RPC",
    };
    let pos = &p.position;
    format!(
        "• <b>{}</b> <code>{}</code> · {} → {} · {}",
        e(&pos.token_symbol),
        signed_pct(pos.pnl_percent),
        eth(Some(pos.entry_eth_amount)),
        eth(Some(pos.current_eth_value)),
        src
    )
}

pub fn dashboard(s: &SystemSnapshot) -> String {
    let mut out = format!(
        "🤖 <b>AUTOTRADE SNIPER — DASHBOARD</b>\n{RULE}\n\
         ⚡ <b>Intake:</b> {}\n\
         ⛓️ <b>Chain ID:</b> <code>{}</code>\n\
         ⚙️ <b>Eksekusi:</b> {}\n\n",
        if s.breaker.is_some() {
            "🚨 CIRCUIT BREAKER TERKUNCI (intake mati)"
        } else if s.engine_running {
            "🟢 AKTIF"
        } else {
            "🔴 PAUSE"
        },
        s.chain_id,
        execution_line(&s.settings)
    );

    out.push_str("💼 <b>Saldo</b>\n");
    if s.accounts.is_empty() {
        out.push_str("<i>saldo tidak terbaca</i>\n");
    }
    for a in &s.accounts {
        let inactive = a.index > 0 && a.index > s.settings.active_carriers();
        out.push_str(&format!(
            "• {} <code>{}</code>: <code>{}</code>{}{}\n",
            role_label(a.index),
            short(&a.address),
            eth(a.balance_eth),
            if a.low { " ⚠️ rendah" } else { "" },
            if inactive { " <i>(tidak aktif)</i>" } else { "" }
        ));
    }
    out.push('\n');
    out.push_str(&feeds_block(s));
    out.push_str(&mirror_line(s));
    out.push('\n');
    if let Some((enabled, staged, stats)) = &s.presign {
        out.push_str(&format!(
            "🧷 <b>Pre-sign AOT:</b> {} · {} batch siap · {} paket di muka · {} re-sign proaktif\n",
            if *enabled { "ON" } else { "OFF" },
            staged,
            stats.packets_signed_ahead,
            stats.proactive_resigns
        ));
    }
    out.push('\n');
    out.push_str(&format!("📈 <b>Posisi terbuka ({})</b>\n", s.open.len()));
    if s.open.is_empty() {
        out.push_str("<i>tidak ada posisi terbuka</i>\n");
    }
    for p in &s.open {
        out.push_str(&position_line(p));
        out.push('\n');
    }
    out.push_str(&format!(
        "🛡️ exit berjalan: {} · entri di jalan: {} · order Prysm antre: {} · watchlist: {}\n",
        s.exits_in_flight, s.entries_in_flight, s.prysm_pending, s.watchlist
    ));
    if let Some(o) = &s.outbox {
        let lost = o.dropped_full + o.expired + o.failed;
        out.push_str(&format!("📬 notifikasi: {} terkirim · {} digabung · {} hilang\n", o.sent, o.coalesced, lost));
    }
    out.push_str(&format!("{RULE}\n<i>Panic Sell langsung menghentikan intake dan menjual semua posisi.</i>"));
    out
}

pub fn telemetry(s: &SystemSnapshot) -> String {
    let mut out = format!("📡 <b>TELEMETRI ENGINE</b>\n{RULE}\n");
    out.push_str(&feeds_block(s));
    for f in &s.feeds {
        out.push_str(&format!(
            "   <code>{}</code>: {} frame · {} menang · {} duplikat · {} sesi · {} parse error\n",
            e(&f.stats.label),
            f.stats.frames,
            f.stats.wins,
            f.stats.dups,
            f.stats.sessions,
            f.stats.parse_errors
        ));
    }
    out.push('\n');
    out.push_str(&mirror_line(s));
    out.push('\n');
    if let Some(m) = &s.mirror {
        for c in &m.curves {
            let state = match &c.read {
                crate::risk::curve_mirror::MirrorRead::Fresh(snap) => format!(
                    "🟢 lokal · anchor <code>#{}</code> +{} trade · valid s/d <code>#{}</code>",
                    snap.anchor_block, snap.unconfirmed_trades, snap.validated_through
                ),
                crate::risk::curve_mirror::MirrorRead::Stale(r) => format!("🟠 RPC ({})", e(&format!("{:?}", r))),
            };
            out.push_str(&format!("   <code>{}</code> {}\n", short(&c.curve), state));
        }
        let st = &m.stats;
        out.push_str(&format!(
            "   baca: {} lokal / {} fallback · trade {} · anchor {} ok / {} gagal\n   koreksi drift {} (maks {} ppb) · reorg {} · gap {} · exit terkonfirmasi {}\n",
            st.reads_fresh,
            st.reads_stale,
            st.trades_applied,
            st.anchors_ok,
            st.anchors_failed,
            st.drift_corrections,
            st.max_drift_ppb,
            st.reorgs,
            st.gaps,
            st.exit_confirmations
        ));
    }
    if let Some((enabled, staged, p)) = &s.presign {
        out.push_str(&format!(
            "\n🧷 <b>Pre-sign AOT</b> ({})\n   batch siap {} · armed {} · klaim {} · paket di muka {}\n   terkirim pre-built {} · re-sign basi {} · re-sign proaktif {}\n   lease dilepas {} · dibatalkan {} · kedaluwarsa {}\n",
            if *enabled { "ON" } else { "OFF" },
            staged,
            p.armed,
            p.claimed,
            p.packets_signed_ahead,
            p.prebuilt_sent,
            p.resigned_stale,
            p.proactive_resigns,
            p.leases_released_unused,
            p.cancelled,
            p.expired
        ));
    }
    if let Some(o) = &s.outbox {
        out.push_str(&format!(
            "\n📬 <b>Outbox Telegram</b>\n   antre {} · terkirim {} · digabung {} · retry {}\n   penuh {} · kedaluwarsa {} · gagal {} · disanitasi {}\n",
            o.enqueued, o.sent, o.coalesced, o.retries, o.dropped_full, o.expired, o.failed, o.sanitized
        ));
    }
    out.push_str(RULE);
    out
}

pub fn settings(v: &SettingsView) -> String {
    let fee = match v.preset {
        Some(p) => format!("{} (tip {:.1}G, maks {:.1}G)", p.label(), v.priority_gwei, v.max_fee_gwei),
        None => format!("Kustom (tip {:.1}G, maks {:.1}G)", v.priority_gwei, v.max_fee_gwei),
    };
    format!(
        "⚙️ <b>EXECUTION &amp; BLOCK-BOOKING SETTINGS</b>\n{RULE}\n\
         • <b>Mode Eksekusi:</b> {}\n\
         • <b>Carrier Anak:</b> {} dipilih, {} aktif\n\
         • <b>Padding carrier:</b> {}\n\
         • <b>Padding master:</b> {}\n\
         • <b>Fee:</b> {}\n\
         • <b>Target Profit:</b> +{:.1}%\n\
         • <b>Trailing Stop:</b> -{:.1}% dari puncak ({})\n\
         • <b>Time-Decay Exit:</b> {} detik\n\
         {RULE}\n\
         <i>Setiap perubahan langsung berlaku: batch pre-sign yang sudah siap dibentuk ulang dan ditandatangani ulang sebelum blok target. Tip preset adalah minimum: engine hanya menawar lebih tinggi bila jaringan melaporkan tip lebih tinggi.</i>",
        v.mode,
        v.carriers,
        v.active_carriers(),
        kb(v.carrier_padding_bytes()),
        kb(v.master_padding_bytes()),
        e(&fee),
        v.target_profit_percent,
        v.trailing_stop_percent,
        if v.trailing_from_runtime { "diatur via Telegram" } else { "dari .env" },
        v.time_decay_exit_secs
    )
}

pub fn apply_report(r: &ApplyReport) -> String {
    let title = if r.aborted {
        "❌ <b>PERUBAHAN DIBATALKAN</b>"
    } else if r.pending {
        "⏳ <b>MASIH DIPROSES ENGINE</b>"
    } else if r.unchanged {
        "ℹ️ <b>TIDAK ADA PERUBAHAN</b>"
    } else {
        "✅ <b>SETELAN DITERAPKAN</b>"
    };
    let mut out = format!("{title}\n");
    for c in &r.changes {
        out.push_str(&format!("• {}\n", e(c)));
    }
    let p = &r.presign;
    if p.batches > 0 {
        out.push_str(&format!(
            "🧷 Pre-sign: {} batch dibentuk ulang · -{} slot · +{} slot · {} ditandatangani ulang\n",
            p.batches, p.slots_released, p.slots_added, p.slots_resigned
        ));
    }
    if r.nonces_synced > 0 {
        out.push_str(&format!("🔢 Nonce {} carrier disinkron dari chain\n", r.nonces_synced));
    }
    for w in &r.warnings {
        out.push_str(&format!("⚠️ {}\n", e(w)));
    }
    out
}

pub fn portfolio(s: &SystemSnapshot) -> String {
    let total: f64 = s.accounts.iter().filter_map(|a| a.balance_eth).sum();
    let unknown = s.accounts.iter().filter(|a| a.balance_eth.is_none()).count();
    let mut out = format!(
        "📈 <b>PORTFOLIO &amp; PnL</b>\n{RULE}\n💰 <b>Total saldo treasury:</b> <code>{:.4} ETH</code>{}\n\n",
        total,
        if unknown > 0 { format!(" <i>({} akun tidak terbaca)</i>", unknown) } else { String::new() }
    );
    let invested: f64 = s.open.iter().map(|p| p.position.entry_eth_amount).sum();
    let value: f64 = s.open.iter().map(|p| p.position.current_eth_value).sum();
    out.push_str(&format!("📊 <b>Posisi terbuka ({})</b>\n", s.open.len()));
    for p in &s.open {
        out.push_str(&position_line(p));
        out.push('\n');
    }
    if !s.open.is_empty() {
        let pnl = if invested > 0.0 { (value / invested - 1.0) * 100.0 } else { 0.0 };
        out.push_str(&format!(
            "Σ modal <code>{:.4}</code> → nilai <code>{:.4} ETH</code> (<code>{}</code>)\n",
            invested,
            value,
            signed_pct(pnl)
        ));
    } else {
        out.push_str("<i>tidak ada posisi terbuka</i>\n");
    }
    out.push_str(&format!("\n<b>Riwayat terakhir</b> ({} posisi ditutup)\n", s.closed_count));
    if s.recent_closed.is_empty() {
        out.push_str("<i>belum ada riwayat posisi</i>\n");
    }
    for p in &s.recent_closed {
        out.push_str(&format!(
            "⚪ <b>{}</b> <code>{}</code> · {}\n",
            e(&p.token_symbol),
            signed_pct(p.pnl_percent),
            e(p.exit_reason.as_deref().unwrap_or("Closed"))
        ));
    }
    out.push_str(RULE);
    out
}

pub fn treasury(s: &SystemSnapshot) -> String {
    let mut out = format!("💼 <b>TREASURY &amp; WALLETS</b>\n{RULE}\n");
    if s.accounts.is_empty() {
        out.push_str("<i>akun tidak bisa diturunkan / saldo tidak terbaca</i>\n");
    }
    for a in &s.accounts {
        let active = a.index == 0 || a.index <= s.settings.active_carriers();
        out.push_str(&format!(
            "• <b>{}</b> {}\n  <code>{:?}</code>\n  saldo <code>{}</code>{}\n",
            e(&a.role),
            if active { "🟢" } else { "⚪" },
            a.address,
            eth(a.balance_eth),
            if a.low { " ⚠️ di bawah minimum" } else { "" }
        ));
    }
    out.push_str(&format!("{RULE}\n<i>Carrier aktif: {}. Rebalance mengisi carrier dari Ayah.</i>", s.settings.active_carriers()));
    out
}

pub fn new_wallet_confirm(blocked: Option<&str>) -> String {
    match blocked {
        Some(why) => format!(
            "⛔ <b>AKUN INDUK TIDAK BISA DIGANTI</b>\n{RULE}\nAlasan: {}\n<i>Tutup / batalkan dulu semuanya: token di dompet lama tidak bisa dijual dari dompet baru.</i>",
            e(why)
        ),
        None => format!(
            "⚠️ <b>GANTI AKUN INDUK?</b>\n{RULE}\nMnemonic baru akan menggantikan Ayah dan semua Anak. Dana di alamat lama tetap di sana dan hanya bisa diakses dengan mnemonic lama.\n{RULE}"
        ),
    }
}

pub fn new_wallet_created(master: &Address, phrase: &str) -> String {
    format!(
        "🔑 <b>AKUN INDUK BARU DIBUAT</b>\n{RULE}\n<b>Alamat Ayah:</b>\n<code>{:?}</code>\n\n<b>Mnemonic (ketuk untuk membuka):</b>\n<tg-spoiler>{}</tg-spoiler>\n\n⚠️ <i>Simpan frasa ini secara offline lalu hapus pesan ini. Semua Anak diturunkan dari frasa ini.</i>",
        master,
        e(phrase)
    )
}

pub fn rebalance_result(results: &[GasRebalanceResult]) -> String {
    let mut out = format!("⛽ <b>HASIL REBALANCE OPERASIONAL</b>\n{RULE}\n");
    if results.is_empty() {
        out.push_str("Seluruh carrier sudah di atas ambang saldo minimum.\n");
    }
    for r in results {
        out.push_str(&format!(
            "{} <code>{}</code>: <code>{:.4} ETH</code>\n  TX <code>{}</code>\n",
            if r.success { "✅" } else { "❌" },
            e(&r.sub_account_address),
            r.amount_eth,
            e(&r.tx_hash)
        ));
    }
    out.push_str(RULE);
    out
}

pub fn breaker_started(open_positions: usize) -> String {
    format!(
        "🚨 <b>CIRCUIT BREAKER AKTIF</b>\n{RULE}\n\
         ⚡ Intake: 🔴 BERHENTI (seketika)\n\
         🧹 Antrean order Prysm dan batch pre-sign: dibatalkan\n\
         💥 Likuidasi: {} posisi terbuka dijual lewat exit receipt-confirmed (slippage darurat)\n\
         {RULE}\n<i>Laporan per posisi dikirim begitu semua penjualan terkonfirmasi.</i>",
        open_positions
    )
}

pub fn breaker_report(o: &BreakerOutcome) -> String {
    let r = &o.report;
    let mut out = format!(
        "🚨 <b>LAPORAN CIRCUIT BREAKER</b>\n{RULE}\n\
         ⏱️ Selesai dalam <code>{:.1} s</code> · {} posisi saat dipicu\n\
         🧹 {} order Prysm dibatalkan · {} batch pre-sign dilepas\n",
        r.elapsed.as_secs_f64(),
        r.positions,
        o.prysm_orders_dropped,
        o.presign_batches_released
    );
    if o.entries_in_flight > 0 {
        out.push_str(&format!(
            "🛫 {} entri sedang dikirim saat breaker: otomatis dijual begitu terdaftar (notifikasi exit menyusul)\n",
            o.entries_in_flight
        ));
    }
    if !r.sold.is_empty() {
        out.push_str(&format!("\n✅ <b>Terjual &amp; terkonfirmasi ({})</b>\n", r.sold.len()));
        for (_, sym, tx) in &r.sold {
            out.push_str(&format!("• <b>{}</b> TX <code>{}</code>\n", e(sym), e(tx)));
        }
    }
    if !r.nothing_held.is_empty() {
        out.push_str(&format!("\n⚪ <b>Ditutup tanpa jual (saldo token 0) ({})</b>\n", r.nothing_held.len()));
        for (_, sym) in &r.nothing_held {
            out.push_str(&format!("• <b>{}</b>\n", e(sym)));
        }
    }
    if !r.already_exiting.is_empty() {
        out.push_str(&format!("\n⏳ <b>Exit lain sedang berjalan ({})</b>\n", r.already_exiting.len()));
        for (_, sym) in &r.already_exiting {
            out.push_str(&format!("• <b>{}</b>\n", e(sym)));
        }
    }
    if !r.still_open.is_empty() {
        out.push_str(&format!("\n❌ <b>MASIH TERBUKA ({})</b>: watchdog terus mencoba\n", r.still_open.len()));
        for (_, sym, why) in &r.still_open {
            out.push_str(&format!("• <b>{}</b>: {}\n", e(sym), e(why)));
        }
    }
    if r.positions == 0 {
        out.push_str("\n<i>Tidak ada posisi terbuka saat breaker dipicu.</i>\n");
    }
    out.push_str(&format!(
        "{RULE}\n<i>Breaker tetap terkunci: intake hanya bisa aktif lagi lewat tombol 🔓 setelah likuidasi selesai.</i>"
    ));
    out
}

pub fn pause_result(r: &ApplyReport, running: bool) -> String {
    let head = if running {
        "▶️ <b>ENGINE INTAKE AKTIF</b>".to_string()
    } else {
        "⏸ <b>ENGINE INTAKE PAUSE</b>\n🛡️ Supervisi risiko tetap mengawal posisi terbuka.".to_string()
    };
    format!("{}\n{RULE}\n{}", head, apply_report(r))
}

pub fn sweep(open: &[Position]) -> String {
    let mut out = format!(
        "🧹 <b>SWEEP ASET</b>\n{RULE}\n<i>Sweep token residual belum diimplementasikan: tombol ini tidak mengirim transaksi.</i>\n\n<b>Posisi yang tercatat ({}):</b>\n",
        open.len()
    );
    if open.is_empty() {
        out.push_str("<i>tidak ada</i>\n");
    }
    for p in open {
        out.push_str(&format!("• <b>{}</b> <code>{}</code>\n", e(&p.token_symbol), e(&p.token_address)));
    }
    out.push_str(&format!("{RULE}\n<i>Untuk menjual semua posisi sekarang: 🚨 PANIC SELL ALL.</i>"));
    out
}

pub fn watchlist(addresses: &[Address]) -> String {
    let mut out = format!("📋 <b>DAFTAR AKUN PANTAUAN</b>\n{RULE}\n");
    if addresses.is_empty() {
        out.push_str("<i>Belum ada alamat dalam daftar pantauan.</i>\n");
    }
    for (i, a) in addresses.iter().enumerate() {
        out.push_str(&format!("{}. <code>{:?}</code>\n", i + 1, a));
    }
    out.push_str(RULE);
    out
}

pub fn signals_menu(count: usize) -> String {
    format!("🎯 <b>SIGNAL ACCOUNTS MANAGER</b>\n{RULE}\nSaat ini ada <b>{}</b> alamat pantauan aktif.\nPilih aksi di bawah:", count)
}

pub fn prysm_dashboard(v: &PrysmView) -> String {
    format!(
        "🔬 <b>STRATEGI: SERI PRYSM (FAST-PATH PATTERN)</b>\n{RULE}\n\
         • <b>Status Model</b>: {}\n\
         • <b>Modal Entri</b>: <code>{:.4} ETH</code>\n\
         • <b>Mode Exit</b>: {}\n\
         • <b>Pola Aktif</b>: A (Prysmcloud), B (.lat), C (Companion)\n\
         • <b>Max MCap Entri</b>: ${:.0} USD\n\
         • <b>Order antre</b>: {}\n\
         {RULE}\n\
         <i>Sistem mengevaluasi metadata calldata peluncuran token secara mandiri.</i>",
        if v.enabled { "AKTIF 🟢" } else { "NONAKTIF 🔴" },
        v.size_eth,
        e(&v.exit_mode.to_string()),
        v.max_mcap_usd,
        v.pending
    )
}

pub fn prysm_stats(c: &PrysmCounts, recent: &[PrysmMatchReport], max_mcap_usd: f64) -> String {
    let mut recent_str = String::new();
    for (i, r) in recent.iter().enumerate() {
        recent_str.push_str(&format!(
            "{}. <b>{}</b> <code>{:?}</code> ({})\n   <i>{}</i>\n",
            i + 1,
            e(&r.pattern.to_string()),
            r.token_address,
            if r.guardrails_passed { "✅ Eksekusi" } else { "❌ Ditolak" },
            e(&r.reason)
        ));
    }
    if recent_str.is_empty() {
        recent_str = "<i>Belum ada riwayat kecocokan pola.</i>\n".to_string();
    }
    format!(
        "📊 <b>STATISTIK &amp; RIWAYAT MATCH SERI PRYSM</b>\n{RULE}\n\
         🔍 <b>Transaksi dipindai:</b> {}\n\
         🎯 <b>Pola A (Prysmcloud):</b> {} match\n\
         🌐 <b>Pola B (.lat / 0.021 ETH):</b> {} match\n\
         👥 <b>Pola C (Companion):</b> {} match\n\n\
         🚀 <b>Order tereksekusi:</b> {}\n\
         ⏱️ <b>Ditolak (sinyal basi &gt; 15 s):</b> {}\n\
         📈 <b>Ditolak (MCap &gt; ${:.0}):</b> {}\n\
         {RULE}\n<b>Riwayat terakhir:</b>\n{}{RULE}",
        c.evaluated, c.a, c.b, c.c, c.executed, c.rejected_stale, max_mcap_usd, c.rejected_mcap, recent_str
    )
}

// ───────────────────────────── alerts ─────────────────────────────

pub fn signal_alert(signal: &TradeSignal) -> String {
    format!(
        "🔔 <b>SINYAL TERDETEKSI</b>{}\n\
         👤 Akun: <code>{:?}</code>\n\
         🪙 Token: <code>{:?}</code>\n\
         🏊 Pool: <code>{:?}</code>\n\
         💵 Entry: <code>{} wei</code> · blok <code>#{}</code>",
        signal.strategy_tag.as_deref().map(|t| format!(" · {}", e(t))).unwrap_or_default(),
        signal.monitored_account,
        signal.target_token,
        signal.pool_or_router,
        signal.entry_amount_wei,
        signal.block_number
    )
}

pub fn dispatch_alert(report: &DispatchReport) -> String {
    let mut txs = String::new();
    for (i, hash) in report.tx_hashes.iter().enumerate() {
        let role = report.account_roles.get(i).map(|s| s.as_str()).unwrap_or("Account");
        txs.push_str(&format!("• {}: <code>{}</code>\n", e(role), e(hash)));
    }
    format!(
        "🚀 <b>DISPATCH BERHASIL</b> · blok <code>#{}</code>\n\
         ⚙️ {} · calldata <code>{} B</code> · gas <code>{}</code> · tip <code>{:.2}G</code>\n\
         ⚡ latensi internal <code>{} µs</code> · modal <code>{:.4} ETH</code>\n{}",
        report.block_number,
        report.execution_mode,
        report.calldata_size_bytes,
        report.gas_limit,
        report.priority_fee_gwei,
        report.latency_micros,
        report.trade_eth,
        txs.trim_end()
    )
}

pub fn position_alert(pos: &Position) -> String {
    // An exit alert for a position that is still open means the liquidation gave up.
    let title = if pos.is_open { "⚠️ <b>EXIT GAGAL: TOKEN MASIH DI WALLET</b>" } else { "📊 <b>POSISI DITUTUP</b>" };
    format!(
        "{title}\n\
         🪙 <b>{}</b> · PnL <code>{}</code>\n\
         💵 <code>{:.4}</code> → <code>{:.4} ETH</code>\n\
         📌 {}\n\
         📝 TX: <code>{}</code>",
        e(&pos.token_symbol),
        signed_pct(pos.pnl_percent),
        pos.entry_eth_amount,
        pos.current_eth_value,
        e(pos.exit_reason.as_deref().unwrap_or("Manual Close")),
        e(pos.exit_tx_hash.as_deref().unwrap_or("tidak ada (tidak ada jual on-chain)"))
    )
}
