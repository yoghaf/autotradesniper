use autotradesniper::config::AppConfig;
use autotradesniper::dispatcher::Dispatcher as EngineDispatcher;
use autotradesniper::models::PrysmEngine;
use autotradesniper::payload::DynamicFeeAdjuster;
use autotradesniper::risk::RiskManager;
use autotradesniper::stream::StreamListener;
use autotradesniper::telegram::TelegramBot;
use autotradesniper::types::{DispatchReport, Position, TradeSignal};
use autotradesniper::wallet::WalletManager;

use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use teloxide::prelude::*;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing logger
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "autotradesniper=info,tower=warn".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    println!(
        r#"
╔═══════════════════════════════════════════════════════════════════════╗
║   HIGH-FREQUENCY ON-CHAIN SIGNAL REPLICATOR & BATCH-SEQUENCING ENGINE ║
║   Network: Arbitrum Nitro / Robinhood Chain (Chain ID: 4663)          ║
║   Latency: < 100 µs Internal Compute | Zero-Copy & Pre-computed Nonce ║
║   Architecture: HD Master Treasury & Deterministic Satellite Sub-Accts║
╚═══════════════════════════════════════════════════════════════════════╝
"#
    );

    // 1. Load application configuration
    let config = AppConfig::load_from_env()?;
    info!("Target Chain ID: {}", config.chain_id);
    info!("RPC HTTP Endpoint: {}", config.rpc_http_url);
    info!("RPC WebSocket Feed: {}", config.rpc_ws_url);

    // 2. Initialize Dynamic Fee Adjuster (EMA tracking)
    let min_prio = *config.max_priority_fee_gwei.read().await;
    let fee_adjuster = Arc::new(DynamicFeeAdjuster::new(min_prio, 1.25));

    // 3. Initialize Hierarchical Deterministic Wallet Manager
    let wallet_manager = WalletManager::new(config.clone()).await?;
    let master = wallet_manager.get_master_account().await?;
    info!("Master Account (Derivation m/44'/60'/0'/0/0): {:?}", master.address);

    let sub_accounts = wallet_manager.get_sub_accounts().await?;
    for sub in &sub_accounts {
        info!("Satellite Sub-Account #{} ({}): {:?}", sub.index, sub.derivation_path, sub.address);
    }

    // Nonce cursors must be anchored before the first dispatch, or it goes out with nonce 0.
    match tokio::time::timeout(Duration::from_secs(10), wallet_manager.sync_initial_nonces()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("Initial nonce sync failed: {:#}. Cursors resync after the first failed broadcast.", e),
        Err(_) => warn!("Initial nonce sync timed out. Cursors resync after the first failed broadcast."),
    }

    // 4. Shared Positions State & Channels
    let positions = Arc::new(RwLock::new(Vec::<Position>::new()));

    let (signal_tx, mut signal_rx) = mpsc::channel::<TradeSignal>(1000);
    let (report_tx, mut report_rx) = mpsc::channel::<DispatchReport>(1000);
    let (exit_alert_tx, mut exit_alert_rx) = mpsc::channel::<Position>(1000);

    // 5. Initialize Core Modules with Optimized Components
    let dispatcher = Arc::new(EngineDispatcher::new(
        config.clone(),
        wallet_manager.clone(),
        fee_adjuster.clone(),
        report_tx,
    ));
    let keep_warm = dispatcher.broadcaster().spawn_keep_warm(Duration::from_secs(20));

    let risk_manager = Arc::new(RiskManager::new(
        config.clone(),
        wallet_manager.clone(),
        positions.clone(),
        exit_alert_tx,
    ));

    let prysm_engine = Arc::new(PrysmEngine::new(config.clone(), signal_tx.clone()));

    let stream_listener = Arc::new(StreamListener::new(
        config.clone(),
        fee_adjuster.clone(),
        prysm_engine.clone(),
        signal_tx,
    ));

    let authorized_chat_id = config.telegram_authorized_chat_id;
    let bot_token = config.telegram_bot_token.clone();

    // 6. Initialize Telegram Bot Controller (if configured)
    let telegram_bot = if !bot_token.is_empty() && !bot_token.contains("Example") {
        if authorized_chat_id == 0 {
            warn!("TELEGRAM_AUTHORIZED_CHAT_ID is not set: the bot will ignore every chat.");
        }
        Some(Arc::new(TelegramBot::new(
            bot_token.clone(),
            config.clone(),
            wallet_manager.clone(),
            risk_manager.clone(),
            prysm_engine.clone(),
            positions.clone(),
        )))
    } else {
        warn!("Telegram Bot token not provided or is example. Running in terminal headless mode.");
        None
    };

    // Task A: Signal Consumer & Pipelined Dispatch Executor Loop
    let dispatcher_clone = dispatcher.clone();
    let risk_clone = risk_manager.clone();
    let telegram_clone = telegram_bot.clone();

    let signal_task = tokio::spawn(async move {
        while let Some(signal) = signal_rx.recv().await {
            info!("Received Signal for replication from {:?}", signal.monitored_account);
            // Dispatch first: the Telegram alert (string formatting + enqueue) is off the hot path.
            let dispatch = dispatcher_clone.dispatch_signal(&signal).await;
            if let Some(ref bot_ctrl) = telegram_clone {
                bot_ctrl.broadcast_signal_detected(authorized_chat_id, &signal).await;
            }

            match dispatch {
                Ok(report) => {
                    // Size the position from what was actually sent, only after the master
                    // entry was accepted by an endpoint.
                    risk_clone
                        .register_entry(signal.target_token, signal.pool_or_router, report.trade_eth, signal.strategy_tag)
                        .await;
                }
                Err(e) => {
                    error!("Error during signal dispatch: {:#}", e);
                }
            }
        }
    });

    // Task B: Dispatch Report Broadcaster Loop
    let telegram_report_clone = telegram_bot.clone();
    tokio::spawn(async move {
        while let Some(report) = report_rx.recv().await {
            info!(
                "Dispatch Report: {} transactions at Block #{} in {} µs",
                report.tx_hashes.len(),
                report.block_number,
                report.latency_micros
            );
            if let Some(ref bot_ctrl) = telegram_report_clone {
                bot_ctrl.broadcast_dispatch_success(authorized_chat_id, &report).await;
            }
        }
    });

    // Task C: Position Exit / Realization Broadcaster Loop
    let telegram_exit_clone = telegram_bot.clone();
    tokio::spawn(async move {
        while let Some(pos) = exit_alert_rx.recv().await {
            info!(
                "Position Realized: {} PnL: {:.1}% ({})",
                pos.token_symbol,
                pos.pnl_percent,
                pos.exit_reason.as_deref().unwrap_or("Exit")
            );
            if let Some(ref bot_ctrl) = telegram_exit_clone {
                bot_ctrl.broadcast_position_realized(authorized_chat_id, &pos).await;
            }
        }
    });

    // Task D: Sub-Second Risk Manager Lifecycle Monitor (restarted if it ever panics)
    let risk_monitor = risk_manager.clone();
    let risk_task = supervise("risk-lifecycle-monitor", move || {
        let risk_monitor = risk_monitor.clone();
        async move { risk_monitor.start_lifecycle_monitor().await }
    });

    // Task E: High-Frequency Sequencer Stream Listener Loop (restarted if it ever panics)
    let stream_runner = stream_listener.clone();
    let stream_task = supervise("sequencer-stream", move || {
        let stream_runner = stream_runner.clone();
        async move {
            if let Err(e) = stream_runner.run().await {
                error!("Stream listener loop terminated: {:#}", e);
            }
        }
    });

    // Task F: Telegram Bot Polling (if bot is configured)
    if let Some(bot_ctrl) = telegram_bot {
        let bot_instance = Bot::new(bot_token);
        let bot_ctrl_for_msg = bot_ctrl.clone();
        let bot_ctrl_for_cb = bot_ctrl.clone();

        info!("Starting interactive Telegram Bot listener (Non-Blocking UI)...");

        let handler = dptree::entry()
            .branch(
                Update::filter_message().endpoint(move |msg: Message| {
                    let ctrl = bot_ctrl_for_msg.clone();
                    async move {
                        if let Err(e) = ctrl.handle_message(msg).await {
                            error!("Error handling message: {}", e);
                        }
                        respond(())
                    }
                }),
            )
            .branch(
                Update::filter_callback_query().endpoint(move |q: CallbackQuery| {
                    let ctrl = bot_ctrl_for_cb.clone();
                    async move {
                        if let Err(e) = ctrl.handle_callback(q).await {
                            error!("Error handling callback: {}", e);
                        }
                        respond(())
                    }
                }),
            );

        teloxide::dispatching::Dispatcher::builder(bot_instance, handler)
            .enable_ctrlc_handler()
            .build()
            .dispatch()
            .await;
    } else {
        info!("Running in headless server mode. Press Ctrl+C to terminate.");
        tokio::signal::ctrl_c().await?;
    }

    info!("Shutting down engine gracefully.");
    config.is_engine_active.store(false, std::sync::atomic::Ordering::SeqCst);
    for task in [stream_task, signal_task, risk_task, keep_warm] {
        task.abort();
    }

    Ok(())
}

/// Run `make()` forever, restarting it (after 1s) if it returns or panics.
///
/// A panic inside a plain `tokio::spawn` only kills that task: without supervision the engine
/// would keep serving Telegram while being blind to the feed.
fn supervise<F, Fut>(name: &'static str, make: F) -> tokio::task::JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            match tokio::spawn(make()).await {
                Ok(()) => warn!("Task '{}' exited; restarting in 1s", name),
                Err(e) if e.is_panic() => error!("Task '{}' panicked; restarting in 1s", name),
                Err(_) => return, // cancelled during shutdown
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}
