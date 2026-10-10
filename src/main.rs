mod config;
mod dispatcher;
mod risk;
mod stream;
mod telegram;
mod types;
mod wallet;

use config::AppConfig;
use dispatcher::Dispatcher as EngineDispatcher;
use risk::RiskManager;
use stream::StreamListener;
use telegram::TelegramBot;
use types::{DispatchReport, Position, TradeSignal};
use wallet::WalletManager;

use anyhow::Result;
use std::sync::Arc;
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
║   Payload: ~94,500 Byte Calldata Buffer | Static Gas: 600,000 gas     ║
║   Architecture: HD Master Treasury & Deterministic Satellite Sub-Accts║
╚═══════════════════════════════════════════════════════════════════════╝
"#
    );

    // 1. Load application configuration
    let config = AppConfig::load_from_env()?;
    info!("Target Chain ID: {}", config.chain_id);
    info!("RPC HTTP Endpoint: {}", config.rpc_http_url);
    info!("RPC WebSocket Feed: {}", config.rpc_ws_url);

    // 2. Initialize Hierarchical Deterministic Wallet Manager
    let wallet_manager = WalletManager::new(config.clone()).await?;
    let master = wallet_manager.get_master_account().await?;
    info!("Master Account (Derivation m/44'/60'/0'/0/0): {:?}", master.address);

    let sub_accounts = wallet_manager.get_sub_accounts().await?;
    for sub in &sub_accounts {
        info!("Satellite Sub-Account #{} ({}): {:?}", sub.index, sub.derivation_path, sub.address);
    }

    // 3. Shared Positions State & Channels
    let positions = Arc::new(RwLock::new(Vec::<Position>::new()));

    let (signal_tx, mut signal_rx) = mpsc::channel::<TradeSignal>(100);
    let (report_tx, mut report_rx) = mpsc::channel::<DispatchReport>(100);
    let (exit_alert_tx, mut exit_alert_rx) = mpsc::channel::<Position>(100);

    // 4. Initialize Core Modules
    let dispatcher = Arc::new(EngineDispatcher::new(
        config.clone(),
        wallet_manager.clone(),
        report_tx,
    ));

    let risk_manager = Arc::new(RiskManager::new(
        config.clone(),
        wallet_manager.clone(),
        positions.clone(),
        exit_alert_tx,
    ));

    let stream_listener = Arc::new(StreamListener::new(
        config.clone(),
        signal_tx,
    ));

    let authorized_chat_id = config.telegram_authorized_chat_id;
    let bot_token = config.telegram_bot_token.clone();

    // 5. Initialize Telegram Bot Controller (if configured)
    let telegram_bot = if !bot_token.is_empty() && !bot_token.contains("Example") {
        Some(Arc::new(TelegramBot::new(
            bot_token.clone(),
            config.clone(),
            wallet_manager.clone(),
            positions.clone(),
        )))
    } else {
        warn!("Telegram Bot token not provided or is example. Running in terminal headless mode.");
        None
    };

    // Task A: Signal Consumer & Dispatch Executor Loop
    let dispatcher_clone = dispatcher.clone();
    let risk_clone = risk_manager.clone();
    let telegram_clone = telegram_bot.clone();
    let config_clone = config.clone();

    tokio::spawn(async move {
        while let Some(signal) = signal_rx.recv().await {
            info!("Received Signal for replication from {:?}", signal.monitored_account);

            // Broadcast alert to Telegram
            if let Some(ref bot_ctrl) = telegram_clone {
                bot_ctrl.broadcast_signal_detected(authorized_chat_id, &signal).await;
            }

            // Execute Batch-Sequenced Dispatch
            match dispatcher_clone.dispatch_signal(signal.clone()).await {
                Ok(_report) => {
                    let trade_eth = *config_clone.default_trade_eth.read().await;
                    // Register position into Risk Manager for profit realization & time decay exit
                    risk_clone.register_entry(
                        signal.target_token,
                        signal.pool_or_router,
                        trade_eth,
                    ).await;
                }
                Err(e) => {
                    error!("Error during signal dispatch: {}", e);
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

    // Task D: Risk Manager Lifecycle Monitor
    let risk_monitor = risk_manager.clone();
    tokio::spawn(async move {
        risk_monitor.start_lifecycle_monitor().await;
    });

    // Task E: Sequencer Stream Listener Loop
    let stream_runner = stream_listener.clone();
    tokio::spawn(async move {
        if let Err(e) = stream_runner.run().await {
            error!("Stream listener loop terminated: {}", e);
        }
    });

    // Task F: Telegram Bot Polling (if bot is configured)
    if let Some(bot_ctrl) = telegram_bot {
        let bot_instance = Bot::new(bot_token);
        let bot_ctrl_for_msg = bot_ctrl.clone();
        let bot_ctrl_for_cb = bot_ctrl.clone();

        info!("Starting interactive Telegram Bot listener...");

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
        info!("Shutting down engine gracefully.");
    }

    Ok(())
}
