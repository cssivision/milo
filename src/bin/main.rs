use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use log::info;
use milo::{
    agent::RigAgent,
    config::{Config, GatewayConfig},
    gateway::{GatewayRuntime, WecomBot, serve_http},
};

#[derive(Debug, Parser)]
#[command(about = "WeCom Bot backed by a Rig agent")]
struct Args {
    /// TOML configuration file.
    #[arg(short = 'c', long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::from_file(&args.config)?;
    let log_level = config
        .log
        .level
        .parse()
        .with_context(|| "log.level must be one of off, error, warn, info, debug, or trace")?;
    env_logger::Builder::new().filter_level(log_level).init();

    let gateway_count = config.gateway.len();
    let agent = RigAgent::new(config.model, config.agent);
    let gateway = GatewayRuntime::new(agent);
    let mut tasks = tokio::task::JoinSet::new();

    info!(
        "starting milo: config={}, gateways={gateway_count}",
        args.config.display()
    );
    for config in config.gateway {
        let gateway = gateway.clone();
        match config {
            GatewayConfig::Wecom(config) => {
                tasks.spawn(async move {
                    WecomBot::new(config, gateway).run_forever().await;
                    Ok::<(), anyhow::Error>(())
                });
            }
            GatewayConfig::Http(config) => {
                tasks.spawn(async move { serve_http(config, gateway).await });
            }
        }
    }
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            info!("shutdown signal received");
        }
        result = tasks.join_next() => match result {
            Some(Ok(Err(error))) => return Err(error),
            Some(Err(error)) => return Err(error.into()),
            Some(Ok(Ok(()))) | None => return Ok(()),
        },
    }
    Ok(())
}
