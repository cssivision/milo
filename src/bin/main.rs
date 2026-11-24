use std::{env, path::PathBuf};

use anyhow::{Context, Result};
use getopts::Options;
use log::info;
use milo::{
    agent::{RigAgent, profile::FileProfileStore},
    config::{Config, GatewayConfig},
    gateway::{GatewayRuntime, WecomBot, serve_http},
};

fn parse_config_path() -> Result<PathBuf> {
    let mut options = Options::new();
    options.optopt("c", "config", "TOML configuration file", "PATH");
    options.optflag("h", "help", "print this help message");

    let program = env::args().next().unwrap_or_else(|| "milo".to_owned());
    let matches = options.parse(env::args().skip(1)).map_err(|error| {
        anyhow::anyhow!(
            "{error}\n{}",
            options.usage(&format!("Usage: {program} [options]"))
        )
    })?;

    if matches.opt_present("h") {
        print!("{}", options.usage(&format!("Usage: {program} [options]")));
        std::process::exit(0);
    }

    if !matches.free.is_empty() {
        anyhow::bail!(
            "unexpected positional arguments: {}",
            matches.free.join(" ")
        );
    }

    Ok(matches
        .opt_str("c")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml")))
}

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = parse_config_path()?;
    let config = Config::from_file(&config_path)?;
    let log_level = config
        .log
        .level
        .parse()
        .with_context(|| "log.level must be one of off, error, warn, info, debug, or trace")?;
    env_logger::Builder::new().filter_level(log_level).init();

    let gateway_count = config.gateway.len();
    let agent = RigAgent::new(
        config.model,
        config.agent,
        FileProfileStore::new(config.profile_dir),
        config.storage_dir,
    )?;
    let gateway = GatewayRuntime::new(agent);
    let mut tasks = tokio::task::JoinSet::new();

    info!(
        "starting milo: config={}, gateways={gateway_count}",
        config_path.display()
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
