use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use thieving_eyes_service::config::{Config, default_config_path};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Capacity-aware background Agent execution daemon")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();
    let args = Args::parse();
    let path = args.config.map_or_else(default_config_path, Ok)?;
    let config = Config::load(&path).await?;
    thieving_eyes_service::run(config).await
}
