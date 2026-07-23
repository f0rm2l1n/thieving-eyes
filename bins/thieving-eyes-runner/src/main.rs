use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version, about = "thieving-eyes local execution runner")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(hide = true)]
    Supervisor,
    #[command(hide = true)]
    Worker,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .without_time()
        .init();
    match Args::parse().command {
        Command::Supervisor => thieving_eyes_runner::supervisor().await,
        Command::Worker => thieving_eyes_runner::worker().await,
    }
}
