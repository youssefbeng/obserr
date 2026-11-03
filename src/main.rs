use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use clap::{Parser, Subcommand};
use obserr::{AppState, config::Config, store::Store, web};

#[derive(Parser)]
#[command(name = "obserr", about = "Lightweight observability for dbt Core and Airflow")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server and web dashboard
    Serve {
        /// Path to the TOML configuration file
        #[arg(short, long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve { config } => {
            let cfg = Config::from_file(&config)?;
            let listen_addr = cfg.server.listen.clone();
            let store = Store::open(&cfg.server.database_path)?;
            let state = AppState::from_config_file(&config, store)?;
            let _ = state.tick(Utc::now()).await;

            let ticker_state = state.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    let _ = ticker_state.tick(Utc::now()).await;
                }
            });

            let app = web::router(state);
            let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
            axum::serve(listener, app).await?;
        }
    }
    Ok(())
}
