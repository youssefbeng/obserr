use std::path::PathBuf;

use anyhow::Result;
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
            let state = AppState::new(cfg, store);
            let app = web::router(state);
            let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
            axum::serve(listener, app).await?;
        }
    }
    Ok(())
}
