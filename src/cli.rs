use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "p2p-aggregator", about = "High-performance P2P Aggregator Daemon in Rust")]
pub struct Cli {
    /// Path to the configuration file
    #[arg(short, long, default_value = "config.toml")]
    pub config: String,

    /// Override metrics exporter port
    #[arg(short, long)]
    pub metrics_port: Option<u16>,

    /// Override P2P listening port
    #[arg(short, long)]
    pub p2p_port: Option<u16>,
}
