use clap::{Parser, Subcommand};
use std::os::unix::net::UnixStream;
use std::io::{Read, Write};

#[derive(Parser)]
#[command(name = "p2p-cli", about = "CLI client for P2P Aggregator Daemon IPC server")]
struct Cli {
    #[arg(short, long, default_value = "/run/p2p-aggregator/p2p-aggregator.sock")]
    socket: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Inspect live consensus role, term, and commit index
    Status,
    /// List all connected authenticated P2P peers
    Peers,
    /// View runtime telemetry and performance metrics
    Metrics,
    /// Submit a new log entry payload (Leader only)
    Submit {
        payload: String,
    },
}

fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    let mut stream = match UnixStream::connect(&args.socket) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to daemon IPC socket at '{}': {}", args.socket, e);
            eprintln!("Ensure the p2p-aggregator daemon is running.");
            std::process::exit(1);
        }
    };

    let cmd_str = match &args.command {
        Commands::Status => "status".to_string(),
        Commands::Peers => "peers".to_string(),
        Commands::Metrics => "metrics".to_string(),
        Commands::Submit { payload } => format!("submit {}", payload),
    };

    stream.write_all(cmd_str.as_bytes())?;
    
    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    print!("{}", response);
    Ok(())
}
