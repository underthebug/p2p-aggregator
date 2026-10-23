use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::fs;
use std::path::Path;
use crate::ipc::commands::{IpcCommand, handle_command};

fn handle_client(mut stream: UnixStream) {
    let mut buffer = [0; 512];
    match stream.read(&mut buffer) {
        Ok(n) if n > 0 => {
            let request = String::from_utf8_lossy(&buffer[..n]);
            let command = IpcCommand::from_str(&request).unwrap_or_else(|_| IpcCommand::Unknown(request.to_string()));
            
            // Log processed command type
            match &command {
                IpcCommand::Status => println!("Processed Command: status"),
                IpcCommand::Peers => println!("Processed Command: peers"),
                IpcCommand::Metrics => println!("Processed Command: metrics"),
                IpcCommand::Connect(addr) => println!("Processed Command: connect {}", addr),
                IpcCommand::LogLevel(lvl) => println!("Processed Command: loglevel {}", lvl),
                IpcCommand::Unknown(c) => println!("Processed Command: unknown ({})", c.trim()),
            }

            let response = handle_command(&request);
            let _ = stream.write_all(response.as_bytes());
        }
        _ => {}
    }
}

pub fn start_ipc_server(socket_path: &str) -> std::io::Result<()> {
    if Path::new(socket_path).exists() {
        fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    println!("IPC server listening on {}", socket_path);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    handle_client(stream);
                }
                Err(e) => {
                    eprintln!("IPC connection failed: {}", e);
                    break;
                }
            }
        }
    });

    Ok(())
}
