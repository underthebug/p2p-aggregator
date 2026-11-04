pub mod commands;
// The IPC server listens on a Unix domain socket, which is unavailable on Windows.
#[cfg(unix)]
pub mod server;
