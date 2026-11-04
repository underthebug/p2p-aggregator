// Windows-specific helpers: per-user data directory, opening links, launching apps.
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Stops a spawned console program from flashing a terminal window.
pub fn hide_window(command: &mut Command) -> &mut Command {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
}

/// `%LOCALAPPDATA%\p2p-aggregator`; every relative `data/...` path resolves under it.
pub fn app_data_dir() -> PathBuf {
    std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("p2p-aggregator")
}

/// Creates the data directory and makes it the working directory.
pub fn enter_app_data_dir() -> std::io::Result<PathBuf> {
    let dir = app_data_dir();
    std::fs::create_dir_all(dir.join("data"))?;
    std::env::set_current_dir(&dir)?;
    Ok(dir)
}

/// Opens an http(s) URL in the default browser without going through a shell.
pub fn open_url(url: &str) -> std::io::Result<()> {
    let mut command = Command::new("rundll32");
    command.args(["url.dll,FileProtocolHandler", url]);
    hide_window(&mut command).spawn().map(|_| ())
}

/// Builds a command for a whitelisted helper app, preferring its default install location.
pub fn app_command(name: &str) -> Command {
    let relative = match name {
        "qbittorrent" => r"qBittorrent\qbittorrent.exe",
        "vlc" => r"VideoLAN\VLC\vlc.exe",
        _ => name,
    };
    let installed = ["ProgramFiles", "ProgramFiles(x86)"]
        .iter()
        .filter_map(|var| std::env::var_os(var))
        .map(|root| Path::new(&root).join(relative))
        .find(|path| path.is_file());
    Command::new(installed.unwrap_or_else(|| PathBuf::from(format!("{name}.exe"))))
}
