// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod db;
mod network;

use db::DbManager;
use std::sync::Arc;

#[tauri::command]
fn fetch_mesh_broadcasts(state: tauri::State<'_, Arc<DbManager>>) -> Result<Vec<db::SecureBroadcast>, String> {
    state.get_broadcasts(50).map_err(|e| e.to_string())
}

fn main() {
    // Ensure data directory exists
    std::fs::create_dir_all("data").expect("Failed to create data directory");
    
    // Initialize SQLite database manager pointing to local state
    let db_path = "data/state_41871.db";
    let db_manager = DbManager::new(db_path).expect("Failed to initialize SQLite database");
    let shared_db = Arc::new(db_manager);

    // Initialize Tokio runtime to run background libp2p listener
    let db_clone = Arc::clone(&shared_db);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            if let Err(e) = network::start_p2p_listener(db_clone).await {
                eprintln!("libp2p listener error: {e}");
            }
        });
    });

    tauri::Builder::default()
        .manage(shared_db)
        .invoke_handler(tauri::generate_handler![
            fetch_mesh_broadcasts
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
