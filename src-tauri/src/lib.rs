#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // App entry point is managed in main.rs
}

// Add inside your Tauri builder setup:
// .invoke_handler(tauri::generate_handler![
//     feeds::get_user_feeds,
//     feeds::add_user_feed,
//     feeds::remove_user_feed
// ])
