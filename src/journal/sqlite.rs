pub fn init_state_db(_path: &str) -> Result<rusqlite::Connection, Box<dyn std::error::Error>> {
    Ok(rusqlite::Connection::open(_path)?)
}
