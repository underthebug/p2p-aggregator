pub fn init_telemetry_db(path: &str) -> Result<rocksdb::DB, Box<dyn std::error::Error>> {
    let opts = rocksdb::Options::default();
    Ok(rocksdb::DB::open(&opts, path)?)
}
