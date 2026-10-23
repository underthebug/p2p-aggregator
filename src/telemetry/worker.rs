use std::thread;
use std::time::Duration;

pub fn start_telemetry_worker() {
    thread::spawn(|| {
        loop {
            // Periodic flush worker for RocksDB telemetry records & peer stats
            thread::sleep(Duration::from_secs(60));
        }
    });
}
