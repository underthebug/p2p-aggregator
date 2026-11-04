use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use crate::peer::manager::PeerManager;
use crate::telemetry::worker::TelemetryMetrics;

pub async fn start_metrics_exporter(
    bind_addr: &str,
    peer_manager: Arc<PeerManager>,
    telemetry: Arc<TelemetryMetrics>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    println!("Prometheus metrics exporter active on http://{}/metrics", bind_addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _addr)) => {
                    let pm = Arc::clone(&peer_manager);
                    let tel = Arc::clone(&telemetry);
                    tokio::spawn(async move {
                        let mut buf = [0; 1024];
                        let _ = socket.read(&mut buf).await;
                        
                        let peer_count = pm.get_peer_count();
                        let (uptime, events) = tel.get_raw_metrics();
                        
                        let body = format!(
                            "# HELP p2p_aggregator_uptime_seconds Total uptime in seconds\n\
                             # TYPE p2p_aggregator_uptime_seconds counter\n\
                             p2p_aggregator_uptime_seconds {}\n\
                             # HELP p2p_aggregator_events_processed_total Total events processed\n\
                             # TYPE p2p_aggregator_events_processed_total counter\n\
                             p2p_aggregator_events_processed_total {}\n\
                             # HELP p2p_aggregator_peers_connected Number of active connected peers\n\
                             # TYPE p2p_aggregator_peers_connected gauge\n\
                             p2p_aggregator_peers_connected {}\n",
                            uptime, events, peer_count
                        );
                        
                        let response = format!(
                            "HTTP/1.1 200 OK\r\n\
                             Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                             Content-Length: {}\r\n\
                             Connection: close\r\n\
                             \r\n\
                             {}",
                            body.len(),
                            body
                        );
                        
                        let _ = socket.write_all(response.as_bytes()).await;
                    });
                }
                Err(e) => {
                    eprintln!("Metrics accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}
