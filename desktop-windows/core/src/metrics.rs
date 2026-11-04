
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};

async fn handle_request(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let body = "# HELP p2p_aggregator_uptime_seconds Daemon uptime\n# TYPE p2p_aggregator_uptime_seconds counter\np2p_aggregator_uptime_seconds 1\n";
    Ok(Response::new(Body::from(body)))
}

pub fn start_metrics_server(bind_addr: &str) -> Result<(SocketAddr, ()), Box<dyn std::error::Error>> {
    let addr: SocketAddr = bind_addr.parse()?;
    let new_service = make_service_fn(move |_| async {
        Ok::<_, hyper::Error>(service_fn(handle_request))
    });

    let server = Server::bind(&addr).serve(new_service);
    tokio::spawn(async move {
        if let Err(e) = server.await {
            eprintln!("Metrics server background error: {}", e);
        }
    });

    Ok((addr, ()))
}
