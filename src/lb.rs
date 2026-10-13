use std::collections::HashSet;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Bytes, HeaderMap, Method, Request, Response, Uri, Version};
use hyper_util::rt::TokioIo;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

use crate::{Algorithm, LoadBalancer, Server};
mod algos;
use algos::least_connections::LeastConnections;
use algos::least_response_time::LeastResponseTime;
use algos::round_robin::RoundRobin;
use algos::weighted_least_connections::WeightedLeastConnections;
use algos::weighted_least_response_time::WeightedLeastResponseTime;
use algos::weighted_round_robin::WeightedRoundRobin;

fn uri_to_socket_addr(uri: &Uri) -> Result<SocketAddr, &'static str> {
    // takes Uri and returns SocketAddr
    let authority = uri
        .authority()
        .ok_or("URI does not have an authority part")?; // Ensure the URI has an authority part (host and port)

    let host = authority.host(); // Extract host and port
    let port = authority.port_u16().ok_or("URI does not have a port")?;

    let addr_str = format!("{}:{}", host, port); // Combine host and port into a SocketAddr
    SocketAddr::from_str(&addr_str).map_err(|_| "Failed to parse SocketAddr")
}

#[tokio::main]
pub async fn start_lb(lb: LoadBalancer) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // starts the load balancer
    let lb = Arc::new(Mutex::new(lb));

    let (health_check_interval, len, health_check_timeout, report, save, save_file) = {
        // gets health check interval and number of servers
        let lb_lock = lb.lock().unwrap();
        (
            lb_lock.health_check_interval,
            lb_lock.servers.len(),
            lb_lock.health_check_timeout,
            lb_lock.report,
            lb_lock.save,
            lb_lock.save_file.clone(),
        )
    };
    let lb_clone = Arc::clone(&lb); //creates a clone for health checker

    tokio::task::spawn(async move {
        let mut health_checks: u32 = 0;
        loop {
            let mut tasks = Vec::new();

            for index in 0..len {
                let lb_clone: Arc<Mutex<LoadBalancer>> = Arc::clone(&lb_clone);

                let task = tokio::task::spawn(async move {
                    let server = {
                        // gets a local copy of server
                        let mut lb = lb_clone.lock().unwrap();

                        if let Some(server) = lb.servers.get_mut(index) {
                            server.connections_served += 1;
                            server.connections += 1;
                        }

                        lb.servers[index].clone()
                    };

                    let start = Instant::now();
                    let response = timeout(
                        health_check_timeout,
                        reqwest::get(server.addr.clone().to_string()),
                    )
                    .await; // sends a request to server
                    let duration = start.elapsed(); // get's the response time

                    let mut lb = lb_clone.lock().unwrap();

                    lb.servers[index].response_time = duration;

                    if lb.servers[index].connections > 0 {
                        lb.servers[index].connections -= 1;
                    }

                    match response {
                        Ok(response) => {
                            if response.is_err()
                                || lb.servers[index].connections
                                    >= lb.servers[index].max_connections
                            {
                                lb.servers[index].connections = 0;
                                lb.servers[index].alive = false;
                            } else {
                                lb.servers[index].alive = true;
                            }
                        }
                        Err(_) => {
                            lb.servers[index].connections = 0;
                            lb.servers[index].alive = false;
                        }
                    }
                });
                tasks.push(task);
            }

            for task in tasks {
                drop(task.await); // waits for all the servers to get updated
            }

            if report || save {
                health_checks += 1;
                let lb_lock = lb_clone.lock().unwrap();
                let report_bool = report;
                let mut report = String::new();
                report += &format!("Health check #{}\n", health_checks);
                report += "+-------------------------------------------------------------------------------------------+\n";
                report += "|                                       Server Report                                       |\n";
                report += "+------------------------+---------+---------------+-------------+--------------------------+\n";
                report += "| Address                | Status  | Response Time | Connections | Connections Served       |\n";
                report += "+------------------------+---------+---------------+-------------+--------------------------+\n";

                let mut total_connections = 0;
                let mut total_connections_served = 0;
                let mut total_response_time = Duration::from_secs(0);
                let mut alive_servers = 0;

                for server in lb_lock.servers.iter() {
                    let status = if server.alive { "Alive  " } else { "Dead   " };
                    let bar = if server.alive { "🟢" } else { "🔴" };
                    let response_time = format!("{:?}", server.response_time);
                    report += &format!(
                        "| {:<20} | {:<7} | {:<13} | {:<11} | {:<24} | {}\n",
                        server.addr,
                        status,
                        response_time,
                        server.connections,
                        server.connections_served,
                        bar
                    );

                    total_connections += server.connections;
                    total_connections_served += server.connections_served;
                    if server.alive {
                        alive_servers += 1;
                        total_response_time += server.response_time;
                    }
                }

                report += "+------------------------+---------+---------------+-------------+--------------------------+\n";

                let avg_response_time = if alive_servers > 0 {
                    total_response_time / alive_servers as u32
                } else {
                    Duration::from_secs(0)
                };

                report += "\n+----------------------------------+\n";
                report += "|           LB Summary             |\n";
                report += "+----------------------------------+\n";
                report += &format!("| Total Connections    : {:<10}|\n", total_connections);
                report += &format!(
                    "| Connections Served   : {:<10}|\n",
                    total_connections_served
                );
                report += &format!("| Avg Response Time    : {:<10?}|\n", avg_response_time);
                report += &format!("| Alive Servers        : {:<10}|\n", alive_servers);
                report += "+----------------------------------+\n";

                if report_bool {
                    print!("\x1B[2J\x1B[H");
                    println!("{}", report);
                }

                if save {
                    let mut file = std::fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(&save_file)
                        .unwrap();

                    let t = chrono::prelude::Local::now()
                        .format("%d/%m/%y %H:%M:%S")
                        .to_string();
                    drop(writeln!(file, "Log: {:?}", t));
                    drop(writeln!(file, "{}", report));
                }
            }

            sleep(health_check_interval).await;
        }
    });

    let algo = {
        let lb_lock = lb.lock().unwrap();
        lb_lock.algo.clone()
    };

    match algo {
        Algorithm::RoundRobin => {
            let load_balancer = Arc::new(Mutex::new(RoundRobin::new()));
            drop(listen(lb, load_balancer).await);
        }
        Algorithm::WeightedRoundRobin => {
            let load_balancer = Arc::new(Mutex::new(WeightedRoundRobin::new()));
            drop(listen(lb, load_balancer).await);
        }
        Algorithm::LeastConnections => {
            let load_balancer = Arc::new(Mutex::new(LeastConnections::new()));
            drop(listen(lb, load_balancer).await);
        }
        Algorithm::WeightedLeastConnections => {
            let load_balancer = Arc::new(Mutex::new(WeightedLeastConnections::new()));
            drop(listen(lb, load_balancer).await);
        }
        Algorithm::LeastResponseTime => {
            let load_balancer = Arc::new(Mutex::new(LeastResponseTime::new()));
            drop(listen(lb, load_balancer).await);
        }
        Algorithm::WeightedLeastResponseTime => {
            let load_balancer = Arc::new(Mutex::new(WeightedLeastResponseTime::new()));
            drop(listen(lb, load_balancer).await);
        }
    }

    Ok(())
}

async fn listen<T>(
    lb: Arc<Mutex<LoadBalancer>>,
    load_balancer: Arc<Mutex<T>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: Loadbalancer + Send + 'static,
{
    let addr = uri_to_socket_addr(&lb.lock().unwrap().load_balancer).unwrap();
    let listener = match TcpListener::bind(addr).await {
        // We create a TcpListener and bind it to load balancer address
        Ok(listener) => {
            println!("Load balancer is running on http://{}", addr);
            listener
        }
        Err(err) => {
            eprintln!("{}", err);
            return Ok(());
        }
    };

    loop {
        // starting a loop to continuously accept incoming connections
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        let lb_clone = Arc::clone(&lb);
        let load_balancer_clone = Arc::clone(&load_balancer);

        // !!!!!!!!!! idk smtg virtual thread thing should go down here
        tokio::task::spawn(async move {
            // spawns a tokio task to server multiple connections concurrently
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(req, Arc::clone(&lb_clone), Arc::clone(&load_balancer_clone))
                    }),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
async fn handle_request<T>(
    req: Request<hyper::body::Incoming>,
    lb: Arc<Mutex<LoadBalancer>>,
    load_balancer: Arc<Mutex<T>>,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    T: Loadbalancer,
{
    let (parts, body) = req.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(err) => {
            eprintln!("Failed to read incoming request body: {err}");
            return Ok(Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from_static(b"Invalid request body")))
                .expect("static response is valid"));
        }
    };

    let retryable = matches!(
        parts.method.as_str(),
        "GET" | "HEAD" | "PUT" | "DELETE" | "OPTIONS" | "TRACE"
    );
    let mut attempted = HashSet::new();
    let mut last_failure = None;

    loop {
        match get_request(
            &parts.method,
            &parts.uri,
            parts.version,
            &parts.headers,
            &body,
            &mut attempted,
            Arc::clone(&lb),
            Arc::clone(&load_balancer),
        )
        .await
        {
            Ok(response) => return Ok(response),
            Err(ProxyFailure::NoBackend) => {
                return Ok(failure_response(
                    last_failure.unwrap_or(ProxyFailure::NoBackend),
                ));
            }
            Err(failure) => {
                if !retryable {
                    return Ok(failure_response(failure));
                }
                last_failure = Some(match (last_failure, failure) {
                    (Some(ProxyFailure::GatewayTimeout), _) | (_, ProxyFailure::GatewayTimeout) => {
                        ProxyFailure::GatewayTimeout
                    }
                    _ => ProxyFailure::BadGateway,
                });
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ProxyFailure {
    NoBackend,
    BadGateway,
    GatewayTimeout,
}

fn failure_response(failure: ProxyFailure) -> Response<Full<Bytes>> {
    let (status, message) = match failure {
        ProxyFailure::NoBackend => (
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            "No backend is currently available",
        ),
        ProxyFailure::BadGateway => (
            hyper::StatusCode::BAD_GATEWAY,
            "All attempted backends failed",
        ),
        ProxyFailure::GatewayTimeout => (
            hyper::StatusCode::GATEWAY_TIMEOUT,
            "Backend request timed out",
        ),
    };

    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(message.as_bytes())))
        .expect("static proxy error response is valid")
}

async fn get_request<T>(
    method: &Method,
    incoming_uri: &Uri,
    version: Version,
    headers: &HeaderMap,
    body: &Bytes,
    attempted: &mut HashSet<usize>,
    lb: Arc<Mutex<LoadBalancer>>,
    load_balancer: Arc<Mutex<T>>,
) -> Result<Response<Full<Bytes>>, ProxyFailure>
where
    T: Loadbalancer,
{
    let (timeout_duration, index, uri) = {
        let mut lb = lb.lock().unwrap();

        let eligible: Vec<(usize, &Server)> = lb
            .servers
            .iter()
            .enumerate()
            .filter(|(index, server)| {
                server.alive
                    && server.connections < server.max_connections
                    && !attempted.contains(index)
            })
            .collect();
        if eligible.is_empty() {
            return Err(ProxyFailure::NoBackend);
        }
        let eligible_refs: Vec<&Server> = eligible.iter().map(|(_, server)| *server).collect();
        let Some(selected) = load_balancer.lock().unwrap().get_index(&eligible_refs) else {
            return Err(ProxyFailure::NoBackend);
        };
        let Some((index, server)) = eligible.get(selected) else {
            return Err(ProxyFailure::NoBackend);
        };
        let index = *index;
        let backend = server.addr.clone();
        attempted.insert(index);
        let uri = backend_uri(&backend, incoming_uri).map_err(|_| ProxyFailure::BadGateway)?;

        lb.servers[index].connections += 1;
        lb.servers[index].connections_served += 1;
        (
            lb.servers[index].request_timeout.unwrap_or(lb.timeout),
            index,
            uri,
        )
    };

    let start = Instant::now();

    let data = timeout(
        timeout_duration,
        send_request(uri, method, version, headers, body),
    )
    .await;

    let duration = start.elapsed();

    let mut lb = lb.lock().unwrap();

    lb.servers[index].response_time = duration;

    if lb.servers[index].connections > 0 {
        lb.servers[index].connections -= 1;
    }

    match data {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(err)) => {
            eprintln!("Backend request failed: {err}");
            lb.servers[index].connections = 0;
            lb.servers[index].alive = false;
            Err(ProxyFailure::BadGateway)
        }
        Err(_) => {
            lb.servers[index].connections = 0;
            lb.servers[index].alive = false;
            Err(ProxyFailure::GatewayTimeout)
        }
    }
}

fn backend_uri(backend: &Uri, incoming: &Uri) -> Result<Uri, String> {
    let authority = backend
        .authority()
        .ok_or_else(|| "backend URI has no authority".to_string())?;
    let scheme = backend.scheme_str().unwrap_or("http");
    let path_and_query = incoming
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or("/");

    Uri::builder()
        .scheme(scheme)
        .authority(authority.as_str())
        .path_and_query(path_and_query)
        .build()
        .map_err(|err| err.to_string())
}

async fn send_request(
    uri: Uri,
    method: &Method,
    version: Version,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let host = uri.host().ok_or("backend URI has no host")?;
    let port = uri.port_u16().unwrap_or(80);
    let address = if host.contains(':') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    };

    let stream = TcpStream::connect(address).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("Backend connection failed: {err}");
        }
    });

    let mut forwarded_headers = headers.clone();
    forwarded_headers.remove(hyper::header::CONNECTION);
    forwarded_headers.remove(hyper::header::HeaderName::from_static("keep-alive"));
    forwarded_headers.remove(hyper::header::PROXY_AUTHENTICATE);
    forwarded_headers.remove(hyper::header::PROXY_AUTHORIZATION);
    forwarded_headers.remove(hyper::header::TE);
    forwarded_headers.remove(hyper::header::TRAILER);
    forwarded_headers.remove(hyper::header::TRANSFER_ENCODING);
    forwarded_headers.remove(hyper::header::UPGRADE);
    forwarded_headers.insert(
        hyper::header::HOST,
        uri.authority()
            .ok_or("backend URI has no authority")?
            .as_str()
            .parse()?,
    );

    let mut request = Request::builder()
        .method(method.clone())
        .uri(uri)
        .version(version)
        .body(Full::new(body.clone()))?;
    *request.headers_mut() = forwarded_headers;

    let response = sender.send_request(request).await?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();
    let mut response = Response::from_parts(parts, Full::new(body));

    // The client connection is not reused; remove hop-by-hop response headers.
    response.headers_mut().remove(hyper::header::CONNECTION);
    response
        .headers_mut()
        .remove(hyper::header::HeaderName::from_static("keep-alive"));
    response
        .headers_mut()
        .remove(hyper::header::PROXY_AUTHENTICATE);
    response
        .headers_mut()
        .remove(hyper::header::PROXY_AUTHORIZATION);
    response.headers_mut().remove(hyper::header::TE);
    response.headers_mut().remove(hyper::header::TRAILER);
    response
        .headers_mut()
        .remove(hyper::header::TRANSFER_ENCODING);
    response.headers_mut().remove(hyper::header::UPGRADE);

    Ok(response)
}

pub trait Loadbalancer {
    fn get_index(&mut self, lb: &[&Server]) -> Option<usize>;
}
