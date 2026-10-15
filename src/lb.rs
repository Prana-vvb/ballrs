use std::collections::HashSet;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use http_body_util::{
    combinators::{BoxBody, UnsyncBoxBody},
    BodyExt, Full,
};
use hyper::body::{Body as HttpBody, Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Bytes, HeaderMap, Method, Request, Response, Uri, Version};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout, Instant as TokioInstant, Sleep};

use crate::{Algorithm, LoadBalancer, Server};
mod algos;
use algos::least_connections::LeastConnections;
use algos::least_response_time::LeastResponseTime;
use algos::round_robin::RoundRobin;
use algos::weighted_least_connections::WeightedLeastConnections;
use algos::weighted_least_response_time::WeightedLeastResponseTime;
use algos::weighted_round_robin::WeightedRoundRobin;

type BoxError = Box<dyn StdError + Send + Sync>;
type ProxyBody = BoxBody<Bytes, BoxError>;
type RequestBody = UnsyncBoxBody<Bytes, BoxError>;
type BackendClient = Client<HttpConnector, RequestBody>;

const UPLOAD_ACTIVE: u8 = 0;
const UPLOAD_COMPLETE: u8 = 1;
const UPLOAD_READ_FAILED: u8 = 2;
const UPLOAD_TIMED_OUT: u8 = 3;

struct MonitoredRequestBody {
    inner: Pin<Box<RequestBody>>,
    idle_deadline: Pin<Box<Sleep>>,
    idle_timeout: Duration,
    has_started: bool,
    state: Arc<AtomicU8>,
    started_signal: Option<oneshot::Sender<()>>,
    completion: Option<oneshot::Sender<Option<Instant>>>,
}

impl MonitoredRequestBody {
    fn new(
        inner: RequestBody,
        idle_timeout: Duration,
        state: Arc<AtomicU8>,
        started_signal: oneshot::Sender<()>,
        completion: oneshot::Sender<Option<Instant>>,
    ) -> Self {
        let already_complete = inner.is_end_stream();
        let started_at = already_complete.then(Instant::now);
        let mut started_signal = Some(started_signal);
        let mut completion = Some(completion);
        if already_complete {
            state.store(UPLOAD_COMPLETE, Ordering::Release);
            if let Some(signal) = started_signal.take() {
                let _ = signal.send(());
            }
            if let Some(signal) = completion.take() {
                let _ = signal.send(started_at);
            }
        }
        Self {
            inner: Box::pin(inner),
            idle_deadline: Box::pin(tokio::time::sleep(idle_timeout)),
            idle_timeout,
            has_started: already_complete,
            state,
            started_signal,
            completion,
        }
    }

    fn finish(&mut self, state: u8, completed_at: Option<Instant>) {
        self.state.store(state, Ordering::Release);
        if let Some(completion) = self.completion.take() {
            let _ = completion.send(completed_at);
        }
    }
}

impl HttpBody for MonitoredRequestBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if !this.has_started {
            this.has_started = true;
            this.idle_deadline
                .as_mut()
                .reset(TokioInstant::now() + this.idle_timeout);
            if let Some(started_signal) = this.started_signal.take() {
                let _ = started_signal.send(());
            }
        }
        if this.idle_deadline.as_mut().poll(cx).is_ready() {
            this.finish(UPLOAD_TIMED_OUT, None);
            return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "client request body upload timed out",
            )))));
        }

        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                this.idle_deadline
                    .as_mut()
                    .reset(TokioInstant::now() + this.idle_timeout);
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.finish(UPLOAD_READ_FAILED, None);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.finish(UPLOAD_COMPLETE, Some(Instant::now()));
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

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
    let connector = HttpConnector::new();
    let backend_client: Arc<BackendClient> =
        Arc::new(Client::builder(TokioExecutor::new()).build(connector));

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

    let load_balancer: Arc<Mutex<dyn Loadbalancer>> = match algo {
        Algorithm::RoundRobin => Arc::new(Mutex::new(RoundRobin::new())),
        Algorithm::WeightedRoundRobin => Arc::new(Mutex::new(WeightedRoundRobin::new())),
        Algorithm::LeastConnections => Arc::new(Mutex::new(LeastConnections::new())),
        Algorithm::WeightedLeastConnections => {
            Arc::new(Mutex::new(WeightedLeastConnections::new()))
        }
        Algorithm::LeastResponseTime => Arc::new(Mutex::new(LeastResponseTime::new())),
        Algorithm::WeightedLeastResponseTime => {
            Arc::new(Mutex::new(WeightedLeastResponseTime::new()))
        }
    };
    listen(lb, load_balancer, Arc::clone(&backend_client)).await?;

    Ok(())
}

async fn listen<T>(
    lb: Arc<Mutex<LoadBalancer>>,
    load_balancer: Arc<Mutex<T>>,
    backend_client: Arc<BackendClient>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: Loadbalancer + ?Sized + 'static,
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
        let backend_client = Arc::clone(&backend_client);

        tokio::task::spawn(async move {
            // spawns a tokio task to server multiple connections concurrently
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(
                            req,
                            Arc::clone(&lb_clone),
                            Arc::clone(&load_balancer_clone),
                            Arc::clone(&backend_client),
                        )
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
    req: Request<Incoming>,
    lb: Arc<Mutex<LoadBalancer>>,
    load_balancer: Arc<Mutex<T>>,
    backend_client: Arc<BackendClient>,
) -> Result<Response<ProxyBody>, Infallible>
where
    T: Loadbalancer + ?Sized,
{
    let (parts, body) = req.into_parts();
    let body_is_empty = body.is_end_stream() && body.size_hint().exact() == Some(0);
    let retryable = body_is_empty
        && matches!(
            parts.method.as_str(),
            "GET" | "HEAD" | "PUT" | "DELETE" | "OPTIONS" | "TRACE"
        );
    let mut request_body = Some(
        body.map_err(|err| {
            eprintln!("Failed to read incoming request body: {err}");
            Box::new(err) as BoxError
        })
        .boxed_unsync(),
    );
    let mut attempted = HashSet::new();
    let mut last_failure = None;

    loop {
        let body = request_body.take().unwrap_or_else(empty_request_body);
        match get_request(
            &parts.method,
            &parts.uri,
            parts.version,
            &parts.headers,
            body,
            &mut attempted,
            Arc::clone(&lb),
            Arc::clone(&load_balancer),
            Arc::clone(&backend_client),
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
                if !retryable
                    || matches!(
                        failure,
                        ProxyFailure::BadRequestBody | ProxyFailure::ClientUploadTimeout
                    )
                {
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
    BadRequestBody,
    ClientUploadTimeout,
}

enum SendFailure {
    Backend(BoxError),
    BackendTimeout,
    ClientBody,
    ClientUploadTimeout,
}

fn failure_response(failure: ProxyFailure) -> Response<ProxyBody> {
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
        ProxyFailure::BadRequestBody => (
            hyper::StatusCode::BAD_REQUEST,
            "Failed to read request body",
        ),
        ProxyFailure::ClientUploadTimeout => (
            hyper::StatusCode::REQUEST_TIMEOUT,
            "Client request body upload timed out",
        ),
    };

    Response::builder()
        .status(status)
        .body(full_proxy_body(Bytes::from_static(message.as_bytes())))
        .expect("static proxy error response is valid")
}

struct ConnectionGuard {
    lb: Arc<Mutex<LoadBalancer>>,
    index: usize,
    started_at: Option<Instant>,
}

impl ConnectionGuard {
    fn mark_failed(&self) {
        if let Ok(mut lb) = self.lb.lock() {
            if let Some(server) = lb.servers.get_mut(self.index) {
                server.alive = false;
            }
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Ok(mut lb) = self.lb.lock() {
            if let Some(server) = lb.servers.get_mut(self.index) {
                server.connections = server.connections.saturating_sub(1);
                if let Some(started_at) = self.started_at {
                    server.response_time = started_at.elapsed();
                }
            }
        }
    }
}

struct StreamingBody {
    inner: Option<Pin<Box<Incoming>>>,
    deadline: Pin<Box<Sleep>>,
    connection: Option<ConnectionGuard>,
}

impl StreamingBody {
    fn new(body: Incoming, deadline: TokioInstant, connection: ConnectionGuard) -> Self {
        Self {
            inner: Some(Box::pin(body)),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            connection: Some(connection),
        }
    }

    fn finish(&mut self, failed: bool) {
        self.inner.take();
        if failed {
            if let Some(connection) = &self.connection {
                connection.mark_failed();
            }
        }
        self.connection.take();
    }
}

impl HttpBody for StreamingBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.inner.is_none() {
            return Poll::Ready(None);
        }

        if this.deadline.as_mut().poll(cx).is_ready() {
            this.finish(true);
            return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "backend response body timed out",
            )))));
        }

        let frame = this
            .inner
            .as_mut()
            .expect("body checked above")
            .as_mut()
            .poll_frame(cx);
        match frame {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(error))) => {
                this.finish(true);
                Poll::Ready(Some(Err(Box::new(error))))
            }
            Poll::Ready(None) => {
                this.finish(false);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn full_proxy_body(bytes: Bytes) -> ProxyBody {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

fn empty_request_body() -> RequestBody {
    Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed_unsync()
}

async fn get_request<T>(
    method: &Method,
    incoming_uri: &Uri,
    version: Version,
    headers: &HeaderMap,
    body: RequestBody,
    attempted: &mut HashSet<usize>,
    lb: Arc<Mutex<LoadBalancer>>,
    load_balancer: Arc<Mutex<T>>,
    backend_client: Arc<BackendClient>,
) -> Result<Response<ProxyBody>, ProxyFailure>
where
    T: Loadbalancer + ?Sized,
{
    let (timeout_duration, client_upload_timeout, index, uri) = {
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
            lb.client_upload_timeout,
            index,
            uri,
        )
    };

    let mut connection = ConnectionGuard {
        lb: Arc::clone(&lb),
        index,
        started_at: None,
    };
    match send_request(
        backend_client.as_ref(),
        uri,
        method,
        version,
        headers,
        body,
        timeout_duration,
        client_upload_timeout,
    )
    .await
    {
        Ok((response, backend_started_at)) => {
            connection.started_at = Some(backend_started_at);
            let deadline = TokioInstant::from_std(backend_started_at + timeout_duration);
            let (parts, body) = response.into_parts();
            let body = StreamingBody::new(body, deadline, connection).boxed();
            Ok(Response::from_parts(parts, body))
        }
        Err(SendFailure::Backend(error)) => {
            eprintln!("Backend request failed: {error}");
            connection.mark_failed();
            Err(ProxyFailure::BadGateway)
        }
        Err(SendFailure::BackendTimeout) => {
            connection.mark_failed();
            Err(ProxyFailure::GatewayTimeout)
        }
        Err(SendFailure::ClientBody) => Err(ProxyFailure::BadRequestBody),
        Err(SendFailure::ClientUploadTimeout) => Err(ProxyFailure::ClientUploadTimeout),
    }
}

fn backend_uri(backend: &Uri, incoming: &Uri) -> Result<Uri, String> {
    let authority = backend
        .authority()
        .ok_or_else(|| "backend URI has no authority".to_string())?;
    let scheme = backend.scheme_str().unwrap_or("http");
    let base_path = backend.path().trim_end_matches("/");
    let incoming_path = incoming.path();
    let path = if incoming_path == "*" {
        "*".to_owned()
    } else if base_path.is_empty() || base_path == "/" {
        incoming_path.to_owned()
    } else if incoming_path == "/" {
        format!("{base_path}/")
    } else {
        format!("{base_path}{incoming_path}")
    };

    let query = match (backend.query(), incoming.query()) {
        (Some(base), Some(request)) => Some(format!("{base}&{request}")),
        (Some(base), None) => Some(base.to_owned()),
        (None, Some(request)) => Some(request.to_owned()),
        (None, None) => None,
    };
    let path_and_query = match query {
        Some(query) => format!("{path}?{query}"),
        None => path,
    };

    Uri::builder()
        .scheme(scheme)
        .authority(authority.as_str())
        .path_and_query(path_and_query.as_str())
        .build()
        .map_err(|err| err.to_string())
}

async fn send_request(
    client: &BackendClient,
    uri: Uri,
    method: &Method,
    version: Version,
    headers: &HeaderMap,
    body: RequestBody,
    response_timeout: Duration,
    upload_idle_timeout: Duration,
) -> Result<(Response<Incoming>, Instant), SendFailure> {
    let upload_state = Arc::new(AtomicU8::new(UPLOAD_ACTIVE));
    let (upload_started_tx, mut upload_started_rx) = oneshot::channel();
    let (upload_completed_tx, mut upload_completed_rx) = oneshot::channel();
    let body = MonitoredRequestBody::new(
        body,
        upload_idle_timeout,
        Arc::clone(&upload_state),
        upload_started_tx,
        upload_completed_tx,
    )
    .boxed_unsync();

    let mut forwarded_headers = headers.clone();
    forwarded_headers.remove(hyper::header::CONNECTION);
    forwarded_headers.remove(hyper::header::HeaderName::from_static("keep-alive"));
    forwarded_headers.remove(hyper::header::PROXY_AUTHENTICATE);
    forwarded_headers.remove(hyper::header::PROXY_AUTHORIZATION);
    forwarded_headers.remove(hyper::header::TE);
    forwarded_headers.remove(hyper::header::TRAILER);
    forwarded_headers.remove(hyper::header::TRANSFER_ENCODING);
    forwarded_headers.remove(hyper::header::UPGRADE);
    let authority = uri.authority().ok_or_else(|| {
        SendFailure::Backend(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "backend URI has no authority",
        )))
    })?;
    let host = authority
        .as_str()
        .parse()
        .map_err(|error| SendFailure::Backend(Box::new(error)))?;
    forwarded_headers.insert(hyper::header::HOST, host);

    let mut request = Request::builder()
        .method(method.clone())
        .uri(uri)
        .version(version)
        .body(body)
        .map_err(|error| SendFailure::Backend(Box::new(error)))?;
    *request.headers_mut() = forwarded_headers;

    let mut request_future = Box::pin(client.request(request));
    let mut upload_completed_at = None;
    let mut connect_deadline = Box::pin(tokio::time::sleep(response_timeout));
    let response = tokio::select! {
        result = &mut request_future => {
            classify_client_result(result, upload_state.load(Ordering::Acquire))?
        }
        _ = &mut upload_started_rx => {
            tokio::select! {
                result = &mut request_future => {
                    classify_client_result(result, upload_state.load(Ordering::Acquire))?
                }
                completed = &mut upload_completed_rx => {
                    match completed {
                        Ok(Some(completed_at)) => {
                            upload_completed_at = Some(completed_at);
                            match timeout(response_timeout, &mut request_future).await {
                                Ok(result) => classify_client_result(
                                    result,
                                    upload_state.load(Ordering::Acquire),
                                )?,
                                Err(_) => return Err(SendFailure::BackendTimeout),
                            }
                        }
                        Ok(None) => return Err(classify_upload_state(
                            upload_state.load(Ordering::Acquire),
                        )),
                        Err(_) => match timeout(response_timeout, &mut request_future).await {
                            Ok(result) => classify_client_result(
                                result,
                                upload_state.load(Ordering::Acquire),
                            )?,
                            Err(_) => return Err(SendFailure::BackendTimeout),
                        },
                    }
                }
            }
        }
        _ = connect_deadline.as_mut() => return Err(SendFailure::BackendTimeout),
    };
    let mut response = response;
    let backend_started_at = upload_completed_at
        .or_else(|| upload_completed_rx.try_recv().ok().flatten())
        .unwrap_or_else(Instant::now);
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

    Ok((response, backend_started_at))
}

fn classify_client_result(
    result: Result<Response<Incoming>, hyper_util::client::legacy::Error>,
    upload_state: u8,
) -> Result<Response<Incoming>, SendFailure> {
    result.map_err(|error| match upload_state {
        UPLOAD_READ_FAILED => SendFailure::ClientBody,
        UPLOAD_TIMED_OUT => SendFailure::ClientUploadTimeout,
        _ => SendFailure::Backend(Box::new(error)),
    })
}

fn classify_upload_state(upload_state: u8) -> SendFailure {
    match upload_state {
        UPLOAD_READ_FAILED => SendFailure::ClientBody,
        UPLOAD_TIMED_OUT => SendFailure::ClientUploadTimeout,
        _ => SendFailure::Backend(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "request body ended before completion",
        ))),
    }
}

pub trait Loadbalancer: Send {
    fn get_index(&mut self, lb: &[&Server]) -> Option<usize>;
}
