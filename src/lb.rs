use std::collections::HashSet;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Context as AnyhowContext};
use http_body_util::{
    combinators::{BoxBody, UnsyncBoxBody},
    BodyExt, Full,
};
use hyper::body::{Body as HttpBody, Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Bytes, HeaderMap, Method, Request, Response, Uri, Version};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpListener};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, Instant as TokioInstant, Sleep};

use crate::{Algorithm, BackendHealthUpdate, LoadBalancer, Server, ServerSnapshot};
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
type BackendConnector = HttpsConnector<HttpConnector>;
type BackendClient = Client<BackendConnector, RequestBody>;

pub(crate) struct ProxyState {
    pub(crate) config: Arc<crate::RuntimeConfig>,
    pub(crate) servers: Arc<[Arc<Server>]>,
    pub(crate) load_balancer: Arc<dyn Loadbalancer>,
    pub(crate) backend_client: Arc<BackendClient>,
}

pub(crate) struct RequestMetrics {
    pub(crate) attempted: HashSet<usize>,
    pub(crate) request_span: tracing::Span,
    pub(crate) request_started_at: Instant,
}

pub(crate) struct IncomingParts {
    pub(crate) method: Method,
    pub(crate) incoming_uri: Uri,
    pub(crate) version: Version,
    pub(crate) headers: HeaderMap,
}

#[derive(Clone, Copy)]
struct RequestTimeoutConfig {
    response: Duration,
    upload_idle: Duration,
}

#[derive(Clone)]
pub(crate) struct ListenerContext {
    pub(crate) proxy: Arc<ProxyState>,
    pub(crate) connection_limit: Arc<Semaphore>,
}

struct ReportSnapshot {
    health_check: u32,
    probe_interval: Duration,
    servers: Vec<ServerReportSnapshot>,
}

struct ServerReportSnapshot {
    addr: Uri,
    alive: bool,
    probe_latency: Duration,
    conns: u32,
    conns_served: u32,
}

fn render_report(snapshot: &ReportSnapshot) -> String {
    use std::fmt::Write as _;

    let rows: Vec<_> = snapshot
        .servers
        .iter()
        .map(|server| {
            (
                server.addr.to_string(),
                if server.alive { "Healthy" } else { "Unhealthy" },
                format!("{:?}", server.probe_latency),
                server.conns.to_string(),
                server.conns_served.to_string(),
            )
        })
        .collect();

    let addr_width = rows
        .iter()
        .map(|row| row.0.len())
        .max()
        .unwrap_or(0)
        .max("Backend".len());
    let health_width = "Health".len().max("Unhealthy".len());
    let latency_width = rows
        .iter()
        .map(|row| row.2.len())
        .max()
        .unwrap_or(0)
        .max("Probe latency".len());
    let active_width = rows
        .iter()
        .map(|row| row.3.len())
        .max()
        .unwrap_or(0)
        .max("Active".len());
    let served_width = rows
        .iter()
        .map(|row| row.4.len())
        .max()
        .unwrap_or(0)
        .max("Served".len());

    let mut report = String::new();
    let _ = writeln!(report, "Health check #{}", snapshot.health_check);
    let _ = writeln!(
        report,
        "{:<addr_width$}  {:<health_width$}  {:>latency_width$}  {:>active_width$}  {:>served_width$}",
        "Backend", "Health", "Probe latency", "Active", "Served"
    );
    let _ = writeln!(
        report,
        "{:-<addr_width$}  {:-<health_width$}  {:-<latency_width$}  {:-<active_width$}  {:-<served_width$}",
        "", "", "", "", ""
    );

    for row in &rows {
        let _ = writeln!(
            report,
            "{:<addr_width$}  {:<health_width$}  {:>latency_width$}  {:>active_width$}  {:>served_width$}",
            row.0, row.1, row.2, row.3, row.4
        );
    }

    let active_conns: u64 = snapshot
        .servers
        .iter()
        .map(|server| u64::from(server.conns))
        .sum();
    let conns_served: u64 = snapshot
        .servers
        .iter()
        .map(|server| u64::from(server.conns_served))
        .sum();
    let alive_servers = snapshot
        .servers
        .iter()
        .filter(|server| server.alive)
        .count();
    let tot_probe_latency = snapshot
        .servers
        .iter()
        .fold(Duration::ZERO, |total, server| total + server.probe_latency);

    let avg_probe_latency = if snapshot.servers.is_empty() {
        None
    } else {
        Some(tot_probe_latency / snapshot.servers.len() as u32)
    };
    let configured_probe_rate = if snapshot.probe_interval.is_zero() {
        None
    } else {
        Some(snapshot.servers.len() as f64 / snapshot.probe_interval.as_secs_f64())
    };

    let _ = writeln!(report);
    match configured_probe_rate {
        Some(rate) => {
            let _ = writeln!(
                report,
                "Probe schedule: one per backend every {:?}; configured aggregate rate: {rate:.2} probes/s",
                snapshot.probe_interval
            );
        }
        None => {
            let _ = writeln!(
                report,
                "Probe schedule: interval is zero (invalid configuration)"
            );
        }
    }

    let _ = writeln!(report, "Summary");
    let _ = writeln!(
        report,
        "Backends healthy: {alive_servers}/{}",
        snapshot.servers.len()
    );
    let _ = writeln!(report, "Active connections: {active_conns}");
    let _ = writeln!(report, "Connections served: {conns_served}");

    match avg_probe_latency {
        Some(latency) => {
            let _ = writeln!(report, "Average probe latency: {latency:?}");
        }
        None => {
            let _ = writeln!(report, "Average probe latency: n/a");
        }
    }
    let _ = writeln!(report);

    report
}

fn record_health_probe(server: &Server, started_at: Instant, healthy: bool, latency: Duration) {
    server.record_health_update(BackendHealthUpdate::Probe {
        started_at,
        healthy,
        latency,
    });
}

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

async fn uri_to_socket_addrs(uri: &Uri) -> std::io::Result<Vec<SocketAddr>> {
    let authority = uri.authority().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "URI has no authority")
    })?;

    let authority_host = authority.host();
    let host = authority_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(authority_host);
    let port = authority.port_u16().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "URI has no explicit port")
    })?;

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addresses = deduplicate_socket_addrs(lookup_host((host, port)).await?);
    if addresses.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            format!("hostname {host:?} resolved to no listener addresses"),
        ));
    }

    Ok(addresses)
}

fn deduplicate_socket_addrs(addresses: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    let mut seen = HashSet::new();
    addresses
        .into_iter()
        .filter(|address| seen.insert(*address))
        .collect()
}

async fn bind_available_listeners(
    addresses: &[SocketAddr],
) -> (Vec<(SocketAddr, TcpListener)>, Vec<String>) {
    let mut listeners = Vec::with_capacity(addresses.len());
    let mut bind_errors = Vec::new();

    for &address in addresses {
        match TcpListener::bind(address).await {
            Ok(listener) => {
                tracing::info!(%address, "Load balancer listener is running");
                listeners.push((address, listener));
            }
            Err(error) => {
                tracing::warn!(%address, %error, "Failed to bind listener address");
                bind_errors.push(format!("{address}: {error}"));
            }
        }
    }

    (listeners, bind_errors)
}

async fn serve_listener(
    address: SocketAddr,
    listener: TcpListener,
    context: ListenerContext,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut connection_tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    break;
                }
                continue;
            }
            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                result.context("connection task set unexpectedly became empty")??;
                continue;
            }
            result = listener.accept() => result
                .with_context(|| format!("listener {address} failed while accepting a connection"))?,
        };

        let (stream, _) = accepted;
        let connection_permit = match Arc::clone(&context.connection_limit).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::debug!(
                    %address,
                    limit = context.proxy.config.max_client_connections,
                    "Closing downstream connection because the global connection limit is full"
                );
                drop(stream);
                continue;
            }
        };
        let io = TokioIo::new(stream);

        let proxy = Arc::clone(&context.proxy);
        let mut connection_shutdown = shutdown.clone();

        connection_tasks.spawn(async move {
            let _connection_permit: OwnedSemaphorePermit = connection_permit;
            let connection = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| {
                        handle_request(request, Arc::clone(&proxy))
                    }),
                );

            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => {
                    if let Err(error) = result {
                        tracing::error!(%address, error = %error, "Error serving downstream connection");
                    }
                }

                changed = connection_shutdown.changed() => {
                    if changed.is_err() || *connection_shutdown.borrow_and_update() {
                        connection.as_mut().graceful_shutdown();
                        if let Err(error) = connection.await {
                            tracing::error!(%address, error = %error, "Error draining downstream connection");
                        }
                    }
                }
            }
        });
    }

    while let Some(result) = connection_tasks.join_next().await {
        result.context("connection task failed to join")?;
    }

    Ok(())
}

#[tokio::main]
pub async fn start_lb(lb: LoadBalancer) -> anyhow::Result<()> {
    let algo = lb.algo;
    let config = Arc::new(crate::RuntimeConfig {
        load_balancer: lb.load_balancer,
        max_client_connections: lb.max_client_connections,
        backend_tcp_nodelay: lb.backend_tcp_nodelay,
        timeout: lb.timeout,
        client_upload_timeout: lb.client_upload_timeout,
        health_check_interval: lb.health_check_interval,
        report: lb.report,
        save_file: lb.save_file,
        save: lb.save,
    });
    let servers: Arc<[Arc<Server>]> = lb
        .servers
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>()
        .into();
    let mut http_connector = HttpConnector::new();

    http_connector.enforce_http(false);
    http_connector.set_nodelay(config.backend_tcp_nodelay);

    let connector = HttpsConnectorBuilder::new()
        .with_native_roots()
        .context("failed to load system TLS root certificates")?
        .https_or_http()
        .enable_http1()
        .wrap_connector(http_connector);
    let backend_client: Arc<BackendClient> =
        Arc::new(Client::builder(TokioExecutor::new()).build(connector));
    let health_check_client = reqwest::Client::builder()
        .build()
        .context("failed to initialize health-check HTTP client")?;

    let health_check_interval = config.health_check_interval;
    let report = config.report;
    let save = config.save;
    let save_file = config.save_file.clone();
    let health_servers = Arc::clone(&servers);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let health_task = tokio::task::spawn(async move {
        let mut health_checks: u32 = 0;
        let mut health_check_timer = tokio::time::interval(health_check_interval);
        health_check_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut shutdown = shutdown_rx;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        break;
                    }
                }
                _ = health_check_timer.tick() => {}
            }
            health_checks = health_checks.saturating_add(1);
            let mut tasks = Vec::new();

            for server in health_servers.iter().cloned() {
                let health_check_client = health_check_client.clone();

                let task = tokio::task::spawn(async move {
                    let probe_started_at = Instant::now();
                    let healthy = match backend_uri(&server.addr, &server.health_check.path) {
                        Ok(uri) => matches!(
                            timeout(
                                server.health_check.timeout,
                                health_check_client.get(uri.to_string()).send(),
                            )
                            .await,
                            Ok(Ok(response)) if response.status().is_success()
                        ),
                        Err(error) => {
                            tracing::warn!(backend = %server.addr, %error, "Invalid health-check URI");
                            false
                        }
                    };
                    let latency = probe_started_at.elapsed();

                    record_health_probe(&server, probe_started_at, healthy, latency);
                });

                tasks.push(task);
            }

            for task in tasks {
                drop(task.await); // waits for all the servers to get updated
            }

            if report || save {
                let snapshot = ReportSnapshot {
                    health_check: health_checks,
                    probe_interval: health_check_interval,
                    servers: health_servers
                        .iter()
                        .map(|server| {
                            let metrics = server.snapshot();
                            ServerReportSnapshot {
                                addr: server.addr.clone(),
                                alive: metrics.alive,
                                probe_latency: metrics.health_check_response_time,
                                conns: metrics.connections,
                                conns_served: metrics.connections_served,
                            }
                        })
                        .collect(),
                };

                let report_text = render_report(&snapshot);

                if report {
                    let mut stdout = tokio::io::stdout();
                    if let Err(error) = stdout.write_all(report_text.as_bytes()).await {
                        tracing::error!(error = %error, "Failed to write load balancer report to stdout");
                    }
                }

                if save {
                    let timestamp = chrono::prelude::Local::now()
                        .format("%d/%m/%y %H:%M:%S")
                        .to_string();
                    let saved_report = format!("Log: {:?}\n{}\n", timestamp, report_text);
                    let save_result = async {
                        let mut file = tokio::fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&save_file)
                            .await?;
                        file.write_all(saved_report.as_bytes()).await?;
                        file.flush().await
                    }
                    .await;

                    if let Err(error) = save_result {
                        tracing::error!(path = %save_file, error = %error, "Failed to append load balancer report");
                    }
                }
            }
        }
    });

    let load_balancer: Arc<dyn Loadbalancer> = match algo {
        Algorithm::RoundRobin => Arc::new(RoundRobin::new()),
        Algorithm::WeightedRoundRobin => Arc::new(WeightedRoundRobin::new()),
        Algorithm::LeastConnections => Arc::new(LeastConnections::new()),
        Algorithm::WeightedLeastConnections => Arc::new(WeightedLeastConnections::new()),
        Algorithm::LeastResponseTime => Arc::new(LeastResponseTime::new()),
        Algorithm::WeightedLeastResponseTime => Arc::new(WeightedLeastResponseTime::new()),
    };
    let listen_result = listen(
        config,
        servers,
        load_balancer,
        backend_client,
        shutdown_tx.clone(),
        shutdown_tx.subscribe(),
    )
    .await
    .context("listener setup or serving failed");

    let _ = shutdown_tx.send(true);
    health_task
        .await
        .context("health-check task failed to join")?;
    listen_result?;

    Ok(())
}

async fn listen(
    config: Arc<crate::RuntimeConfig>,
    servers: Arc<[Arc<Server>]>,
    load_balancer: Arc<dyn Loadbalancer>,
    backend_client: Arc<BackendClient>,
    shutdown: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let addresses = uri_to_socket_addrs(&config.load_balancer)
        .await
        .with_context(|| format!("failed to resolve listener URI {}", config.load_balancer))?;

    let (listeners, bind_errors) = bind_available_listeners(&addresses).await;
    if listeners.is_empty() {
        return Err(anyhow!(
            "failed to bind any address for listener URI {} ({})",
            config.load_balancer,
            bind_errors.join("; ")
        ));
    }

    let mut listener_tasks = JoinSet::new();
    let connection_limit = Arc::new(Semaphore::new(config.max_client_connections));
    let listener_context = ListenerContext {
        proxy: Arc::new(ProxyState {
            config,
            servers,
            load_balancer,
            backend_client,
        }),
        connection_limit,
    };
    for (address, listener) in listeners {
        listener_tasks.spawn(serve_listener(
            address,
            listener,
            listener_context.clone(),
            shutdown_rx.clone(),
        ));
    }

    loop {
        tokio::select! {
            signal = shutdown_signal() => {
                signal.context("failed to listen for shutdown signal")?;
                tracing::info!("Shutdown signal received; stopping listeners");
                let _ = shutdown.send(true);
                while let Some(result) = listener_tasks.join_next().await {
                    result.context("listener task failed to join")??;
                }
                break;
            }
            result = listener_tasks.join_next() => {
                match result {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => return Err(error).context("listener task failed to join"),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

async fn handle_request(
    req: Request<Incoming>,
    proxy: Arc<ProxyState>,
) -> Result<Response<ProxyBody>, Infallible> {
    let request_started_at = Instant::now();
    let request_span = tracing::info_span!(
        "http.request",
        method = %req.method(),
        path = %req.uri().path(),
        backend_index = tracing::field::Empty,
        status = tracing::field::Empty,
    );
    let (parts, body) = req.into_parts();
    let incoming = IncomingParts {
        method: parts.method,
        incoming_uri: parts.uri,
        version: parts.version,
        headers: parts.headers,
    };
    let body_is_empty = body.is_end_stream() && body.size_hint().exact() == Some(0);
    let retryable = body_is_empty
        && matches!(
            incoming.method.as_str(),
            "GET" | "HEAD" | "PUT" | "DELETE" | "OPTIONS" | "TRACE"
        );
    let mut request_body = Some(
        body.map_err({
            let request_span = request_span.clone();
            move |err| {
                tracing::warn!(parent: &request_span, outcome = "client_upload_failed", "Failed to read incoming request body");
            Box::new(err) as BoxError
        }})
        .boxed_unsync(),
    );
    let mut metrics = RequestMetrics {
        attempted: HashSet::new(),
        request_span,
        request_started_at,
    };
    let mut last_failure = None;

    loop {
        let body = request_body.take().unwrap_or_else(empty_request_body);
        match get_request(&proxy, &incoming, body, &mut metrics).await {
            Ok(response) => {
                metrics
                    .request_span
                    .record("status", response.status().as_u16());
                return Ok(response);
            }
            Err(ProxyFailure::NoBackend) => {
                let failure = last_failure.unwrap_or(ProxyFailure::NoBackend);
                let response = failure_response(failure);
                metrics
                    .request_span
                    .record("status", response.status().as_u16());
                tracing::warn!(parent: &metrics.request_span, outcome = "proxy_failed", status = response.status().as_u16(), "Request could not be sent to a backend");

                return Ok(response);
            }
            Err(failure) => {
                if !retryable
                    || matches!(
                        failure,
                        ProxyFailure::BadRequestBody | ProxyFailure::ClientUploadTimeout
                    )
                {
                    let response = failure_response(failure);
                    metrics
                        .request_span
                        .record("status", response.status().as_u16());
                    tracing::warn!(parent: &metrics.request_span, outcome = "proxy_failed", status = response.status().as_u16(), "Request could not be completed");

                    return Ok(response);
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
    Backend,
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
    server: Arc<Server>,
    response_latency: Option<Duration>,
}

impl ConnectionGuard {
    fn mark_failed(&self) {
        let observed_at = Instant::now();
        self.server
            .record_health_update(BackendHealthUpdate::RequestFailure { observed_at });
    }

    fn record_completed_latency(&self) {
        if let Some(latency) = self.response_latency {
            self.server.record_request_latency(latency);
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.server.release_connection();
    }
}

struct StreamingBody {
    inner: Option<Pin<Box<Incoming>>>,
    deadline: Pin<Box<Sleep>>,
    connection: Option<ConnectionGuard>,
    request_span: tracing::Span,
    request_started_at: Instant,
}

impl StreamingBody {
    fn new(
        body: Incoming,
        deadline: TokioInstant,
        connection: ConnectionGuard,
        request_span: tracing::Span,
        request_started_at: Instant,
    ) -> Self {
        Self {
            inner: Some(Box::pin(body)),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            connection: Some(connection),
            request_span,
            request_started_at,
        }
    }

    fn finish(&mut self, failed: bool) {
        self.inner.take();
        let elapsed_ms = self.request_started_at.elapsed().as_millis() as u64;
        if failed {
            tracing::warn!(parent: &self.request_span, outcome = "response_stream_failed", elapsed_ms, "Backend response stream failed");
        } else {
            tracing::info!(parent: &self.request_span, outcome = "complete", elapsed_ms, "Request completed");
        }

        if failed {
            if let Some(connection) = &self.connection {
                connection.mark_failed();
            }
        } else if let Some(connection) = &self.connection {
            connection.record_completed_latency();
        }
        self.connection.take();
    }
}

impl Drop for StreamingBody {
    fn drop(&mut self) {
        if self.connection.is_some() {
            tracing::warn!(
                parent: &self.request_span,
                outcome = "downstream_cancelled",
                elapsed_ms = self.request_started_at.elapsed().as_millis() as u64,
                "Downstream stopped reading the response"
            );
        }
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
            Poll::Ready(Some(Ok(frame))) => {
                let end_stream = this
                    .inner
                    .as_ref()
                    .is_some_and(|body| body.as_ref().get_ref().is_end_stream());
                if end_stream {
                    this.finish(false);
                }

                Poll::Ready(Some(Ok(frame)))
            }
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

async fn get_request(
    proxy: &ProxyState,
    incoming: &IncomingParts,
    body: RequestBody,
    metrics: &mut RequestMetrics,
) -> Result<Response<ProxyBody>, ProxyFailure> {
    let (index, server, uri) = loop {
        let is_eligible = |index: usize, server: &ServerSnapshot| {
            server.alive
                && server.connections < server.max_connections
                && !metrics.attempted.contains(&index)
        };

        let Some(index) = proxy.load_balancer.get_index(&proxy.servers, &is_eligible) else {
            return Err(ProxyFailure::NoBackend);
        };
        let Some(server) = proxy.servers.get(index).cloned() else {
            return Err(ProxyFailure::NoBackend);
        };

        metrics.attempted.insert(index);
        let uri = backend_uri(&server.addr, &incoming.incoming_uri)
            .map_err(|_| ProxyFailure::BadGateway)?;
        if server.try_reserve() {
            break (index, server, uri);
        }
    };

    metrics.request_span.record("backend_index", index + 1);
    let timeout_duration = server.request_timeout.unwrap_or(proxy.config.timeout);
    let timeouts = RequestTimeoutConfig {
        response: timeout_duration,
        upload_idle: proxy.config.client_upload_timeout,
    };

    let mut connection = ConnectionGuard {
        server,
        response_latency: None,
    };

    match send_request(proxy.backend_client.as_ref(), uri, incoming, body, timeouts).await {
        Ok((response, backend_started_at, response_latency)) => {
            connection.response_latency = response_latency;
            let deadline = TokioInstant::from_std(backend_started_at + timeout_duration);
            let (parts, body) = response.into_parts();
            let body = StreamingBody::new(
                body,
                deadline,
                connection,
                metrics.request_span.clone(),
                metrics.request_started_at,
            )
            .boxed();

            Ok(Response::from_parts(parts, body))
        }
        Err(SendFailure::Backend) => {
            tracing::error!(parent: &metrics.request_span, outcome = "backend_request_failed", "Backend request failed");
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
    incoming: &IncomingParts,
    body: RequestBody,
    timeouts: RequestTimeoutConfig,
) -> Result<(Response<Incoming>, Instant, Option<Duration>), SendFailure> {
    let upload_state = Arc::new(AtomicU8::new(UPLOAD_ACTIVE));
    let (upload_started_tx, mut upload_started_rx) = oneshot::channel();
    let (upload_completed_tx, mut upload_completed_rx) = oneshot::channel();
    let body = MonitoredRequestBody::new(
        body,
        timeouts.upload_idle,
        Arc::clone(&upload_state),
        upload_started_tx,
        upload_completed_tx,
    )
    .boxed_unsync();

    let mut forwarded_headers = incoming.headers.clone();
    forwarded_headers.remove(hyper::header::CONNECTION);
    forwarded_headers.remove(hyper::header::HeaderName::from_static("keep-alive"));
    forwarded_headers.remove(hyper::header::PROXY_AUTHENTICATE);
    forwarded_headers.remove(hyper::header::PROXY_AUTHORIZATION);
    forwarded_headers.remove(hyper::header::TE);
    forwarded_headers.remove(hyper::header::TRAILER);
    forwarded_headers.remove(hyper::header::TRANSFER_ENCODING);
    forwarded_headers.remove(hyper::header::UPGRADE);
    let authority = uri.authority().ok_or(SendFailure::Backend)?;
    let host = authority
        .as_str()
        .parse()
        .map_err(|_| SendFailure::Backend)?;
    forwarded_headers.insert(hyper::header::HOST, host);

    let mut request = Request::builder()
        .method(incoming.method.clone())
        .uri(uri)
        .version(incoming.version)
        .body(body)
        .map_err(|_| SendFailure::Backend)?;
    *request.headers_mut() = forwarded_headers;

    let request_started_at = Instant::now();
    let mut request_future = Box::pin(client.request(request));
    let mut upload_completed_at = None;
    let mut connect_deadline = Box::pin(tokio::time::sleep(timeouts.response));

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
                            match timeout(timeouts.response, &mut request_future).await {
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
                        Err(_) => match timeout(timeouts.response, &mut request_future).await {
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
    let upload_finished_at =
        upload_completed_at.or_else(|| upload_completed_rx.try_recv().ok().flatten());
    let backend_started_at = upload_finished_at.unwrap_or(request_started_at);
    let response_headers_at = Instant::now();
    let response_latency =
        upload_finished_at.map(|started_at| response_headers_at.duration_since(started_at));

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

    Ok((response, backend_started_at, response_latency))
}

fn classify_client_result(
    result: Result<Response<Incoming>, hyper_util::client::legacy::Error>,
    upload_state: u8,
) -> Result<Response<Incoming>, SendFailure> {
    result.map_err(|_| match upload_state {
        UPLOAD_READ_FAILED => SendFailure::ClientBody,
        UPLOAD_TIMED_OUT => SendFailure::ClientUploadTimeout,
        _ => SendFailure::Backend,
    })
}

fn classify_upload_state(upload_state: u8) -> SendFailure {
    match upload_state {
        UPLOAD_READ_FAILED => SendFailure::ClientBody,
        UPLOAD_TIMED_OUT => SendFailure::ClientUploadTimeout,
        _ => SendFailure::Backend,
    }
}

pub trait Loadbalancer: Send + Sync {
    fn get_index(
        &self,
        servers: &[Arc<Server>],
        is_eligible: &dyn Fn(usize, &crate::ServerSnapshot) -> bool,
    ) -> Option<usize>;
}
