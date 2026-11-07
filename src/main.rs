use anyhow::{Context, Result};
use clap::{command, Arg, ArgAction, Command};
use std::io;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

mod config;
mod lb;

#[derive(Debug)]
struct LoadBalancer {
    load_balancer: hyper::Uri,
    max_client_connections: usize,
    backend_tcp_nodelay: bool,
    algo: Algorithm,
    servers: Vec<Server>,
    timeout: Duration,
    client_upload_timeout: Duration,
    health_check_timeout: Duration,
    health_check_interval: Duration,
    health_check_healthy_threshold: u32,
    health_check_unhealthy_threshold: u32,
    report: bool,
    save_file: String,
    save: bool,
}

#[derive(Clone)]
struct RuntimeConfig {
    load_balancer: hyper::Uri,
    max_client_connections: usize,
    backend_tcp_nodelay: bool,
    timeout: Duration,
    client_upload_timeout: Duration,
    health_check_interval: Duration,
    report: bool,
    save_file: String,
    save: bool,
}

#[derive(Clone, Copy)]
enum BackendHealthUpdate {
    Probe {
        started_at: Instant,
        healthy: bool,
        latency: Duration,
    },
    RequestFailure {
        observed_at: Instant,
    },
}

#[derive(Debug)]
struct Server {
    addr: hyper::Uri,
    weight: u32,
    max_connections: u32,
    request_timeout: Option<Duration>,
    health_check: HealthCheckConfig,
    metrics: Mutex<BackendMetrics>,
}

#[derive(Debug)]
struct HealthCheckConfig {
    path: hyper::Uri,
    timeout: Duration,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
}

#[derive(Debug, Clone, Copy)]
struct BackendMetrics {
    request_latency_ewma: Option<Duration>,
    health_check_response_time: Duration,
    health_check_sampled: bool,
    consecutive_probe_successes: u32,
    consecutive_probe_failures: u32,
    connections: u32,
    alive: bool,
    availability_updated_at: Instant,
    connections_served: u32,
}

#[derive(Debug, Clone, Copy)]
struct ServerSnapshot {
    weight: u32,
    max_connections: u32,
    request_latency_ewma: Option<Duration>,
    health_check_response_time: Duration,
    health_check_sampled: bool,
    connections: u32,
    connections_served: u32,
    alive: bool,
}

impl ServerSnapshot {
    fn response_time_estimate(self) -> Option<Duration> {
        self.request_latency_ewma.or_else(|| {
            self.health_check_sampled
                .then_some(self.health_check_response_time)
        })
    }
}

fn lock_recover<'a, T>(mutex: &'a Mutex<T>, lock_name: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(lock = lock_name, "Recovering poisoned runtime metrics lock");
            mutex.clear_poison();
            poisoned.into_inner()
        }
    }
}

impl Server {
    fn snapshot(&self) -> ServerSnapshot {
        let metrics = lock_recover(&self.metrics, "backend_metrics");
        ServerSnapshot {
            weight: self.weight,
            max_connections: self.max_connections,
            request_latency_ewma: metrics.request_latency_ewma,
            health_check_response_time: metrics.health_check_response_time,
            health_check_sampled: metrics.health_check_sampled,
            connections: metrics.connections,
            connections_served: metrics.connections_served,
            alive: metrics.alive,
        }
    }

    fn try_reserve(&self) -> bool {
        let mut metrics = lock_recover(&self.metrics, "backend_metrics");
        if !metrics.alive || metrics.connections >= self.max_connections {
            return false;
        }
        metrics.connections += 1;
        metrics.connections_served = metrics.connections_served.saturating_add(1);
        true
    }

    fn release_connection(&self) {
        let mut metrics = lock_recover(&self.metrics, "backend_metrics");
        metrics.connections = metrics.connections.saturating_sub(1);
    }

    fn record_health_update(&self, update: BackendHealthUpdate) {
        let mut metrics = lock_recover(&self.metrics, "backend_metrics");
        match update {
            BackendHealthUpdate::Probe {
                started_at,
                healthy,
                latency,
            } => {
                if started_at >= metrics.availability_updated_at {
                    metrics.health_check_response_time = latency;
                    metrics.health_check_sampled = healthy;
                    if healthy {
                        metrics.consecutive_probe_failures = 0;
                        metrics.consecutive_probe_successes =
                            metrics.consecutive_probe_successes.saturating_add(1);
                        if metrics.consecutive_probe_successes
                            >= self.health_check.healthy_threshold
                        {
                            metrics.alive = true;
                        }
                    } else {
                        metrics.consecutive_probe_successes = 0;
                        metrics.consecutive_probe_failures =
                            metrics.consecutive_probe_failures.saturating_add(1);
                        if metrics.consecutive_probe_failures
                            >= self.health_check.unhealthy_threshold
                        {
                            metrics.alive = false;
                        }
                    }
                    metrics.availability_updated_at = started_at;
                }
            }
            BackendHealthUpdate::RequestFailure { observed_at } => {
                if observed_at >= metrics.availability_updated_at {
                    metrics.consecutive_probe_successes = 0;
                    metrics.alive = false;
                    metrics.availability_updated_at = observed_at;
                }
            }
        };
    }

    fn record_request_latency(&self, sample: Duration) {
        const NANOS_PER_SECOND: u128 = 1_000_000_000;

        let mut metrics = lock_recover(&self.metrics, "backend_metrics");
        let next_nanos = match metrics.request_latency_ewma {
            Some(previous) => (previous.as_nanos() * 4 + sample.as_nanos()) / 5,
            None => sample.as_nanos(),
        };
        metrics.request_latency_ewma = Some(Duration::new(
            (next_nanos / NANOS_PER_SECOND) as u64,
            (next_nanos % NANOS_PER_SECOND) as u32,
        ));
    }

    fn new(
        addr: hyper::Uri,
        weight: u32,
        max_connections: u32,
        request_timeout: Option<Duration>,
        health_check: HealthCheckConfig,
    ) -> Self {
        Server {
            addr,
            weight,
            max_connections,
            request_timeout,
            health_check,
            metrics: Mutex::new(BackendMetrics {
                request_latency_ewma: None,
                health_check_response_time: Duration::ZERO,
                health_check_sampled: false,
                consecutive_probe_successes: 0,
                consecutive_probe_failures: 0,
                connections: 0,
                alive: false,
                availability_updated_at: Instant::now(),
                connections_served: 0,
            }),
        }
    }
}

impl Default for LoadBalancer {
    fn default() -> Self {
        LoadBalancer {
            load_balancer: hyper::Uri::from_static("http://127.0.0.1:8000"),
            max_client_connections: 1024,
            backend_tcp_nodelay: false,
            algo: Algorithm::RoundRobin, // using round robin as default algorithm
            servers: Vec::new(),
            timeout: Duration::from_secs(0),
            client_upload_timeout: Duration::from_secs(60),
            health_check_timeout: Duration::from_secs(0),
            health_check_interval: Duration::from_secs(0),
            health_check_healthy_threshold: 1,
            health_check_unhealthy_threshold: 2,
            report: true,
            save_file: String::from("data.txt"),
            save: false,
        }
    }
}

impl LoadBalancer {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, path: &str) -> Result<&LoadBalancer> {
        let config = config::load(path)
            .with_context(|| format!("failed to load configuration from {path:?}"))?;
        let algorithm =
            get_algo(&config.algorithm).context("invalid algorithm in configuration")?;
        self.load_balancer = config.load_balancer;
        self.max_client_connections = config.max_client_connections;
        self.backend_tcp_nodelay = config.backend_tcp_nodelay;
        self.algo = algorithm;
        self.servers = config
            .servers
            .into_iter()
            .map(|server| {
                Server::new(
                    server.addr,
                    server.weight,
                    server.max_connections,
                    server.request_timeout,
                    HealthCheckConfig {
                        path: server.health_check_path,
                        timeout: server.health_check_timeout,
                        healthy_threshold: config.health_check_healthy_threshold,
                        unhealthy_threshold: config.health_check_unhealthy_threshold,
                    },
                )
            })
            .collect();
        self.timeout = config.timeout;
        self.client_upload_timeout = config.client_upload_timeout;
        self.health_check_timeout = config.health_check_timeout;
        self.health_check_interval = config.health_check_interval;
        self.health_check_healthy_threshold = config.health_check_healthy_threshold;
        self.health_check_unhealthy_threshold = config.health_check_unhealthy_threshold;

        Ok(self)
    }
}

#[derive(Debug, Clone)]
enum Algorithm {
    RoundRobin,
    WeightedRoundRobin,
    LeastConnections,
    WeightedLeastConnections,
    LeastResponseTime,
    WeightedLeastResponseTime,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;

    cli(LoadBalancer::new())
}

fn cli(mut lb: LoadBalancer) -> Result<()> {
    let res = command!()
        .arg_required_else_help(true)
        .about(
            r#"
 ____        _ _
|  _ \      | | |
| |_) | __ _| | |_ __ ___
|  _ < / _` | | | '__/ __|
| |_) | (_| | | | |  \__ \
|____/ \__,_|_|_|_|  |___/

L7 Load Balancer Implemented in Rust 🦀
    "#,
        )
        .subcommand(
            Command::new("start")
                .about("Start the load balancer")
                .arg(
                    Arg::new("address")
                        .short('u')
                        .long("address")
                        .help("Starts load balancer at specified address"),
                )
                .arg(Arg::new("algorithm").short('a').long("algorithm").help(
                    "Starts load balancer with specified algorithm.
Available algorithms: round_robin/rr, weighted_round_robin/wrr, least_connections/lc,
weighted_least_connections/wlc, least_response_time/lrt, weighted_least_response_time/wlrt",
                ))
                .arg(
                    Arg::new("path")
                        .short('p')
                        .long("path")
                        .default_value("config.yaml")
                        .help("Specify path to config file"),
                )
                .arg(
                    Arg::new("report")
                        .short('r')
                        .long("report")
                        .action(ArgAction::SetTrue)
                        .help("Prints server status"),
                )
                .arg(
                    Arg::new("save file")
                        .short('s')
                        .long("save")
                        .help("Saves report data to specified file"),
                ),
        )
        .get_matches();

    match res.subcommand_name() {
        Some("start") => {
            tracing::info!("Starting load balancer");
            let start_args = res
                .subcommand_matches("start")
                .context("start subcommand arguments were unavailable")?;
            let path = start_args
                .get_one::<String>("path")
                .context("configuration path argument was unavailable")?;
            let address = start_args.get_one::<String>("address");
            let algorithm = start_args.get_one::<String>("algorithm");
            let report = start_args.get_one::<bool>("report");
            let save = start_args.get_one::<String>("save file");

            lb.update(path)?;

            if let Some(address) = address {
                lb.load_balancer = address.trim().parse::<hyper::Uri>().with_context(|| {
                    format!("invalid listener URI supplied by --address: {address:?}")
                })?;
            }

            if let Some(algorithm) = algorithm {
                lb.algo = get_algo(algorithm).with_context(|| {
                    format!("invalid algorithm supplied by --algorithm: {algorithm:?}")
                })?;
            }

            if let Some(report) = report {
                lb.report = *report;
            }

            if let Some(save) = save {
                lb.save_file.clone_from(save);
                lb.save = true;
            }

            lb::start_lb(lb)
                .context("load balancer failed during startup or listener operation")?;
        }
        _ => anyhow::bail!("missing `start` subcommand"),
    }

    Ok(())
}

fn get_algo(algo: &str) -> io::Result<Algorithm> {
    let normalized = algo.trim().to_ascii_lowercase();
    let algorithm = match normalized.as_str() {
        "round_robin" | "rr" => Algorithm::RoundRobin,
        "weighted_round_robin" | "wrr" => Algorithm::WeightedRoundRobin,
        "least_connections" | "lc" => Algorithm::LeastConnections,
        "weighted_least_connections" | "wlc" => Algorithm::WeightedLeastConnections,
        "least_response_time" | "lrt" => Algorithm::LeastResponseTime,
        "weighted_least_response_time" | "wlrt" => Algorithm::WeightedLeastResponseTime,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported algorithm {algo:?}. Supported algorithms: round_robin/rr, \
                     weighted_round_robin/wrr, least_connections/lc, \
                     weighted_least_connections/wlc, least_response_time/lrt, \
                     weighted_least_response_time/wlrc/wlrt"
                ),
            ));
        }
    };

    Ok(algorithm)
}
