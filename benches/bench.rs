use std::env;
use std::fs::{self, File};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const ALGORITHMS: [&str; 6] = [
    "round_robin",
    "weighted_round_robin",
    "least_connections",
    "weighted_least_connections",
    "least_response_time",
    "weighted_least_response_time",
];
const CONCURRENCIES: [usize; 3] = [1, 8, 32];
const BODY_SIZE: usize = 64 * 1024;

struct Options {
    quick: bool,
    output: Option<PathBuf>,
}

struct Artifacts(PathBuf);

impl Drop for Artifacts {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct RunningBalancer(Child);

impl Drop for RunningBalancer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Get,
    Post64K,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Get => "GET-1KiB",
            Self::Post64K => "POST-64KiB",
        }
    }
}

struct Metrics {
    requests_per_second: f64,
    failed: u64,
    duration_seconds: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

struct Row {
    target: String,
    workload: String,
    tcp_nodelay: String,
    concurrency: usize,
    repetition: usize,
    metrics: Metrics,
}

fn parse_options() -> Result<Options> {
    let mut args = env::args().skip(1);
    let mut quick = false;
    let mut output = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--quick" => quick = true,
            // Cargo appends this marker when launching a custom bench target.
            "--bench" => {}
            "--output" => {
                let path = args.next().context("--output requires a file path")?;
                output = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                println!("Usage: cargo bench --bench bench -- [--quick] [--output PATH]");
                println!(
                    "  --quick       Use 10 warmups, 100 measured requests, and one repetition"
                );
                println!("  --output PATH Write CSV results to PATH (default: timestamped file)");
                std::process::exit(0);
            }
            _ => bail!("unknown benchmark option {arg:?}; use --help"),
        }
    }
    Ok(Options { quick, output })
}

fn ab_path() -> Result<PathBuf> {
    let fixed = Path::new("/sbin/ab");
    if fixed.is_file() {
        return Ok(fixed.to_owned());
    }
    if let Some(path) = env::var_os("PATH") {
        for directory in env::split_paths(&path) {
            let candidate = directory.join("ab");
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }
    bail!("ApacheBench is required; install `ab` or put it on PATH")
}

async fn backend_service(
    mut request: Request<Incoming>,
) -> std::result::Result<Response<Full<Bytes>>, io::Error> {
    while let Some(frame) = request.body_mut().frame().await {
        frame.map_err(io::Error::other)?;
    }
    let body = Full::new(Bytes::from(vec![b'G'; 1024]));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .body(body)
        .map_err(io::Error::other)
}

async fn start_backend() -> Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let _ = stream.set_nodelay(true);
            tokio::spawn(async move {
                let service = service_fn(|request| async move {
                    match backend_service(request).await {
                        Ok(response) => Ok::<_, io::Error>(response),
                        Err(_) => Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Full::new(Bytes::new()))
                            .expect("static fixture response is valid")),
                    }
                });
                let _ = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok((address, task))
}

fn artifact_dir() -> Result<Artifacts> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = env::temp_dir().join(format!("balrs-bench-{}-{nonce}", std::process::id()));
    fs::create_dir(&path)?;
    Ok(Artifacts(path))
}

fn binary_path() -> Result<PathBuf> {
    let current = env::current_exe().context("cannot locate benchmark executable")?;
    let release_dir = current
        .parent()
        .and_then(Path::parent)
        .context("benchmark executable is not under target/<profile>/deps")?;
    let binary = release_dir.join(format!("Balrs{}", env::consts::EXE_SUFFIX));
    if !binary.is_file() {
        bail!(
            "Balrs binary not found at {}; build it first with `cargo build --release --bin Balrs`",
            binary.display()
        );
    }
    Ok(binary)
}

fn write_config(
    path: &Path,
    listener: SocketAddr,
    backends: &[SocketAddr],
    algorithm: &str,
    tcp_nodelay: bool,
) -> Result<()> {
    let backend_urls = backends
        .iter()
        .map(|address| format!("http://{address}"))
        .collect::<Vec<_>>()
        .join(", ");
    let yaml = format!(
        "load balancer: http://{listener}\n\
         max client connections: 256\n\
         backend tcp nodelay: {tcp_nodelay}\n\
         algorithm: {algorithm}\n\
         servers: {backend_urls}\n\
         weights: 1, 2, 3\n\
         max connections: 256, 256, 256\n\
         timeout: 120\n\
         client upload timeout: 120\n\
         health check timeout: 2\n\
         health check interval: 1\n"
    );
    fs::write(path, yaml).with_context(|| format!("failed to write {}", path.display()))
}

async fn start_balancer(binary: &Path, config: &Path, log: &Path) -> Result<RunningBalancer> {
    let output =
        File::create(log).with_context(|| format!("failed to create {}", log.display()))?;
    let child = Command::new(binary)
        .args(["start", "--path"])
        .arg(config)
        .env("RUST_LOG", "error")
        .stdout(Stdio::from(output.try_clone()?))
        .stderr(Stdio::from(output))
        .spawn()
        .with_context(|| format!("failed to start {}", binary.display()))?;
    Ok(RunningBalancer(child))
}

async fn wait_ready(address: SocketAddr, process: &mut RunningBalancer) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = process.0.try_wait()? {
            bail!("Balrs exited before becoming ready: {status}");
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("Balrs did not serve a healthy backend at {address} within 15 seconds");
        }
        if let Ok(Ok(true)) = tokio::time::timeout(Duration::from_millis(250), async {
            let mut stream = TcpStream::connect(address).await?;
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<bool, io::Error>(response.starts_with(b"HTTP/1.1 200"))
        })
        .await
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn parse_ab(output: &str) -> Result<Metrics> {
    fn value(output: &str, prefix: &str) -> Result<f64> {
        let line = output
            .lines()
            .find(|line| line.trim_start().starts_with(prefix))
            .with_context(|| format!("ApacheBench output omitted {prefix:?}"))?;
        line.split_whitespace()
            .find_map(|part| part.parse::<f64>().ok())
            .with_context(|| format!("could not parse ApacheBench line {line:?}"))
    }

    fn percentile(output: &str, prefix: &str) -> Result<f64> {
        let line = output
            .lines()
            .find(|line| line.trim_start().starts_with(prefix))
            .with_context(|| format!("ApacheBench output omitted percentile {prefix:?}"))?;
        line.split_whitespace()
            .find_map(|part| part.parse::<f64>().ok())
            .with_context(|| format!("could not parse ApacheBench percentile {line:?}"))
    }

    let failed = value(output, "Failed requests:")? as u64;
    if failed != 0 {
        bail!("ApacheBench reported {failed} failed requests\n{output}");
    }
    Ok(Metrics {
        requests_per_second: value(output, "Requests per second:")?,
        failed,
        duration_seconds: value(output, "Time taken for tests:")?,
        p50_ms: percentile(output, "50%")?,
        p95_ms: percentile(output, "95%")?,
        p99_ms: percentile(output, "99%")?,
    })
}

fn run_ab(
    ab: &Path,
    url: &str,
    body_path: &Path,
    workload: Workload,
    concurrency: usize,
    count: usize,
) -> Result<Metrics> {
    let mut command = Command::new(ab);
    command.args(["-n", &count.max(concurrency).to_string()]);
    command.args(["-c", &concurrency.to_string(), "-k", "-q"]);
    if matches!(workload, Workload::Post64K) {
        command.args(["-p", body_path.to_str().context("non-UTF8 body path")?]);
        command.args(["-T", "application/octet-stream"]);
    }
    command.arg(url);
    let output = command.output().context("failed to run ApacheBench")?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        bail!("ApacheBench exited with {}\n{text}", output.status);
    }
    parse_ab(&text)
}

fn write_csv(path: &Path, rows: &[Row]) -> Result<()> {
    let mut csv = String::from(
        "target,workload,tcp_nodelay,concurrency,repetition,requests_per_second,failed,duration_seconds,p50_ms,p95_ms,p99_ms\n",
    );
    for row in rows {
        csv.push_str(&format!(
            "{},{},{},{},{},{:.2},{},{:.3},{:.1},{:.1},{:.1}\n",
            row.target,
            row.workload,
            row.tcp_nodelay,
            row.concurrency,
            row.repetition,
            row.metrics.requests_per_second,
            row.metrics.failed,
            row.metrics.duration_seconds,
            row.metrics.p50_ms,
            row.metrics.p95_ms,
            row.metrics.p99_ms,
        ));
    }
    fs::write(path, csv).with_context(|| format!("failed to write {}", path.display()))
}

fn default_output_path() -> PathBuf {
    let timestamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
    PathBuf::from(format!("benchmark-results-{timestamp}.csv"))
}

async fn run_scenario(
    ab: &Path,
    target: &str,
    nodelay: Option<bool>,
    base_url: &str,
    workload: Workload,
    concurrency: usize,
    warmup: usize,
    requests: usize,
    repetitions: usize,
    body_path: &Path,
    rows: &mut Vec<Row>,
) -> Result<()> {
    run_ab(ab, base_url, body_path, workload, concurrency, warmup)?;
    for repetition in 1..=repetitions {
        let metrics = run_ab(ab, base_url, body_path, workload, concurrency, requests)?;
        println!(
            "{target:28} {:10} nodelay={:<5} c={concurrency:2} r={repetition} {:9.1} req/s p95={:.1}ms p99={:.1}ms",
            workload.name(),
            nodelay.map_or_else(|| "n/a".to_owned(), |value| value.to_string()),
            metrics.requests_per_second,
            metrics.p95_ms,
            metrics.p99_ms,
        );
        rows.push(Row {
            target: target.to_owned(),
            workload: workload.name().to_owned(),
            tcp_nodelay: nodelay.map_or_else(|| "n/a".to_owned(), |value| value.to_string()),
            concurrency,
            repetition,
            metrics,
        });
    }
    Ok(())
}

async fn run() -> Result<()> {
    let options = parse_options()?;
    let ab = ab_path()?;
    let binary = binary_path()?;
    let artifacts = artifact_dir()?;
    let body_path = artifacts.0.join("post-64k.bin");
    fs::write(&body_path, vec![b'P'; BODY_SIZE])?;
    let output_path = options.output.unwrap_or_else(default_output_path);

    let mut backend_addrs = Vec::new();
    let mut backend_tasks = Vec::new();
    for _ in 0..3 {
        let (address, task) = start_backend().await?;
        backend_addrs.push(address);
        backend_tasks.push(task);
    }

    let warmup = if options.quick { 10 } else { 1_000 };
    let requests = if options.quick { 100 } else { 10_000 };
    let repetitions = if options.quick { 1 } else { 3 };
    let workloads = [Workload::Get, Workload::Post64K];
    let scenarios = workloads.len() * CONCURRENCIES.len();
    let targets = std::iter::once("direct")
        .chain(ALGORITHMS.iter().copied())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();

    for target in targets {
        let base_url = if target == "direct" {
            format!("http://{}/", backend_addrs[0])
        } else {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            drop(listener);
            let config_path = artifacts.0.join(format!("{target}.yaml"));
            let log_path = artifacts.0.join(format!("{target}.log"));
            write_config(&config_path, address, &backend_addrs, target, false)?;
            let mut process = start_balancer(&binary, &config_path, &log_path).await?;
            wait_ready(address, &mut process)
                .await
                .with_context(|| format!("Balrs target {target} failed readiness"))?;

            let mut order = Vec::new();
            for workload in workloads {
                for concurrency in CONCURRENCIES {
                    order.push((workload, concurrency));
                }
            }
            let shift = ALGORITHMS
                .iter()
                .position(|algorithm| *algorithm == target)
                .unwrap_or(0)
                % scenarios;
            order.rotate_left(shift);
            for (workload, concurrency) in order {
                run_scenario(
                    &ab,
                    target,
                    Some(false),
                    &format!("http://{address}/"),
                    workload,
                    concurrency,
                    warmup,
                    requests,
                    repetitions,
                    &body_path,
                    &mut rows,
                )
                .await?;
            }
            drop(process);
            continue;
        };

        let mut order = Vec::new();
        for workload in workloads {
            for concurrency in CONCURRENCIES {
                order.push((workload, concurrency));
            }
        }
        for (workload, concurrency) in order {
            run_scenario(
                &ab,
                target,
                None,
                &base_url,
                workload,
                concurrency,
                warmup,
                requests,
                repetitions,
                &body_path,
                &mut rows,
            )
            .await?;
        }
    }

    // Reuse the ordinary round-robin POST rows as the false side of the A/B comparison.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    let config_path = artifacts.0.join("round_robin_nodelay_true.yaml");
    let log_path = artifacts.0.join("round_robin_nodelay_true.log");
    write_config(&config_path, address, &backend_addrs, "round_robin", true)?;
    let mut process = start_balancer(&binary, &config_path, &log_path).await?;
    wait_ready(address, &mut process)
        .await
        .context("Balrs with backend TCP_NODELAY enabled failed readiness")?;
    for concurrency in CONCURRENCIES {
        run_scenario(
            &ab,
            "round_robin",
            Some(true),
            &format!("http://{address}/"),
            Workload::Post64K,
            concurrency,
            warmup,
            requests,
            repetitions,
            &body_path,
            &mut rows,
        )
        .await?;
    }
    drop(process);

    for task in backend_tasks {
        task.abort();
    }
    write_csv(&output_path, &rows)?;
    println!(
        "\nSaved {} measured rows to {}",
        rows.len(),
        output_path.display()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
