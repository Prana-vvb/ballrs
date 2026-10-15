use clap::{command, Arg, ArgAction, Command};
use std::error::Error;
use std::io;
use std::time::Duration;

mod config;
mod lb;

#[derive(Debug)]
struct LoadBalancer {
    load_balancer: hyper::Uri,
    algo: Algorithm,
    servers: Vec<Server>,
    timeout: Duration,
    client_upload_timeout: Duration,
    health_check_timeout: Duration,
    health_check_interval: Duration,
    report: bool,
    save_file: String,
    save: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct Server {
    addr: hyper::Uri,
    weight: u32,
    response_time: Duration,
    connections: u32,
    max_connections: u32,
    request_timeout: Option<Duration>,
    alive: bool,
    connections_served: u32,
}

impl Server {
    fn new(
        addr: hyper::Uri,
        weight: u32,
        max_connections: u32,
        request_timeout: Option<Duration>,
    ) -> Self {
        Server {
            addr,
            weight,
            max_connections,
            response_time: Duration::from_secs(0),
            connections: 0,
            alive: true,
            connections_served: 0,
            request_timeout,
        }
    }
}

impl LoadBalancer {
    fn new() -> Self {
        LoadBalancer {
            load_balancer: "http://127.0.0.1:8000".parse::<hyper::Uri>().unwrap(), // default address for load balancer
            algo: Algorithm::RoundRobin, // using round robin as default algorithm
            servers: Vec::new(),
            timeout: Duration::from_secs(0),
            client_upload_timeout: Duration::from_secs(60),
            health_check_timeout: Duration::from_secs(0),
            health_check_interval: Duration::from_secs(0),
            report: true,
            save_file: String::from("data.txt"),
            save: false,
        }
    }
    fn update(&mut self, path: &str) -> io::Result<&LoadBalancer> {
        let config = config::load(path)?;
        let algorithm = get_algo(&config.algorithm)?;
        self.load_balancer = config.load_balancer;
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
                )
            })
            .collect();
        self.timeout = config.timeout;
        self.client_upload_timeout = config.client_upload_timeout;
        self.health_check_timeout = config.health_check_timeout;
        self.health_check_interval = config.health_check_interval;

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

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut lb = LoadBalancer::new();
    lb.update("config.yaml")?;

    cli(lb)
    // drop(lb::start_lb(lb));
    // Ok(())
}

fn cli(mut lb: LoadBalancer) -> Result<(), Box<dyn Error + Send + Sync>> {
    let res = command!()
        .arg_required_else_help(true)
        .about(
            r#"
 ________  ________  ___           ________  ________
|\   __  \|\   __  \|\  \         |\   __  \|\   ____\
\ \  \|\ /\ \  \|\  \ \  \        \ \  \|\  \ \  \___|_
 \ \   __  \ \   __  \ \  \        \ \   _  _\ \_____  \
  \ \  \|\  \ \  \ \  \ \  \____  __\ \  \\  \\|____|\  \
   \ \_______\ \__\ \__\ \_______\\__\ \__\\ _\ ____\_\  \
    \|_______|\|__|\|__|\|_______\|__|\|__|\|__|\_________\
                                               \|_________|
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
                )
                .arg(
                    Arg::new("save config")
                        .short('w')
                        .long("write")
                        .action(ArgAction::SetTrue)
                        .help("Writes CLI config changes to config.yaml"),
                ),
        )
        .get_matches();

    match res.subcommand_name() {
        Some("start") => {
            println!("Starting load balancer");
            let start_args = res.subcommand_matches("start").unwrap();
            let path = start_args.get_one::<String>("path");
            let address = start_args.get_one::<String>("address");
            let algorithm = start_args.get_one::<String>("algorithm");
            let report = start_args.get_one::<bool>("report");
            let save = start_args.get_one::<String>("save file");
            let write = start_args.get_one::<bool>("save config");
            let config_path = path.map(String::as_str).unwrap_or("config.yaml");

            if let Some(path) = path {
                lb.servers = Vec::new();
                lb.update(path)?;
            }

            if let Some(address) = address {
                lb.load_balancer = address.trim().parse::<hyper::Uri>().unwrap();
            }

            if let Some(algorithm) = algorithm {
                lb.algo = get_algo(algorithm)?;
            }

            if let Some(report) = report {
                lb.report = *report;
            }

            if let Some(save) = save {
                lb.save_file.clone_from(save);
                lb.save = true;
            }

            if write.copied().unwrap_or(false) {
                write_to_config(&lb, config_path)?;
            }

            lb::start_lb(lb)?;
        }
        _ => println!("Invalid command"),
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

fn write_to_config(lb: &LoadBalancer, path: &str) -> io::Result<()> {
    let algorithm = match lb.algo {
        Algorithm::RoundRobin => "round_robin",
        Algorithm::WeightedRoundRobin => "weighted_round_robin",
        Algorithm::LeastConnections => "least_connections",
        Algorithm::WeightedLeastConnections => "weighted_least_connections",
        Algorithm::LeastResponseTime => "least_response_time",
        Algorithm::WeightedLeastResponseTime => "weighted_least_response_time",
    };
    let servers = lb
        .servers
        .iter()
        .map(|server| server.addr.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let weights = lb
        .servers
        .iter()
        .map(|server| server.weight.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let max_connections = lb
        .servers
        .iter()
        .map(|server| server.max_connections.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let server_timeouts = lb
        .servers
        .iter()
        .map(|server| {
            server
                .request_timeout
                .map(|timeout| timeout.as_secs().to_string())
                .unwrap_or_else(|| "default".to_string())
        })
        .collect::<Vec<_>>()
        .join(", ");
    let config = format!(
        "load balancer: {}\nalgorithm: {}\nservers: {}\nweights: {}\nmax connections: {}\ntimeout: {}\nclient upload timeout: {}\nserver timeouts: {}\nhealth check timeout: {}\nhealth check interval: {}\n",
        lb.load_balancer,
        algorithm,
        servers,
        weights,
        max_connections,
        lb.timeout.as_secs(),
        lb.client_upload_timeout.as_secs(),
        server_timeouts,
        lb.health_check_timeout.as_secs(),
        lb.health_check_interval.as_secs(),
    );

    std::fs::write(path, config)
}
