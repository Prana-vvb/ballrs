use std::fmt::Display;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use hyper::Uri;

#[derive(Debug)]
pub(crate) struct ServerConfig {
    pub(crate) addr: Uri,
    pub(crate) weight: u32,
    pub(crate) max_connections: u32,
    pub(crate) request_timeout: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) load_balancer: Uri,
    pub(crate) algorithm: String,
    pub(crate) servers: Vec<ServerConfig>,
    pub(crate) timeout: Duration,
    pub(crate) health_check_timeout: Duration,
    pub(crate) health_check_interval: Duration,
}

pub(crate) fn load(path: &str) -> io::Result<Config> {
    let file = std::fs::File::open(Path::new(path))?;
    let reader = BufReader::new(file);

    let mut load_balancer = parse_value("load balancer", "http://127.0.0.1:8000")?;
    let mut algorithm = String::from("round_robin");
    let mut timeout = Duration::ZERO;
    let mut health_check_timeout = None;
    let mut health_check_interval = Duration::ZERO;
    let mut server_addrs = None;
    let mut weights = None;
    let mut max_connections = None;
    let mut server_timeouts = None;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();

        match key {
            "load balancer" => load_balancer = parse_value(key, value)?,
            "algorithm" => algorithm = value.to_owned(),
            "servers" => server_addrs = Some(parse_list(key, value)?),
            "weights" => weights = Some(parse_list(key, value)?),
            "max connections" => max_connections = Some(parse_list(key, value)?),
            "server timeouts" => {
                server_timeouts = Some(
                    value
                        .split(',')
                        .map(|entry| {
                            let entry = entry.trim();
                            if entry.eq_ignore_ascii_case("default") {
                                Ok(None)
                            } else {
                                parse_value::<u64>(key, entry)
                                    .map(Duration::from_secs)
                                    .map(Some)
                            }
                        })
                        .collect::<io::Result<Vec<_>>>()?,
                );
            }
            "timeout" => timeout = Duration::from_secs(parse_value(key, value)?),
            "health check timeout" => {
                health_check_timeout = Some(Duration::from_secs(parse_value(key, value)?));
            }
            "health check interval" => {
                health_check_interval = Duration::from_secs(parse_value(key, value)?);
            }
            _ => {}
        }
    }

    let server_addrs = server_addrs.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing 'servers' config entry")
    })?;
    if server_addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "'servers' must contain at least one backend",
        ));
    }

    let weights = weights.unwrap_or_else(|| vec![1; server_addrs.len()]);
    let max_connections = max_connections.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "missing 'max connections' config entry",
        )
    })?;
    validate_list_len("weights", weights.len(), server_addrs.len())?;
    validate_list_len("max connections", max_connections.len(), server_addrs.len())?;

    let server_timeouts = server_timeouts.unwrap_or_else(|| vec![None; server_addrs.len()]);
    validate_list_len("server timeouts", server_timeouts.len(), server_addrs.len())?;

    let servers = server_addrs
        .into_iter()
        .zip(weights)
        .zip(max_connections)
        .zip(server_timeouts)
        .map(
            |(((addr, weight), max_connections), request_timeout)| ServerConfig {
                addr,
                weight,
                max_connections,
                request_timeout,
            },
        )
        .collect();

    Ok(Config {
        load_balancer,
        algorithm,
        servers,
        timeout,
        health_check_timeout: health_check_timeout.unwrap_or(timeout),
        health_check_interval,
    })
}

fn parse_value<T>(field: &str, value: &str) -> io::Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    value.parse().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid '{field}' value '{value}': {err}"),
        )
    })
}

fn parse_list<T>(field: &str, values: &str) -> io::Result<Vec<T>>
where
    T: FromStr,
    T::Err: Display,
{
    values
        .split(',')
        .map(|value| parse_value(field, value.trim()))
        .collect()
}

fn validate_list_len(field: &str, actual: usize, expected: usize) -> io::Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("'{field}' has {actual} entries but 'servers' has {expected}"),
        ))
    }
}
