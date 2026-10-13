use std::fmt::Display;
use std::io::{self, Read};
use std::str::FromStr;
use std::time::Duration;

use hyper::Uri;
use serde::Deserialize;

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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawList<T> {
    Values(Vec<T>),
    CommaSeparated(String),
    Single(T),
}

impl<T> RawList<T> {
    fn into_values(self, mut parse_item: impl FnMut(&str) -> io::Result<T>) -> io::Result<Vec<T>> {
        match self {
            Self::Values(values) => Ok(values),
            Self::CommaSeparated(values) => values
                .split(',')
                .map(|value| parse_item(value.trim()))
                .collect(),
            Self::Single(value) => Ok(vec![value]),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawServerTimeout {
    Seconds(u64),
    Text(String),
}

impl RawServerTimeout {
    fn into_duration(self) -> io::Result<Option<Duration>> {
        match self {
            Self::Seconds(seconds) => Ok(Some(Duration::from_secs(seconds))),
            Self::Text(value) if value.eq_ignore_ascii_case("default") => Ok(None),
            Self::Text(value) => Err(invalid_value(
                "server timeouts",
                &value,
                "expected a number of seconds or 'default'",
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct RawConfig {
    #[serde(rename = "load balancer")]
    load_balancer: String,
    algorithm: String,
    servers: Option<RawList<String>>,
    weights: Option<RawList<u32>>,
    #[serde(rename = "max connections")]
    max_connections: Option<RawList<u32>>,
    timeout: u64,
    #[serde(rename = "server timeouts")]
    server_timeouts: Option<RawList<RawServerTimeout>>,
    #[serde(rename = "health check timeout")]
    health_check_timeout: Option<u64>,
    #[serde(rename = "health check interval")]
    health_check_interval: u64,
}

impl Default for RawConfig {
    fn default() -> Self {
        Self {
            load_balancer: String::from("http://127.0.0.1:8000"),
            algorithm: String::from("round_robin"),
            servers: None,
            weights: None,
            max_connections: None,
            timeout: 0,
            server_timeouts: None,
            health_check_timeout: None,
            health_check_interval: 0,
        }
    }
}

pub(crate) fn load(path: &str) -> io::Result<Config> {
    let mut file = std::fs::File::open(path)?;
    let mut yaml = String::new();
    file.read_to_string(&mut yaml)?;
    let raw: RawConfig = serde_yaml::from_str(&yaml).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid config YAML: {error}"),
        )
    })?;

    let load_balancer = parse_value("load balancer", &raw.load_balancer)?;
    let server_addr_values = raw
        .servers
        .ok_or_else(|| missing_field("servers"))?
        .into_values(|value| Ok(value.to_owned()))?;
    let server_addrs = server_addr_values
        .iter()
        .map(|value| parse_value("servers", value))
        .collect::<io::Result<Vec<Uri>>>()?;
    if server_addrs.is_empty() {
        return Err(invalid_value(
            "servers",
            "[]",
            "at least one backend is required",
        ));
    }

    let weights = match raw.weights {
        Some(weights) => weights.into_values(|value| parse_value("weights", value))?,
        None => vec![1; server_addrs.len()],
    };
    let max_connections = raw
        .max_connections
        .ok_or_else(|| missing_field("max connections"))?
        .into_values(|value| parse_value("max connections", value))?;
    validate_list_len("weights", weights.len(), server_addrs.len())?;
    validate_list_len("max connections", max_connections.len(), server_addrs.len())?;

    let server_timeouts = match raw.server_timeouts {
        Some(timeouts) => timeouts
            .into_values(parse_server_timeout)?
            .into_iter()
            .map(RawServerTimeout::into_duration)
            .collect::<io::Result<Vec<_>>>()?,
        None => vec![None; server_addrs.len()],
    };
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

    let timeout = Duration::from_secs(raw.timeout);
    Ok(Config {
        load_balancer,
        algorithm: raw.algorithm,
        servers,
        timeout,
        health_check_timeout: Duration::from_secs(raw.health_check_timeout.unwrap_or(raw.timeout)),
        health_check_interval: Duration::from_secs(raw.health_check_interval),
    })
}

fn parse_server_timeout(value: &str) -> io::Result<RawServerTimeout> {
    if value.eq_ignore_ascii_case("default") {
        return Ok(RawServerTimeout::Text(String::from("default")));
    }

    value
        .parse::<u64>()
        .map(RawServerTimeout::Seconds)
        .map_err(|error| {
            invalid_value(
                "server timeouts",
                value,
                &format!("expected a number of seconds or 'default': {error}"),
            )
        })
}

fn parse_value<T>(field: &str, value: &str) -> io::Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse::<T>()
        .map_err(|error| invalid_value(field, value, &error.to_string()))
}

fn missing_field(field: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("missing '{field}' config entry"),
    )
}

fn invalid_value(field: &str, value: &str, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid '{field}' value '{value}': {reason}"),
    )
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
