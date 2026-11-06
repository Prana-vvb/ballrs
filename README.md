# Bal.rs
```text
  ____        _ _          
 |  _ \      | | |         
 | |_) | __ _| | |_ __ ___ 
 |  _ < / _` | | | '__/ __|
 | |_) | (_| | | | |  \__ \
 |____/ \__,_|_|_|_|  |___/
                           
L7 Load Balancer Implemented in Rust 🦀
```
> This project was a part of [Tilde 3.0](https://github.com/homebrew-ec-foss/Tilde-3.0) HSP PESU-ECC's summer mentoring program.

Now updated!

## Getting started
To build the application locally, ensure you have the [Rust compiler and Cargo package manager installed](https://doc.rust-lang.org/book/ch01-01-installation.html). Once installed, clone the repository and build the application using Cargo:
```sh
git clone https://github.com/homebrew-ec-foss/bal.rs
cd bal.rs
cargo build
```
For a production-ready build, you can use:
```sh
cargo build --release
```

## How to Use
### The Application
After building, the main executable will be located in `/target/debug` or `/target/release` based on the build command used.
Navigate to the directory and type
```sh
Balrs help
```
in the terminal to get a list of available commands.

Alternatively, from the root directory of Bal.rs, you can use:
```sh
cargo run help
```
for the same result.

### The Config file
The `config.yaml` file allows you to customize the Load Balancer settings:

- <mark>**Load Balancer address**</mark>: The URL of the Load Balancer(Default: http://localhost:8000)
- <mark>**Algorithm**</mark>: The load balancing algorithm to be used by Balrs(Default: round robin)
- <mark>**Servers**</mark>: List of server URLs to which Balrs can send requests
- <mark>**Weights**</mark>: List of weights of respective servers, used by weighted load balancing algorithms
- <mark>**Max connections**</mark>: List of the maximum number of connections each server is allowed to handle
- <mark>**Max client connections**</mark>: Global maximum number of downstream TCP connections across listener addresses (Default: 1024); new connections are closed when the limit is full
- <mark>**Backend TCP_NODELAY**</mark>: Enables `TCP_NODELAY` on backend TCP connections (Default: false)
- <mark>**Timeout**</mark>: Maximum time allowed for a server to respond before it is marked as dead
- <mark>**Health check interval**</mark>: Time interval at which server health checks are performed

The listener accepts plaintext HTTP/1 only; terminate inbound TLS in front of Balrs. Backend URLs may use `http://` or `https://` and use HTTP/1. HTTPS certificates are verified against the system root certificate store.

### Benchmarks

Install Hey (`go install github.com/rakyll/hey@v0.1.5`) and put its Go bin directory on `PATH`, then build Balrs and run the persistent benchmark harness:

```sh
cargo build --release --bin Ballrs
cargo bench --bench bench
```

The harness follows the Loggly/Hey load profile with one million requests per case at concurrency 50, 250, and 500. The comparison workload is a fixed 1 KiB GET sent to the direct backend and each of the six Balrs algorithms. A supplemental streamed 64 KiB POST set compares the direct backend with round-robin using the default backend socket setting and with backend `TCP_NODELAY` enabled; this targets the streamed upload performance question without multiplying the full matrix. Hey reports request rate, latency percentiles, status codes, and transport errors; results go to a timestamped CSV. HTTPS listener testing is omitted because Balrs currently accepts plaintext HTTP only. Use `cargo bench --bench bench -- --quick` for a 1,000-request smoke run, or add `--output path.csv` to choose a CSV path.
