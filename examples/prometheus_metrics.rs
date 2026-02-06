//! Minimal Prometheus metrics example.
//!
//! This target exists because Cargo.toml exposes it as `--example prometheus_metrics`.
//! It is intentionally small and dependency-light.

use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;

fn main() {
    // Install a Prometheus recorder and serve on localhost.
    // The exporter depends on `metrics` internally, so the macros are available in examples.
    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("install prometheus recorder");

    metrics::counter!("kameo_example_counter").increment(1);

    eprintln!("Prometheus scrape endpoint: {}", handle.render());
    // Keep the process alive briefly so users can see it in action.
    std::thread::sleep(Duration::from_secs(2));
}

