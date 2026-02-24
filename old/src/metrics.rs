use metrics::{counter, histogram};
use std::time::Instant;
use tracing::warn;

/// Creates a `TimingSpan` from a base name literal, deriving both
/// `{base}.total` (counter) and `{base}.duration_seconds` (histogram)
/// at compile time with zero allocation.
macro_rules! timing {
    ($base:literal) => {
        $crate::metrics::TimingSpan::new(
            concat!($base, ".total"),
            concat!($base, ".duration_seconds"),
        )
    };
}
pub(crate) use timing;

/// RAII guard that increments a counter on creation and records a duration
/// histogram on drop.
pub struct TimingSpan {
    duration_name: &'static str,
    start: Instant,
}

impl TimingSpan {
    pub fn new(count_name: &'static str, duration_name: &'static str) -> Self {
        counter!(count_name).increment(1);
        Self {
            duration_name,
            start: Instant::now(),
        }
    }
}

impl Drop for TimingSpan {
    fn drop(&mut self) {
        histogram!(self.duration_name).record(self.start.elapsed().as_secs_f64());
    }
}

pub fn init(port: u16) {
    if let Err(err) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
    {
        warn!(
            port,
            error = %err,
            "failed to install Prometheus metrics exporter; continuing without metrics endpoint"
        );
    }
}
