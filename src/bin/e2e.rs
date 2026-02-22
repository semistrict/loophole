//! E2E test runner for loophole.
//!
//! Run with: `cargo run --bin e2e [filter]`
//!
//! Discovers Python test functions from tests/test_*.py, queues them,
//! and runs N at a time (default 4) via `docker compose exec` with
//! separate shards (bucket + mount paths) to avoid collisions.
//! Slowest tests are started first to minimize wall-clock time.

use std::process::Stdio;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::Semaphore;

const DEFAULT_CONCURRENCY: usize = 4;

/// Test files ordered slowest-first so the long-running tests start immediately.
/// Files matching these prefixes are sorted first (slowest-first).
const SLOW_PREFIXES: &[&str] = &["test_stress", "test_fallocate"];

static NEXT_SHARD: AtomicU32 = AtomicU32::new(0);

/// Discover all test_*.py files and collect individual test IDs via pytest.
/// Returns test IDs sorted with slow tests first.
async fn collect_all_tests() -> Vec<String> {
    let output = Command::new("docker")
        .args([
            "compose", "exec", "-T", "-w", "/tests", "e2e",
            "pytest", "--collect-only", "-q",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("docker compose exec pytest --collect-only");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut tests: Vec<String> = stdout
        .lines()
        .filter(|line| line.contains("::test_"))
        .map(|line| line.trim().to_string())
        .collect();

    // Sort: slow-prefix tests first, rest alphabetical.
    tests.sort_by(|a, b| {
        let a_slow = SLOW_PREFIXES.iter().any(|p| a.starts_with(p));
        let b_slow = SLOW_PREFIXES.iter().any(|p| b.starts_with(p));
        match (a_slow, b_slow) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        }
    });

    tests
}

async fn run_test(test_id: &str, shard: u32) -> (bool, String, Duration) {
    let start = Instant::now();
    let bucket = format!("e2e-shard-{shard}");
    let shard_suffix = format!("-{shard}");

    let result = Command::new("docker")
        .args([
            "compose", "exec",
            "-e", &format!("BUCKET={bucket}"),
            "-e", &format!("SHARD={shard_suffix}"),
            "-T", "-w", "/tests",
            "e2e",
            "pytest", "-xvs", test_id,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    let elapsed = start.elapsed();

    match result {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let combined = format!("{stdout}{stderr}");
            (output.status.success(), combined, elapsed)
        }
        Err(e) => (false, format!("failed to exec: {e}"), elapsed),
    }
}

#[tokio::main]
async fn main() {
    let concurrency = std::env::var("E2E_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let filter: Option<String> = std::env::args().nth(1);

    // Build + start services
    println!("=== Building Docker image ===");
    let status = Command::new("docker")
        .args(["compose", "build"])
        .status()
        .await
        .expect("docker compose build");
    if !status.success() {
        eprintln!("docker compose build failed");
        std::process::exit(1);
    }

    println!("=== Starting services ===");
    let status = Command::new("docker")
        .args(["compose", "up", "-d"])
        .status()
        .await
        .expect("docker compose up");
    if !status.success() {
        eprintln!("docker compose up failed");
        std::process::exit(1);
    }

    // Global cleanup inside container
    let _ = Command::new("docker")
        .args([
            "compose", "exec", "-T", "e2e", "bash", "-c",
            "umount /mnt/ext4* /mnt/loophole* 2>/dev/null; losetup -D 2>/dev/null; rm -rf /tmp/loophole-cache*; true",
        ])
        .status()
        .await;

    // Collect tests (slowest first)
    println!("=== Collecting tests ===");
    let all_tests = collect_all_tests().await;

    let tests: Vec<String> = if let Some(ref f) = filter {
        all_tests
            .into_iter()
            .filter(|t| t.contains(f.as_str()))
            .collect()
    } else {
        all_tests
    };

    if tests.is_empty() {
        eprintln!("no tests matched filter {:?}", filter);
        std::process::exit(1);
    }

    let total = tests.len();
    println!("\n=== Running {total} tests (concurrency={concurrency}) ===\n");

    let sem = Arc::new(Semaphore::new(concurrency));
    let passed = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();

    for test_id in tests {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let passed = Arc::clone(&passed);
        let failed = Arc::clone(&failed);
        let shard = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);

        handles.push(tokio::spawn(async move {
            println!("  START   {test_id}  (shard {shard})");
            let (ok, output, elapsed) = run_test(&test_id, shard).await;
            drop(permit);

            let p = passed.load(Ordering::Relaxed);
            let f = failed.load(Ordering::Relaxed);
            let done = p + f + 1;

            if ok {
                passed.fetch_add(1, Ordering::Relaxed);
                println!(
                    "  [{done}/{total}] PASS  {test_id}  ({:.1}s)",
                    elapsed.as_secs_f64()
                );
            } else {
                failed.fetch_add(1, Ordering::Relaxed);
                println!(
                    "  [{done}/{total}] FAIL  {test_id}  ({:.1}s)",
                    elapsed.as_secs_f64()
                );
                for line in output.lines() {
                    println!("         | {line}");
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();
    let p = passed.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);

    println!();
    if f == 0 {
        println!(
            "=== ALL {p} TESTS PASSED ({:.1}s) ===",
            elapsed.as_secs_f64()
        );
    } else {
        println!(
            "=== {f} FAILED, {p} passed ({:.1}s) ===",
            elapsed.as_secs_f64()
        );
        std::process::exit(1);
    }
}
