//! E2E test runner for loophole.
//!
//! Run with: `cargo run --bin e2e [filter]`
//!
//! On Linux: discovers Python test functions from tests/test_*.py, queues them,
//! and runs N at a time (default 4) via `docker compose exec` with
//! separate shards (bucket + mount paths) to avoid collisions.
//!
//! On macOS: runs tests directly (no Docker) using `uv run pytest`.
//! Requires a local S3-compatible server (e.g., RustFS via Docker).

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::Semaphore;

const DEFAULT_CONCURRENCY: usize = 4;

/// Test files ordered slowest-first so the long-running tests start immediately.
/// Files matching these prefixes are sorted first (slowest-first).
const SLOW_PREFIXES: &[&str] = &["test_stress", "test_fallocate"];

static NEXT_SHARD: AtomicU32 = AtomicU32::new(0);

// ── Test collection ────────────────────────────────────────────────────

async fn collect_all_tests_docker() -> Vec<String> {
    let output = Command::new("docker")
        .args([
            "compose", "exec", "-T", "-w", "/tests", "e2e",
            "uv", "run", "pytest", "--collect-only", "-q",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("docker compose exec pytest --collect-only");

    parse_test_ids(&String::from_utf8_lossy(&output.stdout))
}

async fn collect_all_tests_native() -> Vec<String> {
    let output = Command::new("uv")
        .args(["run", "pytest", "--collect-only", "-q"])
        .current_dir("tests")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("uv run pytest --collect-only");

    parse_test_ids(&String::from_utf8_lossy(&output.stdout))
}

fn parse_test_ids(stdout: &str) -> Vec<String> {
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

// ── Test execution ─────────────────────────────────────────────────────

async fn run_test_docker(test_id: &str, shard: u32) -> (bool, String, Duration) {
    let start = Instant::now();
    let bucket = format!("e2e-shard-{shard}");
    let shard_suffix = format!("-{shard}");

    let mut args = vec![
        "compose".to_string(), "exec".to_string(),
        "-e".to_string(), format!("BUCKET={bucket}"),
        "-e".to_string(), format!("SHARD={shard_suffix}"),
    ];
    if let Ok(mode) = std::env::var("LOOPHOLE_MODE") {
        args.push("-e".to_string());
        args.push(format!("LOOPHOLE_MODE={mode}"));
    }
    args.extend([
        "-T".to_string(), "-w".to_string(), "/tests".to_string(),
        "e2e".to_string(),
        "uv".to_string(), "run".to_string(), "pytest".to_string(),
        "-xvs".to_string(), test_id.to_string(),
    ]);

    let result = Command::new("docker")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    finish_result(result, start)
}

async fn run_test_native(
    test_id: &str,
    shard: u32,
    s3_endpoint: &str,
) -> (bool, String, Duration) {
    let start = Instant::now();
    let bucket = format!("e2e-shard-{shard}");
    let shard_suffix = format!("-{shard}");

    let result = Command::new("uv")
        .args(["run", "pytest", "-xvs", test_id])
        .current_dir("tests")
        .env("BUCKET", &bucket)
        .env("SHARD", &shard_suffix)
        .env("S3_ENDPOINT", s3_endpoint)
        .env("AWS_ACCESS_KEY_ID", std::env::var("AWS_ACCESS_KEY_ID").unwrap_or("rustfsadmin".into()))
        .env("AWS_SECRET_ACCESS_KEY", std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or("rustfsadmin".into()))
        .env("AWS_REGION", std::env::var("AWS_REGION").unwrap_or("us-east-1".into()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    finish_result(result, start)
}

fn finish_result(
    result: Result<std::process::Output, std::io::Error>,
    start: Instant,
) -> (bool, String, Duration) {
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

// ── Docker setup ───────────────────────────────────────────────────────

async fn setup_docker() {
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

    // Global cleanup inside container.
    let _ = Command::new("docker")
        .args([
            "compose", "exec", "-T", "e2e", "bash", "-c",
            "umount /mnt/ext4* /mnt/loophole* 2>/dev/null; losetup -D 2>/dev/null; rm -r /tmp/loophole-cache* 2>/dev/null; true",
        ])
        .status()
        .await;
}

// ── macOS native setup ─────────────────────────────────────────────────

async fn setup_native() -> String {
    // Check that sudo mount_nfs is allowed without a password.
    let check = Command::new("sudo")
        .args(["-n", "-l", "/sbin/mount_nfs"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    if !check.map(|s| s.success()).unwrap_or(false) {
        eprintln!("ERROR: sudo mount_nfs requires a password.");
        eprintln!("NFS mount on macOS needs passwordless sudo for mount_nfs and umount.");
        eprintln!("Add to /etc/sudoers (via `sudo visudo`):");
        eprintln!(
            "  {} ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount",
            std::env::var("USER").unwrap_or_else(|_| "<username>".into())
        );
        std::process::exit(1);
    }

    // Add target/debug to PATH so tests find the `loophole` binary.
    // The binary should already be built (e.g. by `cargo e2e` alias).
    let cargo_target = std::env::current_dir()
        .unwrap()
        .join("target/debug")
        .to_string_lossy()
        .to_string();
    let path = std::env::var("PATH").unwrap_or_default();
    // SAFETY: no other threads are running yet at this point.
    unsafe { std::env::set_var("PATH", format!("{cargo_target}:{path}")) };

    // Ensure uv dependencies are synced.
    println!("=== Syncing Python dependencies ===");
    let status = Command::new("uv")
        .args(["sync"])
        .current_dir("tests")
        .status()
        .await
        .expect("uv sync");
    if !status.success() {
        eprintln!("uv sync failed");
        std::process::exit(1);
    }

    // Start S3 via docker compose (just the s3 service).
    println!("=== Starting S3 service ===");
    let status = Command::new("docker")
        .args(["compose", "up", "-d", "s3"])
        .status()
        .await
        .expect("docker compose up s3");
    if !status.success() {
        eprintln!("docker compose up s3 failed");
        std::process::exit(1);
    }

    // S3 is exposed on localhost:9000.
    "http://localhost:9000".to_string()
}

// ── Main ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let concurrency = std::env::var("E2E_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let filter: Option<String> = std::env::args().nth(1);
    let native = std::env::var("E2E_NATIVE").is_ok_and(|v| v == "1" || v == "true");

    let s3_endpoint = if native {
        setup_native().await
    } else {
        setup_docker().await;
        "http://s3:9000".to_string()
    };

    // Collect tests (slowest first).
    println!("=== Collecting tests ===");
    let all_tests = if native {
        collect_all_tests_native().await
    } else {
        collect_all_tests_docker().await
    };

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
    let s3_endpoint = Arc::new(s3_endpoint);

    let mut handles = Vec::new();

    for test_id in tests {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let passed = Arc::clone(&passed);
        let failed = Arc::clone(&failed);
        let s3_endpoint = Arc::clone(&s3_endpoint);
        let shard = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);

        handles.push(tokio::spawn(async move {
            println!("  START   {test_id}  (shard {shard})");
            let (ok, output, elapsed) = if native {
                run_test_native(&test_id, shard, &s3_endpoint).await
            } else {
                run_test_docker(&test_id, shard).await
            };
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
