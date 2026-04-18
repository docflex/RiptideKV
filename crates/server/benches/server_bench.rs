//! Throughput benchmarks for the RiptideKV RESP server.
//!
//! Measures end-to-end latency and throughput of SET / GET / PIPELINE
//! requests through a real TCP socket, exercising the full RESP2 stack.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use engine::Engine;
use server::db::SharedDb;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

// ─── Benchmark infrastructure ─────────────────────────────────────────────────

/// Bind a server on a random port, return (runtime, address, SharedDb).
fn start_server() -> (Runtime, std::net::SocketAddr, SharedDb) {
    let rt = Runtime::new().unwrap();
    let dir = tempdir().unwrap();

    let engine = Engine::new(
        dir.path().join("wal.log"),
        dir.path().join("sst"),
        64 * 1024 * 1024, // 64 MiB — no flushing during bench
        false,
    )
    .unwrap();
    let db = SharedDb::new(engine);

    let listener = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let addr = listener.local_addr().unwrap();
    let db2 = db.clone();
    rt.spawn(async move {
        server::serve(listener, db2).await.ok();
    });

    // Give the server a moment to be ready.
    std::thread::sleep(Duration::from_millis(20));
    (rt, addr, db)
}

/// Encode a RESP2 array command.
fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

/// Read until we have consumed `n` RESP responses (each ending with \r\n).
/// Simple heuristic: count top-level CRLF-terminated lines we care about.
fn drain_responses(stream: &mut TcpStream, n: usize) {
    let mut buf = [0u8; 4096];
    let mut responses = 0;
    loop {
        let got = stream.read(&mut buf).unwrap();
        for &b in &buf[..got] {
            if b == b'\n' {
                responses += 1;
                if responses >= n {
                    return;
                }
            }
        }
    }
}

// ─── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_ping(c: &mut Criterion) {
    let (_rt, addr, _db) = start_server();

    c.bench_function("server_ping_1k", |b| {
        b.iter_batched(
            || TcpStream::connect(addr).unwrap(),
            |mut stream| {
                let cmd = resp_cmd(&["PING"]);
                for _ in 0..1_000 {
                    stream.write_all(&cmd).unwrap();
                }
                drain_responses(&mut stream, 1_000);
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_set(c: &mut Criterion) {
    let (_rt, addr, _db) = start_server();
    let value = "x".repeat(64);

    let mut group = c.benchmark_group("server_set");
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("set_1k_64b_values", |b| {
        b.iter_batched(
            || TcpStream::connect(addr).unwrap(),
            |mut stream| {
                for i in 0u64..1_000 {
                    let key = format!("bench:key:{}", i);
                    let cmd = resp_cmd(&["SET", &key, &value]);
                    stream.write_all(&cmd).unwrap();
                }
                drain_responses(&mut stream, 1_000);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let (rt, addr, db) = start_server();

    // Pre-populate 1 000 keys.
    rt.block_on(async {
        let mut state = db.state.write().await;
        for i in 0u64..1_000 {
            let k = format!("bench:key:{}", i).into_bytes();
            let v = b"hello-world".to_vec();
            state.engine.set(k, v).unwrap();
        }
    });

    let mut group = c.benchmark_group("server_get");
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("get_1k_existing_keys", |b| {
        b.iter_batched(
            || TcpStream::connect(addr).unwrap(),
            |mut stream| {
                for i in 0u64..1_000 {
                    let key = format!("bench:key:{}", i);
                    let cmd = resp_cmd(&["GET", &key]);
                    stream.write_all(&cmd).unwrap();
                }
                drain_responses(&mut stream, 1_000);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_pipeline_set_get(c: &mut Criterion) {
    let (_rt, addr, _db) = start_server();
    let value = "v".repeat(32);

    let mut group = c.benchmark_group("server_pipeline");
    group.throughput(Throughput::Elements(500)); // 500 SET + 500 GET = 1k ops

    group.bench_function("pipeline_500_set_500_get", |b| {
        b.iter_batched(
            || TcpStream::connect(addr).unwrap(),
            |mut stream| {
                let mut batch = Vec::new();
                for i in 0u64..500 {
                    let key = format!("pipe:key:{}", i);
                    batch.extend_from_slice(&resp_cmd(&["SET", &key, &value]));
                }
                for i in 0u64..500 {
                    let key = format!("pipe:key:{}", i);
                    batch.extend_from_slice(&resp_cmd(&["GET", &key]));
                }
                stream.write_all(&batch).unwrap();
                drain_responses(&mut stream, 1_000);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_mset_mget(c: &mut Criterion) {
    let (_rt, addr, _db) = start_server();

    c.bench_function("server_mset_100_keys", |b| {
        b.iter_batched(
            || TcpStream::connect(addr).unwrap(),
            |mut stream| {
                let mut args = vec!["MSET"];
                let pairs: Vec<String> = (0..100)
                    .flat_map(|i| [format!("mk:{}", i), format!("mv:{}", i)])
                    .collect();
                for p in &pairs {
                    args.push(p.as_str());
                }
                let cmd = resp_cmd(&args);
                stream.write_all(&cmd).unwrap();
                drain_responses(&mut stream, 1);
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    bench_ping,
    bench_set,
    bench_get,
    bench_pipeline_set_get,
    bench_mset_mget,
);
criterion_main!(benches);
