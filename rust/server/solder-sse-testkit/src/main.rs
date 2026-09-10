//! `solder-sse-testkit`: serve the chaos server for a browser or a foreign
//! client to prove itself against.
//!
//! ```text
//! solder-sse-testkit [--port 8090] [--cut never|after:N|abort:N|rotate:MS]
//!                    [--produce-every MS] [--restart-on N]
//! ```
use solder_sse_testkit::{Chaos, Cut};
use std::net::SocketAddr;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: solder-sse-testkit [--port 8090] [--cut never|after:N|abort:N|rotate:MS] [--produce-every MS] [--restart-on N]"
    );
    std::process::exit(2)
}

fn parse_cut(spec: &str) -> Option<Cut> {
    match spec.split_once(':') {
        None if spec == "never" => Some(Cut::Never),
        Some(("after", n)) => n.parse().ok().map(Cut::After),
        Some(("abort", n)) => n.parse().ok().map(Cut::Abort),
        Some(("rotate", ms)) => ms
            .parse()
            .ok()
            .map(|ms| Cut::Rotate(Duration::from_millis(ms))),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    let mut port: u16 = 8090;
    let mut cut = Cut::Never;
    let mut produce_every: Option<Duration> = None;
    let mut restart_on = 0usize;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--port" => port = value.parse().unwrap_or_else(|_| usage()),
            "--cut" => cut = parse_cut(&value).unwrap_or_else(|| usage()),
            "--produce-every" => {
                let ms: u64 = value.parse().unwrap_or_else(|_| usage());
                produce_every = (ms > 0).then(|| Duration::from_millis(ms));
            }
            "--restart-on" => restart_on = value.parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
    }

    let chaos = Chaos::new(cut)
        .restart_on_connection(restart_on)
        .with_keep_alive(Duration::from_secs(15));
    let served = chaos
        .clone()
        .serve_on(SocketAddr::from(([127, 0, 0, 1], port)))
        .await;
    println!(
        "solder-sse-testkit listening on {}/stream — cut: {cut:?}, producer: {}",
        served.base_url,
        produce_every.map_or("none (POST /publish)".to_string(), |d| format!(
            "every {d:?}"
        ))
    );

    if let Some(every) = produce_every {
        let producer = chaos.clone();
        tokio::spawn(async move {
            let mut n = 0u64;
            loop {
                n += 1;
                producer.publish(n).await;
                tokio::time::sleep(every).await;
            }
        });
    }

    let _ = tokio::signal::ctrl_c().await;
    drop(served);
}
