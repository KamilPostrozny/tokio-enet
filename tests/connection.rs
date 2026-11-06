use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio_enet::{Event, Host, HostConfig, Packet, PacketMode};

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const POLL_DEADLINE: Duration = Duration::from_secs(5);

/// Poll `host.service()` in a loop until `pred` matches an event or the deadline is reached.
async fn poll_until(host: &mut Host, pred: impl Fn(&Event) -> bool) -> Option<Event> {
    let deadline = Instant::now() + POLL_DEADLINE;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let timeout = remaining.min(POLL_INTERVAL);
        if let Some(event) = host.service(timeout).await.unwrap() {
            if pred(&event) {
                return Some(event);
            }
        }
    }
}

/// Poll `host.service()` in a loop, expecting no matching event before the short deadline.
async fn poll_expect_none(host: &mut Host, duration: Duration) {
    let deadline = Instant::now() + duration;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let timeout = remaining.min(POLL_INTERVAL);
        let event = host.service(timeout).await.unwrap();
        assert!(event.is_none(), "expected no event, got {event:?}");
    }
}

/// Test that a client can connect to a server and exchange reliable data.
#[tokio::test]
async fn connect_and_exchange_reliable() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("tokio_enet=trace")
        .try_init();

    // Create server host.
    let server_config = HostConfig {
        address: Some("127.0.0.1:0".parse().unwrap()),
        peer_count: 4,
        channel_limit: 2,
        ..Default::default()
    };
    let mut server = Host::new(server_config).unwrap();
    let server_addr = server.local_addr().unwrap();

    // Create client host.
    let client_config = HostConfig {
        peer_count: 1,
        channel_limit: 2,
        ..Default::default()
    };
    let mut client = Host::new(client_config).unwrap();

    // Client initiates connection.
    let peer_id = client.connect(server_addr, 2, 42).unwrap();
    client.flush().await.unwrap();

    // Server receives Connect, sends ACK + VerifyConnect. No event yet (needs ACK for VerifyConnect).
    poll_expect_none(&mut server, Duration::from_millis(200)).await;

    // Client receives VerifyConnect, transitions to Connected.
    let event = poll_until(&mut client, |e| matches!(e, Event::Connect { .. })).await;
    assert!(
        matches!(&event, Some(Event::Connect { peer_id: id, .. }) if *id == peer_id),
        "expected Connect event for peer {peer_id}, got {event:?}"
    );
    // Flush ACK for VerifyConnect to server.
    client.flush().await.unwrap();

    // Server receives ACK for VerifyConnect, transitions to Connected.
    let event = poll_until(&mut server, |e| matches!(e, Event::Connect { .. })).await;
    assert!(
        matches!(&event, Some(Event::Connect { .. })),
        "expected Connect event on server, got {event:?}"
    );

    // Client sends reliable data.
    let payload = b"Hello, ENet!";
    {
        let peer = client.peer_mut(peer_id).unwrap();
        peer.send(0, Packet::new(payload, PacketMode::ReliableSequenced))
            .unwrap();
    }
    client.flush().await.unwrap();

    // Server receives the data.
    let event = poll_until(&mut server, |e| matches!(e, Event::Receive { .. })).await;
    match event {
        Some(Event::Receive {
            channel_id, packet, ..
        }) => {
            assert_eq!(channel_id, 0);
            assert_eq!(packet.data(), payload);
        }
        other => panic!("expected Receive event, got {other:?}"),
    }

    // Server sends reliable data back.
    let response = b"Hello back!";
    let server_peer_id = match server
        .peers()
        .find(|p| p.state() == tokio_enet::PeerState::Connected)
    {
        Some(p) => p.id(),
        None => panic!("no connected peer on server"),
    };
    {
        let peer = server.peer_mut(server_peer_id).unwrap();
        peer.send(0, Packet::new(response, PacketMode::ReliableSequenced))
            .unwrap();
    }
    server.flush().await.unwrap();

    // Client receives the response.
    let event = poll_until(&mut client, |e| matches!(e, Event::Receive { .. })).await;
    match event {
        Some(Event::Receive {
            channel_id, packet, ..
        }) => {
            assert_eq!(channel_id, 0);
            assert_eq!(packet.data(), response);
        }
        other => panic!("expected Receive event, got {other:?}"),
    }
}

/// Test that disconnect events are delivered.
#[tokio::test]
async fn disconnect_event() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("tokio_enet=trace")
        .try_init();

    // Set up server and client.
    let server_config = HostConfig {
        address: Some("127.0.0.1:0".parse().unwrap()),
        peer_count: 1,
        channel_limit: 1,
        ..Default::default()
    };
    let mut server = Host::new(server_config).unwrap();
    let server_addr = server.local_addr().unwrap();

    let client_config = HostConfig {
        peer_count: 1,
        channel_limit: 1,
        ..Default::default()
    };
    let mut client = Host::new(client_config).unwrap();

    // Connect.
    let peer_id = client.connect(server_addr, 1, 0).unwrap();
    client.flush().await.unwrap();

    // Server processes Connect, sends VerifyConnect.
    poll_expect_none(&mut server, Duration::from_millis(200)).await;

    // Client processes VerifyConnect, transitions to Connected.
    let event = poll_until(&mut client, |e| matches!(e, Event::Connect { .. })).await;
    assert!(event.is_some(), "expected Connect event on client");
    client.flush().await.unwrap();

    // Server receives ACK → Connected.
    let event = poll_until(&mut server, |e| matches!(e, Event::Connect { .. })).await;
    assert!(event.is_some(), "expected Connect event on server");

    // Client disconnects.
    {
        let peer = client.peer_mut(peer_id).unwrap();
        peer.disconnect(99);
    }
    client.flush().await.unwrap();

    // Server should get a disconnect event.
    let event = poll_until(&mut server, |e| {
        matches!(e, Event::Disconnect { data: 99, .. })
    })
    .await;
    assert!(
        matches!(&event, Some(Event::Disconnect { data: 99, .. })),
        "expected Disconnect event with data=99, got {event:?}"
    );
}

/// A UDP relay between a client and `server` that drops the first datagram from the server
/// carrying `marker`, as one lost Wi-Fi frame would. Returns the address clients connect to and
/// the number of datagrams the relay has dropped.
async fn relay_losing_once(
    server: SocketAddr,
    marker: &'static [u8],
) -> (SocketAddr, Arc<AtomicUsize>) {
    let client_side = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_side = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    server_side.connect(server).await.unwrap();
    let relay = client_side.local_addr().unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let drops = Arc::clone(&dropped);
    tokio::spawn(async move {
        let mut client = None;
        let mut from_client = [0u8; 2048];
        let mut from_server = [0u8; 2048];
        loop {
            tokio::select! {
                Ok((length, address)) = client_side.recv_from(&mut from_client) => {
                    client = Some(address);
                    let _ = server_side.send(&from_client[..length]).await;
                }
                Ok(length) = server_side.recv(&mut from_server) => {
                    let datagram = &from_server[..length];
                    if drops.load(Ordering::SeqCst) == 0
                        && datagram.windows(marker.len()).any(|window| window == marker)
                    {
                        drops.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    if let Some(client) = client {
                        let _ = client_side.send_to(datagram, client).await;
                    }
                }
            }
        }
    });
    (relay, dropped)
}

/// A lost reliable packet is resent after about one round trip, as C ENet does
/// (`RTT + min(RTT, 4·variance)`), not after the 5 s `timeout_minimum`.
#[tokio::test]
async fn lost_reliable_packet_is_resent_after_about_one_round_trip() {
    let server_config = HostConfig {
        address: Some("127.0.0.1:0".parse().unwrap()),
        peer_count: 1,
        channel_limit: 1,
        ..Default::default()
    };
    let mut server = Host::new(server_config).unwrap();
    let (relay, dropped) = relay_losing_once(server.local_addr().unwrap(), b"lost once").await;

    let client_config = HostConfig {
        peer_count: 1,
        channel_limit: 1,
        ..Default::default()
    };
    let mut client = Host::new(client_config).unwrap();
    client.connect(relay, 1, 0).unwrap();
    client.flush().await.unwrap();
    poll_expect_none(&mut server, Duration::from_millis(200)).await;
    let event = poll_until(&mut client, |e| matches!(e, Event::Connect { .. })).await;
    assert!(event.is_some(), "expected Connect event on client");
    client.flush().await.unwrap();
    let Some(Event::Connect { peer_id, .. }) =
        poll_until(&mut server, |e| matches!(e, Event::Connect { .. })).await
    else {
        panic!("expected Connect event on server");
    };

    // The server measures the round trip from acknowledged packets; it starts at 500 ms.
    for _ in 0..50 {
        let warm_up = Packet::new(b"warm up", PacketMode::ReliableSequenced);
        server.peer_mut(peer_id).unwrap().send(0, warm_up).unwrap();
        server.flush().await.unwrap();
        let event = poll_until(&mut client, |e| matches!(e, Event::Receive { .. })).await;
        assert!(event.is_some(), "expected the warm-up packet on the client");
        client.flush().await.unwrap();
        server.service(Duration::from_millis(5)).await.unwrap();
    }
    let round_trip = server.peer(peer_id).unwrap().round_trip_time();
    assert!(round_trip < 50, "loopback round trip {round_trip} ms");

    let lost = Packet::new(b"lost once", PacketMode::ReliableSequenced);
    server.peer_mut(peer_id).unwrap().send(0, lost).unwrap();
    server.flush().await.unwrap();
    let sent = Instant::now();
    let resent = loop {
        assert!(sent.elapsed() < Duration::from_secs(10), "never resent");
        server.service(Duration::from_millis(1)).await.unwrap();
        if let Some(Event::Receive { packet, .. }) =
            client.service(Duration::from_millis(1)).await.unwrap()
            && packet.data() == b"lost once"
        {
            break sent.elapsed();
        }
    };
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the relay never dropped the first \"lost once\" datagram"
    );
    assert!(
        resent < Duration::from_millis(250),
        "resent after {resent:?} with a {round_trip} ms round trip"
    );
}
