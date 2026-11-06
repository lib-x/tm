use tokio::io::AsyncWriteExt;
use tunnel_mux::{
    pb::{self, packet::Body},
    session::packet,
    transport,
};

#[tokio::test]
async fn reliable_stream_binding_is_independent_of_chunk_boundaries() {
    // A small duplex capacity forces byte-stream fragmentation like any carrier.
    let (mut tx, mut rx) = tokio::io::duplex(3);
    let expected = packet(
        0,
        Body::Ping(pb::Ping {
            token: 9_007_199_254_740_993,
        }),
    );
    let sending = expected.clone();
    let writer = tokio::spawn(async move {
        transport::write_packet(&mut tx, &sending).await.unwrap();
        tx.shutdown().await.unwrap();
    });
    assert_eq!(
        transport::read_packet(&mut rx).await.unwrap(),
        Some(expected)
    );
    assert_eq!(transport::read_packet(&mut rx).await.unwrap(), None);
    writer.await.unwrap();
}

#[tokio::test]
async fn stream_binding_rejects_oversized_and_truncated_frames() {
    let mut oversized: &[u8] = &[0, 2, 0, 1];
    assert!(transport::read_packet(&mut oversized).await.is_err());
    let mut truncated: &[u8] = &[0, 0, 0, 2, 0x8a];
    assert!(transport::read_packet(&mut truncated).await.is_err());
}
