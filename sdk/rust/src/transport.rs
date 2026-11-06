//! Stream adapter for authenticated SSH channels, QUIC bidirectional streams,
//! TLS sockets, or a WebSocket binary-to-byte adapter. No transport names enter
//! the session protocol. Call serially; EOF ends the entire link.
use crate::{
    Error, Result,
    framing::{self, MAX_PACKET},
    pb,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Read one frame. A clean EOF ends the entire TMP link.
///
/// # Cancellation
/// This future keeps partial-frame progress locally and is not cancellation
/// safe. If dropped (for example by a timeout/select), close the carrier and
/// call `Session::close`; never resume reads on that carrier.
pub async fn read_packet(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<pb::Packet>> {
    let mut header = [0u8; 4];
    if reader.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let n = u32::from_be_bytes(header) as usize;
    if n == 0 || n > MAX_PACKET {
        return Err(Error::Protocol("packet length"));
    }
    let mut payload = vec![0; n];
    reader.read_exact(&mut payload).await?;
    Ok(Some(framing::decode_payload(payload.as_slice())?))
}
/// Write and flush one packet, in order with all other writes.
///
/// # Cancellation
/// Dropping this future may leave a partial frame on the carrier. Cancellation
/// or an I/O error must close the carrier and `Session`; never retry the packet.
pub async fn write_packet(
    writer: &mut (impl AsyncWrite + Unpin),
    packet: &pb::Packet,
) -> Result<()> {
    writer.write_all(&framing::encode(packet)?).await?;
    writer.flush().await?;
    Ok(())
}
