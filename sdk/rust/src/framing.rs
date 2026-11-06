use crate::{Error, Result, pb};
use prost::Message;
pub const MAX_PACKET: usize = 131072;

pub fn encode(packet: &pb::Packet) -> Result<Vec<u8>> {
    let len = packet.encoded_len();
    if len == 0 || len > MAX_PACKET {
        return Err(Error::Protocol("packet length"));
    }
    let mut out = Vec::with_capacity(len + 4);
    out.extend_from_slice(&(len as u32).to_be_bytes());
    packet
        .encode(&mut out)
        .map_err(|_| Error::Invalid("encode"))?;
    Ok(out)
}

/// At most one partial frame is retained. Call the callback immediately for
/// each complete packet; it must apply its own work/rate budget.
#[derive(Default)]
pub struct Decoder {
    buf: Vec<u8>,
    target: usize,
    failed: bool,
    finished: bool,
}
impl Decoder {
    pub fn feed(
        &mut self,
        mut bytes: &[u8],
        mut on_packet: impl FnMut(pb::Packet) -> Result<()>,
    ) -> Result<()> {
        if self.failed || self.finished {
            return Err(Error::Protocol("decoder failed or finished"));
        }
        let result = (|| {
            while !bytes.is_empty() {
                let target = if self.target == 0 { 4 } else { self.target };
                let n = (target - self.buf.len()).min(bytes.len());
                self.buf.extend_from_slice(&bytes[..n]);
                bytes = &bytes[n..];
                if self.buf.len() != target {
                    continue;
                }
                if self.target == 0 {
                    let len = u32::from_be_bytes(
                        self.buf[..4]
                            .try_into()
                            .map_err(|_| Error::Protocol("header"))?,
                    ) as usize;
                    if len == 0 || len > MAX_PACKET {
                        return Err(Error::Protocol("packet length"));
                    }
                    self.buf.clear();
                    self.target = len;
                } else {
                    let packet = decode_payload(self.buf.as_slice())?;
                    self.buf.clear();
                    self.target = 0;
                    on_packet(packet)?;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn finish(&mut self) -> Result<()> {
        self.finished = true;
        if self.failed || self.target != 0 || !self.buf.is_empty() {
            self.failed = true;
            return Err(Error::Protocol("truncated or failed frame"));
        }
        Ok(())
    }
}

/// Strictly freeze the v1 envelope while leaving nested messages extensible.
pub fn decode_payload(bytes: &[u8]) -> Result<pb::Packet> {
    let mut pos = 0;
    let mut seen_id = false;
    let mut seen_body = false;
    while pos < bytes.len() {
        let key = varint(bytes, &mut pos)?;
        match key {
            8 if !seen_id => {
                seen_id = true;
                if varint(bytes, &mut pos)? > u32::MAX as u64 {
                    return Err(Error::Protocol("stream id overflow"));
                }
            }
            k if (82..=154).contains(&k) && k % 8 == 2 && !seen_body => {
                seen_body = true;
                let n = varint(bytes, &mut pos)?;
                if n > (bytes.len() - pos) as u64 {
                    return Err(Error::Protocol("truncated body"));
                }
                pos += n as usize;
            }
            _ => return Err(Error::Protocol("unknown or duplicate envelope field")),
        }
    }
    if !seen_body {
        return Err(Error::Protocol("missing body"));
    }
    Ok(pb::Packet::decode(bytes)?)
}
fn varint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut n = 0u64;
    for shift in (0..70).step_by(7) {
        let b = *bytes.get(*pos).ok_or(Error::Protocol("truncated varint"))?;
        *pos += 1;
        if shift == 63 && b > 1 {
            return Err(Error::Protocol("varint overflow"));
        }
        n |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(n);
        }
    }
    Err(Error::Protocol("varint overflow"))
}
