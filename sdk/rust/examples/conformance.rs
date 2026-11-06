use std::io::{self, BufRead, Write};
use tunnel_mux::{
    Event, Role, Session, default_settings, framing,
    pb::{self, packet::Body},
    session::packet,
};

fn write_packet(p: &pb::Packet) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = framing::encode(p)?;
    let mut out = io::stdout().lock();
    for b in bytes {
        write!(out, "{b:02x}")?;
    }
    writeln!(out)?;
    out.flush()?;
    Ok(())
}
fn parse(line: &str) -> Result<pb::Packet, Box<dyn std::error::Error>> {
    if !line.len().is_multiple_of(2) {
        return Err("odd hex length".into());
    }
    let bytes = (0..line.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&line[i..i + 2], 16))
        .collect::<Result<Vec<_>, _>>()?;
    let mut packets = Vec::new();
    let mut decoder = framing::Decoder::default();
    decoder.feed(&bytes, |p| {
        packets.push(p);
        Ok(())
    })?;
    decoder.finish()?;
    if packets.len() != 1 {
        return Err("expected one packet".into());
    }
    Ok(packets.remove(0))
}
fn transcript() -> Vec<pb::Packet> {
    vec![
        packet(
            0,
            Body::Hello(pb::Hello {
                version: 1,
                settings: Some(default_settings()),
                profiles: vec!["bytes.v1".into()],
            }),
        ),
        packet(
            0,
            Body::Welcome(pb::Welcome {
                version: 1,
                settings: Some(default_settings()),
                profiles: vec!["bytes.v1".into()],
            }),
        ),
        packet(
            1,
            Body::Open(pb::Open {
                profile: "bytes.v1".into(),
                route: "echo".into(),
                class: 0,
                metadata: vec![],
            }),
        ),
        packet(1, Body::OpenAck(pb::OpenAck {})),
        packet(
            1,
            Body::Data(pb::Data {
                payload: "你好 TMP".as_bytes().to_vec(),
                fin: true,
            }),
        ),
        packet(
            1,
            Body::Data(pb::Data {
                payload: b"ok".to_vec(),
                fin: true,
            }),
        ),
        packet(1, Body::Window(pb::Window { credit: 10 })),
        packet(0, Body::Window(pb::Window { credit: 10 })),
        packet(1, Body::Window(pb::Window { credit: 2 })),
        packet(0, Body::Window(pb::Window { credit: 2 })),
    ]
}
fn endpoint(role: Role) -> Result<(), Box<dyn std::error::Error>> {
    let mut s = Session::new(role, default_settings(), vec!["bytes.v1".into()])?;
    if role == Role::Initiator {
        write_packet(&s.start()?)?;
    }
    for line in io::stdin().lock().lines() {
        let out = s.receive(parse(&line?)?)?;
        for p in out.outbound {
            write_packet(&p)?;
        }
        for event in out.events {
            match event {
                Event::Ready if role == Role::Initiator => {
                    let (_, p) = s.open(pb::Open {
                        profile: "bytes.v1".into(),
                        route: "echo".into(),
                        ..Default::default()
                    })?;
                    write_packet(&p)?;
                }
                Event::Open(id, _) => write_packet(&s.accept(id)?)?,
                Event::Accepted(id) => {
                    write_packet(&s.send(id, "你好 TMP".as_bytes().to_vec(), true)?)?
                }
                Event::Data(id, bytes, fin) => {
                    let expected = if role == Role::Responder {
                        "你好 TMP".as_bytes()
                    } else {
                        b"ok"
                    };
                    if bytes != expected || !fin {
                        return Err("unexpected data".into());
                    }
                    for p in s.consume(id, bytes.len() as u32)? {
                        write_packet(&p)?;
                    }
                    if role == Role::Responder {
                        write_packet(&s.send(id, b"ok".to_vec(), true)?)?;
                    } else {
                        for p in s.goaway(pb::ErrorCode::Cancelled, String::new())? {
                            write_packet(&p)?;
                        }
                    }
                }
                Event::GoAway(_) => {
                    if role == Role::Responder {
                        for p in s.goaway(pb::ErrorCode::Cancelled, String::new())? {
                            write_packet(&p)?;
                        }
                    }
                    return Ok(());
                }
                _ => {}
            }
        }
    }
    Err("unexpected EOF".into())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("emit") => {
            for p in transcript() {
                write_packet(&p)?;
            }
        }
        Some("verify") => {
            let actual = io::stdin()
                .lock()
                .lines()
                .map(|l| parse(&l?))
                .collect::<Result<Vec<_>, _>>()?;
            if actual != transcript() {
                return Err("transcript mismatch".into());
            }
        }
        Some("endpoint") => endpoint(match args.get(2).map(String::as_str) {
            Some("initiator") => Role::Initiator,
            Some("responder") => Role::Responder,
            _ => return Err("expected role".into()),
        })?,
        _ => return Err("usage: conformance emit|verify|endpoint initiator|responder".into()),
    }
    Ok(())
}
