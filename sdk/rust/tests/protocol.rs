use tunnel_mux::{
    Event, Role, Session, default_settings,
    framing::{self, Decoder},
    pb::{self, packet::Body},
    session::packet,
};
fn pair() -> (Session, Session) {
    let mut a = Session::new(Role::Initiator, default_settings(), vec!["bytes.v1".into()]).unwrap();
    let mut b = Session::new(Role::Responder, default_settings(), vec!["bytes.v1".into()]).unwrap();
    let welcome = b.receive(a.start().unwrap()).unwrap().outbound.remove(0);
    a.receive(welcome).unwrap();
    (a, b)
}
fn opened(a: &mut Session, b: &mut Session) -> u32 {
    let (id, p) = a
        .open(pb::Open {
            profile: "bytes.v1".into(),
            route: "echo".into(),
            class: 0,
            metadata: vec![],
        })
        .unwrap();
    b.receive(p).unwrap();
    a.receive(b.accept(id).unwrap()).unwrap();
    id
}
#[test]
fn duplex_fin_consumption_and_credit() {
    let (mut a, mut b) = pair();
    let id = opened(&mut a, &mut b);
    let out = b
        .receive(a.send(id, b"hello".to_vec(), true).unwrap())
        .unwrap();
    assert!(matches!(&out.events[0],Event::Data(1,p,true) if p==b"hello"));
    assert!(a.send(id, vec![1], false).is_err());
    assert!(b.consume(id, 6).is_err());
    // Peer FIN does not shut down the reverse direction.
    a.receive(b.send(id, b"world".to_vec(), true).unwrap())
        .unwrap();
    for p in b.consume(id, 5).unwrap() {
        a.receive(p).unwrap();
    }
    for p in a.consume(id, 5).unwrap() {
        b.receive(p).unwrap();
    }
    assert!(a.consume(id, 1).is_err());
}
#[test]
fn no_optimistic_data_and_bounded_windows() {
    let (mut a, mut b) = pair();
    let (id, p) = a
        .open(pb::Open {
            profile: "bytes.v1".into(),
            ..Default::default()
        })
        .unwrap();
    assert!(a.send(id, vec![1], false).is_err());
    b.receive(p).unwrap();
    assert!(
        b.receive(packet(
            id,
            Body::Data(pb::Data {
                payload: vec![1],
                fin: false
            })
        ))
        .is_err()
    );
    assert!(b.accept(id).is_err());
    let (mut a, mut b) = pair();
    let id = opened(&mut a, &mut b);
    for _ in 0..4 {
        b.receive(a.send(id, vec![0; 16384], false).unwrap())
            .unwrap();
    }
    assert!(a.send(id, vec![0], false).is_err());
    for p in b.consume(id, 16384).unwrap() {
        a.receive(p).unwrap();
    }
    b.receive(a.send(id, vec![0; 16384], false).unwrap())
        .unwrap();
    assert!(
        a.receive(packet(0, Body::Window(pb::Window { credit: 0 })))
            .is_err()
    );
}
#[test]
fn reset_returns_connection_credit_and_late_data_is_refunded() {
    let (mut a, mut b) = pair();
    let id = opened(&mut a, &mut b);
    b.receive(a.send(id, vec![0; 10], false).unwrap()).unwrap();
    let packets = b
        .reset(id, pb::ErrorCode::Cancelled, "stop".into())
        .unwrap();
    assert!(matches!(packets[0].body, Some(Body::Reset(_))));
    assert!(matches!(
        packets[1].body,
        Some(Body::Window(pb::Window { credit: 10 }))
    ));
    for p in packets {
        a.receive(p).unwrap();
    }
    let late = b
        .receive(packet(
            id,
            Body::Data(pb::Data {
                payload: vec![1; 3],
                fin: false,
            }),
        ))
        .unwrap();
    assert!(matches!(
        late.outbound[0].body,
        Some(Body::Window(pb::Window { credit: 3 }))
    ));
    assert!(
        b.receive(packet(
            99,
            Body::Data(pb::Data {
                payload: vec![1],
                fin: false
            })
        ))
        .is_err()
    );
}
#[test]
fn exact_stream_limit_and_reverse_open() {
    let mut settings = default_settings();
    settings.max_streams = 1;
    let mut a = Session::new(Role::Initiator, default_settings(), vec!["bytes.v1".into()]).unwrap();
    let mut b = Session::new(Role::Responder, settings, vec!["bytes.v1".into()]).unwrap();
    let hello = a.start().unwrap();
    let welcome = b.receive(hello).unwrap().outbound.remove(0);
    a.receive(welcome).unwrap();
    opened(&mut a, &mut b);
    assert!(
        a.open(pb::Open {
            profile: "bytes.v1".into(),
            ..Default::default()
        })
        .is_err()
    );
    let (id, p) = b
        .open(pb::Open {
            profile: "bytes.v1".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(id, 2);
    a.receive(p).unwrap();
    b.receive(a.accept(id).unwrap()).unwrap();
}
#[test]
fn goaway_rejects_pending_and_cannot_revoke_accepted_streams() {
    let (mut a, mut b) = pair();
    let id = opened(&mut a, &mut b);
    let (pending, p) = a
        .open(pb::Open {
            profile: "bytes.v1".into(),
            ..Default::default()
        })
        .unwrap();
    b.receive(p).unwrap();
    let packets = b.goaway(pb::ErrorCode::Cancelled, String::new()).unwrap();
    assert_eq!(packets.len(), 2);
    assert!(b.accept(pending).is_err());
    for p in packets {
        a.receive(p).unwrap();
    }
    assert!(
        a.open(pb::Open {
            profile: "bytes.v1".into(),
            ..Default::default()
        })
        .is_err()
    );
    b.receive(a.send(id, vec![1], false).unwrap()).unwrap();
    assert!(
        a.receive(packet(
            0,
            Body::GoAway(pb::GoAway {
                last_stream_id: 0,
                code: 0,
                reason: String::new()
            })
        ))
        .is_err()
    );
}
#[test]
fn framing_fragmentation_coalescing_and_strict_envelope() {
    let ping = packet(0, Body::Ping(pb::Ping { token: u64::MAX }));
    let bytes = framing::encode(&ping).unwrap();
    let mut decoder = Decoder::default();
    let mut packets = vec![];
    for b in bytes.iter().cycle().take(bytes.len() * 2) {
        decoder
            .feed(&[*b], |p| {
                packets.push(p);
                Ok(())
            })
            .unwrap();
    }
    decoder.finish().unwrap();
    assert_eq!(packets, vec![ping.clone(), ping]);
    for invalid in [
        &[0, 0, 0, 0][..],
        &[0, 2, 0, 1][..],
        &[0, 0, 0, 2, 0x52, 0][..],
    ] {
        let mut d = Decoder::default();
        let result = d.feed(invalid, |_| Ok(()));
        // Empty Hello is syntactically valid; only Session rejects its content.
        if invalid[4..].is_empty() {
            assert!(result.is_err());
        }
    }
    for invalid in [
        vec![0x8a, 1, 0, 0x92, 1, 0],
        vec![8, 1, 8, 2, 0x8a, 1, 0],
        vec![0xa2, 1, 0],
        vec![8, 0xff, 0xff, 0xff, 0xff, 0x1f, 0x8a, 1, 0],
    ] {
        assert!(framing::decode_payload(&invalid).is_err());
    }
    let mut d = Decoder::default();
    d.feed(&[0, 0], |_| Ok(())).unwrap();
    assert!(d.finish().is_err());
}
#[test]
fn bad_handshake_scope_settings_and_credit_are_terminal() {
    let mut b = Session::new(Role::Responder, default_settings(), vec![]).unwrap();
    assert!(
        b.receive(packet(0, Body::Ping(pb::Ping { token: 1 })))
            .is_err()
    );
    assert!(
        b.receive(packet(
            0,
            Body::Hello(pb::Hello {
                version: 1,
                settings: Some(default_settings()),
                profiles: vec![]
            })
        ))
        .is_err()
    );
    let (mut a, mut b) = pair();
    let id = opened(&mut a, &mut b);
    assert!(
        a.receive(packet(id, Body::Window(pb::Window { credit: 1 })))
            .is_err()
    );
    assert!(a.ping(1).is_err());
}
#[test]
fn uint64_token_survives_generated_models() {
    let ping = packet(
        0,
        Body::Ping(pb::Ping {
            token: 9_007_199_254_740_993,
        }),
    );
    let encoded = framing::encode(&ping).unwrap();
    assert_eq!(framing::decode_payload(&encoded[4..]).unwrap(), ping);
}

#[test]
fn diagnostic_reasons_have_utf8_byte_limit_and_argument_errors_are_atomic() {
    let (mut a, mut b) = pair();
    let id = opened(&mut a, &mut b);
    assert!(
        a.reset(id, pb::ErrorCode::Cancelled, "你".repeat(86))
            .is_err()
    );
    b.receive(a.send(id, vec![1], false).unwrap()).unwrap();
    let frames = a
        .reset(id, pb::ErrorCode::Cancelled, "x".repeat(256))
        .unwrap();
    for p in frames {
        b.receive(p).unwrap();
    }
    let (mut a, _) = pair();
    assert!(a.goaway(pb::ErrorCode::Cancelled, "x".repeat(257)).is_err());
    assert!(
        a.open(pb::Open {
            profile: "bytes.v1".into(),
            ..Default::default()
        })
        .is_ok()
    );
    assert!(
        a.receive(packet(
            1,
            Body::Reset(pb::Reset {
                code: 0,
                reason: "x".repeat(257)
            })
        ))
        .is_err()
    );
}

#[test]
fn decoder_finish_cannot_resume_after_truncation_or_eof() {
    let bytes = framing::encode(&packet(0, Body::Ping(pb::Ping { token: 1 }))).unwrap();
    let mut d = Decoder::default();
    d.feed(&bytes[..2], |_| Ok(())).unwrap();
    assert!(d.finish().is_err());
    assert!(d.feed(&bytes[2..], |_| Ok(())).is_err());
    assert!(d.finish().is_err());
    let mut d = Decoder::default();
    d.feed(&bytes, |_| Ok(())).unwrap();
    d.finish().unwrap();
    assert!(d.feed(&bytes, |_| Ok(())).is_err());
}
