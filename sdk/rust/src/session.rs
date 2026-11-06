use crate::{
    Error, Result,
    pb::{self, packet::Body},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Initiator,
    Responder,
}
#[derive(Debug)]
pub enum Event {
    Ready,
    Open(u32, pb::Open),
    Accepted(u32),
    Data(u32, Vec<u8>, bool),
    Reset(u32, i32, String),
    Pong(u64),
    GoAway(u32),
}
#[derive(Default, Debug)]
pub struct Outcome {
    pub outbound: Vec<pb::Packet>,
    pub events: Vec<Event>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    New,
    Waiting,
    Ready,
    Failed,
}
struct Stream {
    local: bool,
    accepted: bool,
    local_fin: bool,
    remote_fin: bool,
    send_credit: u32,
    outstanding: u32,
}
pub struct Session {
    role: Role,
    phase: Phase,
    local: pb::Settings,
    peer: Option<pb::Settings>,
    profiles: BTreeSet<String>,
    negotiated: BTreeSet<String>,
    streams: BTreeMap<u32, Stream>,
    highest_local: u32,
    highest_remote: u32,
    highest_accepted: u32,
    highest_acked: u32,
    conn_send: u32,
    conn_outstanding: u32,
    draining: bool,
    peer_last: Option<u32>,
}
pub fn packet(id: u32, body: Body) -> pb::Packet {
    pb::Packet {
        stream_id: id,
        body: Some(body),
    }
}
fn protocol(msg: &'static str) -> Error {
    Error::Protocol(msg)
}
fn valid_settings(s: &pb::Settings) -> bool {
    (1024..=65536).contains(&s.max_data)
        && (4096..=16777216).contains(&s.stream_window)
        && (65536..=268435456).contains(&s.connection_window)
        && (1..=4096).contains(&s.max_streams)
}
fn valid_profiles(profiles: &[String]) -> bool {
    let unique: BTreeSet<_> = profiles.iter().collect();
    profiles.len() <= 32
        && unique.len() == profiles.len()
        && profiles.iter().all(|p| {
            !p.is_empty()
                && p.len() <= 64
                && (p.as_bytes()[0].is_ascii_lowercase() || p.as_bytes()[0].is_ascii_digit())
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._/-".contains(&b))
        })
}
fn valid_open(open: &pb::Open) -> bool {
    open.route.len() <= 256
        && open.metadata.len() <= 4096
        && (0..=2).contains(&open.class)
        && valid_profiles(std::slice::from_ref(&open.profile))
}
fn valid_error(code: i32, reason: &str) -> bool {
    (0..=7).contains(&code) && reason.len() <= 256
}
impl Session {
    pub fn new(role: Role, local: pb::Settings, profiles: Vec<String>) -> Result<Self> {
        if !valid_settings(&local) || !valid_profiles(&profiles) {
            return Err(Error::Invalid("settings or profiles"));
        }
        Ok(Self {
            role,
            phase: Phase::New,
            local,
            peer: None,
            profiles: profiles.into_iter().collect(),
            negotiated: BTreeSet::new(),
            streams: BTreeMap::new(),
            highest_local: 0,
            highest_remote: 0,
            highest_accepted: 0,
            highest_acked: 0,
            conn_send: 0,
            conn_outstanding: 0,
            draining: false,
            peer_last: None,
        })
    }
    /// A copy of negotiated peer receive limits; absent before handshake.
    pub fn peer_settings(&self) -> Option<pb::Settings> {
        self.peer
    }
    pub fn negotiated_profiles(&self) -> Vec<String> {
        self.negotiated.iter().cloned().collect()
    }
    pub fn start(&mut self) -> Result<pb::Packet> {
        if self.role != Role::Initiator || self.phase != Phase::New {
            return Err(Error::Invalid("start state"));
        }
        self.phase = Phase::Waiting;
        Ok(packet(
            0,
            Body::Hello(pb::Hello {
                version: 1,
                settings: Some(self.local),
                profiles: self.profiles.iter().cloned().collect(),
            }),
        ))
    }
    fn ready(&self) -> Result<()> {
        if self.phase != Phase::Ready {
            return Err(Error::Invalid("session not ready"));
        }
        Ok(())
    }
    fn is_local(&self, id: u32) -> bool {
        id % 2 == if self.role == Role::Initiator { 1 } else { 0 }
    }
    fn historical(&self, id: u32) -> bool {
        id != 0
            && id
                <= if self.is_local(id) {
                    self.highest_local
                } else {
                    self.highest_remote
                }
    }
    fn retire(&mut self, id: u32) {
        if self
            .streams
            .get(&id)
            .is_some_and(|s| s.local_fin && s.remote_fin && s.outstanding == 0)
        {
            self.streams.remove(&id);
        }
    }
    pub fn open(&mut self, open: pb::Open) -> Result<(u32, pb::Packet)> {
        self.ready()?;
        let peer = self.peer.as_ref().ok_or(Error::Invalid("peer"))?;
        if self.draining || !valid_open(&open) || !self.negotiated.contains(&open.profile) {
            return Err(Error::Invalid("open profile, fields or draining"));
        }
        if self.streams.values().filter(|s| s.local).count() >= peer.max_streams as usize {
            return Err(Error::WouldBlock);
        }
        let id = if self.highest_local == 0 {
            if self.role == Role::Initiator { 1 } else { 2 }
        } else {
            self.highest_local
                .checked_add(2)
                .ok_or(Error::Invalid("stream ids exhausted"))?
        };
        self.highest_local = id;
        self.streams.insert(
            id,
            Stream {
                local: true,
                accepted: false,
                local_fin: false,
                remote_fin: false,
                send_credit: peer.stream_window,
                outstanding: 0,
            },
        );
        Ok((id, packet(id, Body::Open(open))))
    }
    pub fn accept(&mut self, id: u32) -> Result<pb::Packet> {
        self.ready()?;
        let s = self.streams.get_mut(&id).ok_or(Error::Invalid("stream"))?;
        if s.local || s.accepted || self.draining {
            return Err(Error::Invalid("accept state"));
        }
        s.accepted = true;
        self.highest_accepted = self.highest_accepted.max(id);
        Ok(packet(id, Body::OpenAck(pb::OpenAck {})))
    }
    pub fn send(&mut self, id: u32, payload: Vec<u8>, fin: bool) -> Result<pb::Packet> {
        self.ready()?;
        let n = u32::try_from(payload.len()).map_err(|_| Error::Invalid("payload"))?;
        let s = self.streams.get_mut(&id).ok_or(Error::Invalid("stream"))?;
        if !s.accepted
            || s.local_fin
            || (n == 0 && !fin)
            || n > self.peer.as_ref().ok_or(Error::Invalid("peer"))?.max_data
        {
            return Err(Error::Invalid("send state or size"));
        }
        if n > s.send_credit || n > self.conn_send {
            return Err(Error::WouldBlock);
        }
        s.send_credit -= n;
        self.conn_send -= n;
        s.local_fin = fin;
        self.retire(id);
        Ok(packet(id, Body::Data(pb::Data { payload, fin })))
    }
    pub fn consume(&mut self, id: u32, n: u32) -> Result<Vec<pb::Packet>> {
        self.ready()?;
        let s = self.streams.get_mut(&id).ok_or(Error::Invalid("stream"))?;
        if n > s.outstanding {
            return Err(Error::Invalid("overconsume"));
        }
        if n == 0 {
            return Ok(vec![]);
        }
        s.outstanding -= n;
        self.conn_outstanding -= n;
        self.retire(id);
        Ok(vec![
            packet(id, Body::Window(pb::Window { credit: n })),
            packet(0, Body::Window(pb::Window { credit: n })),
        ])
    }
    fn remove(&mut self, id: u32) -> Vec<pb::Packet> {
        match self.streams.remove(&id) {
            Some(s) if s.outstanding > 0 => {
                self.conn_outstanding -= s.outstanding;
                vec![packet(
                    0,
                    Body::Window(pb::Window {
                        credit: s.outstanding,
                    }),
                )]
            }
            _ => vec![],
        }
    }
    pub fn reset(
        &mut self,
        id: u32,
        code: pb::ErrorCode,
        reason: String,
    ) -> Result<Vec<pb::Packet>> {
        self.ready()?;
        if !self.streams.contains_key(&id) || !valid_error(code as i32, &reason) {
            return Err(Error::Invalid("reset"));
        }
        let mut frames = vec![packet(
            id,
            Body::Reset(pb::Reset {
                code: code as i32,
                reason,
            }),
        )];
        frames.extend(self.remove(id));
        Ok(frames)
    }
    pub fn goaway(&mut self, code: pb::ErrorCode, reason: String) -> Result<Vec<pb::Packet>> {
        self.ready()?;
        if !valid_error(code as i32, &reason) {
            return Err(Error::Invalid("goaway reason"));
        }
        self.draining = true;
        let ids: Vec<_> = self
            .streams
            .iter()
            .filter(|(_, s)| !s.local && !s.accepted)
            .map(|(id, _)| *id)
            .collect();
        let mut frames = Vec::new();
        for id in ids {
            self.streams.remove(&id);
            frames.push(packet(
                id,
                Body::Reset(pb::Reset {
                    code: pb::ErrorCode::Refused as i32,
                    reason: "goaway".into(),
                }),
            ));
        }
        frames.push(packet(
            0,
            Body::GoAway(pb::GoAway {
                last_stream_id: self.highest_accepted,
                code: code as i32,
                reason,
            }),
        ));
        Ok(frames)
    }
    pub fn ping(&self, token: u64) -> Result<pb::Packet> {
        self.ready()?;
        Ok(packet(0, Body::Ping(pb::Ping { token })))
    }
    pub fn close(&mut self) -> Vec<Event> {
        self.phase = Phase::Failed;
        let events = self
            .streams
            .keys()
            .map(|id| {
                Event::Reset(
                    *id,
                    pb::ErrorCode::Internal as i32,
                    "transport closed".into(),
                )
            })
            .collect();
        self.streams.clear();
        self.conn_outstanding = 0;
        events
    }
    pub fn receive(&mut self, p: pb::Packet) -> Result<Outcome> {
        if self.phase == Phase::Failed {
            return Err(protocol("session failed"));
        }
        let result = self.receive_inner(p);
        if result.is_err() {
            self.phase = Phase::Failed;
        }
        result
    }
    fn receive_inner(&mut self, p: pb::Packet) -> Result<Outcome> {
        let id = p.stream_id;
        let body = p.body.ok_or(protocol("missing body"))?;
        let link = matches!(
            &body,
            Body::Hello(_) | Body::Welcome(_) | Body::Ping(_) | Body::Pong(_) | Body::GoAway(_)
        );
        if (link && id != 0) || (!link && !matches!(&body, Body::Window(_)) && id == 0) {
            return Err(protocol("body scope"));
        }
        let mut out = Outcome::default();
        if self.phase != Phase::Ready {
            let (settings, profiles) = match body {
                Body::Hello(h)
                    if self.role == Role::Responder
                        && self.phase == Phase::New
                        && h.version == 1 =>
                {
                    (h.settings, h.profiles)
                }
                Body::Welcome(w)
                    if self.role == Role::Initiator
                        && self.phase == Phase::Waiting
                        && w.version == 1 =>
                {
                    (w.settings, w.profiles)
                }
                _ => return Err(protocol("handshake sequence")),
            };
            let settings = settings
                .filter(valid_settings)
                .ok_or(protocol("settings"))?;
            if !valid_profiles(&profiles) {
                return Err(protocol("profiles"));
            }
            if self.role == Role::Initiator && profiles.iter().any(|p| !self.profiles.contains(p)) {
                return Err(protocol("welcome profiles"));
            }
            self.negotiated = profiles
                .into_iter()
                .filter(|p| self.profiles.contains(p))
                .collect();
            self.conn_send = settings.connection_window;
            self.peer = Some(settings);
            self.phase = Phase::Ready;
            if self.role == Role::Responder {
                out.outbound.push(packet(
                    0,
                    Body::Welcome(pb::Welcome {
                        version: 1,
                        settings: Some(self.local),
                        profiles: self.negotiated.iter().cloned().collect(),
                    }),
                ));
            }
            out.events.push(Event::Ready);
            return Ok(out);
        }
        match body {
            Body::Hello(_) | Body::Welcome(_) => return Err(protocol("duplicate handshake")),
            Body::Open(open) => {
                if self.is_local(id) || id <= self.highest_remote {
                    return Err(protocol("open id"));
                }
                self.highest_remote = id;
                if !valid_open(&open) {
                    return Err(protocol("open fields"));
                }
                let code = if self.draining {
                    Some(pb::ErrorCode::Refused)
                } else if !self.negotiated.contains(&open.profile) {
                    Some(pb::ErrorCode::Unsupported)
                } else if self.streams.values().filter(|s| !s.local).count()
                    >= self.local.max_streams as usize
                {
                    Some(pb::ErrorCode::ResourceExhausted)
                } else {
                    None
                };
                if let Some(code) = code {
                    out.outbound.push(packet(
                        id,
                        Body::Reset(pb::Reset {
                            code: code as i32,
                            reason: String::new(),
                        }),
                    ));
                } else {
                    self.streams.insert(
                        id,
                        Stream {
                            local: false,
                            accepted: false,
                            local_fin: false,
                            remote_fin: false,
                            send_credit: self.peer.as_ref().ok_or(protocol("peer"))?.stream_window,
                            outstanding: 0,
                        },
                    );
                    out.events.push(Event::Open(id, open));
                }
            }
            Body::Ping(p) => out
                .outbound
                .push(packet(0, Body::Pong(pb::Pong { token: p.token }))),
            Body::Pong(p) => out.events.push(Event::Pong(p.token)),
            Body::GoAway(g) => {
                if !valid_error(g.code, &g.reason)
                    || g.last_stream_id > self.highest_local
                    || g.last_stream_id < self.highest_acked
                    || (g.last_stream_id != 0 && !self.is_local(g.last_stream_id))
                    || self.peer_last.is_some_and(|last| g.last_stream_id > last)
                {
                    return Err(protocol("goaway"));
                }
                self.draining = true;
                self.peer_last = Some(g.last_stream_id);
                let ids: Vec<u32> = self
                    .streams
                    .iter()
                    .filter(|(_, s)| !s.accepted)
                    .map(|(id, _)| *id)
                    .collect();
                for id in ids {
                    let local = self.streams.get(&id).is_some_and(|s| s.local);
                    if !local {
                        out.outbound.push(packet(
                            id,
                            Body::Reset(pb::Reset {
                                code: pb::ErrorCode::Refused as i32,
                                reason: "goaway".into(),
                            }),
                        ));
                    }
                    out.outbound.extend(self.remove(id));
                    out.events.push(Event::Reset(
                        id,
                        pb::ErrorCode::Refused as i32,
                        "goaway".into(),
                    ));
                }
                out.events.push(Event::GoAway(g.last_stream_id));
            }
            Body::Window(w) if id == 0 => {
                let limit = self
                    .peer
                    .as_ref()
                    .ok_or(protocol("peer"))?
                    .connection_window;
                if w.credit == 0 || w.credit > limit - self.conn_send {
                    return Err(protocol("connection window"));
                }
                self.conn_send += w.credit;
            }
            body => {
                // Validate fields even for retired streams.
                match &body {
                    Body::Reset(r) if !valid_error(r.code, &r.reason) => {
                        return Err(protocol("reset fields"));
                    }
                    Body::Window(w) if w.credit == 0 => return Err(protocol("zero window")),
                    Body::Data(d)
                        if d.payload.len() > self.local.max_data as usize
                            || (d.payload.is_empty() && !d.fin) =>
                    {
                        return Err(protocol("data size"));
                    }
                    _ => {}
                }
                if !self.streams.contains_key(&id) {
                    if !self.historical(id) {
                        return Err(protocol("future stream"));
                    }
                    if let Body::Data(d) = body {
                        let n = d.payload.len() as u32;
                        if n > self.local.connection_window - self.conn_outstanding {
                            return Err(protocol("late data credit"));
                        }
                        if n > 0 {
                            out.outbound
                                .push(packet(0, Body::Window(pb::Window { credit: n })));
                        }
                    }
                    return Ok(out);
                }
                match body {
                    Body::OpenAck(_) => {
                        let s = self.streams.get_mut(&id).ok_or(protocol("stream"))?;
                        if !s.local || s.accepted {
                            return Err(protocol("ack state"));
                        }
                        s.accepted = true;
                        self.highest_acked = self.highest_acked.max(id);
                        out.events.push(Event::Accepted(id));
                    }
                    Body::Data(d) => {
                        let s = self.streams.get_mut(&id).ok_or(protocol("stream"))?;
                        let n = d.payload.len() as u32;
                        if !s.accepted || s.remote_fin {
                            return Err(protocol("data state"));
                        }
                        if n > self.local.stream_window - s.outstanding
                            || n > self.local.connection_window - self.conn_outstanding
                        {
                            return Err(protocol("receive credit"));
                        }
                        s.outstanding += n;
                        self.conn_outstanding += n;
                        s.remote_fin = d.fin;
                        out.events.push(Event::Data(id, d.payload, d.fin));
                        self.retire(id);
                    }
                    Body::Window(w) => {
                        let s = self.streams.get_mut(&id).ok_or(protocol("stream"))?;
                        let limit = self.peer.as_ref().ok_or(protocol("peer"))?.stream_window;
                        if !s.accepted || w.credit > limit - s.send_credit {
                            return Err(protocol("stream window"));
                        }
                        s.send_credit += w.credit;
                    }
                    Body::Reset(r) => {
                        out.outbound.extend(self.remove(id));
                        out.events.push(Event::Reset(id, r.code, r.reason));
                    }
                    _ => return Err(protocol("body")),
                }
            }
        }
        Ok(out)
    }
}
