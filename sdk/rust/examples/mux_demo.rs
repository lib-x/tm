//! Five opaque byte streams over TCP or raw stdin/stdout; no shell is executed,
//! clipboard accessed, or screenshot captured. The client sends deterministic
//! terminal-command, NDJSON, file, clipboard-text, and PNG fixtures; the server
//! echoes arbitrary bytes on those routes, preserving FIN and receive credit.
//!
//! Run: cargo run --example mux_demo -- server [127.0.0.1:7000]
//!      cargo run --example mux_demo -- client [127.0.0.1:7000]
//! Or connect `stdio-client` and `stdio-server` with two binary pipes.
use std::{collections::VecDeque, error::Error, io::Write, time::Duration};
use tokio::{
    io::{self, AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use tunnel_mux::{
    Event, Role, Session, default_settings,
    pb::{self, packet::Body},
    transport,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const ROUTES: [&str; 5] = ["terminal", "events", "file", "clipboard", "screenshot"];
const PROFILE: &str = "bytes.v1";
const CHUNK: usize = 1024;
const DEADLINE: Duration = Duration::from_secs(15);

fn fixture(route: &str) -> Vec<u8> {
    match route {
        "terminal" => "printf '你好 TMP\\n'\r\n".as_bytes().to_vec(),
        "events" => concat!(
            "{\"type\":\"started\",\"seq\":1}\n",
            "{\"type\":\"output\",\"seq\":2,\"text\":\"你好\"}\n"
        )
        .as_bytes()
        .to_vec(),
        "file" => (0..262_144).map(|i| (i % 251) as u8).collect(),
        "clipboard" => "TMP clipboard: 你好 🌍\n".as_bytes().to_vec(),
        "screenshot" => vec![
            137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1,
            8, 4, 0, 0, 0, 181, 28, 12, 2, 0, 0, 0, 11, 73, 68, 65, 84, 120, 218, 99, 252, 255, 31,
            0, 3, 3, 2, 0, 239, 162, 167, 91, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
        ],
        _ => unreachable!("fixture requested for unknown route: {route}"),
    }
}

struct Echo {
    bytes: Vec<u8>,
    offset: usize,
    fin: bool,
}

#[derive(Default)]
struct Stream {
    id: u32,
    route: &'static str,
    payload: Vec<u8>, // Only clients hold fixtures; server input stays opaque.
    sent: usize,
    checked: usize,
    ready: bool,
    done: bool,
    sent_fin: bool,
    echo: VecDeque<Echo>,
}

struct Demo {
    session: Session,
    role: Role,
    stdio: bool,
    streams: Vec<Stream>, // At most five entries, retained to reject duplicate routes.
    goaway_sent: bool,
    finished: bool,
}

// Tasks only move bytes. The driver below remains the sole Session owner.
// Dropping the driver on a deadline aborts both tasks and closes their carrier
// halves; a partially read/written frame is never resumed on that link.
struct Workers {
    reader: JoinHandle<()>,
    writer: JoinHandle<tunnel_mux::Result<()>>,
}

impl Drop for Workers {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}

impl Demo {
    fn report(&self, message: &str) -> Result<()> {
        if self.stdio {
            writeln!(std::io::stderr(), "{message}")?;
        } else {
            writeln!(std::io::stdout(), "{message}")?;
        }
        Ok(())
    }

    fn event(&mut self, event: Event) -> Result<Vec<pb::Packet>> {
        let mut outbound = Vec::new();
        match event {
            Event::Ready => {
                if self.session.negotiated_profiles() != [PROFILE] {
                    return Err("peer did not negotiate bytes.v1".into());
                }
                if self.role == Role::Initiator {
                    for route in ROUTES {
                        let (id, packet) = self.session.open(pb::Open {
                            profile: PROFILE.into(),
                            route: route.into(),
                            ..Default::default()
                        })?;
                        outbound.push(packet);
                        self.streams.push(Stream {
                            id,
                            route,
                            payload: fixture(route),
                            ..Default::default()
                        });
                    }
                }
            }
            Event::Open(id, open) => {
                let route = ROUTES.into_iter().find(|route| *route == open.route);
                if self.role != Role::Responder
                    || open.profile != PROFILE
                    || route.is_none()
                    || self.streams.iter().any(|s| s.route == open.route)
                {
                    return Ok(self.session.reset(
                        id,
                        pb::ErrorCode::Refused,
                        "unknown, duplicate, or unsupported route".into(),
                    )?);
                }
                if let Some(route) = route {
                    outbound.push(self.session.accept(id)?);
                    self.streams.push(Stream {
                        id,
                        route,
                        ready: true,
                        ..Default::default()
                    });
                }
            }
            Event::Accepted(id) => self.stream_mut(id)?.ready = true,
            Event::Data(id, bytes, fin) => {
                let client = self.role == Role::Initiator;
                let stream = self.stream_mut(id)?;
                if client {
                    let end = stream.checked + bytes.len();
                    if stream.done
                        || end > stream.sent
                        || stream.payload.get(stream.checked..end) != Some(bytes.as_slice())
                        || (fin && (end != stream.payload.len() || !stream.sent_fin))
                    {
                        return Err(
                            format!("{}: response bytes or FIN mismatch", stream.route).into()
                        );
                    }
                    stream.checked = end;
                    stream.done = fin;
                    let report = fin.then(|| format!("PASS {} {end} bytes", stream.route));
                    // Zero-byte FIN can retire a stream before consume is called.
                    if !bytes.is_empty() {
                        outbound.extend(self.session.consume(id, bytes.len().try_into()?)?);
                    }
                    if let Some(report) = report {
                        self.report(&report)?;
                    }
                } else {
                    // No consume here: queued bytes still occupy receive credit.
                    stream.echo.push_back(Echo {
                        bytes,
                        offset: 0,
                        fin,
                    });
                }
            }
            Event::GoAway(_) => {
                if self.streams.len() != ROUTES.len()
                    || self.streams.iter().any(|s| !s.done || !s.echo.is_empty())
                    || (self.role == Role::Initiator && !self.goaway_sent)
                {
                    return Err("GOAWAY before all five streams finished".into());
                }
                if self.role == Role::Responder {
                    // TMP uses code zero (CANCELLED) for this graceful demo shutdown.
                    outbound.extend(
                        self.session
                            .goaway(pb::ErrorCode::Cancelled, String::new())?,
                    );
                }
                self.finished = true;
            }
            Event::Reset(id, code, reason) => {
                return Err(format!("stream {id} reset ({code}): {reason}").into());
            }
            Event::Pong(_) => {}
        }
        Ok(outbound)
    }

    fn stream_mut(&mut self, id: u32) -> Result<&mut Stream> {
        self.streams
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or_else(|| format!("unexpected stream {id}").into())
    }

    fn pump(&mut self) -> Result<Vec<pb::Packet>> {
        let mut outbound = Vec::new();
        loop {
            let mut progress = false;
            // One DATA per eligible stream per pass, then repeat round-robin.
            for stream in &mut self.streams {
                if !stream.ready || stream.done {
                    continue;
                }
                let (bytes, fin) = if self.role == Role::Initiator {
                    if stream.sent_fin {
                        continue;
                    }
                    let end = (stream.sent + CHUNK).min(stream.payload.len());
                    (
                        &stream.payload[stream.sent..end],
                        end == stream.payload.len(),
                    )
                } else if let Some(echo) = stream.echo.front() {
                    let end = (echo.offset + CHUNK).min(echo.bytes.len());
                    (
                        &echo.bytes[echo.offset..end],
                        echo.fin && end == echo.bytes.len(),
                    )
                } else {
                    continue;
                };
                let n = bytes.len();
                // send owns its buffer even on WouldBlock; retain the source until success.
                match self.session.send(stream.id, bytes.to_vec(), fin) {
                    Ok(packet) => outbound.push(packet),
                    Err(tunnel_mux::Error::WouldBlock) => continue,
                    Err(error) => return Err(error.into()),
                }
                progress = true;
                if self.role == Role::Initiator {
                    stream.sent += n;
                    stream.sent_fin = fin;
                } else {
                    if let Some(echo) = stream.echo.front_mut() {
                        echo.offset += n;
                        if echo.offset == echo.bytes.len() {
                            stream.echo.pop_front();
                        }
                    }
                    stream.done = fin;
                    // Release receive credit only after send accepted the echo.
                    if n != 0 {
                        outbound.extend(self.session.consume(stream.id, n.try_into()?)?);
                    }
                }
            }
            if !progress {
                break;
            }
        }
        if self.role == Role::Initiator
            && !self.goaway_sent
            && self.streams.len() == ROUTES.len()
            && self.streams.iter().all(|s| s.done)
        {
            outbound.extend(
                self.session
                    .goaway(pb::ErrorCode::Cancelled, String::new())?,
            );
            self.goaway_sent = true;
        }
        Ok(outbound)
    }

    async fn drive<R, W>(&mut self, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (read_tx, mut received) = mpsc::channel(1);
        let (write_tx, mut to_write) = mpsc::channel::<pb::Packet>(1);
        let mut workers = Workers {
            reader: tokio::spawn(async move {
                loop {
                    let packet = transport::read_packet(&mut reader).await;
                    let ended = !matches!(&packet, Ok(Some(_)));
                    if read_tx.send(packet).await.is_err() || ended {
                        return;
                    }
                }
            }),
            writer: tokio::spawn(async move {
                while let Some(packet) = to_write.recv().await {
                    transport::write_packet(&mut writer, &packet).await?;
                }
                writer.shutdown().await?;
                Ok(())
            }),
        };
        let mut outbound = VecDeque::new();
        if self.role == Role::Initiator {
            outbound.push_back(self.session.start()?);
        }
        while !self.finished || !outbound.is_empty() {
            tokio::select! {
                // Keep reading even while a small stdout pipe blocks writes.
                // Limit the FIFO as well as each worker's one-packet channel.
                next = received.recv(), if !self.finished && outbound.len() < 512 => {
                    let packet = next.ok_or("reader stopped before GOAWAY")??
                        .ok_or("EOF before reciprocal GOAWAY")?;
                    if let Some(Body::GoAway(goaway)) = &packet.body
                        && goaway.code != pb::ErrorCode::Cancelled as i32
                    {
                        return Err(format!("peer GOAWAY error {}: {}", goaway.code, goaway.reason).into());
                    }
                    let is_open = matches!(&packet.body, Some(Body::Open(_)));
                    let mut outcome = self.session.receive(packet)?;
                    if self.role == Role::Responder && is_open {
                        for packet in &mut outcome.outbound {
                            if let Some(Body::Reset(reset)) = &mut packet.body
                                && reset.code == pb::ErrorCode::Unsupported as i32
                            {
                                reset.code = pb::ErrorCode::Refused as i32;
                            }
                        }
                    }
                    // FIFO order puts automatic output before every frame from
                    // the corresponding application events and sending pump.
                    outbound.extend(outcome.outbound);
                    for event in outcome.events {
                        outbound.extend(self.event(event)?);
                    }
                    // WINDOW has no event: every packet must resume the pump.
                    outbound.extend(self.pump()?);
                }
                permit = write_tx.reserve(), if !outbound.is_empty() => {
                    permit.map_err(|_| "writer stopped before GOAWAY")?
                        .send(outbound.pop_front().ok_or("empty outbound queue")?);
                }
                result = &mut workers.writer => {
                    result??;
                    return Err("writer stopped before GOAWAY".into());
                }
            }
        }
        drop(write_tx);
        (&mut workers.writer).await??;
        if self.role == Role::Initiator {
            self.report("PASS all 5 streams")?;
        }
        Ok(())
    }
}

async fn run() -> Result<()> {
    let usage = "usage: mux_demo {server|client} [127.0.0.1:7000] | stdio-server | stdio-client";
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or(usage)?;
    let (role, stdio) = match command.as_str() {
        "server" => (Role::Responder, false),
        "client" => (Role::Initiator, false),
        "stdio-server" => (Role::Responder, true),
        "stdio-client" => (Role::Initiator, true),
        _ => return Err(usage.into()),
    };
    let address = args.next();
    if args.next().is_some() || (stdio && address.is_some()) {
        return Err(usage.into());
    }
    let address = address.unwrap_or_else(|| "127.0.0.1:7000".into());
    let address = if address.starts_with(':') {
        format!("127.0.0.1{address}")
    } else {
        address
    };
    let mut demo = Demo {
        session: Session::new(role, default_settings(), vec![PROFILE.into()])?,
        role,
        stdio,
        streams: Vec::new(),
        goaway_sent: false,
        finished: false,
    };
    // Dropping this future closes its carrier; partial I/O is never retried.
    let result = tokio::time::timeout(DEADLINE, async {
        if stdio {
            return demo.drive(io::stdin(), io::stdout()).await;
        }
        let socket = if role == Role::Responder {
            let listener = TcpListener::bind(&address).await?;
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "LISTEN {}", listener.local_addr()?)?;
            stdout.flush()?;
            drop(stdout);
            listener.accept().await?.0
        } else {
            TcpStream::connect(&address).await?
        };
        socket.set_nodelay(true)?;
        let (reader, writer) = socket.into_split();
        demo.drive(reader, writer).await
    })
    .await;
    demo.session.close();
    result.unwrap_or_else(|_| Err("demo deadline exceeded (15s); carrier closed".into()))
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // Tokio stdin uses a blocking read; never let runtime teardown await that
    // uncancellable worker after the whole-run deadline has closed the session.
    runtime.shutdown_timeout(Duration::ZERO);
    result
}
