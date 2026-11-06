#![doc = include_str!("../README.md")]

//! TMP v1: a serial, transport-independent state machine. All returned packets
//! must be sent, in order; a failed transport must never be retried in place.
pub mod pb {
    include!("generated/tmp.v1.rs");
}
pub mod framing;
pub mod session;
pub mod transport;
pub use session::{Event, Outcome, Role, Session};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("invalid operation: {0}")]
    Invalid(&'static str),
    #[error("insufficient send credit")]
    WouldBlock,
    #[error("invalid protobuf: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
pub fn default_settings() -> pb::Settings {
    pb::Settings {
        max_data: 16384,
        stream_window: 65536,
        connection_window: 1048576,
        max_streams: 256,
    }
}
