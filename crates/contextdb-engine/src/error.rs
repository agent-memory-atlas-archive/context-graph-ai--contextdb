#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("received schema for table {table} has no author attestation")]
    UnattestedReceivedSchema { table: String },
    #[error("engine error: {0}")]
    Engine(String),
    #[error("serialization error: {0}")]
    Serde(String),
    #[error(
        "protocol version mismatch: received {received}, supported range {oldest_supported} through {newest_supported} — the peer must use a contextdb release supporting a protocol version in this range"
    )]
    ProtocolVersionMismatch {
        received: u8,
        oldest_supported: u8,
        newest_supported: u8,
    },
    #[error("chunk reassembly error: {0}")]
    ChunkError(String),
}
