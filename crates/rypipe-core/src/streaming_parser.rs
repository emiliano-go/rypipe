//! Incremental, chunk-fed record parsing.
//!
//! The batch engines in this crate need the whole input up front (a path or a
//! byte slice). A live network feed does not have that, and some formats need
//! per-record delivery with stream-global state (a section that spans chunks,
//! a truncation reported only at EOF). `StreamingRecordParser` fills that gap:
//! the adapter consumes whatever bytes are available and reports how many it
//! used; `RecordStream` owns the buffer, compaction and EOF handshake.
//!
//! Unlike the batch `RecordParser`, the emit callback carries an
//! adapter-defined `Record`, so formats whose records are not a flat bag of
//! columns (raw bytes, per-record metadata) are not forced through a
//! `ColumnarSink`.

use crate::diagnostics::ParseDiagnostics;
use crate::Result;

/// What a `RecordStream` did with the bytes it was given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    /// At least one record was emitted; call again to drain more.
    Rows,
    /// The buffer ends mid-record; feed more bytes and call again.
    NeedMore,
    /// The feed is finished (after `finish()`); no more records exist.
    Eof,
}

/// A parser that consumes complete records from an already-buffered prefix.
///
/// `parse_available` must emit every complete record in `bytes` through
/// `emit`, and return the number of bytes consumed (a prefix of `bytes`). A
/// trailing partial record is not consumed unless the parser retains it
/// internally; `eof` tells the parser that no more bytes are coming, so it can
/// flush or report truncated damage.
pub trait StreamingRecordParser: Send {
    /// The record type the adapter emits. May borrow nothing, so the callback
    /// owns its data.
    type Record;

    /// Validate the bytes seen so far (e.g. UTF-8). Called once per fed chunk.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse complete records from `bytes`, returning bytes consumed.
    fn parse_available(
        &mut self,
        bytes: &[u8],
        eof: bool,
        emit: &mut dyn FnMut(Self::Record),
        diag: &dyn ParseDiagnostics,
    ) -> Result<usize>;
}

/// Owns the input buffer, compaction and the EOF handshake around a
/// [`StreamingRecordParser`].
pub struct RecordStream<P: StreamingRecordParser> {
    parser: P,
    buffer: Vec<u8>,
    start: usize,
    eof: bool,
}

/// Compact once the consumed prefix grows past this many bytes.
const COMPACT_AFTER: usize = 64 * 1024;

impl<P: StreamingRecordParser> RecordStream<P> {
    pub fn new(parser: P) -> Self {
        Self {
            parser,
            buffer: Vec::new(),
            start: 0,
            eof: false,
        }
    }

    pub fn parser(&self) -> &P {
        &self.parser
    }

    /// Append a chunk of input. `validate` runs over the chunk here.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<()> {
        self.parser.validate(chunk)?;
        self.buffer.extend_from_slice(chunk);
        Ok(())
    }

    /// Signal that no more bytes are coming.
    pub fn finish(&mut self) {
        self.eof = true;
    }

    pub fn is_eof(&self) -> bool {
        self.eof
    }

    pub fn pending(&self) -> usize {
        self.buffer.len() - self.start
    }

    /// Parse whatever complete records the buffer holds, emitting each.
    pub fn parse(
        &mut self,
        emit: &mut dyn FnMut(P::Record),
        diag: &dyn ParseDiagnostics,
    ) -> Result<StreamState> {
        // Split borrows so the parser can read the buffer.
        let RecordStream {
            parser,
            buffer,
            start,
            eof,
        } = self;
        let consumed = parser.parse_available(&buffer[*start..], *eof, emit, diag)?;
        *start += consumed;

        if *start >= buffer.len() || *start >= COMPACT_AFTER {
            buffer.drain(..*start);
            *start = 0;
        }

        if *eof {
            return Ok(StreamState::Eof);
        }
        if consumed == 0 {
            Ok(StreamState::NeedMore)
        } else {
            Ok(StreamState::Rows)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::NoopDiagnostics;

    /// Emits one `String` per `\n`-terminated line.
    struct LineParser;

    impl StreamingRecordParser for LineParser {
        type Record = String;

        fn validate(&self, bytes: &[u8]) -> Result<()> {
            simdutf8::basic::from_utf8(bytes).map_err(crate::Error::Utf8)?;
            Ok(())
        }

        fn parse_available(
            &mut self,
            bytes: &[u8],
            _eof: bool,
            emit: &mut dyn FnMut(String),
            _diag: &dyn ParseDiagnostics,
        ) -> Result<usize> {
            let mut consumed = 0usize;
            for (idx, &b) in bytes.iter().enumerate() {
                if b == b'\n' {
                    let line = std::str::from_utf8(&bytes[consumed..idx]).unwrap_or("");
                    if !line.is_empty() {
                        emit(line.to_string());
                    }
                    consumed = idx + 1;
                }
            }
            Ok(consumed)
        }
    }

    #[test]
    fn emits_records_across_chunks_and_drops_the_trailing_partial() {
        let mut stream = RecordStream::new(LineParser);
        let diag = NoopDiagnostics;
        let mut rows: Vec<String> = Vec::new();
        let data = b"A=1 B=2\nA=3 B=4\nA=5";

        stream.feed(&data[..10]).unwrap();
        assert_eq!(
            stream.parse(&mut |r| rows.push(r), &diag).unwrap(),
            StreamState::Rows
        );
        assert_eq!(stream.pending(), 2);

        stream.feed(&data[10..]).unwrap();
        assert_eq!(
            stream.parse(&mut |r| rows.push(r), &diag).unwrap(),
            StreamState::Rows
        );
        assert_eq!(stream.pending(), 3);

        stream.finish();
        assert_eq!(
            stream.parse(&mut |r| rows.push(r), &diag).unwrap(),
            StreamState::Eof
        );
        assert_eq!(rows, vec!["A=1 B=2".to_string(), "A=3 B=4".to_string()]);
    }

    #[test]
    fn empty_feed_is_eof() {
        let mut stream = RecordStream::new(LineParser);
        stream.finish();
        let mut n = 0;
        assert_eq!(
            stream.parse(&mut |_| n += 1, &NoopDiagnostics).unwrap(),
            StreamState::Eof
        );
        assert_eq!(n, 0);
    }

    #[test]
    fn invalid_utf8_is_rejected_on_feed() {
        let mut stream = RecordStream::new(LineParser);
        assert!(stream.feed(b"\xff\n").is_err());
    }
}
