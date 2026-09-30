//! Incremental, chunk-fed record parsing.
//!
//! The batch engines in this crate need the whole input up front (a path or a
//! byte slice). A live network feed does not have that, and some formats need
//! per-record delivery with stream-global state (a section that spans chunks,
//! a truncation reported only at EOF). `StreamingRecordParser` fills that gap:
//! the adapter consumes whatever bytes are available and reports how many it
//! used; `RecordStream` owns the buffer, compaction and EOF handshake.
//!
//! Records are emitted through the same `ColumnarSink` the batch parsers use,
//! so an adapter can share one field-extraction path between both engines.

use crate::decoder::ColumnarSink;
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
/// `parse_available` must emit every complete record in `bytes` to `sink` and
/// return the number of bytes consumed (a prefix of `bytes`). A trailing
/// partial record is not consumed: the driver keeps it for the next call.
/// When `finish()` was called on the driver, the final call sees the truncated
/// tail and decides what it means (drop it, or report a diagnostic).
pub trait StreamingRecordParser: Send + Sync {
    /// Validate the bytes seen so far (e.g. UTF-8). Called once per fed chunk.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse complete records from `bytes`, returning bytes consumed.
    fn parse_available(
        &self,
        bytes: &[u8],
        sink: &mut dyn ColumnarSink,
        diag: &dyn ParseDiagnostics,
    ) -> Result<usize>;
}

/// Owns the input buffer, compaction and the EOF handshake around a
/// [`StreamingRecordParser`].
pub struct RecordStream<P> {
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

    /// Parse whatever complete records the buffer holds.
    pub fn parse(
        &mut self,
        sink: &mut dyn ColumnarSink,
        diag: &dyn ParseDiagnostics,
    ) -> Result<StreamState> {
        let consumed = self
            .parser
            .parse_available(&self.buffer[self.start..], sink, diag)?;
        self.start += consumed;
        debug_assert_eq!(self.start, self.start.min(self.buffer.len()));

        if self.start >= self.buffer.len() || self.start >= COMPACT_AFTER {
            self.buffer.drain(..self.start);
            self.start = 0;
        }

        if self.eof {
            return Ok(StreamState::Eof);
        }
        if consumed == 0 {
            Ok(StreamState::NeedMore)
        } else {
            Ok(StreamState::Rows)
        }
    }

    /// Bytes currently buffered but not yet consumed.
    pub fn pending(&self) -> usize {
        self.buffer.len() - self.start
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;

    /// Accumulates emitted rows as `(name, value)` pairs for assertions.
    #[derive(Default)]
    pub struct RowBuffer {
        pub rows: Vec<Vec<(String, String)>>,
        current: Vec<(String, String)>,
    }

    impl ColumnarSink for RowBuffer {
        fn begin_row(&mut self) {
            self.current.clear();
        }
        fn put_field(&mut self, name: &str, value: crate::Value<'_>) {
            let text = match value {
                crate::Value::Str(s) => s.into_owned(),
                other => format!("{other:?}"),
            };
            self.current.push((name.to_string(), text));
        }
        fn end_row(&mut self) {
            self.rows.push(std::mem::take(&mut self.current));
        }
        fn finish(&mut self) -> Result<arrow::record_batch::RecordBatch> {
            Ok(arrow::record_batch::RecordBatch::new_empty(
                std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::RowBuffer;
    use super::*;
    use crate::diagnostics::NoopDiagnostics;
    use crate::Value;
    use std::borrow::Cow;

    struct LineParser;

    impl StreamingRecordParser for LineParser {
        fn validate(&self, bytes: &[u8]) -> Result<()> {
            simdutf8::basic::from_utf8(bytes).map_err(crate::Error::Utf8)?;
            Ok(())
        }

        fn parse_available(
            &self,
            bytes: &[u8],
            sink: &mut dyn ColumnarSink,
            _diag: &dyn ParseDiagnostics,
        ) -> Result<usize> {
            let mut consumed = 0usize;
            for (idx, &b) in bytes.iter().enumerate() {
                if b != b'\n' {
                    continue;
                }
                let text = std::str::from_utf8(&bytes[consumed..idx]).unwrap_or("");
                if !text.is_empty() {
                    sink.begin_row();
                    for token in text.split_whitespace() {
                        if let Some((k, v)) = token.split_once('=') {
                            sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                        }
                    }
                    sink.end_row();
                }
                consumed = idx + 1;
            }
            Ok(consumed)
        }
    }

    #[test]
    fn emits_rows_across_chunks_and_drops_the_trailing_partial() {
        let mut stream = RecordStream::new(LineParser);
        let diag = NoopDiagnostics;
        let mut sink = RowBuffer::default();
        let data = b"A=1 B=2\nA=3 B=4\nA=5";

        stream.feed(&data[..10]).unwrap();
        assert_eq!(stream.parse(&mut sink, &diag).unwrap(), StreamState::Rows);
        assert_eq!(stream.pending(), 2); // "A=" kept

        stream.feed(&data[10..]).unwrap();
        assert_eq!(stream.parse(&mut sink, &diag).unwrap(), StreamState::Rows);
        assert_eq!(stream.pending(), 3); // "A=5" is not a record yet

        stream.finish();
        assert_eq!(stream.parse(&mut sink, &diag).unwrap(), StreamState::Eof);
        assert_eq!(stream.pending(), 3); // truncated tail discarded

        assert_eq!(sink.rows.len(), 2);
        assert_eq!(
            sink.rows[0],
            vec![("A".to_string(), "1".to_string()), ("B".to_string(), "2".to_string())]
        );
    }

    #[test]
    fn empty_feed_is_eof() {
        let mut stream = RecordStream::new(LineParser);
        stream.finish();
        let mut sink = RowBuffer::default();
        assert_eq!(
            stream.parse(&mut sink, &NoopDiagnostics).unwrap(),
            StreamState::Eof
        );
    }

    #[test]
    fn invalid_utf8_is_rejected_on_feed() {
        let mut stream = RecordStream::new(LineParser);
        assert!(stream.feed(b"\xff\n").is_err());
    }
}
