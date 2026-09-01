//! Format-adapter contract tests.
//!
//! These are the `rypipe-core` analogue of xmlstreamer's tokenizer and stream
//! invariants: records must be independent of safe partitioning, incomplete
//! trailing data must not be committed, and every execution mode must preserve
//! the same ordered rows.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use rypipe_core::{
    bounded::MemoryBudget, ColumnarSink, Error, ParallelStreamOpts, Pipeline, RecordParser,
    Splitter, Value,
};

/// Frames are `@<body-byte-length>:id=<id>|note=<note>`. Their boundaries are
/// independent of line endings, so they exercise the splitter contract rather
/// than a newline-only parser.
#[derive(Clone, Debug, Default)]
struct FrameSplitter;

#[derive(Clone, Debug, Default)]
struct FrameParser;

fn decimal(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    bytes.iter().try_fold(0usize, |value, &byte| {
        byte.is_ascii_digit()
            .then(|| value.checked_mul(10)?.checked_add((byte - b'0') as usize))?
    })
}

/// Return byte offsets just past fully present frames. Invalid or incomplete
/// suffixes deliberately have no boundary, leaving parser policy in charge.
fn complete_frame_ends(bytes: &[u8]) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        if bytes[cursor] != b'@' {
            break;
        }
        let header_start = cursor + 1;
        let Some(colon_rel) = bytes[header_start..].iter().position(|&byte| byte == b':') else {
            break;
        };
        let colon = header_start + colon_rel;
        let Some(body_len) = decimal(&bytes[header_start..colon]) else {
            break;
        };
        let Some(end) = colon
            .checked_add(1)
            .and_then(|start| start.checked_add(body_len))
        else {
            break;
        };
        if end > bytes.len() {
            break;
        }
        ends.push(end);
        cursor = end;
    }

    ends
}

impl Splitter for FrameSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
        // Find the next complete frame starting at or after `from`
        let mut cursor = from;
        while cursor < bytes.len() {
            if bytes[cursor] != b'@' {
                return None;
            }
            let header_start = cursor + 1;
            let colon_rel = bytes[header_start..]
                .iter()
                .position(|&byte| byte == b':')?;
            let colon = header_start + colon_rel;
            let body_len = decimal(&bytes[header_start..colon])?;
            let end = colon.checked_add(1)?.checked_add(body_len)?;
            if end > bytes.len() {
                return None;
            }
            return Some(end);
        }
        None
    }
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if bytes.is_empty() || max_chunks <= 1 {
            return vec![0, bytes.len()];
        }

        let stride = (bytes.len() / max_chunks).max(1);
        let mut points = vec![0];
        let mut next_target = stride;
        for end in complete_frame_ends(bytes) {
            if end >= next_target && points.len() < max_chunks {
                points.push(end);
                next_target = next_target.saturating_add(stride);
            }
        }
        if *points.last().unwrap() != bytes.len() {
            points.push(bytes.len());
        }
        points
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let ends = complete_frame_ends(sample);
        ends.last()
            .copied()
            .map(|total| (total / ends.len()).max(1))
            .unwrap_or_else(|| sample.len().max(1))
    }
}

impl RecordParser for FrameParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let mut cursor = 0;
        while cursor < bytes.len() {
            if bytes[cursor] != b'@' {
                return Err(Error::Plan(format!(
                    "frame at byte {cursor} must start with '@'"
                )));
            }
            let header_start = cursor + 1;
            let Some(colon_rel) = bytes[header_start..].iter().position(|&byte| byte == b':')
            else {
                // The final header itself is incomplete, so it cannot form a row.
                break;
            };
            let colon = header_start + colon_rel;
            let body_len = decimal(&bytes[header_start..colon]).ok_or_else(|| {
                Error::Plan(format!("frame at byte {cursor} has an invalid length"))
            })?;
            let body_start = colon + 1;
            let Some(end) = body_start.checked_add(body_len) else {
                return Err(Error::Plan(format!(
                    "frame at byte {cursor} length overflows"
                )));
            };
            if end > bytes.len() {
                // A trailing partial frame is adapter-owned incomplete input and
                // must not call begin_row/end_row.
                break;
            }

            let body = simdutf8::basic::from_utf8(&bytes[body_start..end])?;
            let mut id = None;
            let mut note = None;
            for part in body.split('|') {
                let (name, value) = part.split_once('=').ok_or_else(|| {
                    Error::Plan(format!("frame at byte {cursor} has an invalid field"))
                })?;
                match name {
                    "id" if id.replace(value).is_none() => {}
                    "note" if note.replace(value).is_none() => {}
                    _ => {
                        return Err(Error::Plan(format!(
                            "frame at byte {cursor} has an invalid or duplicate field {name:?}"
                        )));
                    }
                }
            }
            let (Some(id), Some(note)) = (id, note) else {
                return Err(Error::Plan(format!(
                    "frame at byte {cursor} is missing a required field"
                )));
            };

            sink.begin_row();
            sink.put_field("id", Value::Str(std::borrow::Cow::Borrowed(id)));
            sink.put_field("note", Value::Str(std::borrow::Cow::Borrowed(note)));
            sink.end_row();
            cursor = end;
        }
        Ok(())
    }
}

fn pipeline() -> Pipeline<FrameSplitter, FrameParser> {
    Pipeline::new(FrameSplitter, FrameParser)
}

fn frame(id: usize, note: &str) -> Vec<u8> {
    let body = format!("id={id}|note={note}");
    format!("@{}:{body}", body.len()).into_bytes()
}

fn frames(records: &[(usize, String)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (id, note) in records {
        bytes.extend(frame(*id, note));
    }
    bytes
}

fn rows(batches: &[RecordBatch]) -> Vec<(String, String)> {
    let mut output = Vec::new();
    for batch in batches {
        let id = batch
            .column_by_name("id")
            .expect("adapter contract requires an id column")
            .as_string::<i32>();
        let note = batch
            .column_by_name("note")
            .expect("adapter contract requires a note column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            assert!(!id.is_null(row), "id must not be null");
            assert!(!note.is_null(row), "note must not be null");
            output.push((id.value(row).to_owned(), note.value(row).to_owned()));
        }
    }
    output
}

fn expected(records: &[(usize, String)]) -> Vec<(String, String)> {
    records
        .iter()
        .map(|(id, note)| (id.to_string(), note.clone()))
        .collect()
}

fn assert_in_memory_modes(bytes: &[u8], expected_rows: &[(String, String)]) {
    let pipeline = pipeline();
    let single = pipeline
        .read_bytes(bytes)
        .expect("single parse must succeed");
    assert_eq!(rows(&[single]), expected_rows);

    for chunks in [1, 2, 3, 7] {
        let parallel = pipeline
            .read_bytes_par(bytes, chunks)
            .expect("parallel parse must succeed");
        assert_eq!(rows(&parallel), expected_rows, "parallel chunks={chunks}");
    }

    for budget in [16, 64, 256] {
        let streaming = pipeline
            .read_bytes_stream(bytes, MemoryBudget::new(budget))
            .expect("bounded parse must succeed");
        assert_eq!(rows(&streaming), expected_rows, "bounded budget={budget}");
    }
}

static TEMP_FILE_ID: AtomicUsize = AtomicUsize::new(0);

fn temp_path() -> PathBuf {
    let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rypipe_adapter_contract_{}_{}.frames",
        std::process::id(),
        id
    ))
}

#[test]
fn safe_partitions_preserve_frames_across_in_memory_modes() {
    let records = vec![
        (1, "short".to_string()),
        (2, "contains a newline\nand remains one frame".to_string()),
        (
            3,
            "longer value used to force an internal partition boundary".to_string(),
        ),
        (4, "final".to_string()),
    ];
    let bytes = frames(&records);
    assert_in_memory_modes(&bytes, &expected(&records));
}

#[test]
fn partial_trailing_frame_is_never_committed() {
    let records = vec![
        (1, "complete".to_string()),
        (2, "also complete".to_string()),
    ];
    let mut bytes = frames(&records);
    bytes.extend(b"@1000:id=3|note=this frame ends before its declared body");

    assert_in_memory_modes(&bytes, &expected(&records));
}

#[test]
fn oversized_frame_is_delivered_once_under_tiny_budget() {
    let records = vec![
        (1, "X".repeat(4096)),
        (2, "small frame after the oversized one".to_string()),
    ];
    let bytes = frames(&records);
    let expected_rows = expected(&records);
    let pipeline = pipeline();

    let batches = pipeline
        .read_bytes_stream(&bytes, MemoryBudget::new(32))
        .expect("oversized frame must still be emitted");
    assert_eq!(rows(&batches), expected_rows);
}

#[test]
fn malformed_complete_frame_returns_an_error_in_every_in_memory_mode() {
    let bytes = b"@x:id=1|note=not-a-valid-length";
    let pipeline = pipeline();

    assert!(matches!(pipeline.read_bytes(bytes), Err(Error::Plan(_))));
    assert!(matches!(
        pipeline.read_bytes_par(bytes, 3),
        Err(Error::Plan(_))
    ));
    assert!(matches!(
        pipeline.read_bytes_stream(bytes, MemoryBudget::new(32)),
        Err(Error::Plan(_))
    ));
}

#[test]
fn ordered_parallel_streaming_matches_single_parse() {
    let records: Vec<_> = (0..24)
        .map(|id| (id, format!("{:04}-{}", id, "x".repeat(4096))))
        .collect();
    let bytes = frames(&records);
    let expected_rows = expected(&records);
    let pipeline = pipeline();
    let path = temp_path();
    std::fs::write(&path, &bytes).expect("test fixture must be writable");

    {
        let single = pipeline
            .read_path(&path, false, false)
            .expect("path parse must succeed");
        assert_eq!(rows(&[single]), expected_rows);

        let parallel = pipeline
            .read_path_par(&path, 4, false, false)
            .expect("parallel path parse must succeed");
        assert_eq!(rows(&parallel), expected_rows);

        let bounded = pipeline
            .read_path_stream(&path, MemoryBudget::new(4096), false)
            .expect("bounded path parse must succeed");
        assert_eq!(rows(&bounded), expected_rows);

        let opts = ParallelStreamOpts {
            threads: 2,
            ordered: true,
            max_reorder: 2,
            schema: None,
        };
        let parallel_stream = pipeline
            .read_path_stream_par(&path, MemoryBudget::new(128 * 1024), false, opts)
            .expect("parallel stream construction must succeed")
            .collect::<rypipe_core::Result<Vec<_>>>()
            .expect("parallel stream must succeed");
        assert_eq!(rows(&parallel_stream), expected_rows);
    };

    let _ = std::fs::remove_file(&path);
}
