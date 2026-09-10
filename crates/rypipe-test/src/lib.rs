//! Property-based testing helpers and fixtures for rypipe adapters.
//!
//! This crate provides:
//! - Proptest strategies for generating test data
//! - Common test fixtures (malformed UTF-8, nested quotes, edge cases)
//! - Helper functions for verifying parser correctness

use proptest::prelude::*;
use rypipe_core::decoder::{ColumnarSink, RecordParser, Splitter};
use rypipe_core::engine::TableBuilder;
use rypipe_core::plan::ExecutionPlan;
use rypipe_core::value::Value;
use std::borrow::Cow;
use std::sync::Arc;

/// Generate random valid field names (alphanumeric + underscore).
pub fn arb_field_name() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,20}".prop_map(|s| s.to_string())
}

/// Generate random field values (printable ASCII).
pub fn arb_field_value() -> impl Strategy<Value = String> {
    "[\\x20-\\x7E]{1,50}".prop_map(|s| s.to_string())
}

/// Generate a single record as key=value pairs.
pub fn arb_record() -> impl Strategy<Value = Vec<(String, String)>> {
    prop::collection::vec((arb_field_name(), arb_field_value()), 1..10)
}

/// Generate multiple records as newline-separated key=value pairs.
pub fn arb_records(n: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(arb_record(), n).prop_map(|records| {
        records
            .iter()
            .map(|fields| {
                fields
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    })
}

/// Generate malformed UTF-8 bytes.
pub fn arb_malformed_utf8() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..=255, 10..100).prop_map(|bytes| {
        // Ensure at least one invalid UTF-8 sequence
        let mut result = bytes;
        result.push(0xFF); // Invalid UTF-8
        result
    })
}

/// Generate nested quotes (common in CSV-like formats).
pub fn arb_nested_quotes() -> impl Strategy<Value = String> {
    prop::collection::vec("[a-zA-Z ]{1,20}", 1..5).prop_map(|parts| {
        let inner = parts.join(",");
        format!("\"{}\"", inner)
    })
}

/// Generate a test parser that produces key=value records.
pub struct KeyValueParser;

impl RecordParser for KeyValueParser {
    fn validate(&self, _bytes: &[u8]) -> rypipe_core::Result<()> {
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;

        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for token in line.split_whitespace() {
                if let Some((k, v)) = token.split_once('=') {
                    sink.put_field(k, Value::Str(Cow::Borrowed(v)));
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

/// Generate a test splitter that splits on newlines.
pub struct NewlineSplitter;

impl Splitter for NewlineSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        if from >= bytes.len() {
            return None;
        }
        let start = if bytes[from] == b'\n' { from + 1 } else { from };
        if start >= bytes.len() {
            return None;
        }
        memchr::memchr(b'\n', &bytes[start..]).map(|rel| start + rel + 1)
    }

    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let mut points = vec![0usize];
        let mut last = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'\n' {
                let next = i + 1;
                if next > last && points.len() < max_chunks {
                    points.push(next);
                    last = next;
                }
            }
        }
        if *points.last().unwrap() != bytes.len() {
            points.push(bytes.len());
        }
        points
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let newline_count = sample.iter().filter(|&&b| b == b'\n').count().max(1);
        (sample.len() / newline_count).max(1)
    }
}

/// Parse bytes with a test parser and return a TableBuilder.
pub fn parse_test_bytes(bytes: &[u8]) -> TableBuilder {
    let plan = Arc::new(ExecutionPlan::new());
    let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), plan);
    KeyValueParser.parse_chunk(bytes, &mut sink).unwrap();
    sink
}

/// Assert that two RecordBatches have the same schema and data.
pub fn assert_batches_equal(
    left: &arrow::record_batch::RecordBatch,
    right: &arrow::record_batch::RecordBatch,
) {
    assert_eq!(left.num_rows(), right.num_rows(), "row count mismatch");
    assert_eq!(
        left.num_columns(),
        right.num_columns(),
        "column count mismatch"
    );
    for i in 0..left.num_columns() {
        let left_col = left.column(i);
        let right_col = right.column(i);
        assert_eq!(
            left_col.len(),
            right_col.len(),
            "column {} length mismatch",
            left.schema().field(i).name()
        );
        assert_eq!(
            left_col.to_data(),
            right_col.to_data(),
            "column {} data mismatch",
            left.schema().field(i).name()
        );
    }
}

/// Common test fixtures for edge cases.
pub mod fixtures {
    /// Malformed UTF-8 sequences that should be rejected or handled gracefully.
    pub const MALFORMED_UTF8: &[&[u8]] = &[
        &[0xFF],                   // Single invalid byte
        &[0xFE],                   // Another invalid byte
        &[0xC0, 0xAF],             // Overlong encoding
        &[0xE0, 0x80, 0x80],       // Overlong 3-byte
        &[0xF0, 0x80, 0x80, 0x80], // Overlong 4-byte
        &[0xED, 0xA0, 0x80],       // Surrogate half
        b"\xC3\xA9",               // Valid: é
        b"\xE2\x82\xAC",           // Valid: €
    ];

    /// Nested quote patterns that commonly appear in CSV-like formats.
    pub const NESTED_QUOTES: &[&str] = &[
        r#""field""#,                 // Simple quoted field
        r#""field with spaces""#,     // Quoted with spaces
        r#""field,""with""quotes""#,  // Nested quotes
        r#""field\nwith\nnewlines""#, // Newlines in quotes
        r#""field\twith\ttabs""#,     // Tabs in quotes
    ];

    /// Edge cases for empty and whitespace-only values.
    pub const EMPTY_VALUES: &[&str] = &[
        "",     // Empty string
        " ",    // Single space
        "  ",   // Multiple spaces
        "\t",   // Tab
        "\n",   // Newline
        "\r\n", // CRLF
    ];

    /// Very long column names that test buffer limits.
    pub fn long_column_name(len: usize) -> String {
        "x".repeat(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_arb_field_name_valid(s in arb_field_name()) {
            assert!(!s.is_empty());
            assert!(s.chars().next().unwrap().is_ascii_alphabetic() || s.starts_with('_'));
        }

        #[test]
        fn test_arb_record_parseable(record in arb_record()) {
            let text: String = record
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(" ");
            let mut sink = TableBuilder::new();
            KeyValueParser.parse_chunk(text.as_bytes(), &mut sink).unwrap();
            assert_eq!(sink.num_rows(), 1);
        }

        #[test]
        fn test_arb_records_parseable(records in arb_records(5)) {
            let mut sink = TableBuilder::new();
            KeyValueParser.parse_chunk(records.as_bytes(), &mut sink).unwrap();
            assert_eq!(sink.num_rows(), 5);
        }
    }

    #[test]
    fn test_fixtures_malformed_utf8() {
        for &bytes in fixtures::MALFORMED_UTF8 {
            // Should not panic, just error
            let result = std::str::from_utf8(bytes);
            // Some are valid, some aren't - just verify no panic
            let _ = result;
        }
    }

    #[test]
    fn test_parse_test_bytes() {
        let bytes = b"A=1 B=2\nC=3\n";
        let mut sink = parse_test_bytes(bytes);
        assert_eq!(sink.num_rows(), 2);
    }
}
