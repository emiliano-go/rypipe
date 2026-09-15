use std::borrow::Cow;

use arrow::array::AsArray;
use proptest::prelude::*;
use rypipe_core::decoder::{in_skip_region, plan_chunk_count, SkipRegionFinder, SplitMode};
use rypipe_core::{
    find_next_record_boundary, is_continued_newline, ColumnarSink, MemoryBudget, Pipeline,
    RecordParser, Splitter, Value,
};

const COMMENTS: &[&[u8]] = &[b"#", b"!"];

#[derive(Clone, Debug, Default)]
struct StatefulSplitter;

impl Splitter for StatefulSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(bytes, from, Some(b'\\'), COMMENTS, false)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        (sample.len() / sample.iter().filter(|&&b| b == b'\n').count().max(1)).max(1)
    }

    fn continuation_char(&self) -> Option<u8> {
        Some(b'\\')
    }

    fn comment_prefixes(&self) -> &[&[u8]] {
        COMMENTS
    }
}

#[derive(Clone, Debug, Default)]
struct StatefulParser;

impl RecordParser for StatefulParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text = simdutf8::basic::from_utf8(bytes)?;
        let mut record = String::new();
        for physical in text.split_inclusive('\n') {
            let line = physical.trim_end_matches(['\r', '\n']);
            if record.is_empty()
                && COMMENTS
                    .iter()
                    .any(|prefix| line.as_bytes().starts_with(prefix))
            {
                continue;
            }
            let continued = line.bytes().rev().take_while(|&byte| byte == b'\\').count() % 2 == 1;
            record.push_str(if continued {
                &line[..line.len() - 1]
            } else {
                line
            });
            if continued {
                continue;
            }
            if let Some(value) = record.strip_prefix("id=") {
                sink.begin_row();
                sink.put_field("id", Value::Str(Cow::Owned(value.to_owned())));
                sink.end_row();
            }
            record.clear();
        }
        if let Some(value) = record.strip_prefix("id=") {
            sink.begin_row();
            sink.put_field("id", Value::Str(Cow::Owned(value.to_owned())));
            sink.end_row();
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct BlankSplitter;

impl Splitter for BlankSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(bytes, from, None, &[], true)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        sample.len().max(1)
    }
}

fn ids(batches: impl IntoIterator<Item = arrow::record_batch::RecordBatch>) -> Vec<String> {
    batches
        .into_iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_string::<i32>()
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn continuation_helper_handles_bounds_and_escaping() {
    assert!(!is_continued_newline(b"", 0));
    assert!(!is_continued_newline(b"\n", 0));
    assert!(is_continued_newline(b"\\\n", 1));
    assert!(!is_continued_newline(b"\\\\\n", 2));
    assert!(is_continued_newline(b"a\\\nb", 2));
    assert!(is_continued_newline(b"a\\\r\nb", 3));
    assert!(!is_continued_newline(b"a\\\\\r\nb", 4));
    assert!(!is_continued_newline(b"\\", 1));
    assert!(!is_continued_newline(b"x", 2));
}

#[test]
fn boundary_helper_covers_line_comment_continuation_crlf_and_blank_modes() {
    let line = |bytes, from| find_next_record_boundary(bytes, from, Some(b'\\'), COMMENTS, false);
    assert_eq!(line(b"", 0), None);
    assert_eq!(line(b"no newline", 0), None);
    assert_eq!(line(b"x\n", 0), Some(2));
    assert_eq!(line(b"x\\\ny\n", 0), Some(5));
    assert_eq!(line(b"x\\\r\ny\r\n", 0), Some(7));
    assert_eq!(line(b"x\\\\\ny\n", 0), Some(4));
    assert_eq!(line(b"# skip\nx\n", 0), Some(9));
    assert_eq!(line(b"! skip\nx\n", 0), Some(9));
    assert_eq!(line(b"x# kept\ny\n", 0), Some(8));
    assert_eq!(line(b"# skip\nx\n", 3), Some(9));
    assert_eq!(line(b"# continued\\\nx\n", 0), Some(15));
    assert_eq!(line(b"x\n# comment\ny\n", 2), Some(14));
    assert_eq!(line(b"x\n", 9), None);
    assert_eq!(
        find_next_record_boundary(b"x^\r\ny\r\n", 0, Some(b'^'), &[], false),
        Some(7)
    );
    assert_eq!(
        find_next_record_boundary(b"a\n\nb", 2, None, &[], true),
        Some(3)
    );
    assert_eq!(
        find_next_record_boundary(b"a\r\n\r\nb", 3, None, &[], true),
        Some(5)
    );

    assert_eq!(
        find_next_record_boundary(b"a\n\nb", 0, None, &[], true),
        Some(3)
    );
    assert_eq!(
        find_next_record_boundary(b"a\r\n\r\nb", 0, None, &[], true),
        Some(5)
    );
    assert_eq!(
        find_next_record_boundary(b"\n\n", 0, None, &[], true),
        Some(2)
    );
    assert_eq!(
        find_next_record_boundary(b"a\n\nb", 0, None, &[], false),
        Some(2)
    );
}

#[test]
fn boundary_scans_one_megabyte_line_and_eof_without_boundary() {
    let mut bytes = vec![b'x'; 1024 * 1024];
    bytes.extend_from_slice(b"\nid=tail");
    assert_eq!(
        find_next_record_boundary(&bytes, 0, None, &[], false),
        Some(1024 * 1024 + 1)
    );
    assert_eq!(
        find_next_record_boundary(b"# only\n! comments\n", 0, None, COMMENTS, false),
        None
    );
}

struct BlockComment;

impl SkipRegionFinder for BlockComment {
    fn openers(&self) -> &[&'static [u8]] {
        &[b"/*"]
    }

    fn closer_for(&self, _: &[u8]) -> &'static [u8] {
        b"*/"
    }
}

static BLOCK_COMMENT: BlockComment = BlockComment;

#[derive(Clone, Debug, Default)]
struct BlockSplitter;

impl Splitter for BlockSplitter {
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
        find_next_record_boundary(bytes, from, None, &[], false)
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        sample.len().max(1)
    }

    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        Some(&BLOCK_COMMENT)
    }
}

#[test]
fn skip_region_and_chunk_count_public_helpers_are_bounds_safe() {
    let finder = BlockComment;
    assert!(in_skip_region(b"a/* hidden", 10, &finder));
    assert!(!in_skip_region(b"a/* hidden", 99, &finder));
    assert_eq!(
        plan_chunk_count(usize::MAX, usize::MAX, SplitMode::Parallel),
        1024
    );
    assert_eq!(
        plan_chunk_count(usize::MAX, usize::MAX, SplitMode::Streaming),
        1024
    );
}

#[test]
fn declarative_split_points_respect_blank_lines_and_skip_regions() {
    let blank = BlankSplitter.find_split_points(b"a\nb\n\nc\nd\n\ne\n", 4);
    assert_eq!(blank.first(), Some(&0));
    assert_eq!(blank.last(), Some(&12));
    assert!(blank
        .iter()
        .copied()
        .all(|point| point == 0 || point == 5 || point == 10 || point == 12));

    let bytes = b"head\n/* first\nsecond\nthird */\ntail\n";
    let points = BlockSplitter.find_split_points(bytes, 4);
    assert!(points
        .iter()
        .copied()
        .all(|point| !in_skip_region(bytes, point, &BLOCK_COMMENT)));
}

#[test]
fn declarative_splitter_preserves_stateful_records_across_modes() {
    let mut data = String::from("# header\nid=alpha\nid=beta\\\n-tail\n! ignored\n");
    for i in 0..20_000 {
        data.push_str(&format!("id={i:05}\\\n-tail\n"));
    }
    data.push_str("id=omega");
    let mut expected = vec!["alpha".to_owned(), "beta-tail".to_owned()];
    expected.extend((0..20_000).map(|i| format!("{i:05}-tail")));
    expected.push("omega".to_owned());
    let pipeline = Pipeline::new(StatefulSplitter, StatefulParser);
    assert_eq!(
        ids(vec![pipeline.read_bytes(data.as_bytes()).unwrap()]),
        expected
    );
    assert_eq!(
        ids(pipeline.read_bytes_par(data.as_bytes(), 4).unwrap()),
        expected
    );
    assert_eq!(
        ids(pipeline
            .read_bytes_stream(data.as_bytes(), MemoryBudget::new(64 * 1024))
            .unwrap()),
        expected
    );
}

#[test]
fn large_continued_crlf_record_matches_across_modes() {
    let value = "x".repeat(1024 * 1024);
    let data = format!("# header\r\nid={value}\\\r\n-tail\r\nid=end");
    let expected = vec![format!("{value}-tail"), "end".to_owned()];
    let pipeline = Pipeline::new(StatefulSplitter, StatefulParser);
    assert_eq!(
        ids([pipeline.read_bytes(data.as_bytes()).unwrap()]),
        expected
    );
    assert_eq!(
        ids(pipeline.read_bytes_par(data.as_bytes(), 4).unwrap()),
        expected
    );
    assert_eq!(
        ids(pipeline
            .read_bytes_stream(data.as_bytes(), MemoryBudget::new(64 * 1024))
            .unwrap()),
        expected
    );
}

proptest! {
    #[test]
    fn boundary_never_returns_an_invalid_line_position(
        bytes in prop::collection::vec(prop_oneof![Just(b'#'), Just(b'!'), Just(b'\\'), Just(b'\r'), Just(b'\n'), b'a'..=b'z'], 0..256),
        from in 0usize..300,
    ) {
        let found = find_next_record_boundary(&bytes, from, Some(b'\\'), COMMENTS, false);
        if let Some(pos) = found {
            prop_assert!(pos > from && pos <= bytes.len());
            prop_assert_eq!(bytes[pos - 1], b'\n');
            prop_assert!(!is_continued_newline(&bytes, pos - 1));
            let start = bytes[..pos - 1].iter().rposition(|&byte| byte == b'\n').map_or(0, |at| at + 1);
            prop_assert!(!COMMENTS.iter().any(|prefix| bytes[start..pos - 1].starts_with(prefix)));
        }
    }

    #[test]
    fn split_points_are_sorted_and_stateful_safe(
        records in prop::collection::vec("[a-z]{1,12}", 1..80),
    ) {
        let bytes = records.iter().enumerate().map(|(i, id)| {
            if i % 3 == 0 { format!("id={id}\\\n-x\n") } else { format!("id={id}\n") }
        }).collect::<String>();
        let splitter = StatefulSplitter;
        let points = splitter.find_split_points(bytes.as_bytes(), 8);
        prop_assert_eq!(points.first(), Some(&0));
        prop_assert_eq!(points.last(), Some(&bytes.len()));
        prop_assert!(points.windows(2).all(|pair| pair[0] < pair[1]));
        for point in points.iter().copied().filter(|&point| point != 0 && point != bytes.len()) {
            prop_assert_eq!(bytes.as_bytes()[point - 1], b'\n');
            prop_assert!(!is_continued_newline(bytes.as_bytes(), point - 1));
        }
    }
}
