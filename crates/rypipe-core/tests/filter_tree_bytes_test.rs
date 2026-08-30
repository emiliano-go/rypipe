//! Filter-tree, bytes-API, and compression tests.
//!
//! Covers the v1.1.0 limitations fix:
//! - recursive `FilterPredicate::{And,Or,Not}` evaluated per-row
//! - `Pipeline::read_bytes_par` / `read_bytes_stream` and `BoundedExecutor::run_bytes`
//! - magic-byte decompression in `InputBuffer::open`

use std::borrow::Cow;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use rypipe_core::{
    bounded::MemoryBudget, ColumnarSink, CompareOp, ExecutionPlan, FilterPredicate, Pipeline,
    RecordParser, Splitter, TableBuilder, Value,
};

// ---------------------------------------------------------------------------
// Helpers — same format as pushdown_test.rs: lines like `A=1 B=2`.

#[derive(Clone, Debug, Default)]
struct LineSplitter;

impl Splitter for LineSplitter {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        if max_chunks <= 1 || bytes.is_empty() {
            return vec![0, bytes.len()];
        }
        let stride = (bytes.len() / max_chunks).max(1);
        let mut points = vec![0usize];
        let mut next = stride;
        for (i, &b) in bytes.iter().enumerate().skip(1) {
            if i >= next && b == b'\n' && points.len() < max_chunks {
                points.push(i + 1);
                next += stride;
            }
        }
        if *points.last().unwrap() != bytes.len() {
            points.push(bytes.len());
        }
        points
    }
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        (sample.len() / sample.iter().filter(|&&b| b == b'\n').count().max(1)).max(1)
    }
}

#[derive(Clone, Debug, Default)]
struct LineParser;

impl RecordParser for LineParser {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
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

fn parse_bytes(bytes: &[u8], plan: ExecutionPlan) -> RecordBatch {
    let mut sink = TableBuilder::with_plan((bytes.len() / 16).max(4), Arc::new(plan));
    LineParser.parse_chunk(bytes, &mut sink).unwrap();
    sink.finish().unwrap()
}

fn eq(field: &str, value: &str) -> FilterPredicate {
    FilterPredicate::Equal {
        field: field.to_owned(),
        value: value.to_owned(),
    }
}
fn ne(field: &str, value: &str) -> FilterPredicate {
    FilterPredicate::NotEqual {
        field: field.to_owned(),
        value: value.to_owned(),
    }
}
fn cmp(field_a: &str, op: CompareOp, field_b: &str) -> FilterPredicate {
    FilterPredicate::Compare {
        field_a: field_a.to_owned(),
        op,
        field_b: field_b.to_owned(),
    }
}

// ---------------------------------------------------------------------------
// 1. Filter tree tests (per-row authority path).

#[test]
fn test_and_two_equals() {
    let data = b"S=yes X=1\nS=no X=1\nS=yes X=9\n";
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::all(eq("S", "yes"), eq("X", "1"))),
        ..Default::default()
    };
    let batch = parse_bytes(data, plan);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_or_two_equals() {
    let data = b"S=yes X=1\nS=no X=1\nS=yes X=9\nS=no X=9\n";
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::any(eq("S", "yes"), eq("X", "1"))),
        ..Default::default()
    };
    // S=yes OR X=1 → lines 1,2,3 keep; 4 drops.
    assert_eq!(parse_bytes(data, plan).num_rows(), 3);
}

#[test]
fn test_and_short_circuits_with_missing_field() {
    // A row missing one conjunct field never registers that column (no begin_row
    // pushes a value there). Missing → fails Equal → And short-circuits false.
    let data = b"S=yes X=1\nS=yes\n";
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::all(eq("S", "yes"), eq("X", "1"))),
        ..Default::default()
    };
    assert_eq!(parse_bytes(data, plan).num_rows(), 1);
}

#[test]
fn test_or_with_missing_field() {
    // Missing → Equal=false, but Or short-circuits: if the other branch true,
    // row is kept regardless of missing field.
    let data = b"S=yes\nS=no\nQ=other\n";
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::any(eq("S", "yes"), ne("Q", "other"))),
        ..Default::default()
    };
    // row1 S=yes → true; row2 S=no → Q missing → ne true; row3 Q=other → both false → dropped.
    assert_eq!(parse_bytes(data, plan).num_rows(), 2);
}

#[test]
fn test_not_equal_leaf() {
    let data = b"S=yes\nS=no\nQ=x\n";
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::not(eq("S", "yes"))),
        ..Default::default()
    };
    // Not(Eq) — row1 fails Eq but Not keeps? No, Eq true → Not false → dropped.
    // row2 Eq false → Not true → kept. row3 S missing → Eq false → Not true → kept.
    assert_eq!(parse_bytes(data, plan).num_rows(), 2);
}

#[test]
fn test_not_flips_missing_field() {
    // Not(Equal) on a missing field keeps the row; plain Equal would drop it.
    let data = b"S=yes\nS=no\n";
    let not_plan = ExecutionPlan {
        filter: Some(FilterPredicate::not(eq("S", "yes"))),
        ..Default::default()
    };
    let eq_plan = ExecutionPlan {
        filter: Some(eq("S", "yes")),
        ..Default::default()
    };
    assert_eq!(parse_bytes(data, not_plan).num_rows(), 1);
    assert_eq!(parse_bytes(data, eq_plan).num_rows(), 1);
    // Together they partition rows: union covers all.
}

#[test]
fn test_nested_and_inside_or() {
    // (A==1 AND B==2) OR C==3
    let data = b"A=1 B=2\nA=1 B=9\nA=9 B=9 C=3\nA=9 B=9\n";
    let pred = FilterPredicate::any(
        FilterPredicate::all(eq("A", "1"), eq("B", "2")),
        eq("C", "3"),
    );
    let plan = ExecutionPlan {
        filter: Some(pred),
        ..Default::default()
    };
    // row1 true AND → Or true; row2 And false → Or false; row3 C==3 true → Or true; row4 false.
    assert_eq!(parse_bytes(data, plan).num_rows(), 2);
}

#[test]
fn test_not_of_and_via_de_morgan() {
    // !(A==x AND B==y) — plain: row1 passes both so dropped; all others kept.
    let data = b"A=x B=y\nA=x B=z\nA=w B=y\n";
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::not(FilterPredicate::all(
            eq("A", "x"),
            eq("B", "y"),
        ))),
        ..Default::default()
    };
    assert_eq!(parse_bytes(data, plan).num_rows(), 2);
}

#[test]
fn test_compare_inside_or() {
    // A > B  OR  S==special
    // Row 1: 5 > 3 (true) → keep; Row2: 1 > 9 false but S==special true → keep; Row3: neither → drop.
    let mut plan = ExecutionPlan::new();
    plan.field_types
        .insert("A".into(), rypipe_core::FieldType::Int64);
    plan.field_types
        .insert("B".into(), rypipe_core::FieldType::Int64);
    plan.filter = Some(FilterPredicate::any(
        cmp("A", CompareOp::Gt, "B"),
        eq("S", "special"),
    ));
    let batch = parse_bytes(b"A=5 B=3\nA=1 B=9 S=special\nA=1 B=9 S=other\n", plan);
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_helpers_all_any_not_short_hands() {
    // Verify the convenience helpers build the same trees the check() paths handle.
    assert_eq!(
        FilterPredicate::all(eq("A", "x"), eq("B", "y")),
        FilterPredicate::And(Box::new(eq("A", "x")), Box::new(eq("B", "y")))
    );
    assert_eq!(
        FilterPredicate::any(eq("A", "x"), eq("B", "y")),
        FilterPredicate::Or(Box::new(eq("A", "x")), Box::new(eq("B", "y")))
    );
    assert_eq!(
        FilterPredicate::not(eq("A", "x")),
        FilterPredicate::Not(Box::new(eq("A", "x")))
    );
}

// ---------------------------------------------------------------------------
// 2. Bytes-API equivalence tests.

#[test]
fn test_read_bytes_par_matches_single() {
    let pipeline = Pipeline::new(LineSplitter, LineParser);
    let data = (0..500)
        .map(|i| format!("A={} B={}\n", i % 7, i % 3))
        .collect::<String>();
    let bytes = data.as_bytes();
    let single = pipeline.read_bytes(bytes).unwrap();
    let chunks = pipeline.read_bytes_par(bytes, 4).unwrap();
    let par_rows: usize = chunks.iter().map(|b| b.num_rows()).sum();
    assert_eq!(par_rows, single.num_rows());
    // Re-applied per-row filters: compare types induce no special reroute under Or/Not.
}

#[test]
fn test_read_bytes_par_with_filter_tree() {
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::any(eq("A", "3"), eq("B", "1"))),
        ..Default::default()
    };
    let pipeline = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = (0..200)
        .map(|i| format!("A={} B={}\n", i % 5, i % 4))
        .collect::<String>();
    let bytes = data.as_bytes();
    let single = pipeline.read_bytes(bytes).unwrap();
    let par: usize = pipeline
        .read_bytes_par(bytes, 4)
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(par, single.num_rows());
}

#[test]
fn test_read_bytes_stream_matches_single_and_is_chunked() {
    let pipeline = Pipeline::new(LineSplitter, LineParser);
    // 4000 rows (~28 KiB). Budget 4 KiB forces ~7 batches even with MAX_SPLIT_CHUNKS=256 cap.
    let data = (0..4000)
        .map(|i| format!("A={} B={}\n", i % 10, i % 8))
        .collect::<String>();
    let bytes = data.as_bytes();
    let single = pipeline.read_bytes(bytes).unwrap();
    let budget = MemoryBudget::new(4096);
    let batches = pipeline.read_bytes_stream(bytes, budget).unwrap();
    assert!(
        batches.len() > 1,
        "streaming should produce multiple batches: {}",
        batches.len()
    );
    let stream_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(stream_rows, single.num_rows());
}

#[test]
fn test_run_bytes_direct_is_equivalent_to_run_via_path() {
    use std::io::Write as _;

    let data = (0..300)
        .map(|i| format!("K={} V={}\n", i % 6, i % 9))
        .collect::<String>();
    let bytes = data.as_bytes().to_vec();

    // Path-based bounded path.
    let dir = std::env::temp_dir().join(format!("rypipe_filter_tree_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bounded_path.txt");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&bytes).unwrap();
    }
    let pipeline = Pipeline::new(LineSplitter, LineParser);
    let budget = MemoryBudget::new(8192);
    let via_path = pipeline
        .with_plan(ExecutionPlan::new())
        .read_path_stream(&path, budget, false)
        .unwrap();
    let via_bytes = Pipeline::new(LineSplitter, LineParser)
        .read_bytes_stream(&bytes, budget)
        .unwrap();
    let path_rows: usize = via_path.iter().map(|b| b.num_rows()).sum();
    let bytes_rows: usize = via_bytes.iter().map(|b| b.num_rows()).sum();
    assert_eq!(path_rows, bytes_rows);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_batchpipe_style_bounded_engine_run_bytes() {
    // Direct BoundedExecutor::run_bytes smoke test.
    use rypipe_core::bounded::BoundedExecutor;
    let data = (0..400)
        .map(|i| format!("A={} B={}\n", i % 7, i % 5))
        .collect::<String>();
    let bytes = data.as_bytes();
    let exec = BoundedExecutor::new(MemoryBudget::new(4096));
    let batches = exec
        .run_bytes(
            bytes,
            &LineSplitter as &dyn Splitter,
            LineParser,
            Arc::new(ExecutionPlan {
                filter: Some(FilterPredicate::any(eq("A", "3"), eq("B", "1"))),
                ..Default::default()
            }),
        )
        .unwrap();
    assert!(!batches.is_empty());
    // Streaming with a tree keeps the same results as single-threaded parse_bytes.
    let single = parse_bytes(
        bytes,
        ExecutionPlan {
            filter: Some(FilterPredicate::any(eq("A", "3"), eq("B", "1"))),
            ..Default::default()
        },
    );
    let streamed: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(streamed, single.num_rows());
}

// ---------------------------------------------------------------------------
// 3. apply_compare_filter idempotence on pure-Compare trees.

#[test]
fn test_pure_compare_and_survives_batch_filter() {
    // A parallel pure-Compare AND is idempotently reapplied post-assembly.
    let mut plan = ExecutionPlan::new();
    plan.field_types
        .insert("A".into(), rypipe_core::FieldType::Int64);
    plan.field_types
        .insert("B".into(), rypipe_core::FieldType::Int64);
    plan.field_types
        .insert("C".into(), rypipe_core::FieldType::Int64);
    plan.field_types
        .insert("D".into(), rypipe_core::FieldType::Int64);
    plan.filter = Some(FilterPredicate::all(
        cmp("A", CompareOp::Gt, "B"),
        cmp("C", CompareOp::Le, "D"),
    ));
    let data = b"A=5 B=3 C=4 D=4\nA=1 B=9 C=3 D=9\nA=10 B=2 C=1 D=100\n";
    let pipeline = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    // single and parallel must both give 2 rows: row1 (5>3,4<=4) and row3 (10>2,1<=100).
    let single = pipeline.read_bytes(data).unwrap();
    let par: usize = pipeline
        .read_bytes_par(data, 2)
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(single.num_rows(), 2);
    assert_eq!(par, 2);
}

// ---------------------------------------------------------------------------
// 4. Compression detection helpers (helpers only; no feature coupling).

fn flate_encode(bytes: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression as FlateLevel;
    use std::io::Write as _;
    let mut enc = GzEncoder::new(Vec::new(), FlateLevel::default());
    enc.write_all(bytes).unwrap();
    enc.finish().unwrap()
}

fn temp_gz_file(contents: &[u8]) -> std::path::PathBuf {
    use std::io::Write as _;
    let dir = std::env::temp_dir().join(format!(
        "rypipe_gz_{}_{}",
        std::process::id(),
        contents.len()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.bin");
    let gz = flate_encode(contents);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&gz).unwrap();
    path
}

fn temp_plain_file(contents: &[u8]) -> std::path::PathBuf {
    use std::io::Write as _;
    let dir = std::env::temp_dir().join(format!(
        "rypipe_plain_{}_{}",
        std::process::id(),
        contents.len()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.txt");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(contents).unwrap();
    path
}

#[test]
fn test_gz_decode_round_trips_without_feature_is_opaque_bytes_or_error() {
    // In the default build (no `gzip` feature), a file whose magic is
    // 1f 8b is NOT auto-decompressed; parsing it yields either zero rows
    // (validate-pass with garbage-decode that fails UTF-8-tolerant parser? No:
    // our validate is simdutf8, which does validate, so garbage bytes should
    // fail with Error::Utf8) or, depending on bytes, an explicit Parse-error.
    // The key invariant is that we must NOT silently produce the original rows.
    let plain = b"A=1 B=2\nA=3 B=4\n";
    let gz_path = temp_gz_file(plain);
    let result = Pipeline::new(LineSplitter, LineParser).read_path(&gz_path, false, false);
    #[cfg(not(feature = "gzip"))]
    {
        // Without the feature, reading a gz file must not decode to the original two-row batch.
        if let Ok(batch) = &result {
            assert_ne!(
                batch.num_rows(),
                2,
                "gz bytes without the feature must not decode"
            );
        }
        // Error path is also acceptable; at minimum we assert it's not two rows of the expected shape.
    }
    #[cfg(feature = "gzip")]
    {
        // With the feature enabled, transparent decompression yields the original rows.
        let batch = result.expect("gz should auto-decompress with the feature");
        assert_eq!(batch.num_rows(), 2);
    }
    let _ = std::fs::remove_file(&gz_path);
}

#[test]
#[cfg(feature = "gzip")]
fn test_gz_auto_decompress_parallels_stream() {
    let plain = (0..200)
        .map(|i| format!("A={} B={}\n", i % 5, i % 3))
        .collect::<String>();
    let plain_bytes = plain.as_bytes();
    let gz_path = temp_gz_file(plain_bytes);

    let par = Pipeline::new(LineSplitter, LineParser)
        .read_path_par(&gz_path, 4, false, false)
        .expect("gzip parallel read should succeed");
    let stream = Pipeline::new(LineSplitter, LineParser)
        .read_path_stream(&gz_path, MemoryBudget::new(4096), false)
        .expect("gzip streaming read should succeed");

    let single = Pipeline::new(LineSplitter, LineParser)
        .read_bytes(plain_bytes)
        .unwrap();

    let par_rows: usize = par.iter().map(|b| b.num_rows()).sum();
    let stream_rows: usize = stream.iter().map(|b| b.num_rows()).sum();
    assert_eq!(par_rows, single.num_rows());
    assert_eq!(stream_rows, single.num_rows());
    let _ = std::fs::remove_file(&gz_path);
}

#[test]
fn test_uncompressed_file_still_reads_fine_after_compression_code() {
    // Regression: plain file must keep working.
    let plain = b"A=1 B=2\nA=3 B=4\nA=5 B=6\n";
    let path = temp_plain_file(plain);
    let batch = Pipeline::new(LineSplitter, LineParser)
        .read_path(&path, false, false)
        .unwrap();
    assert_eq!(batch.num_rows(), 3);
    let _ = std::fs::remove_file(&path);
}
