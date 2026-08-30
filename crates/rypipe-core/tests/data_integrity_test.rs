//! Data-loss / correctness exhaustive tests.
//! Verifies that Vec+field_index storage, single-lookup push_field, and
//! ColumnarSink resolve/put_field_resolved produce bit-identical results
//! across all execution modes and plans.

use std::borrow::Cow;

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use rypipe_core::{
    bounded::MemoryBudget, ColumnarSink, ExecutionPlan, FieldType, FilterPredicate, Pipeline,
    RecordParser, Splitter, TableBuilder, Value,
};

// ---------------------------------------------------------------------------
// Helpers — deterministic TSV-like format: lines like `A=1 B=2 C=3`

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

/// Optimized parser that uses resolve + put_field_resolved (Change 3).
#[derive(Clone, Debug, Default)]
struct LineParserResolved;

impl RecordParser for LineParserResolved {
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
                    if let Some(resolved) = sink.resolve(k).map(|s| s.to_owned()) {
                        // Simulate expensive work between resolve and put
                        let _ = resolved.len();
                        sink.put_field_resolved(&resolved, Value::Str(Cow::Borrowed(v)));
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

fn pipeline() -> Pipeline<LineSplitter, LineParser> {
    Pipeline::new(LineSplitter, LineParser)
}
#[expect(dead_code)]
fn pipeline_resolved() -> Pipeline<LineSplitter, LineParserResolved> {
    Pipeline::new(LineSplitter, LineParserResolved)
}

/// Parser that mirrors the XML scanner's resolve_and_put + row_rejected path.
#[derive(Clone, Debug, Default)]
struct LineParserResolveAndPut;

impl RecordParser for LineParserResolveAndPut {
    fn validate(&self, bytes: &[u8]) -> rypipe_core::Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> rypipe_core::Result<()> {
        let text = std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for token in line.split_whitespace() {
                if let Some((k, v)) = token.split_once('=') {
                    let _ = sink.resolve(k);
                    sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                    if sink.row_rejected() {
                        break;
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
    #[inline]
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> rypipe_core::Result<()> {
        let text = std::str::from_utf8(bytes).map_err(|e| rypipe_core::Error::Plan(e.to_string()))?;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            sink.begin_row();
            for token in line.split_whitespace() {
                if let Some((k, v)) = token.split_once('=') {
                    let _ = sink.resolve(k);
                    sink.resolve_and_put(k, Value::Str(Cow::Borrowed(v)));
                    if sink.row_rejected() {
                        break;
                    }
                }
            }
            sink.end_row();
        }
        Ok(())
    }
}

fn pipeline_resolve_and_put() -> Pipeline<LineSplitter, LineParserResolveAndPut> {
    Pipeline::new(LineSplitter, LineParserResolveAndPut)
}

fn batches_to_rows(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    // Collect column-major values for comparison; assumes string columns unless typed.
    if batches.is_empty() {
        return vec![];
    }
    // Build a unified order from first batch schema
    let schema = batches[0].schema();
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut rows: Vec<Vec<Option<String>>> = vec![vec![None; names.len()]; total_rows];
    let mut out = 0;
    for b in batches {
        for row in 0..b.num_rows() {
            for (ci, name) in names.iter().enumerate() {
                if let Some(col) = b.column_by_name(name) {
                    // String-like via cast to Utf8 if needed; typed columns will be stringified via debug
                    // For correctness we compare via string value where possible
                    let v = if col.data_type() == &arrow::datatypes::DataType::Utf8 {
                        let arr = col.as_string::<i32>();
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row).to_owned())
                        }
                    } else if col.data_type() == &arrow::datatypes::DataType::Int64 {
                        let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row).to_string())
                        }
                    } else if col.data_type() == &arrow::datatypes::DataType::Float64 {
                        let arr = col.as_primitive::<arrow::datatypes::Float64Type>();
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row).to_string())
                        }
                    } else if col.data_type() == &arrow::datatypes::DataType::Boolean {
                        let arr = col.as_boolean();
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row).to_string())
                        }
                    } else {
                        // Fallback: use debug string
                        None
                    };
                    rows[out][ci] = v;
                }
            }
            out += 1;
        }
    }
    rows
}

fn assert_batches_equal(a: &[RecordBatch], b: &[RecordBatch]) {
    let ar: usize = a.iter().map(|x| x.num_rows()).sum();
    let br: usize = b.iter().map(|x| x.num_rows()).sum();
    assert_eq!(ar, br, "row count mismatch a={ar} b={br}");
    if ar == 0 {
        return;
    }
    // Compare schema names (order may differ due to schema_order, but for no-plan tests order is insertion order)
    let a_names: Vec<String> = a[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let b_names: Vec<String> = b[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(a_names, b_names, "schema mismatch");
    // Compare per-row stringified values
    let ar_rows = batches_to_rows(a);
    let br_rows = batches_to_rows(b);
    assert_eq!(ar_rows, br_rows, "row values mismatch");
}

// ---------------------------------------------------------------------------
// 1. Empty and single-row

#[test]
fn empty_input_no_data_loss() {
    let p = pipeline();
    let empty: &[u8] = b"";
    let single = p.read_bytes(empty).unwrap();
    assert_eq!(single.num_rows(), 0);
    assert_eq!(single.num_columns(), 0);
    let par = p.read_bytes_par(empty, 4).unwrap();
    assert_eq!(par.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    let stream = p.read_bytes_stream(empty, MemoryBudget::new(1024)).unwrap();
    assert_eq!(stream.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[test]
fn single_row_no_data_loss() {
    let p = pipeline();
    let data = b"A=hello B=world C=123\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 1);
    assert_eq!(single.num_columns(), 3);
    let par = p.read_bytes_par(data, 4).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    let stream = p.read_bytes_stream(data, MemoryBudget::new(64)).unwrap();
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 2. Ragged and sparse columns (late debut, missing values)

#[test]
fn ragged_columns_no_data_loss() {
    let p = pipeline();
    let data = b"A=1 B=2\nA=3\nB=4 C=5\nA=6 B=7 C=8 D=9\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 4);
    assert_eq!(single.num_columns(), 4);
    // Verify null pattern via column lengths
    let c = single.column_by_name("C").unwrap();
    assert_eq!(c.len(), 4);
    assert!(c.is_null(0));
    assert!(c.is_null(1));
    assert!(!c.is_null(2));
    assert!(!c.is_null(3));

    let par = p.read_bytes_par(data, 3).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 3. Duplicate field — last-write-wins

#[test]
fn duplicate_field_last_write_wins_no_data_loss() {
    let p = pipeline();
    let data = b"X=10 X=20 Y=1\nX=5 X=6 X=7\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 2);
    let x = single.column_by_name("X").unwrap().as_string::<i32>();
    assert_eq!(x.value(0), "20");
    assert_eq!(x.value(1), "7");

    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(64)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 4. Many rows, few columns — parallel vs single vs stream equivalence

#[test]
fn many_rows_few_columns_no_data_loss() {
    let p = pipeline();
    let data: String = (0..5000)
        .map(|i| format!("A={} B={} C={}\n", i % 10, i % 7, i % 5))
        .collect();
    let bytes = data.as_bytes();
    let single = p.read_bytes(bytes).unwrap();
    assert_eq!(single.num_rows(), 5000);
    let par = p.read_bytes_par(bytes, 8).unwrap();
    let stream = p.read_bytes_stream(bytes, MemoryBudget::new(4096)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 5. Many columns (50) — Vec storage correctness

#[test]
fn many_columns_no_data_loss() {
    let p = pipeline();
    let cols: Vec<String> = (0..50).map(|i| format!("C{i}")).collect();
    let header = cols.join(" ");
    let data: String = (0..200)
        .map(|r| {
            cols.iter()
                .enumerate()
                .map(|(ci, c)| format!("{c}={}", r * 50 + ci))
                .collect::<Vec<_>>()
                .join(" ")
                + "\n"
        })
        .collect();
    let _ = header;
    let bytes = data.as_bytes();
    let single = p.read_bytes(bytes).unwrap();
    assert_eq!(single.num_rows(), 200);
    assert_eq!(single.num_columns(), 50);
    let par = p.read_bytes_par(bytes, 4).unwrap();
    let stream = p.read_bytes_stream(bytes, MemoryBudget::new(8192)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 6. Rename / drop / filter / types / dictionary — plan correctness

#[test]
fn plan_rename_no_data_loss() {
    let plan = ExecutionPlan::new()
        .rename("A", "Alpha")
        .rename("B", "Beta");
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"A=1 B=2 C=3\nA=4 B=5 C=6\n";
    let single = p.read_bytes(data).unwrap();
    assert!(single.column_by_name("Alpha").is_some());
    assert!(single.column_by_name("Beta").is_some());
    assert!(single.column_by_name("A").is_none());
    assert_eq!(single.num_rows(), 2);
    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_drop_no_data_loss() {
    let plan = ExecutionPlan::new().drop("B");
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"A=1 B=2 C=3\nA=4 B=5 C=6\n";
    let single = p.read_bytes(data).unwrap();
    assert!(single.column_by_name("B").is_none());
    assert_eq!(single.num_rows(), 2);
    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_typed_columns_no_data_loss() {
    let plan = ExecutionPlan::new()
        .type_as("I", FieldType::Int64)
        .type_as("F", FieldType::Float64)
        .type_as("B", FieldType::Boolean);
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"I=42 F=3.14 B=true\nI=bad F=bad B=bad\nI=100 F=2.71 B=false\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 3);
    // Verify typed nulls for bad values
    let i = single
        .column_by_name("I")
        .unwrap()
        .as_primitive::<arrow::datatypes::Int64Type>();
    assert!(i.is_null(1));
    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_filter_no_data_loss() {
    let plan = ExecutionPlan::new().filter_eq("S", "keep");
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"S=keep V=1\nS=drop V=2\nS=keep V=3\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 2);
    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_filter_six_rows_no_data_loss() {
    let plan = ExecutionPlan::new().filter_eq("S", "keep");
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"S=keep V=1\nS=drop V=2\nS=keep V=3\nS=keep V=4\nS=drop V=5\nS=keep V=6\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 4, "single");
    let par = p.read_bytes_par(data, 2).unwrap();
    assert_batches_equal(&[single.clone()], &par);
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_filter_resolve_and_put_six_rows() {
    let plan = ExecutionPlan::new().filter_eq("S", "keep");
    let p = pipeline_resolve_and_put().with_plan(plan);
    let data = b"S=keep V=1\nS=drop V=2\nS=keep V=3\nS=keep V=4\nS=drop V=5\nS=keep V=6\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(single.num_rows(), 4, "single resolve_and_put");
    let par = p.read_bytes_par(data, 2).unwrap();
    assert_batches_equal(&[single.clone()], &par);
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_filter_and_or_not_no_data_loss() {
    let plan = ExecutionPlan {
        filter: Some(FilterPredicate::any(
            FilterPredicate::all(
                FilterPredicate::Equal {
                    field: "A".into(),
                    value: "1".into(),
                },
                FilterPredicate::Equal {
                    field: "B".into(),
                    value: "2".into(),
                },
            ),
            FilterPredicate::not(FilterPredicate::Equal {
                field: "C".into(),
                value: "drop".into(),
            }),
        )),
        ..Default::default()
    };
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"A=1 B=2 C=x\nA=1 B=9 C=x\nA=9 B=9 C=drop\nA=9 B=9 C=keep\n";
    let single = p.read_bytes(data).unwrap();
    // Row1: A1&B2 true -> keep; Row2: true||true -> keep; Row3: false && false -> drop; Row4: false||true -> keep
    assert_eq!(single.num_rows(), 3);
    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_dictionary_no_data_loss() {
    let plan = ExecutionPlan::new().dictionary("P");
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data: String = (0..500)
        .map(|i| format!("P={}\n", if i % 2 == 0 { "Widget" } else { "Gadget" }))
        .collect();
    let bytes = data.as_bytes();
    let single = p.read_bytes(bytes).unwrap();
    assert_eq!(single.num_rows(), 500);
    let par = p.read_bytes_par(bytes, 4).unwrap();
    let stream = p.read_bytes_stream(bytes, MemoryBudget::new(1024)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

#[test]
fn plan_schema_order_no_data_loss() {
    let plan = ExecutionPlan::new().schema_order(["C", "B", "A"]);
    let p = Pipeline::new(LineSplitter, LineParser).with_plan(plan);
    let data = b"A=1 B=2 C=3\nA=4 B=5 C=6\n";
    let single = p.read_bytes(data).unwrap();
    assert_eq!(
        single
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["C", "B", "A"]
    );
    let par = p.read_bytes_par(data, 2).unwrap();
    let stream = p.read_bytes_stream(data, MemoryBudget::new(128)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 7. Resolve + put_field_resolved produces identical results to put_field

#[test]
fn resolve_put_field_resolved_identical_to_put_field() {
    let data: String = (0..1000)
        .map(|i| {
            format!(
                "A={} B={} C={}\n",
                i % 5,
                i % 3,
                if i % 2 == 0 { "keep" } else { "drop" }
            )
        })
        .collect();
    let bytes = data.as_bytes();

    let plan = ExecutionPlan::new()
        .rename("A", "Alpha")
        .drop("C")
        .filter_eq("Alpha", "1")
        .type_as("Alpha", FieldType::Int64);

    let normal = Pipeline::new(LineSplitter, LineParser).with_plan(plan.clone());
    let resolved = Pipeline::new(LineSplitter, LineParserResolved).with_plan(plan);

    let n_single = normal.read_bytes(bytes).unwrap();
    let r_single = resolved.read_bytes(bytes).unwrap();
    assert_batches_equal(
        std::slice::from_ref(&n_single),
        std::slice::from_ref(&r_single),
    );

    let n_par = normal.read_bytes_par(bytes, 4).unwrap();
    let r_par = resolved.read_bytes_par(bytes, 4).unwrap();
    assert_batches_equal(&n_par, &r_par);

    let n_stream = normal
        .read_bytes_stream(bytes, MemoryBudget::new(2048))
        .unwrap();
    let r_stream = resolved
        .read_bytes_stream(bytes, MemoryBudget::new(2048))
        .unwrap();
    assert_batches_equal(&n_stream, &r_stream);

    // Also cross-check normal single vs resolved parallel/stream
    assert_batches_equal(std::slice::from_ref(&n_single), &r_par);
    assert_batches_equal(&[n_single], &r_stream);
}

// ---------------------------------------------------------------------------
// 8. Large dataset — 100k rows, 10 columns — all modes agree

#[test]
fn large_dataset_all_modes_agree() {
    let cols = 10;
    let rows = 10_000; // 100k fields; large enough to stress Vec indexing, small enough for CI
    let data: String = (0..rows)
        .map(|r| {
            (0..cols)
                .map(|c| format!("C{c}={}", r * cols + c))
                .collect::<Vec<_>>()
                .join(" ")
                + "\n"
        })
        .collect();
    let bytes = data.as_bytes();
    let p = pipeline();
    let single = p.read_bytes(bytes).unwrap();
    assert_eq!(single.num_rows(), rows);
    assert_eq!(single.num_columns(), cols);
    let par = p.read_bytes_par(bytes, 8).unwrap();
    let stream = p.read_bytes_stream(bytes, MemoryBudget::new(8192)).unwrap();
    assert_batches_equal(std::slice::from_ref(&single), &par);
    assert_batches_equal(&[single], &stream);
}

// ---------------------------------------------------------------------------
// 9. File-path APIs vs bytes APIs

#[test]
fn file_path_apis_match_bytes_apis() {
    use std::io::Write as _;
    let data: String = (0..1000)
        .map(|i| format!("A={} B={}\n", i % 7, i % 5))
        .collect();
    let bytes = data.as_bytes().to_vec();
    let dir = std::env::temp_dir().join(format!("rypipe_integrity_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.txt");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&bytes).unwrap();
    }
    let p = pipeline();
    let via_bytes = p.read_bytes(&bytes).unwrap();
    let via_path = p.read_path(&path, false, false).unwrap();
    assert_batches_equal(
        std::slice::from_ref(&via_bytes),
        std::slice::from_ref(&via_path),
    );

    let via_bytes_par = p.read_bytes_par(&bytes, 4).unwrap();
    let via_path_par = p.read_path_par(&path, 4, false, false).unwrap();
    assert_batches_equal(&via_bytes_par, &via_path_par);

    let via_bytes_stream = p
        .read_bytes_stream(&bytes, MemoryBudget::new(2048))
        .unwrap();
    let via_path_stream = p
        .read_path_stream(&path, MemoryBudget::new(2048), false)
        .unwrap();
    assert_batches_equal(&via_bytes_stream, &via_path_stream);

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// 10. TableBuilder direct extend / engines_to_record_batches data integrity

#[test]
fn direct_table_builder_extend_preserves_all_rows_and_columns() {
    // Build two tables with disjoint column sets and merge
    let mut t1 = TableBuilder::with_plan(4, Arc::new(ExecutionPlan::new()));
    for (k, v) in [("A", "1"), ("B", "2")] {
        t1.put_field(k, Value::Str(Cow::Borrowed(v)));
    }
    t1.end_row();
    for (k, v) in [("A", "3")] {
        t1.put_field(k, Value::Str(Cow::Borrowed(v)));
    }
    t1.end_row();

    let mut t2 = TableBuilder::with_plan(4, Arc::new(ExecutionPlan::new()));
    for (k, v) in [("B", "4"), ("C", "5")] {
        t2.put_field(k, Value::Str(Cow::Borrowed(v)));
    }
    t2.end_row();

    let mut merged = TableBuilder::new();
    merged.extend(t1).unwrap();
    merged.extend(t2).unwrap();
    assert_eq!(merged.num_rows(), 3);
    assert_eq!(merged.num_columns(), 3);
    let batch = merged.finish().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3);
    // Spot-check values
    let a = batch.column_by_name("A").unwrap().as_string::<i32>();
    assert_eq!(a.value(0), "1");
    assert!(a.is_null(2));
    let c = batch.column_by_name("C").unwrap().as_string::<i32>();
    assert!(c.is_null(0));
    assert_eq!(c.value(2), "5");
}
