# Rust API Reference { #rust-api }

This page is a reference for the **rypipe-core** Rust API. For a tutorial,
see [Building Adapters](../building-adapters/index.md).

## Crate structure { #crate-structure }

| Crate | Purpose |
|-------|---------|
| `rypipe-core` | Engine, traits, pipeline, Arrow export |
| `rypipe-python` | PyO3 bindings and helpers |
| `rypipe-test` | Property-based testing helpers and fixtures |

## Core traits { #core-traits }

### Splitter { #splitter }

Finds row boundaries in the byte stream.

```rust
pub trait Splitter: Send + Sync {
    /// Return the byte position of the next record start after `from`,
    /// or None at end of input.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;

    /// Estimate average bytes per row from a sample.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;

    /// Optional: regions where split points must be rejected.
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        None  // default: no skip regions
    }
}
```

### RecordParser { #recordparser }

Extracts field values from each row.

```rust
pub trait RecordParser: Send + Sync {
    /// Validate that bytes are well-formed. Called once per chunk.
    fn validate(&self, bytes: &[u8]) -> Result<()>;

    /// Parse a chunk into field/value events via the sink.
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;

    /// Monomorphized version for inlining (default: delegates to parse_chunk).
    fn parse_chunk_generic<S: ColumnarSink>(&self, bytes: &[u8], sink: &mut S) -> Result<()>
    where
        Self: Sized;
}
```

### ColumnarSink { #columnarsink }

Accumulates values into Arrow columns. The engine provides `TableBuilder`
as the production implementation.

```rust
pub trait ColumnarSink {
    // Required
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);
    fn finish(&mut self) -> Result<RecordBatch>;

    // With defaults
    fn wants(&self, _name: &str) -> bool { true }
    fn resolve<'a>(&'a self, name: &'a str) -> Option<&'a str> { Some(name) }
    fn row_rejected(&self) -> bool { false }
    fn row_satisfied(&self) -> bool { false }
    // ... more methods with defaults
}
```

### SkipRegionFinder { #skipregionfinder }

Defines byte ranges that must not be split on.

```rust
pub trait SkipRegionFinder: Send + Sync {
    fn openers(&self) -> &[&'static [u8]];
    fn closer_for(&self, opener: &[u8]) -> &'static [u8];
    fn window(&self) -> usize { 64 * 1024 }
}
```

!!! note "From the Python side"

    Python never implements these traits. End users import the adapter, not
    **rypipe**; here with the reference adapter [crxml](../crxml-adapter.md):

    ```python
    import crxml

    # engine="auto" picks columnar/parallel/stream from the file size
    table = crxml.CrystalXMLSource("data.xml", row_tag="Details").to_arrow()
    table = crxml.CrystalXMLSource(
        "big.xml", row_tag="Details", engine="parallel"
    ).to_arrow()                                       # Pipeline::read_path_par
    ```

    Everything above (`Splitter`, `RecordParser`, `ColumnarSink`,
    `SkipRegionFinder`) runs inside the adapter's Rust crate; the Python
    call crosses the boundary exactly once, on entry.

### RowObserver { #rowobserver }

Per-row hooks fired by `TableBuilder` during parsing. Attach via
[`ExecutionPlan::with_observer`](#executionplan). All methods default to
no-ops; implement only the ones you need. Hooks fire from parse threads on
every engine (serial, parallel, bounded, streaming), so keep them cheap and
thread-safe.

```rust
pub trait RowObserver: Send + Sync {
    fn on_begin_row(&self, row_index: usize) {}
    fn on_put_field(&self, row_index: usize, resolved_name: &str,
                    slot: usize, value: &Value<'_>) {}
    fn on_row_accepted(&self, row_index: usize) {}
    fn on_row_rejected(&self, row_index: usize) {}
    fn on_chunk_finished(&self, total: usize, accepted: usize, rejected: usize) {}
}
```

```rust
let plan = ExecutionPlan::new().with_observer(Arc::new(MyObserver));
```

See [Observer hooks for diagnostics](../building-adapters/techniques.md#observer-hooks)
for a complete counting example.

`RowObserver` is **Rust-only**: no Python bindings are involved at any
point, so a hook is a plain function call on the parse thread (a few
nanoseconds, runs fully parallel across chunks). The Python equivalent is
passing `observer={"on_row_rejected": fn, ...}` in plan kwargs, which wraps
the callables in a `PyObserver`: every event acquires the GIL and calls
into Python (~50 ns per call), serialized against other Python threads.
Both paths are fine per row; the difference matters when the hook fires per
field or at high row rates. For hot paths, prefer a Rust `RowObserver`.

## Value enum { #value }

Represents a parsed field value.

```rust
pub enum Value<'a> {
    Str(Cow<'a, str>),      // UTF-8 string (borrowed or owned)
    Int64(i64),              // 64-bit signed integer
    Float64(f64),            // 64-bit float
    Bool(bool),              // Boolean
    Date32(i32),             // Days since Unix epoch
    Timestamp(i64),          // Raw integer (unit from field_types)
    Null,                    // Explicit null
}
```

The Python-side type mapping (as seen in `pyarrow` tables and in observer
hook arguments):

| `Value` | Python | `pyarrow` |
|---------|--------|-----------|
| `Str` | `str` | `string`/`dictionary` |
| `Int64` | `int` | `int64` |
| `Float64` | `float` | `float64` |
| `Bool` | `bool` | `bool` |
| `Date32` | `int` (days since epoch) | `date32` |
| `Timestamp` | `int` (unit from `field_types`) | `timestamp` |
| `Null` | `None` | null |

## ExecutionPlan { #executionplan }

Configuration for the parse loop.

```rust
pub struct ExecutionPlan {
    pub field_map: HashMap<String, String>,       // rename columns
    pub drop_fields: HashSet<String>,             // columns to skip
    pub field_types: HashMap<String, FieldType>,  // type overrides
    pub dictionary_columns: HashSet<String>,      // dict-encode columns
    pub filter: Option<FilterPredicate>,          // row filter
    pub schema_order: Vec<String>,                // output column order
    pub auto_dict: bool,                          // auto-dictionary
    pub dict_threshold: Option<f64>,              // auto-dict threshold
    pub dict_max_size: Option<usize>,             // auto-dict max size
}
```

**Builder methods:**

```rust
ExecutionPlan::new()
    .rename("old", "new")
    .drop("field")
    .drop_many(["a", "b"])
    .type_as("col", FieldType::Float64)
    .dictionary("col")
    .filter_eq("status", "active")
    .filter_compare("price", CompareOp::Gt, "cost")
    .schema_order(["a", "b", "c"])
    .with_auto_dict(true)
```

!!! note "From the Python side"

    Each builder method corresponds to a keyword argument on
    `Source.__init__` (or a fusable pipeline stage whose `_plan_kwargs()`
    produces the same key). Adapters re-export the stages, so users write
    `crxml.FilterRows`, never `rypipe.stages.FilterRows`:

    | Rust | Python (crxml) |
    |------|----------------|
    | `.rename(old, new)` | `field_mapping={"old": "new"}` or `crxml.RenameFields` |
    | `.drop()` / `.drop_many()` | `drop_fields=[...]` or `crxml.DropFields` |
    | `.type_as(col, ty)` | `field_types={"col": "float64"}` or `crxml.CastTypes` |
    | `.dictionary(col)` | `dictionary_columns=["col"]` |
    | `.filter_eq(...)` / `.filter_compare(...)` | `filter={...}` spec dict or `crxml.FilterRows` |
    | `.schema_order([...])` | `schema=["a", "b", "c"]` |
    | `.with_auto_dict(true)` | `auto_dict=True` |

    ```python
    import crxml

    table = (
        crxml.CrystalXMLSource(
            "data.xml", row_tag="Details", field_mapping={"usr": "user"}
        )
        | crxml.FilterRows(field="status", op="==", value="active")
        | crxml.RenameFields({"old": "new"})
        | crxml.to_arrow()
    )
    ```

    The pipeline's fusion layer merges stage `_plan_kwargs()` into these
    source kwargs and passes them to the adapter's Rust `read_*` function
    as plain kwargs, which builds this `ExecutionPlan`.

## FieldType { #fieldtype }

```rust
pub enum FieldType {
    String,
    Int64,
    Float64,
    Boolean,
    Dictionary,
    Date32,
    Timestamp(TimeUnit),
}

impl std::str::FromStr for FieldType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err>;
    // Recognized: "string", "int64", "float64", "bool", "boolean",
    // "dictionary", "date32", "timestamp", "timestamp[s]", etc.
}
```

## CompareOp { #compareop }

```rust
pub enum CompareOp { Gt, Lt, Ge, Le, Eq, Ne }

impl std::str::FromStr for CompareOp {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err>;
    // Recognized: ">"|"gt", "<"|"lt", ">="|"ge", "<="|"le", "=="|"eq", "!="|"ne"
}
```

## ArithOp { #arithop }

```rust
pub enum ArithOp { Add, Sub, Mul, Div }
```

## FilterPredicate { #filterpredicate }

```rust
pub enum FilterPredicate {
    // Value comparison
    Equal { field: String, value: String },
    NotEqual { field: String, value: String },
    Compare { field_a: String, op: CompareOp, field_b: String },
    CompareLiteral { field: String, op: CompareOp, value: String },

    // String predicates
    StartsWith { field: String, value: String },
    EndsWith { field: String, value: String },
    Contains { field: String, value: String },
    Strip { field: String, op: CompareOp, value: String },
    Lower { field: String, op: CompareOp, value: String },
    Upper { field: String, op: CompareOp, value: String },
    Replace { field: String, old: String, new: String, op: CompareOp, value: String },
    Length { field: String, op: CompareOp, value: i64 },

    // Membership
    In { field: String, values: Vec<String> },
    NotIn { field: String, values: Vec<String> },

    // Type/null predicates
    IsNull { field: String },
    IsType { field: String, field_type: FieldType },

    // Arithmetic
    ArithmeticCompare {
        field: String,
        arith_op: ArithOp,
        arith_value: f64,
        cmp_op: CompareOp,
        cmp_value: f64,
    },

    // Boolean
    NotField { field: String },
    Always(bool),

    // Regex
    Regex { field: String, re: RegexSpec },

    // Logical combinators
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    Not(Box<FilterPredicate>),
}

/// Compiled regex plus its pattern.
pub struct RegexSpec {
    pub pattern: String,
    pub compiled: regex::Regex,
}

impl RegexSpec {
    pub fn new(pattern: impl Into<String>) -> Result<Self, regex::Error>;
}

impl FilterPredicate {
    pub fn all(a: Self, b: Self) -> Self;
    pub fn any(a: Self, b: Self) -> Self;
    pub fn not(inner: Self) -> Self;
}
```

!!! note "From the Python side"

    Predicates serialize to a plain dict spec (the `filter=` kwarg), so the
    same filter reads identically from both languages. Three ways to build
    one:

    ```python
    # 1. Keyword form (a positional arg is a plain callable, not fusable)
    crxml.FilterRows(field="status", op="==", value="active")

    # 2. Expression API (polars-style; same spec, composes with &, |, ~)
    # (adapters re-export col; users never import rypipe.expr)
    crxml.FilterRows((crxml.col("age") >= 18) & crxml.col("name").startswith("A"))

    # 3. Logical combinators over keyword-form filters
    crxml.FilterRowsAll(     # {"and": [spec, spec]}
        crxml.FilterRows(field="a", op=">", value="1"),
        crxml.FilterRows(field="b", op="==", value="x"),
    )
    crxml.FilterRowsNot(     # {"not": spec}
        crxml.FilterRows(field="status", op="==", value="active")
    )
    ```

    The spec dict is what `FilterRows._plan_kwargs()` returns and what the
    adapter's Rust kwargs hand to `execution_plan_from_kwargs`.

    **Extending expressions:** Python libraries can add new *spec producers*
    freely: any object with a `_to_spec()` method returning a spec dict can
    be passed as `FilterRows(predicate=obj)`, and `Predicate` composes with
    the standard `&`/`|`/`~`. Adding a genuinely new *operator* is a Rust
    change: a new `FilterPredicate` variant in
    `rypipe-core` (`plan.rs`) plus a parsing arm in
    `rypipe-python/src/plan_kwargs.rs` (`parse_filter_spec`). Until both
    land, the spec language cannot express the operator and expression
    construction raises instead of silently falling back to Python. See
    [Expression filters](../architecture/expressions.md).

## Pipeline { #pipeline }

Orchestrates splitting, parsing, and export.

```rust
pub struct Pipeline<S, P> {
    splitter: S,
    parser: P,
    plan: Arc<ExecutionPlan>,
}

impl<S, P> Pipeline<S, P>
where
    S: Splitter + Clone,
    P: RecordParser + Clone,
{
    pub fn new(splitter: S, parser: P) -> Self;
    pub fn with_plan(self, plan: ExecutionPlan) -> Self;

    // Single-threaded
    pub fn read_bytes(&self, bytes: &[u8]) -> Result<RecordBatch>;
    pub fn read_path(&self, path: impl AsRef<Path>, use_mmap: bool, prefault: bool) -> Result<RecordBatch>;

    // Parallel
    pub fn read_bytes_par(&self, bytes: &[u8], num_chunks: usize) -> Result<Vec<RecordBatch>>;
    pub fn read_path_par(&self, path: impl AsRef<Path>, num_chunks: usize, use_mmap: bool, prefault: bool) -> Result<Vec<RecordBatch>>;

    // Streaming
    pub fn read_bytes_stream(&self, bytes: &[u8], budget: MemoryBudget) -> Result<Vec<RecordBatch>>;
    pub fn read_path_stream(&self, path: impl AsRef<Path>, budget: MemoryBudget, prefault: bool) -> Result<Vec<RecordBatch>>;
}
```

!!! note "From the Python side"

    End users import the adapter, not **rypipe**. With the reference
    adapter [crxml](../crxml-adapter.md), the execution modes map to
    `CrystalXMLSource` options:

    | Rust | Python (crxml) |
    |------|----------------|
    | `read_path` | `crxml.CrystalXMLSource(path, row_tag="Row").to_arrow()` |
    | `read_path_par` | `CrystalXMLSource(..., engine="parallel").to_arrow()` |
    | `read_path_stream` (bounded memory) | `src.iter_record_batches(memory="64MiB", batch_size=...)` |
    | `with_plan(plan)` | source kwargs + fused `crxml.*` stages |

    ```python
    src = crxml.CrystalXMLSource("big.xml", row_tag="Row")
    for batch in src.iter_record_batches(memory="64MiB"):
        writer.write_batch(batch)
    ```

    The return value crosses the boundary once as a `pyarrow.Table` /
    `RecordBatch` via the Arrow PyO3 bridge; per-chunk Rust results become
    per-chunk Python objects on streaming paths.

## FrozenSchema { #frozenschema}

Resolved schema for a parse run.

```rust
pub struct FrozenSchema { /* fields private */ }

impl FrozenSchema {
    pub fn from_plan(names: &[&str], plan: &ExecutionPlan) -> Self;
    pub fn from_discovered(names_in_order: &[String], plan: &ExecutionPlan) -> Self;
    pub fn num_columns(&self) -> usize;
    pub fn column_names(&self) -> &[Arc<str>];
    pub fn column_types(&self) -> &[FieldType];
    pub fn resolve(&self, raw_name: &str) -> Option<u32>;
}
```

## DiscoveryOpts { #discoveryopts }

Controls schema discovery behavior.

```rust
pub struct DiscoveryOpts {
    pub full_scan_threshold: u64,   // default: 128 MiB
    pub windows: usize,             // default: 16
    pub window_bytes: usize,        // default: 2 MiB
    pub always_scan_tail: bool,     // default: true
}
```

Internal to the engine: `discover_schema()` builds these from file size
via the dynamic window heuristics; callers do not construct them.

## Python bindings { #python-bindings }

### execution_plan_from_kwargs { #execution-plan-from-kwars }

```rust
pub fn execution_plan_from_kwargs(
    field_mapping: Option<HashMap<String, String>>,
    drop_fields: Option<Vec<String>>,
    filter: Option<&Bound<'_, PyAny>>,
    field_types: Option<HashMap<String, String>>,
    dictionary_columns: Option<Vec<String>>,
    schema: Option<Vec<String>>,
    auto_dict: bool,
    auto_dict_threshold: Option<f64>,
    auto_dict_max_size: Option<usize>,
) -> PyResult<ExecutionPlan>;
```

### Export functions { #export-functions }

```rust
pub fn record_batches_to_pyarrow_table(
    py: Python<'_>,
    batches: &[RecordBatch],
) -> PyResult<PyObject>;

pub fn record_batch_to_pyarrow(
    py: Python<'_>,
    batch: &RecordBatch,
) -> PyResult<PyObject>;
```

### Exceptions { #exceptions }

| Exception | Parent | Meaning |
|-----------|--------|---------|
| `ParseError` | `PyException` | File could not be parsed. |
| `XmlError` | `ParseError` | XML-specific parse error. |
| `PlanError` | `PyException` | Invalid plan kwargs. |
| `MergeError` | `PyException` | Schema mismatch between chunks. |
| `ParserError` | `PyException` | Parser misbehavior (adapter bug). |

## Error enum { #error }

```rust
pub enum Error {
    Utf8(simd_utf8::Utf8Error),       // invalid UTF-8
    Io(std::io::Error),                 // I/O errors
    Plan(String),                       // invalid execution plan
    Merge(String),                      // chunk merge conflict
    Arrow(String),                      // Arrow construction failure
    Parser(String),                     // parser misbehavior (adapter bug)
    Lifetime(String),                   // borrowed value lifetime violation
}
```

`Error::Parser` and `Error::Lifetime` are reserved for adapter bugs.
`Parser` signals that the adapter violated the parse contract (e.g., emitted
values outside `begin_row`/`end_row`). `Lifetime` signals that a borrowed
value outlived the chunk's byte buffer.

## rypipe-test crate { #rypipe-test }

Property-based testing helpers for adapter development.

```toml
[dev-dependencies]
rypipe-test = "2"
```

### Strategies { #strategies }

Proptest strategies for generating test data:

| Strategy | Generates |
|----------|-----------|
| `arb_field_name()` | Random field names (1-20 chars, ASCII) |
| `arb_field_value()` | Random field values (empty, ASCII, Unicode) |
| `arb_record()` | Random `Vec<(String, String)>` records |
| `arb_records()` | Random `Vec<Vec<(String, String)>>` with 1-100 records |
| `arb_malformed_utf8()` | Byte sequences with invalid UTF-8 |
| `arb_nested_quotes()` | Strings with nested quote characters |

### Fixtures { #fixtures }

```rust
use rypipe_test::fixtures::{MALFORMED_UTF8, NESTED_QUOTES, EMPTY_VALUES};
```

| Fixture | Description |
|---------|-------------|
| `MALFORMED_UTF8` | 5 byte sequences that fail UTF-8 validation |
| `NESTED_QUOTES` | 5 strings with nested single/double quotes |
| `EMPTY_VALUES` | 5 empty/blank value variants |

### Helpers { #helpers }

```rust
use rypipe_test::{parse_test_bytes, assert_batches_equal};
use rypipe_test::{KeyValueParser, NewlineSplitter};
```

| Helper | Purpose |
|--------|---------|
| `parse_test_bytes(bytes, splitter, parser, plan)` | Parse bytes into `Vec<RecordBatch>` |
| `assert_batches_equal(batches, expected_rows)` | Assert row count and non-empty batches |
| `KeyValueParser` | `key=value` parser for test adapters |
| `NewlineSplitter` | Newline splitter for test adapters |
