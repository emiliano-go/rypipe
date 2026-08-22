# Architecture

`rypipe` is built around one idea: **separate the parts of parsing that depend
on a file format from the parts that don't.**

The format-specific side answers questions like:

- Where does one row end and the next begin?
- How do I extract field names and values from a row?
- What is the encoding / entity-escaping rule?

The format-agnostic side answers questions like:

- How do I store rows as typed columns?
- How do I rename, drop, cast, filter, and reorder columns?
- How do I export to Arrow?
- How do I parallelize parsing while staying inside a memory budget?

## High-level flow

```
input bytes
    │
    ▼
┌─────────────────┐
│   Splitter      │  ← format-specific: finds safe chunk boundaries
└────────┬────────┘
         │ Vec<Range<usize>>
         ▼
┌─────────────────┐
│ RecordParser    │  ← format-specific: turns bytes into field events
│   begin_row()   │
│   put_field()   │
│   end_row()     │
└────────┬────────┘
         │ Value events
         ▼
┌─────────────────┐
│  TableBuilder   │  ← format-agnostic: columnar storage + plan
│ (ColumnarSink)  │
└────────┬────────┘
         │ RecordBatch
         ▼
┌─────────────────┐
│  Arrow export   │  ← format-agnostic: C Data Interface / compute kernels
└─────────────────┘
```

## Crate overview

### `rypipe-core`

The pure-Rust engine. It has no `pyo3` or `quick-xml` dependency and no
format-specific logic.

| Module | Responsibility |
|--------|----------------|
| `value` | `Value<'a>` enum: `Str(&str)`, `Int64`, `Float64`, `Bool`, `Null`. |
| `plan` | `ExecutionPlan`, `FieldType`, `FilterPredicate`, `CompareOp`. |
| `columnar` | `StrColumn`, `ColumnBuilder`, dictionary encoding, auto-dict heuristic. |
| `engine` | `TableBuilder`, the main `ColumnarSink` implementation. |
| `merge` | `TableBuilder::extend`, `engines_to_record_batches`. |
| `arrow_export` | Build Arrow arrays, apply `Compare` filters via `arrow::compute`. |
| `decoder` | `Splitter`, `RecordParser`, `ColumnarSink` traits. |
| `parallel` | `ParallelExecutor` over a `Splitter` + `RecordParser`. |
| `bounded` | `BoundedExecutor` + `MemoryBudget` for streaming large files. |
| `input` | `InputBuffer` abstraction: mmap or owned `Vec<u8>`. |
| `error` | Unified `Error` / `Result` type. |

### `rypipe-xml`

The first format adapter. It provides:

- `CrystalXmlDecoder` (implements `RecordParser`): emits field events for
  Crystal Reports XML rows (`<Row>`, `<Field>`, `<Text>`, `<Section>`,
  attributes, `FormattedValue` / `Value` / `TextValue`).
- `CrystalXmlSplitter` (implements `Splitter`): finds safe row boundaries
  while skipping comments and CDATA.

### `rypipe-python`

PyO3 bindings that expose the same entry points crxml historically used:

- `read_to_columnar`
- `read_to_columnar_multi`
- `read_to_columnar_par`
- `read_to_columnar_bounded`

It also exposes reusable Rust helpers (`export::record_batches_to_pyarrow_table`)
so downstream crates like crxml can reuse the Python/Arrow boundary.

## The decoder API

The boundary between format-specific and format-agnostic code is three traits.

### `Splitter`

```rust
pub trait Splitter: Send + Sync {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;
}
```

`find_split_points` returns sorted byte offsets. Adjacent offsets define a
chunk range. The first offset should be `0` and the last should be
`bytes.len()`. A good splitter guarantees that each chunk starts at a valid
row boundary so chunks can be parsed independently.

`estimate_bytes_per_row` is used by the bounded executor to size batches.

### `RecordParser`

```rust
pub trait RecordParser: Send + Sync {
    fn validate(&self, bytes: &[u8]) -> Result<()>;
    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()>;
}
```

`validate` checks well-formedness once per chunk (e.g. UTF-8 validity). The
main parse loop then calls `begin_row`, `put_field`, and `end_row` on the sink.

`RecordParser` never sees the `ExecutionPlan`. It only resolves native field
identities to plain names. For XML that means emitting the attribute/child name;
for CSV it means mapping a header index to a column name internally.

### `ColumnarSink`

```rust
pub trait ColumnarSink {
    fn begin_row(&mut self);
    fn put_field(&mut self, name: &str, value: Value<'_>);
    fn end_row(&mut self);
    fn wants(&self, _name: &str) -> bool { true }
    fn finish(&mut self) -> Result<RecordBatch>;
}
```

`TableBuilder` is the canonical implementation. It:

1. Resolves the field name through `ExecutionPlan::resolve_field` (rename-then-drop).
2. Ensures a `ColumnBuilder` exists for the resolved name.
3. Applies **last-write-wins** within the current row.
4. On `end_row`, null-fills missing columns and evaluates per-row filters.
5. On `finish`, sorts columns by `schema_order`, runs auto-dict upgrade, and
   builds Arrow arrays.

`wants` lets parsers skip fields that will be dropped, avoiding wasted
extraction work.

## Execution plan

`ExecutionPlan` is the format-agnostic pushdown target.

```rust
pub struct ExecutionPlan {
    pub field_map: HashMap<String, String>,       // rename
    pub drop_fields: HashSet<String>,             // drop
    pub field_types: HashMap<String, FieldType>,  // cast
    pub dictionary_columns: HashSet<String>,      // explicit dict encoding
    pub filter: Option<FilterPredicate>,          // per-row or post-reduce
    pub schema_order: Vec<String>,                // output column order
    pub auto_dict: bool,                          // auto-dict upgrade
}
```

Field resolution order:

1. `field_map` renames the raw field.
2. `drop_fields` checks the resolved name.
3. `field_types` / `dictionary_columns` chooses the storage type.
4. `filter` rejects rows during `end_row` (for `Equal`/`NotEqual`) or after
   assembly (for `Compare`).

`FilterPredicate::Compare` is evaluated after the `RecordBatch` is built using
`arrow::compute` comparison kernels and `filter_record_batch`. This removes the
previous dependency on calling `pyarrow.compute` from inside Rust.

## Columnar storage

### `StrColumn`

Strings are stored in one contiguous byte arena plus `i32` offsets and a
validity bitmap. This is exactly the Arrow string layout, so export is a block
copy of two buffers.

- `push` appends bytes and an offset; no per-cell `String` allocation.
- `pop` truncates the arena; enables row-level filtering without compaction.
- `append` merges another column by base-shifting offsets.

### `ColumnBuilder`

An enum over storage types:

```rust
pub(crate) enum ColumnBuilder {
    String(StrColumn),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    Dictionary { codes: Vec<Option<i32>>, dict: Vec<String>, index: HashMap<String, i32> },
}
```

Typed builders parse from `Value::Str` or accept native typed `Value` variants
directly. Unparseable strings become `None` (null).

Dictionary encoding uses a `value → i32` side index. When two dictionary
columns are merged, the right-hand dictionary is remapped into the left-hand
one and codes are translated in one pass.

## Parallel execution

`ParallelExecutor::parse` does the following:

1. Calls `Splitter::find_split_points`.
2. Converts points to non-empty `Range<usize>` chunks.
3. Uses `rayon::par_iter` to parse each chunk independently into a
   `TableBuilder`.
4. **Fast path**: if `auto_dict` is false and there is no `Compare` filter,
   each builder is exported as its own `RecordBatch` in parallel via
   `engines_to_record_batches`. No serial merge happens.
5. **Merge path**: if `auto_dict` or a `Compare` filter is present, chunk
   builders are merged sequentially, the filter is applied, and a single
   `RecordBatch` is returned.

Panic catching per chunk prevents one malformed chunk from killing the whole
parallel parse.

## Bounded execution

`BoundedExecutor::run` keeps peak memory near a configured budget:

1. Opens the file via `InputBuffer`.
2. Estimates `bytes_per_row` from the splitter.
3. Computes `rows_per_batch` from the budget.
4. Splits the file into at most 64 batches.
5. Parses each batch into a `TableBuilder`, exports it to a `RecordBatch`, and
   resets the builder.
6. Returns a `Vec<RecordBatch>`; the caller concatenates.

Because the input buffer is dropped before the parse phase begins for bounded
mode, mmap-backed pages are released before downstream work starts.

## Memory model

- `InputBuffer::Mmap` maps the file and applies `MADV_WILLNEED` (prefault) or
  `MADV_SEQUENTIAL` (RSS-sensitive) advice on Unix. The mapping is dropped
  before Arrow export, so no borrowed bytes outlive it.
- `InputBuffer::Owned` simply reads the file into a `Vec<u8>`.
- `StrColumn` owns its bytes; Arrow arrays are built from owned buffers.
- Numeric columns use dense `Vec<Option<T>>`.

## Error handling

`rypipe-core` uses one `Error` enum:

```rust
pub enum Error {
    Utf8(...),
    Plan(String),
    Merge(String),
    Io(...),
    Arrow(...),
}
```

Adapters can map their own errors into this type. `rypipe-python` maps these
to `XmlError`, `PlanError`, and `MergeError` Python exceptions.

## Why this shape?

The original crxml engine was fast but tightly coupled to Crystal Reports XML.
Extracting it into rypipe keeps those performance characteristics (arena
string storage, SIMD UTF-8 validation, zero-copy event parsing, GIL release,
parallel chunking, memory bounding) while making them available to other
formats. A new adapter only needs to answer "where are the rows?" and "what are
the fields?"; the engine handles the rest.
