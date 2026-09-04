# Schema Architecture

This page documents the internal architecture of schema handling in rypipe.
It covers `FrozenSchema`, `DiscoveryOpts`, the schema cache, `ensure_schema`,
and the fast/merge export paths.

For the adapter-author guide, see [Schema](../writing-adapters/schema.md).

## Overview

Schema in rypipe answers three questions:

1. **What columns exist?** (`schema_order` or discovery)
2. **What type is each column?** (`field_types` or default `String`)
3. **What order are they in?** (`schema_order` or first-appearance)

The engine resolves these questions before parsing starts, builds an immutable
`FrozenSchema`, and shares it across all workers. This guarantees that every
batch has identical columns, types, and order.

## Data flow

```
User provides schema_order + field_types
        |
        v
ExecutionPlan (compiled, shared via Arc)
        |
        +---> FrozenSchema::from_plan (explicit) -----> ensure_schema
        |                                                      |
        +---> FrozenSchema::from_discovered (auto) ----> ensure_schema
                                                               |
                                                               v
                                                    TableBuilder per chunk
                                                               |
                                                               v
                                                    sort_columns (finish)
                                                               |
                                                               v
                                                    RecordBatch export
```

## FrozenSchema

`FrozenSchema` is the engine's immutable representation of the output column
layout. It is defined in `crates/rypipe-core/src/schema.rs`.

### Structure

```rust
pub struct FrozenSchema {
    /// Output column names in final order.
    names: Vec<Arc<str>>,

    /// Raw input field name -> output slot index.
    /// None = dropped by plan.
    index: FxHashMap<Box<str>, Option<u32>>,

    /// Output column types, parallel to names.
    types: Vec<FieldType>,

    /// true = full scan, false = sampled.
    exact: bool,
}
```

Key design decisions:

- **`names` uses `Arc<str>`**: shared across workers without cloning
- **`index` maps raw names to `Option<u32>`**: collapses rename, drop, and
  lookup into a single hash probe
- **`types` is parallel to `names`**: type for column `i` is `types[i]`
- **`exact` tracks discovery method**: affects error messages and cache behavior

### Construction: explicit schema

When `schema_order` is provided, `FrozenSchema::from_plan` builds the schema
directly from the declared names:

```rust
pub fn from_plan(names: &[&str], plan: &ExecutionPlan) -> Self {
    let mut index = FxHashMap::default();
    let mut types = Vec::with_capacity(names.len());

    for (slot, &name) in names.iter().enumerate() {
        let ty = plan.column_type(name);
        types.push(ty);
        index.insert(Box::from(name), Some(slot as u32));
    }

    FrozenSchema {
        names: names.iter().map(|n| Arc::from(*n)).collect(),
        index,
        types,
        exact: true,
    }
}
```

Properties:

- **O(n)** construction where n = number of columns
- **No I/O**: the schema is pure computation
- **Exact**: all columns are known, no sampling uncertainty
- **`exact = true`**: enables the "unknown field is an error" behavior

### Construction: discovered schema

When `schema_order` is empty, the engine discovers column names from the file.
`FrozenSchema::from_discovered` applies the plan's renames and drops:

```rust
pub fn from_discovered(names_in_order: &[String], plan: &ExecutionPlan) -> Self {
    let mut index = FxHashMap::default();
    let mut out_names = Vec::new();
    let mut types = Vec::new();

    for name in names_in_order {
        let resolved = plan.resolve_field(name);
        match resolved {
            Some(resolved_name) => {
                if plan.drop_fields.contains(resolved_name) {
                    index.insert(Box::from(name.as_str()), None);
                    continue;
                }
                let slot = out_names.len() as u32;
                let ty = plan.column_type(resolved_name);
                index.insert(Box::from(name.as_str()), Some(slot));
                out_names.push(Arc::from(resolved_name));
                types.push(ty);
            }
            None => {
                index.insert(Box::from(name.as_str()), None);
            }
        }
    }

    FrozenSchema {
        names: out_names,
        index,
        types,
        exact: false,
    }
}
```

Properties:

- **Applies renames**: `field_map` entries are resolved during construction
- **Applies drops**: dropped fields get `None` in the index
- **`exact = false`**: sampled discovery may miss rare columns
- **Order**: follows discovery order (file order, then document order)

### Resolution: the hot path

`FrozenSchema::resolve` maps a raw field name to its output slot:

```rust
#[inline]
pub fn resolve(&self, raw_name: &str) -> Option<u32> {
    self.index.get(raw_name).copied().flatten()
}
```

This is called in the hot path for every field in every row. With
`FxHashMap`, it costs ~15 cycles per probe (one hash, one comparison).

The `Option<u32>` return value:

- `Some(slot)`: field maps to output column `slot`
- `None`: field is dropped (parser should skip it)

## DiscoveryOpts

`DiscoveryOpts` controls how schema discovery works for files without explicit
schema.

### Structure

```rust
pub struct DiscoveryOpts {
    /// Files smaller than this are fully scanned (cheap, exact).
    pub full_scan_threshold: u64,

    /// Number of strided windows for large files.
    pub windows: usize,

    /// Bytes per window.
    pub window_bytes: usize,
}
```

### Default values

```rust
impl Default for DiscoveryOpts {
    fn default() -> Self {
        Self {
            full_scan_threshold: 128 * 1024 * 1024, // 128 MiB
            windows: 16,
            window_bytes: 2 * 1024 * 1024, // 2 MiB
        }
    }
}
```

### Discovery strategy

**Small files (<=128 MiB)**:

- Full scan of the entire file
- Exact: every column is found
- Cost: one I/O pass + parsing

**Large files (>128 MiB)**:

- Sample 16 windows of 2 MiB each (32 MiB total, ~6% of 533 MiB)
- Windows are strided evenly through the file
- Columns found in any window are included
- Cost: ~5.3 ms on 16 threads (parallel sampling)

### Accuracy

Sampled discovery captures columns that appear in at least one window. For
typical CR XML exports:

- 100% of columns appear in the first 2 MiB (header section)
- `FieldG` (30% sparse) is found in ~94% of samples
- `Text21` (1% sparse) is found in ~94% of samples
- A column appearing only in the last 1% of the file would be missed

When a missed column is encountered at parse time, the engine raises
`MergeError` with the message:

```
unknown field "LateColumn" not in frozen schema (10 columns, exact=false);
pass schema=[...] with full column list or use full-scan discovery
```

## Schema cache

For batch workloads (many files with the same layout), the engine caches
discovered schemas to avoid redundant discovery.

### Cache structure

```rust
pub static SCHEMA_CACHE: LazyLock<RwLock<FxHashMap<SchemaCacheKey, SchemaCacheValue>>>
    = LazyLock::new(|| RwLock::new(FxHashMap::default()));

type SchemaCacheKey = (u64, u64);   // (file_len, sample_hash)
type SchemaCacheValue = Arc<Vec<String>>;  // discovered column names
```

### Layout signature

`layout_signature` computes a cheap key for caching:

```rust
pub fn layout_signature(bytes: &[u8], opts: &DiscoveryOpts) -> (u64, u64) {
    let mut hasher = FxHasher::default();
    if (bytes.len() as u64) < opts.full_scan_threshold {
        // Small file: hash the entire content
        bytes.hash(&mut hasher);
    } else {
        // Large file: hash 4 strided windows
        let n = 4usize;
        let w = opts.window_bytes.min(bytes.len());
        for i in 0..n {
            let start = (bytes.len() - w) * i / n.max(1);
            bytes[start..start + w].hash(&mut hasher);
        }
    }
    (bytes.len() as u64, hasher.finish())
}
```

Properties:

- **Fast**: ~1 us for small files, ~5 us for large files
- **Collision-resistant**: 4-window hash for large files
- **Stable**: same layout always produces the same key

### Cache operations

**Insert** (`insert_schema_cache`):

```rust
pub fn insert_schema_cache(sig: (u64, u64), order: Arc<Vec<String>>) {
    let mut cache = SCHEMA_CACHE.write().unwrap();
    if cache.len() >= SCHEMA_CACHE_CAP {
        if let Some(oldest) = cache.keys().next().copied() {
            cache.remove(&oldest);
        }
    }
    cache.insert(sig, order);
}
```

- Evicts arbitrary entry when at capacity (128)
- `FxHashMap` does not track insertion order; this is acceptable for a
  bounded cache

**Lookup**: done via `SCHEMA_CACHE.read()` in the discovery path

**Clear** (`clear_schema_cache`):

```rust
pub fn clear_schema_cache() {
    SCHEMA_CACHE.write().unwrap().clear();
    SCHEMA_CACHE_HITS.store(0, Ordering::Relaxed);
    SCHEMA_CACHE_MISSES.store(0, Ordering::Relaxed);
}
```

### Cache statistics

```rust
pub static SCHEMA_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static SCHEMA_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

pub fn schema_cache_stats() -> (u64, u64) {
    (
        SCHEMA_CACHE_HITS.load(Ordering::Relaxed),
        SCHEMA_CACHE_MISSES.load(Ordering::Relaxed),
    )
}
```

For 1,000 files with the same layout:

- First file: miss (5.3 ms discovery + cache insert)
- Files 2-1000: hit (cache lookup ~1 us each)
- Total: 5.3 ms + 1 ms = 6.3 ms (vs 5.3 s without cache)

## ensure_schema

`ensure_schema` is called by each worker before parsing a chunk. It guarantees
that all columns from the `FrozenSchema` exist in the `TableBuilder`.

### Algorithm

```rust
pub fn ensure_schema(&mut self, schema: &FrozenSchema) -> Result<()> {
    for (idx, name) in schema.column_names().iter().enumerate() {
        if !self.column_names().contains(&name.to_string()) {
            let ty = schema.column_types()[idx];
            self.add_typed_column(name, ty, self.num_rows());
        }
    }
    Ok(())
}
```

Steps:

1. Iterate over all columns in the schema
2. For each column not already in the builder:
   a. Create a typed column builder for the correct `FieldType`
   b. Pre-fill with nulls up to the current row count
3. After this call, the builder has all declared columns

### Why pre-fill with nulls?

Consider a file where column "D" appears only in rows 500-1000. Without
`ensure_schema`:

- Chunk 1 (rows 0-499): no column "D"
- Chunk 2 (rows 500-999): has column "D"
- At merge time: column "D" is missing from chunk 1, causing schema mismatch

With `ensure_schema`:

- Chunk 1: column "D" is added with 500 nulls
- Chunk 2: column "D" has values
- At export time: batches have identical schemas

### Performance

`ensure_schema` is called once per chunk, not per row. For 16 chunks, it
adds ~0.1 ms total (negligible vs parse time).

## sort_columns

`sort_columns` reorders the internal column list to match `schema_order`.
It is called at finish time when `schema_order` is non-empty.

### Algorithm

```rust
pub fn sort_columns(&mut self) {
    if self.plan.schema_order.is_empty() {
        return;
    }

    let order = &self.plan.schema_order;
    let mut indices: Vec<usize> = (0..self.columns.len()).collect();

    indices.sort_by_key(|&i| {
        let name = &self.column_names()[i];
        order.iter().position(|n| n == name)
            .unwrap_or(order.len() + i)
    });

    // Reorder columns and their metadata
    self.reorder_columns(&indices);
}
```

Properties:

- **Stable sort**: columns not in `schema_order` preserve relative order
- **O(n log n)** where n = number of columns (typically <100)
- **Called once**: at finish time, not per row

### Example

Given columns `["D", "A", "C", "B"]` and `schema_order = ["A", "B", "C"]`:

1. `["D", "A", "C", "B"]` with sort keys `[5, 0, 3, 1]`
   - "D" not in schema_order -> key = 5 + 0 = 5
   - "A" at position 0 -> key = 0
   - "C" at position 2 -> key = 2
   - "B" at position 1 -> key = 1
2. Sorted: `["A", "B", "C", "D"]`

## Fast path vs merge path

The engine has two export paths. The choice depends on whether all batches
have identical schemas.

### Fast path (parallel export)

Conditions:

- `auto_dict` is false
- All batches have identical schemas (guaranteed by `ensure_schema` +
  `schema_order`)

Behavior:

- Each batch is exported independently in parallel
- `engines_to_record_batches` builds one `RecordBatch` per chunk
- No merge step needed
- Throughput: ~4,980 MB/s on 533 MB

### Merge path (sequential export)

Conditions:

- `auto_dict` is true, OR
- Batches may have different schemas (auto discovery without `schema_order`)

Behavior:

- Batches are merged sequentially via `extend`
- `unify_variants` reconciles column types
- `promote_to_variant` handles type mismatches
- Throughput: ~4,497 MB/s on 533 MB (-10%)

### Path selection

```rust
fn select_export_path(
    auto_dict: bool,
    schemas_consistent: bool,
) -> ExportPath {
    if !auto_dict && schemas_consistent {
        ExportPath::Fast
    } else {
        ExportPath::Merge
    }
}
```

`schemas_consistent` checks that all engines agree on column variant keys.
This is guaranteed when `schema_order` is set.

## Column type resolution

`ExecutionPlan::column_type` resolves the storage type for a column:

```rust
pub fn column_type(&self, name: &str) -> FieldType {
    // 1. Explicit type override
    if let Some(ty) = self.field_types.get(name) {
        return ty.clone();
    }

    // 2. Dictionary column
    if self.dictionary_columns.contains(name) {
        return FieldType::Dictionary;
    }

    // 3. Default: string
    FieldType::String
}
```

Priority:

1. `field_types[name]` (explicit override)
2. `dictionary_columns` contains `name` (dictionary encoding)
3. `FieldType::String` (default)

This is called during `ensure_schema` to create the correct column builder.

## UnknownFieldPolicy

When a field appears in the file but not in the schema, the engine needs to
know what to do.

```rust
pub enum UnknownFieldPolicy {
    /// Return an error (default).
    Error,
    /// Ignore the field, count occurrences, report at end.
    Skip,
}
```

### Error behavior (default)

With explicit schema (`exact = true`), unknown fields cause a hard error:

```
unknown field "LateColumn" not in frozen schema (10 columns, exact=false);
pass schema=[...] with full column list or use full-scan discovery
```

This is the safe default: it prevents silent data loss.

### Skip behavior

With `UnknownFieldPolicy::Skip`, unknown fields are ignored. The engine
counts occurrences and reports at the end:

```
Warning: 3 unknown fields skipped (field_99, field_100, field_101)
```

Use this when the file may contain fields not in the schema and you want
to process only the known fields.

## Integration with the pipeline

### How schema flows through the engine

```
ExecutionPlan
    |
    +---> FrozenSchema::from_plan
    |         |
    |         +---> ParallelStreamOpts.schema
    |         |         |
    |         |         +---> DiscoverySink (if empty)
    |         |         |         |
    |         |         |         +---> sampled scan
    |         |         |         +---> FrozenSchema::from_discovered
    |         |         |
    |         |         +---> FrozenSchema::from_plan (if set)
    |         |
    |         +---> TableBuilder.ensure_schema (per chunk)
    |         |         |
    |         |         +---> add_typed_column (per missing column)
    |         |
    |         +---> sort_columns (at finish)
    |         |         |
    |         |         +---> reorder columns to match schema_order
    |         |
    |         +---> engines_to_record_batches (fast path)
    |                   |
    |                   +---> one RecordBatch per chunk (parallel)
    |
    +---> TableBuilder.push_field
              |
              +---> schema.resolve(raw_name)
              |         |
              |         +---> Some(slot) -> put into column slot
              |         +---> None -> skip (dropped)
              |
              +---> column_type(name) -> typed builder
```

### How schema interacts with projection

When `DropFields` is used, the plan's `drop_fields` set is populated.
During schema construction:

- `from_plan`: the dropped field is still in the schema (caller declared it)
- `from_discovered`: the dropped field gets `None` in the index

During parsing:

- `wants("dropped_field")` returns `false` (engine checks `drop_fields`)
- The parser skips the field entirely (no scanning, no decoding)

At export time:

- Dropped fields are not in the output (they were never built)

### How schema interacts with rename

When `RenameFields` is used, the plan's `field_map` is populated. During
schema construction:

- `from_plan`: the output name is used (rename already applied by caller)
- `from_discovered`: `resolve_field(name)` applies the rename, then maps
  to the output slot

During parsing:

- `put_field("raw_name", ...)` is resolved via `field_map` to the output name
- The value goes into the renamed column

### How schema interacts with filter

When `FilterRows` is used, the plan's `filter` is populated. During parsing:

- After each row, the engine checks the predicate
- If the row fails, all column values are popped (via `pop_row`)
- If `field_types` is set, the predicate uses native comparison
- If `field_types` is not set, the predicate falls back to string comparison

## Performance budget

For a 533 MB file on a Ryzen 5800X:

| Phase | Time | Notes |
|-------|------|-------|
| Schema resolution | 0.005 ms | `from_plan` construction |
| `ensure_schema` | 0.1 ms | 16 chunks x 10 columns |
| `sort_columns` | 0.01 ms | Once at finish |
| `resolve` per field | 0.000015 ms | ~15 cycles (FxHash) |
| Total schema overhead | ~0.12 ms | <0.01% of parse time |

Without explicit schema:

| Phase | Time | Notes |
|-------|------|-------|
| Discovery (parallel) | 5.3 ms | 16x2 MiB sampling |
| `from_discovered` | 0.01 ms | Apply renames/drops |
| `ensure_schema` | 0.1 ms | Same as above |
| `sort_columns` | 0.01 ms | Same as above |
| Cache lookup | 0.001 ms | If cached |
| Total schema overhead | ~5.4 ms | ~0.4% of parse time |

Explicit schema saves ~5.3 ms per file. For 1,000 files, that is 5.3 seconds.

## Thread safety

`FrozenSchema` is `Clone + Send + Sync`. It is built once and shared via
`Arc<FrozenSchema>` across all workers. No locks are needed after construction.

The schema cache uses `RwLock<FxHashMap>` for concurrent access:

- Read lock: schema lookup (common, concurrent)
- Write lock: schema insert (rare, serialized)

Cache statistics use `AtomicU64` for lock-free increment.
