# The Splitter Trait { #the-splitter-trait }

The `Splitter` trait decides where it is safe to divide an input byte stream
into independent chunks. The engine calls `find_split_points` to get byte
offsets, then parses each chunk concurrently via rayon.

See [Architecture](../architecture/index.md) for how the engine
uses split points internally.

## Trait definition { #trait-definition }

```rust
pub trait Splitter: Send + Sync {
    /// The only required method: the next record boundary at or after `from`.
    /// Must return a position where a record starts, or None.
    fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize>;

    /// Estimate the average bytes per record from a sample of the input.
    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize;

    /// Optional: byte ranges where a candidate boundary must be rejected
    /// (comments, CDATA, quoted fields, string literals). See Skip regions.
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> { None }

    /// Provided. Do not override without a measured reason.
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize>;
}
```

## Required: `next_record_start` { #required-next_record_start }

This is the only method you must implement. Given a byte position, return the
position where the next record starts at or after that position.

```rust
fn next_record_start(&self, bytes: &[u8], from: usize) -> Option<usize> {
    // For newline-delimited formats:
    memchr::memchr(b'\n', &bytes[from..])
        .map(|rel| from + rel + 1)
}
```

**Rules:**
- Return `Some(position)` where `position` is the first byte of the next record.
- Return `None` if no more records exist after `from`.
- The position must be valid: `position <= bytes.len()`.
- Do not return a position at a delimiter; return the position of the first
  byte of the record itself.

**Why this works:** The engine calls `next_record_start` at nominal offsets
(`bytes.len() * i / n`) to find the nearest record boundary. Your implementation
just needs to answer "where does the next record start from here?" The engine
handles deduplication, sorting, and chunk planning.

## Required: `estimate_bytes_per_row` { #required-estimate_bytes_per_row }

Called once on a sample of the input (first 64 KB) to estimate row size. The
bounded executor uses this to plan chunk sizes. Simple newline-counting
suffices for most formats:

```rust
fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
    let newline_count = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    (sample.len() / newline_count).max(1)
}
```

## Optional: `skip_regions` { #optional-skip_regions }

If your format has regions where a candidate delimiter must be ignored (comments,
CDATA, quoted fields, string literals), implement `skip_regions()`:

```rust
fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
    Some(&MySkipRegions)
}
```

See [Skip regions](./skip-regions.md) for the full `SkipRegionFinder` interface
and implementation examples.

## Default: `find_split_points` { #default-find_split_points }

**Do not override this method** unless you have a measured reason. The default
implementation provides:

1. **Nominal offsets** at `bytes.len() * i / n` for `i in 1..n`
2. **Parallel search** via `par_iter`, each calling `next_record_start`
3. **Skip-region rejection** via `in_skip_region` (bounded backward scan)
4. **Dedup** and sort
5. **Chunk floor** via `plan_chunk_count` (2 MiB minimum, thread caps)

```
fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
    let n = plan_chunk_count(bytes.len(), max_chunks, SplitMode::Parallel);
    let nominals: Vec<usize> = (1..n).map(|i| bytes.len() * i / n).collect();

    // par_iter over nominals, each calling next_record_start
    // reject candidates inside skip regions
    // dedup, sort, prepend 0, append bytes.len()
}
```

The default is strictly better than hand-rolled splitting because it applies
the measured chunk-size floor (`MIN_CHUNK_BYTES = 2 MiB`) that prevents the
sub-1 MB chunk collapse. See [Chunk planning](./chunk-planning.md).

## What the engine does with split points { #what-the-engine-does-with-split-points }

1. `find_split_points` returns `vec![0, 13, 26, 39, ..., bytes.len()]`
2. The engine converts these to ranges: `[0..13, 13..26, 26..39, ...]`
3. Each range is parsed independently by `parse_chunk` on a rayon thread
4. Results are merged into a single `RecordBatch` (or kept chunked for streaming)

The engine guarantees:
- Every chunk contains whole records (no mid-record splits)
- Empty chunks are discarded
- Chunks are parsed in parallel with no shared mutable state

## Performance characteristics { #performance-characteristics }

- `next_record_start` is called once per nominal offset (typically 100-200 times)
- Each call scans forward from the nominal to find the next record boundary
- The scan is O(chunk_size / row_size) on average
- Skip-region rejection adds O(window × num_openers) per candidate
- Total split time is < 1% of single-threaded parse time on 500 MB

/// warning

Do not override `find_split_points` unless you have a measured reason. The
default implementation applies the chunk-size floor (`MIN_CHUNK_BYTES = 2 MiB`)
that prevents sub-1 MB chunk collapse, handles skip-region rejection, and
deduplicates split points. Hand-rolled versions almost always get this wrong.

///

/// tip

For most newline-delimited formats, `memchr::memchr(b'\n', ...)` is the
optimal `next_record_start` implementation. The `memchr` crate uses AVX2 on
x86_64 and NEON on ARM, scanning 16-32 bytes per cycle. Do not hand-roll
byte iteration for single-delimiter searches.

///

## Common mistakes { #common-mistakes }

1. **Overriding `find_split_points`**: Bypasses the chunk floor and skip-region
   rejection. The default is almost always better.

2. **Splitting inside records**: Each chunk must contain whole records. Split at
   record boundaries, not at arbitrary byte offsets.

3. **Not handling empty input**: `find_split_points` returns `vec![0, 0]` for
   empty input. Your `next_record_start` should return `None` for empty input.

4. **Returning positions at delimiters**: Split points should be at the first
   byte of a record, not at the delimiter itself. For CSV, return the byte
   after `\n`, not the `\n` itself.

5. **Ignoring skip regions**: If your format has comments or quoted fields,
   implement `skip_regions`. Without it, the engine may split inside a comment
   or quoted string, producing corrupt chunks.

6. **Using `find_split_points` for row-level iteration**: `find_split_points`
   is for chunk-level splitting only. Row-level iteration uses `parse_chunk`.
