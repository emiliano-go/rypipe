# Real-world adapter: crxml

`crxml` is a high-throughput adapter for Crystal Reports XML exports. It is a good example of what a production `rypipe` adapter looks like: a small Rust crate that implements `rypipe-core`'s `Splitter` and `RecordParser` traits, plus a thin Python layer that registers the adapter with `rypipe`.

## What it does

Crystal Reports exports tabular data inside XML elements like `<Field Name="X"><Value>123</Value></Field>` and `<Text Name="Y"><TextValue>abc</TextValue></Text>`. `crxml` reads these exports and turns them into Arrow tables or DataFrames.

On the same workstation used for the rypipe engine benchmarks (AMD Ryzen 9 5900X 3.8 GHz, Arch Linux, 5800X 8C/16T measured), `crxml` parses Crystal Reports XML at **2.6–3.0 GB/s parallel** (1 GB, 926k `Details` rows, `par32` 2994 MB/s, `par16` 2720 MB/s, `par8` 2485 MB/s) and **714 MB/s single** (`read_to_columnar`), all warm-cache best-of-3. Streaming (`CrxmlReader` `lib.rs:534`, now also scanner-based via `RowSink` `lib.rs:564`) is **508 MB/s** 100 MB / **498 MB/s** 1 GB (was 251/234 `quick-xml`), within 30% of columnar (was 174% gap). The 1 GB `drop_all` pushdown reaches **4183 MB/s** parallel (CPU-bound, not I/O: `cat` 33 GB/s, `prefault` only +6%). That number is parser-bound; the `rypipe-core` engine (`Vec<ColumnBuilder>`+`field_index` `engine.rs:16`, `row_dirty` `engine.rs:26`) keeps up without being the bottleneck.

## How it fits into rypipe

Legend: `(ADAPTER BOUND)` lives in the adapter crate (`crxml`); `(CORE)` lives in `rypipe`.

```text
Crystal Reports XML file  (ADAPTER BOUND) input (row_tag = "Details" etc.)
    |
    v
CrystalXmlSplitter  : finds row-tag boundaries          (ADAPTER BOUND)  rypipe_core::Splitter impl (crxml-core/src/xml/splitter.rs)
    |
    v  Vec<Range<usize>>  (CORE) helper
CrystalXmlDecoder   : extracts fields from each row     (ADAPTER BOUND)  rypipe_core::RecordParser impl (crxml-core/src/xml/decoder.rs)
    |  Value events (Str, Int64, ...)  (CORE) enum
    v
rypipe-core engine  : typed builders, filters, projection, Arrow export  (CORE)  rypipe-core/src/engine.rs plus columnar.rs plus plan.rs
    |  RecordBatch  (CORE) Arrow
    v
pyarrow.Table / pandas.DataFrame  [PYTHON CORE] via rypipe-python C Data Interface plus (ADAPTER BOUND) registration (crxml/rypipe_adapter.py)
```

The Rust side lives in `crxml-core`. The Python side is a small `CrystalXMLAdapter` that calls the Rust core and registers itself with `rypipe`.

## Rust implementation

### Splitter

`crxml` implements `rypipe_core::Splitter` in `src/xml/splitter.rs` (also shared with the scanner via `find_special_regions` `splitter.rs:61` and `next_row_start` `splitter.rs:107`). Its job is to find safe chunk boundaries for parallel parsing.

Key ideas:

- Scan for the row tag with `memchr::memmem`, which is SIMD-accelerated on most platforms.
- Skip comment (`<!-- ... -->`) and CDATA (`<![CDATA[ ... ]]>`) regions so a `<Row` string inside them is not mistaken for a real row start.
- Validate that a candidate tag is followed by whitespace, `>`, or `/` to avoid prefix collisions such as `<RowItem`.
- Estimate bytes per row from a 64 KiB sample so the scheduler can size chunks and memory budgets (`TableBuilder::with_plan` `lib.rs:275` `cap = len/est_row`).

```rust
use rypipe_core::Splitter;
use memchr;

pub struct CrystalXmlSplitter { row_tag: Vec<u8> }

impl Splitter for CrystalXmlSplitter {
    fn find_split_points(&self, bytes: &[u8], max_chunks: usize) -> Vec<usize> {
        let ranges = compute_splits(bytes, &self.row_tag, max_chunks);
        let mut points = vec![0];
        for r in ranges { points.push(r.end); }
        points.dedup();
        if points.last() != Some(&bytes.len()) { points.push(bytes.len()); }
        points
    }

    fn estimate_bytes_per_row(&self, sample: &[u8]) -> usize {
        let sample_end = sample.len().min(65536);
        let row_tag_count = memchr::memmem::find_iter(&sample[..sample_end], &self.row_tag).count();
        sample_end
            .checked_div(row_tag_count)
            .unwrap_or(512)
            .max(1)
    }
}
```

### Record parser

`CrystalXmlDecoder` implements `rypipe_core::RecordParser` in `src/xml/decoder.rs`. Since `crxml 1.2` it uses a hand-rolled `memchr`/`memmem` scanner `src/xml/scanner.rs` (not `quick_xml`): the same scanner backs both the columnar and the super-optimized streaming path (`lib.rs:534` `RowParser`+`RowSink`+`scan_one_row` `scanner.rs:81`).

For each row element it:

1. Emits row attributes as fields (`emit_all_attrs` `scanner.rs:180`).
2. Walks child elements via `scan_child` `scanner.rs:121` (`next_lt` `scanner.rs:412` `memchr(b'<')` + `in_region` `scanner.rs:418` fast-path for `find_special_regions`).
3. Recognizes `<Field Name="..."><Value>...</Value></Field>`, `<Text Name="..."><TextValue>...</TextValue></Text>`, and `<Section SectionNumber="..."/>` via `field_element` `scanner.rs:202`, `text_element` `scanner.rs:280`, `section_element` `scanner.rs:352`.
4. Byte-jumps dropped fields: `if !sink.wants(&key)` `scanner.rs:210` → `find_close_after` with `LazyLock<Finder>` `scanner.rs:569` for `</Field>` etc., without visiting `<Value>` children.
5. Calls `sink.put_field(key, Value::Str(value))` (`row` attrs `scanner.rs:180`) or `wants`+`put_field` for `Field`/`Text` so the engine builds typed columns. The `resolve`/`put_field_resolved` pair `decoder.rs:46`/`53` is available for expensive extraction (one hash instead of two), used for `Section`/unknown.

A simplified excerpt (columnar):

```rust
use rypipe_core::{ColumnarSink, RecordParser, Value, Result};

pub struct CrystalXmlDecoder { row_tag: Vec<u8> }

impl RecordParser for CrystalXmlDecoder {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        crate::xml::scanner::scan_chunk(bytes, &self.row_tag, sink)
    }
}
```

Streaming reuses the same scanner row-by-row:

```rust
// lib.rs:534 RowParser with InputBuffer (mmap) + RowSink
let mut sink = RowSink { row: &mut self.row };
crate::xml::scanner::scan_one_row(bytes, self.pos, &row_tag, &regions, &mut sink);
```

The scanner is `wants`-driven and region-aware, so chunked and streaming stay correct without a serial pre-pass.

## Python adapter registration

`crxml` exposes a `CrystalXMLAdapter` that wraps `CrystalXMLSource` and registers it with `rypipe`:

```python
import rypipe
from crxml.source import CrystalXMLSource

class CrystalXMLAdapter:
    def read(self, path: str, **kwargs):
        return CrystalXMLSource(path, **kwargs).to_arrow()

rypipe.register_adapter("crxml", CrystalXMLAdapter(), extensions=[".xml"])
```

Importing `crxml` now makes the adapter available automatically:

```python
import rypipe

table = rypipe.read("report.xml", format="crxml", row_tag="Row")
```

The same `row_tag`, `field_types`, `filter`, `memory`, and `chunks` options from `CrystalXMLSource` are passed through, so users get the full engine feature set through the generic `rypipe` API.

## Why it is fast

| technique | benefit |
|-----------|---------|
| Hand-rolled `memchr`/`memmem` scanner `scanner.rs:29` (`memchr`, `memchr3`, `Finder` `scanner.rs:569`) | No `quick-xml` event loop (was 42% wall `lib.rs:598`); SIMD `scan_open_tag` `scanner.rs:414`, `field_element` `scanner.rs:202`, `Find-er` byte-jump for dropped `Field` (`wants` `scanner.rs:210` → `find_close_after`) |
| `memchr::memmem` row-tag scan + skip-region | SIMD `next_row_start` `splitter.rs:107`, `find_special_regions` `splitter.rs:61` (`<!--`, `<![CDATA[`) with `is_empty` fast path `scanner.rs:418` |
| SIMD UTF-8 validation | `simdutf8` `decoder.rs:32` validates chunk bulk; `utf8_unchecked` `scanner.rs:39` + conditional `unescape` only if `b'&'` `scanner.rs:662` |
| `rypipe-core` `Vec<ColumnBuilder>`+`field_index` `engine.rs:16` single `get` + `row_dirty` `engine.rs:26` bitmask | `push_field` was double `HashMap` probe → single `field_index.get` → `columns[idx]`; `finish_row` now only null-fills clear bits, not all `C` columns |
| `InputBuffer` `mmap` `lib.rs:25` `auto_mmap` >50 MB + `cap` via `estimate_bytes_per_row` `splitter.rs:41` | Avoids `fs::read` `rep_movs` 3% + over-reserve `Vec` 2× |
| Parallel fast path `parallel.rs:82` + streaming `RowSink` `lib.rs:564` | `auto_dict`/`Compare` off → per-chunk `RecordBatch` export in parallel; streaming reuses same scanner row-by-row via `scan_one_row` `scanner.rs:81` without `TableBuilder` |

## Lessons for adapter authors (from `crxml` super-optimization)

1. **Specialize the parser**: generic line splitting is fine, but real throughput comes from a format-aware `memchr` scanner (`crxml` went `quick-xml` 251 MB/s → scanner 508 MB/s streaming, 489→714 columnar single).
2. **Find split points cheaply**: one `memmem` `splitter.rs:27` + `find_special_regions` `splitter.rs:61` with `is_empty` fast path beats byte-by-byte; `estimate_bytes_per_row` `splitter.rs:41` sizes `TableBuilder` `lib.rs:275`.
3. **Handle boundary cases**: chunks can start inside a row; `scan_one_row` `scanner.rs:81` (`Recover` → `pos+1`) and `wants`-driven `find_close_after` keep parallel correct.
4. **Borrow strings into the engine**: `Value::Str(&str)` slices via `utf8_unchecked` `scanner.rs:39` + conditional `&` `scanner.rs:662` avoids `Cow` alloc (94% of values are plain ASCII). `RowSink` `lib.rs:564` pushes directly without `TableBuilder` hash/arena for streaming.
5. **Skip dropped fields in the scanner**: check `wants`/`resolve` *before* visiting `<Value>` children: `field_element` `scanner.rs:210` byte-jumps to `</Field>` via `Finder` (drop_all 4183 MB/s, 66% win).
6. **Reuse the scanner for both engines**: columnar `scan_chunk` `scanner.rs:54` and streaming `scan_one_row` `scanner.rs:81` share `parse_row` `scanner.rs:73`, so one optimization benefits `stream`/`columnar`/`parallel`/`bounded`.
7. **Measure first**: `perf` `scan_open_tag` 8.3% + `field_element` 8.6% + `push_field_resolved` 2.76% vs `rep_movs` 3% tells you `mmap` is 3% but scanner is 35%: focus there. `benchmarks/bench_extended.py` (104 benchmarks/file) covers all engines×sinks×pushdowns×chunk/bounded/batch/pipeline.
8. **Register with `rypipe`**: thin `CrystalXMLAdapter` keeps `rypipe.read(format="crxml")` while Rust stays fast.

## Source

The full implementation is in the [crxml repository](https://github.com/emiliano-go/crxml), especially:

- `src/crxml_core/src/xml/splitter.rs`
- `src/crxml_core/src/xml/decoder.rs`
- `src/crxml/rypipe_adapter.py`
