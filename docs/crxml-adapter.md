# Real-world adapter: crxml

`crxml` is a high-throughput adapter for Crystal Reports XML exports. It is a good example of what a production `rypipe` adapter looks like: a small Rust crate that implements `rypipe-core`'s `Splitter` and `RecordParser` traits, plus a thin Python layer that registers the adapter with `rypipe`.

## What it does

Crystal Reports exports tabular data inside XML elements like `<Field Name="X"><Value>123</Value></Field>` and `<Text Name="Y"><TextValue>abc</TextValue></Text>`. `crxml` reads these exports and turns them into Arrow tables or DataFrames.

On the same workstation used for the rypipe engine benchmarks (AMD Ryzen 9 5900X), `crxml` parses Crystal Reports XML at roughly **2.4 GB/s**. That number is parser-bound; the `rypipe-core` engine keeps up without being the bottleneck.

## How it fits into rypipe

```text
Crystal Reports XML file
    |
    v
CrystalXmlSplitter  -- finds row-tag boundaries
    |
    v
CrystalXmlDecoder   -- extracts fields from each row
    |
    v
rypipe-core engine  -- typed builders, filters, projection, Arrow export
    |
    v
pyarrow.Table / pandas.DataFrame
```

The Rust side lives in `crxml-core`. The Python side is a small `CrystalXMLAdapter` that calls the Rust core and registers itself with `rypipe`.

## Rust implementation

### Splitter

`crxml` implements `rypipe_core::Splitter` in `src/xml/splitter.rs`. Its job is to find safe chunk boundaries for parallel parsing.

Key ideas:

- Scan for the row tag with `memchr::memmem`, which is SIMD-accelerated on most platforms.
- Skip comment (`<!-- ... -->`) and CDATA (`<![CDATA[ ... ]]>`) regions so a `<Row` string inside them is not mistaken for a real row start.
- Validate that a candidate tag is followed by whitespace, `>`, or `/` to avoid prefix collisions such as `<RowItem`.
- Estimate bytes per row from a 64 KiB sample so the scheduler can size chunks and memory budgets.

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

`CrystalXmlDecoder` implements `rypipe_core::RecordParser` in `src/xml/decoder.rs`. It uses `quick_xml` in borrowed-slice mode so events reference the input bytes directly instead of copying into a scratch buffer.

For each row element it:

1. Emits row attributes as fields.
2. Walks child elements.
3. Recognizes `<Field Name="..."><Value>...</Value></Field>`, `<Text Name="..."><TextValue>...</TextValue></Text>`, and `<Section SectionNumber="..."/>`.
4. Calls `sink.put_field(key, Value::Str(value))` so the engine builds typed columns.

A simplified excerpt:

```rust
use rypipe_core::{ColumnarSink, RecordParser, Value, Result};
use quick_xml::events::Event;
use quick_xml::Reader;

pub struct CrystalXmlDecoder { row_tag: Vec<u8> }

impl RecordParser for CrystalXmlDecoder {
    fn validate(&self, bytes: &[u8]) -> Result<()> {
        simdutf8::basic::from_utf8(bytes)?;
        Ok(())
    }

    fn parse_chunk(&self, bytes: &[u8], sink: &mut dyn ColumnarSink) -> Result<()> {
        let mut reader = Reader::from_reader(bytes);
        reader.config_mut().check_end_names = false;
        // ... walk events, emit fields ...
        Ok(())
    }
}
```

The decoder also has a `parse_tail` fallback that rescans orphan close-tags at chunk boundaries, so chunked parsing stays correct without a serial pre-pass.

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
| Borrowed-slice `quick_xml` reader | XML events point into the input buffer; no per-event copy. |
| `memchr::memmem` row-tag scan | SIMD-accelerated boundary search for parallel chunks. |
| Skip-region handling | Comments/CDATA do not create false split points. |
| SIMD UTF-8 validation | `simdutf8` validates each chunk in bulk. |
| `rypipe-core` typed builders | Strings are copied into Arrow arrays only once, during parse. |
| Parallel fast path | When `auto_dict` and compare filters are off, chunks export independently. |

## Lessons for adapter authors

1. **Specialize the parser**: generic line splitting is fine for engine benchmarks, but real throughput comes from a format-aware parser.
2. **Find split points cheaply**: a single `memmem` scan beats scanning byte-by-byte.
3. **Handle boundary cases**: chunks can start or end inside a row; have a fallback path that rescans from the nearest safe row start.
4. **Borrow strings into the engine**: pass `Value::Str(&str)` slices whenever the input is valid UTF-8.
5. **Register with `rypipe`**: a thin adapter class lets users call `rypipe.read()` while you keep the fast Rust core.

## Source

The full implementation is in the [crxml repository](https://github.com/emiliano-go/crxml), especially:

- `src/crxml_core/src/xml/splitter.rs`
- `src/crxml_core/src/xml/decoder.rs`
- `src/crxml/rypipe_adapter.py`
