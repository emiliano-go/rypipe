# Case study: crxml { #case-study-crxml }

`crxml` is a high-throughput adapter for Crystal Reports XML exports. It is a concrete example of how the techniques from the other advanced pages combine to reach ~4.2 GB/s on a single workstation.

## What it parses { #what-it-parses }

Crystal Reports exports tabular data inside XML elements such as:

```xml
<Field Name="amount"><Value>123.45</Value></Field>
<Text Name="status"><TextValue>active</TextValue></Text>
```

`crxml` reads these exports and turns them into Arrow tables or DataFrames. The speed is parser-bound; the `rypipe-core` engine keeps up without being the bottleneck.

## Architecture { #architecture }

```text
Crystal Reports XML file
    |
    v
CrystalXmlSplitter : finds row-tag boundaries
    |
    v
CrystalXmlDecoder : extracts fields from each row
    |
    v
rypipe-core engine : typed builders, filters, projection, Arrow export
    |
    v
pyarrow.Table / pandas.DataFrame
```

The Rust side lives in `crxml-core`. The Python side is a thin `CrystalXMLAdapter` that calls the Rust core and registers itself with `rypipe`.

## Techniques from this section { #techniques-from-this-section }

| Page | Technique used in crxml |
|------|-------------------------|
| [Adapter design](./adapter-design.md) | `memchr::memmem` splitter; skip comments/CDATA; validate tag boundaries. |
| [Adapter design](./adapter-design.md) | Hand-rolled `memchr`/`memmem` scanner in `scanner.rs`; XML events point into the input buffer. |
| [Schema and types](./schema-and-types.md) | `field_types` casts strings to numbers during parse. |
| [Dictionary encoding](./dictionary-encoding.md) | `dictionary_columns` for low-cardinality string fields. |
| [Parallelism](./parallelism.md) | Parallel fast path when `auto_dict` and compare filters are off. |
| [Execution modes](./execution-modes.md) | `columnar`, `parallel`, and `stream` modes exposed through `rypipe`. |
| [I/O tuning](./io-tuning.md) | `mmap` with `prefault` for cached files; bounded streaming for huge files. |

## The splitter { #the-splitter }

`CrystalXmlSplitter` uses `memchr::memmem` to scan for the row tag. It is SIMD-accelerated on most platforms. It skips `<!-- ... -->` and `<![CDATA[ ... ]]>` regions so a `<Row` string inside them is not mistaken for a real row start. It also validates that a candidate tag is followed by whitespace, `>`, or `/` to avoid prefix collisions such as `<RowItem`.

## The decoder { #the-decoder }

`CrystalXmlDecoder` uses the hand-rolled `memchr`/`memmem` scanner in `scanner.rs`. Events reference the input bytes directly instead of copying into a scratch buffer. For each row element it:

1. Emits row attributes as fields.
2. Walks child elements.
3. Recognizes `<Field>`, `<Text>`, and `<Section>` patterns.
4. Calls `sink.put_field(key, Value::Str(value))` so the engine builds typed columns.

The decoder also has a `parse_tail` fallback that rescans orphan close-tags at chunk boundaries, so chunked parsing stays correct without a serial pre-pass.

## Why it is fast { #why-it-is-fast }

| Technique | Benefit |
|-----------|---------|
| Hand-rolled `memchr`/`memmem` scanner (`scanner.rs`) | XML events point into the input buffer; no per-event copy. |
| `memchr::memmem` row-tag scan | SIMD-accelerated boundary search for parallel chunks. |
| Skip-region handling | Comments/CDATA do not create false split points. |
| SIMD UTF-8 validation | `simdutf8` validates each chunk in bulk. |
| `rypipe-core` typed builders | Strings are copied into Arrow arrays only once, during parse. |
| Parallel fast path | When `auto_dict` and compare filters are off, chunks export independently. |

## Lessons for adapter authors { #lessons-for-adapter-authors }

1. Specialize the parser. Generic line splitting is fine for engine benchmarks, but real throughput comes from a format-aware parser.
2. Find split points cheaply. A single `memmem` scan beats scanning byte-by-byte.
3. Handle boundary cases. Chunks can start or end inside a row; have a fallback path that rescans from the nearest safe row start.
4. Borrow strings into the engine. Pass `Value::Str(Cow::Borrowed(&str))` slices whenever the input is valid UTF-8.
5. Register with `rypipe`. A thin adapter class lets users call `rypipe.read()` while you keep the fast Rust core.

## Source { #source }

The full implementation is in the [crxml repository](https://github.com/emiliano-go/crxml), especially:

- `src/crxml_core/src/xml/splitter.rs`
- `src/crxml_core/src/xml/decoder.rs`
- `src/crxml/rypipe_adapter.py`
