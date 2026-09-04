# Skip Regions { #skip-regions }

When splitting input into chunks, some byte ranges must not be split on.
Comments, CDATA sections, quoted fields, and string literals all contain
bytes that look like record boundaries but aren't.

## The `SkipRegionFinder` trait { #the-skipregionfinder-trait }

```rust
pub trait SkipRegionFinder: Send + Sync {
    /// Openers that start a skip region (e.g., `b"<!--"`, `b"<![CDATA["`).
    fn openers(&self) -> &[&'static [u8]];

    /// The closer for a given opener (e.g., `"-->"` for `"<!--"`).
    fn closer_for(&self, opener: &[u8]) -> &'static [u8];

    /// Maximum backward scan window in bytes. Default 64 KiB.
    fn window(&self) -> usize { 64 * 1024 }
}
```

## How it works { #how-it-works }

The engine calls `in_skip_region(bytes, at, finder)` for each candidate split
point. The function does a bounded backward scan from `at` (capped at
`window()` bytes), looking for an unclosed opener.

If `openers()` is empty, the function returns `false` immediately (O(1)).

## Implementation example: XML comments + CDATA { #implementation-example-xml-comments-cdata }

```rust
struct XmlSkipRegions;

impl SkipRegionFinder for XmlSkipRegions {
    fn openers(&self) -> &[&'static [u8]] {
        &[b"<!--", b"<![CDATA["]
    }

    fn closer_for(&self, opener: &[u8]) -> &'static [u8] {
        if opener == b"<!--" { b"-->" } else { b"]]>" }
    }
}

// Wire into your splitter:
impl Splitter for MyXmlSplitter {
    fn skip_regions(&self) -> Option<&dyn SkipRegionFinder> {
        Some(&XmlSkipRegions)
    }
    // ...
}
```

## Implementation example: CSV quoted fields { #implementation-example-csv-quoted-fields }

```rust
struct CsvSkipRegions;

impl SkipRegionFinder for CsvSkipRegions {
    fn openers(&self) -> &[&'static [u8]] {
        &[b"\""]  // double-quote opens a quoted field
    }

    fn closer_for(&self, _opener: &[u8]) -> &'static [u8] {
        b"\""  // double-quote closes it
    }
}
```

## Performance { #performance }

The backward scan is bounded by `window()` (default 64 KiB). For formats with
no skip regions, `openers()` returns an empty slice and `in_skip_region`
returns `false` in O(1).

When regions exist, the cost is O(window × num_openers) per candidate. For
typical XML/CSV with 1-2 openers and 64 KiB windows, this is negligible
compared to the chunk-planning cost.
