# Properties adapter example

`examples/rypipe-properties` parses plain Java-style property lines into
`key` and `value` columns. It accepts `=`, `:`, or the first whitespace as the
separator, trims leading whitespace, and skips blank lines plus `#` and `!`
comments. A line without a separator becomes a key with an empty value.

The splitter uses newline boundaries. The parser rejects backslash escapes and
continuations with a `Parser` error because this example does not implement
Java escape decoding or cross-line state. Rejecting those records keeps the
result explicit instead of silently changing their meaning.

The Python `PropertiesSource` forwards the path and shared plan options to the
Rust `read_properties` binding:

```console
maturin develop --release --manifest-path examples/rypipe-properties/Cargo.toml
```

```python
from rypipe_properties import PropertiesSource
table = PropertiesSource("app.properties", schema=["key", "value"]).to_arrow()
```

Use this adapter for the supported plain subset. Add escape and continuation
semantics only with parser tests for the exact Java forms required.
