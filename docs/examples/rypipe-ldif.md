# LDIF adapter example

`examples/rypipe-ldif` treats blank lines as LDIF record boundaries. Its
splitter delegates boundary detection to `find_next_record_boundary` in blank
line mode. The parser starts one row per entry, emits `key: value` fields, and
flushes the last entry at EOF. Comment lines are skipped.

The parser rejects folded continuation lines, base64 values (`::`), and URL
values (`:<`) with `Parser` errors. Those forms need decoding and policy
choices, so silently treating them as plain text would corrupt values.

The Python `LdifSource` forwards all shared plan options to Rust through the
PyO3 wrapper. Build and use it like this:

```console
maturin develop --release --manifest-path examples/rypipe-ldif/Cargo.toml
```

```python
from rypipe_ldif import LdifSource
table = LdifSource("directory.ldif", schema=["dn", "cn"]).to_arrow()
```

Parallel parsing is safe when blank-line boundaries remain valid. Test input
with CRLF and a final record without a trailing blank line before relying on
it for a new LDIF variant.
