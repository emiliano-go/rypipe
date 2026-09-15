# INI adapter example

`examples/rypipe-ini` turns INI key/value lines into one row per setting. The
Rust `IniParser` keeps the current `[section]`, skips blank and `;`/`#`
comment lines, and accepts both `=` and `:` separators. The `IniSplitter`
chooses section starts as safe parallel boundaries, while the parser carries
section state so each emitted key retains its section name.

The Python `IniSource` is deliberately thin. It inherits `rypipe.Adapter` and
forwards the path and keyword plan options to the PyO3 `read_ini` function.
That keeps projection, renames, types, filters, dictionary settings, mmap,
and prefault behavior in the shared core.

Build/install the workspace adapter, then read a file:

```console
maturin develop --release --manifest-path examples/rypipe-ini/Cargo.toml
```

```python
from rypipe_ini import IniSource
table = IniSource("settings.ini", field_types={"port": "int64"}).to_arrow()
```

The adapter intentionally handles plain INI syntax only. Escapes and
continuations need format-specific rules before adding support.
