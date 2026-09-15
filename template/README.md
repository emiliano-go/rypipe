# {{project-name}}

{{description}}. Starter parser reads one `key=value` pair per line into
`key` and `value` columns. Replace `AdapterParser` and `LineSplitter` for your format.

## Development

Create a virtual environment, activate it, and install the build tools:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
python -m pip install maturin pytest
maturin develop --release
python -m pytest tests
```

Template requires matching `rypipe-core` and `rypipe-python` 0.3.2 releases.
Before they are published, add local overrides to `Cargo.toml` and install
the Python engine from that checkout with `maturin develop --release`:

```toml
[patch.crates-io]
rypipe-core = { path = "/path/to/rypipe/crates/rypipe-core" }
rypipe-python = { path = "/path/to/rypipe/crates/rypipe-python" }
```

## Usage

Given `data.{{crate_name}}` containing `answer=42`:

```python
from {{crate_name}} import AdapterSource, CastTypes, read

table = (AdapterSource("data.{{crate_name}}") | CastTypes({"value": int})).to_arrow()
assert table.to_pylist() == [{"key": "answer", "value": 42}]
table = read("data.{{crate_name}}")
```

Importing the package registers its extension. Use `format="{{project-name}}"`
for files with other extensions. Install `{{project-name}}[pandas]` to call
`to_pandas()`. Starter materializes tables; streaming requires an adapter
streaming implementation.

## License

MIT
