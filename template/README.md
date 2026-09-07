# {{project-name}}

A rypipe adapter for [your format].

## Installation

```bash
pip install {{project-name}}
```

## Usage

```python
from {{project-name}} import {{project-name}}Source
import rypipe

source = {{project-name}}Source("path/to/file")
df = rypipe.read(source).to_pandas()
```

## Development

```bash
# Install maturin
pip install maturin

# Build and install
maturin develop

# Run tests
cargo test
maturin develop --release
```

## License

MIT
