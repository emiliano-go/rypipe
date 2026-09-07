# Lambda Compiler { #lambda-compiler }

When you pass a lambda to `FilterRows`, **rypipe** analyzes its bytecode at
construction time and tries to compile it into a fusable filter spec. Detected
patterns are pushed into the Rust parse loop; unknown patterns fall back to
Python execution.

## How it works { #how-it-works }

```
FilterRows(lambda r: r["amount"] > 100)
  │
  ▼
_analyze_lambda(fn) → dict | None
  │  Inspects bytecode via dis.get_instructions()
  │
  ├─ pattern detected → convert to filter spec dict
  │  {"field": "amount", "op": ">", "value": "100"}
  │  → fusable via existing Rust CompareLiteral path
  │
  └─ pattern unknown → fall back to Python (current behavior)
```

The compiler runs once, at `FilterRows` construction time. If the lambda
matches a known pattern, the original lambda is replaced with a fusable
predicate. If not, the lambda runs in Python as before.

## Supported patterns { #supported-patterns }

| Pattern | Example | Compiled to |
|---------|---------|-------------|
| field op literal | `r["amount"] > 100` | `CompareLiteral` |
| field_a op field_b | `r["price"] > r["cost"]` | `Compare` |
| field.startswith("x") | `r["name"].startswith("A")` | `StartsWith` |
| field.endswith("x") | `r["name"].endswith("z")` | `EndsWith` |
| field.contains("x") | `r["name"].contains("ali")` | `Contains` |
| field.strip() op "x" | `r["name"].strip() == "Alice"` | `Strip` |
| field.lower() op "x" | `r["name"].lower() == "alice"` | `Lower` |
| field.upper() op "x" | `r["name"].upper() == "ALICE"` | `Upper` |
| field.replace(a,b) op "x" | `r["name"].replace("o","x") == "Bxb"` | `Replace` |
| len(field) op N | `len(r["name"]) > 3` | `Length` |
| field in tuple/list | `r["s"] in ("a","b")` | `In` |
| field in frozenset | `r["s"] in frozenset({"a","b"})` | `In` |
| not field | `not r["active"]` | `NotField` |
| not field.startswith("x") | `not r["name"].startswith("A")` | `Not(StartsWith)` |
| not field.endswith("x") | `not r["name"].endswith("z")` | `Not(EndsWith)` |
| True/False | `lambda r: True` | `Always` |
| and | `r["a"] > 1 and r["b"] < 2` | `And(...)` |
| or | `r["a"] > 1 or r["b"] < 2` | `Or(...)` |
| nested compound | `(r["a"] > 1 or r["b"] < 2) and r["c"] == "x"` | `And(Or(...), ...)` |
| closure constant | `threshold=100; r["x"] > threshold` | `CompareLiteral` (captured at construction) |
| cast + compare | `int(r["age"]) > 30` | `CompareLiteral` (typed) |
| arithmetic | `r["x"] * 2 > 100` | `ArithmeticCompare` |

### How pattern detection works { #pattern-detection }

The compiler uses `dis.get_instructions()` to inspect the lambda's bytecode.
Each pattern has a specific instruction sequence:

**field op literal:**
```
LOAD_FAST(r)  →  LOAD_CONST(field)  →  BINARY_OP([])
LOAD_CONST(value)  →  COMPARE_OP(op)  →  RETURN_VALUE
```

**field.startswith("x"):**
```
LOAD_FAST(r)  →  LOAD_CONST(field)  →  BINARY_OP([])
LOAD_ATTR(startswith)  →  LOAD_CONST(arg)  →  CALL  →  RETURN_VALUE
```

**compound AND:**
```
... first comparison ...
COPY  →  TO_BOOL  →  POP_JUMP_IF_FALSE(target)
... second comparison ...
target: RETURN_VALUE
```

The compiler filters out `RESUME`, `CACHE`, `COPY`, `TO_BOOL`, and jump
instructions before pattern matching. This makes it resilient to Python
version differences in bytecode encoding.

## Limitations { #limitations }

The compiler detects common patterns but cannot handle everything:

### Closures with non-constant values { #closures }

```python
import datetime
now = datetime.now()
FilterRows(lambda r: r["timestamp"] > now)  # falls back to Python
```

The compiler resolves closures at construction time, but only for constant
types (int, float, str, bool). Complex objects like datetime are not resolved.

### Method chains { #method-chains }

```python
FilterRows(lambda r: r["name"].strip().lower() == "alice")  # falls back
```

Single method calls are supported (`strip()`, `lower()`, `upper()`), but
chains with multiple calls are not. Use keyword form for multi-step transforms.

### Deeply nested compound logic { #nested-compound }

Single-level nesting is supported:

```python
# Supported:
FilterRows(lambda r: (r["a"] > 1 or r["b"] < 2) and r["c"] == "x")

# Falls back (3+ levels):
FilterRows(lambda r: (r["a"] > 1 and r["b"] < 2) or (r["c"] == "x" and r["d"] > 0))
```

Use keyword combinators for deeply nested logic:

```python
from my_adapter import FilterRowsAny, FilterRowsAll, FilterRowsNot

FilterRowsAny(
    FilterRowsAll(
        FilterRows(field="a", op=">", value="1"),
        FilterRows(field="b", op="<", value="2"),
    ),
    FilterRowsAll(
        FilterRows(field="c", op="==", value="x"),
        FilterRows(field="d", op=">", value="0"),
    ),
)
```

## What happens when compilation fails { #compilation-fallback}

When the compiler cannot match a pattern, it returns `None` and the original
lambda runs in Python:

```python
# This lambda falls back to Python execution
f = FilterRows(lambda r: r["name"].strip().lower() == "alice")

# f._filter_spec is None (not fusable)
# f._predicate is the original lambda (runs in Python)
```

The filter still works correctly, but it runs in Python over the full table
instead of in the Rust parse loop. For small files this is fine. For large
files, consider rewriting the lambda as a keyword form.

## Performance impact { #performance}

For the common case (`r["field"] > value`), the compiler eliminates:

* Python function call overhead per row (~50ns)
* Python comparison overhead per row (~20ns)
* The need for a `CastTypes` stage when comparing numbers

On a 10M row file, this saves ~700ms of Python overhead.

## Diagnostics { #diagnostics}

To check whether a lambda was compiled, inspect the filter spec:

```python
f = FilterRows(lambda r: r["amount"] > 100)
print(f._filter_spec)
# {'field': 'amount', 'op': '>', 'value': '100'}  ← compiled

f2 = FilterRows(lambda r: r["name"].strip().lower() == "alice")
print(f2._filter_spec)
# None  ← fell back to Python (method chain)
```

## Recap { #recap }

* The lambda compiler analyzes bytecode at `FilterRows` construction time.
* Common patterns are compiled to fusable filter specs:
  - Field comparisons: `r["field"] > 100`, `r["a"] > r["b"]`
  - String methods: `r["name"].startswith("A")`, `r["name"].endswith("z")`, `r["name"].contains("x")`
  - String transforms: `r["name"].strip()`, `.lower()`, `.upper()`, `.replace()`
  - Length: `len(r["name"]) > 3`
  - Membership: `r["status"] in ("active", "pending")`, `r["s"] in frozenset({...})`
  - Truthiness: `not r["active"]`, `not r["name"].startswith("A")`
  - Compound logic: `a and b`, `a or b`, nested `(a or b) and c`
  - Cast + compare: `int(r["age"]) > 30`
  - Arithmetic: `r["amount"] * 2 > 100`
  - Closures: `threshold=100; r["x"] > threshold` (captured at construction)
  - Constants: `lambda r: True`, `lambda r: False`
* Unknown patterns (method chains, non-constant closures) fall back to Python.
* The compiler is a best-effort optimization: if it cannot detect a pattern,
  the lambda still works correctly, just slower.
