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

| Pattern | Example | Bytecode signature | Compiled to |
|---------|---------|-------------------|-------------|
| field op literal | `r["amount"] > 100` | `LOAD_FAST(r)` → `LOAD_CONST(field)` → `LOAD_CONST(value)` → `COMPARE_OP(op)` | `CompareLiteral` |
| field_a op field_b | `r["price"] > r["cost"]` | Two `LOAD_FAST(r)` → `LOAD_CONST` sequences | `Compare` |
| field.startswith("x") | `r["name"].startswith("A")` | `LOAD_ATTR(startswith)` → `CALL` | `StartsWith` |
| field.endswith("x") | `r["name"].endswith("z")` | `LOAD_ATTR(endswith)` → `CALL` | `EndsWith` |
| compound AND | `r["a"] > 1 and r["b"] < 2` | `COPY`/`TO_BOOL`/`POP_JUMP_IF_FALSE` | `And(CompareLiteral, CompareLiteral)` |
| cast + compare | `int(r["age"]) > 30` | `LOAD_GLOBAL(int)` → `CALL` → `COMPARE_OP` | Not yet supported |

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

## Limitations { #limitations}

The compiler detects common patterns but cannot handle everything:

### Closures { #closures}

```python
threshold = 100
FilterRows(lambda r: r["amount"] > threshold)  # falls back to Python
```

The value `threshold` is loaded via `LOAD_GLOBAL`, not `LOAD_CONST`. The
compiler cannot resolve it at construction time.

### Nested function calls { #nested-calls}

```python
FilterRows(lambda r: int(r["amount"]) > 100)  # falls back to Python
```

The `int()` call introduces a `CALL` instruction that the compiler does not
handle. You can work around this by using `CastTypes` + a simple comparison:

```python
# Instead of:
FilterRows(lambda r: int(r["amount"]) > 100)

# Use:
source | CastTypes({"amount": int}) | FilterRows(field="amount", op=">", value="100")
```

### Complex boolean logic { #complex-boolean-logic}

```python
# This works (compound AND):
FilterRows(lambda r: r["a"] > 1 and r["b"] < 2)

# This does not (OR, NOT, nested):
FilterRows(lambda r: r["a"] > 1 or r["b"] < 2)  # falls back to Python
```

The compiler only detects AND. For OR and NOT, use the keyword combinators:

```python
from my_adapter import FilterRowsAny, FilterRowsNot

FilterRowsAny(
    FilterRows(field="a", op=">", value="1"),
    FilterRows(field="b", op="<", value="2"),
)
```

### String methods { #string-methods}

Only `startswith` and `endswith` are supported. Other string methods
(`strip`, `lower`, `contains`, etc.) fall back to Python.

### Arithmetic and math { #arithmetic-and-math}

```python
FilterRows(lambda r: r["amount"] * 2 > 100)  # falls back to Python
```

Any expression beyond simple comparison is not detected.

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

f2 = FilterRows(lambda r: r["name"].strip() == "alice")
print(f2._filter_spec)
# None  ← fell back to Python
```

## Recap { #recap }

* The lambda compiler analyzes bytecode at `FilterRows` construction time.
* Common patterns (field comparisons, startswith, compound AND) are compiled
  to fusable filter specs.
* Unknown patterns (closures, nested calls, complex logic) fall back to Python.
* The compiler is a best-effort optimization: if it cannot detect a pattern,
  the lambda still works correctly, just slower.
