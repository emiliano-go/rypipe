# Expression filters { #expression-filters }

`FilterRows` accepts an expression predicate built with `rypipe.expr.col`.
Expressions construct the same filter spec dicts the Rust engine fuses,
declaratively and without any bytecode analysis:

```python
from rypipe import col, FilterRows

source | FilterRows(col("amount") > 100)
source | FilterRows((col("age") >= 18) & col("name").startswith("A"))
```

Anything the spec language cannot express raises at construction time, so
there is no silent fallback to per-row Python.

## How it works { #how-it-works }

```
col("amount") > 100
  │
  ▼
Predicate({"field": "amount", "op": ">", "value": "100"})
  │
  ▼
FilterRows(predicate) → _plan_kwargs() → {"filter": spec}
  │
  ▼
ExecutionPlan.filter = CompareLiteral { field: "amount", op: CompareOp::Gt, value: "100" }
```

An expression compares to a literal or another column; the result is a
`Predicate` whose `_to_spec()` produces the plan spec. `Predicate` objects
compose with `&` (and), `|` (or), and `~` (not), producing nested
`{"and": [...]}`, `{"or": [...]}`, `{"not": ...}` specs.

## Supported operations { #supported-operations }

| Expression | Spec |
|------------|------|
| `col("x") == 1`, `!=`, `>`, `<`, `>=`, `<=` | `{"field": "x", "op": "==", "value": "1"}` |
| `col("a") > col("b")` | `{"field_a": "a", "op": ">", "field_b": "b"}` |
| `col("s").startswith("A")` | `{"op": "starts_with", ...}` |
| `col("s").endswith("z")` | `{"op": "ends_with", ...}` |
| `col("s").contains("x")` | `{"op": "contains", ...}` |
| `col("s").matches(r"^ERR\d+$")` | `{"op": "regex", ...}` |
| `col("n").between(2, 6)` | `{"and": [{">="}, {"<="}]}` (inclusive) |
| `col("s").isin(["a", "b"])` | `{"op": "in", "values": [...]}` |
| `col("s").not_in(["a"])` | `{"op": "not_in", "values": [...]}` |
| `col("x").is_null()` | `{"op": "is_null"}` |
| `col("x").is_not_null()` | `{"not": {"op": "is_null"}}` |
| `col("x").is_type("int64")` | `{"op": "is_type", "value": "int64"}` |
| `p & q`, `p \| q`, `~p` | `{"and": ...}`, `{"or": ...}`, `{"not": ...}` |

Literals may be `int`, `float`, `str`, or `bool`; they are coerced to the
string form specs carry, and the engine compares with native-typed numeric
promotion. `matches()` validates the pattern with `re.compile` at
construction time and the engine applies it as a regex search against the
string form of the value.

## Plain callables fall back to Python { #python-fallback }

A plain lambda or function still works as a `FilterRows` predicate, but it
is opaque to fusion: it runs in Python per row after the table is parsed.
Earlier versions shipped an in-tree bytecode analyzer that tried to compile
lambdas into specs; that was removed in favor of this explicit expression
API. If you want lambda compilation, use the standalone
[lambda-compiler](https://github.com/emiliano-go/lambda-compiler) package.

```python
# Runs in Python; not fusable
f = FilterRows(lambda r: r["name"].strip().lower() == "alice")

# f._filter_spec is None (not fusable)
# f._predicate is the original lambda (runs in Python)
```

The filter still works correctly, just slower. For large files, rewrite the
predicate with `col(...)` or the keyword form.

## Diagnostics { #diagnostics }

Inspect the spec to confirm a predicate is fusable:

```python
f = FilterRows(col("amount") > 100)
print(f._filter_spec)
# {'field': 'amount', 'op': '>', 'value': '100'}

f2 = FilterRows(lambda r: r["name"].strip().lower() == "alice")
print(f2._filter_spec)
# None  (Python fallback)
```

## Recap { #recap }

* `col(name)` starts an expression; comparisons and methods produce
  `Predicate` objects that carry a fusable spec.
* `Predicate` composes with `&`, `|`, `~` into arbitrarily nested trees.
* Regex (`matches`) and inclusive ranges (`between`) are first-class.
* Plain lambdas are never analyzed; they run in Python as a fallback.
