# Stages { #stages }

Stages are the building blocks of pipelines. Each stage transforms the data
as it flows through, like a Unix pipe. **crxml** ships four of them:
`RenameFields`, `DropFields`, `CastTypes`, and `FilterRows`.

```python
from crxml import CrystalXMLSource, RenameFields, FilterRows, to_pandas

source = CrystalXMLSource("report.xml", row_tag="Details")

df = to_pandas(
    source
    | RenameFields({"Name": "name"})                        # renames a column
    | FilterRows(field="Status", op="==", value="Active")   # keeps matching rows
)
```

This page is a tour of each stage. All examples run against
[report.xml](../examples/report.xml) (15 rows, fields `Name`, `Department`,
`Amount`, `Status`, `Date`).

## RenameFields { #renamefields }

Renames columns. Columns not in the mapping pass through unchanged:

```python
from crxml import CrystalXMLSource, RenameFields

src = CrystalXMLSource("report.xml", row_tag="Details")

row = next(iter(src | RenameFields({"Name": "name", "Amount": "amount"})))
print(row)
# {'name': 'Alice Johnson', 'Department': 'Sales', 'amount': '15000.50',
#  'Status': 'Active', 'Date': '2026-01-05'}
```

## DropFields { #dropfields }

Removes columns entirely:

```python
from crxml import CrystalXMLSource, DropFields

src = CrystalXMLSource("report.xml", row_tag="Details")

row = next(iter(src | DropFields(["Status", "Date"])))
print(row)
# {'Name': 'Alice Johnson', 'Department': 'Sales', 'Amount': '15000.50'}
```

!!! warning

    `DropFields` expects a `list[str]`, not a bare string:

    ```python
    DropFields("Status")    # wrong: raises TypeError
    DropFields(["Status"])  # correct
    ```

!!! tip

    Dropped columns are the cheapest optimization. The engine skips all
    parsing work for them.

## CastTypes { #casttypes }

Casts column values to Python types. Files are text, so everything starts
as a string; `CastTypes` gives you real numbers and booleans:

```python
from crxml import CrystalXMLSource, CastTypes

src = CrystalXMLSource("report.xml", row_tag="Details")

row = next(iter(src | CastTypes({"Amount": float})))
print(row["Amount"], type(row["Amount"]))
# 15000.5 <class 'float'>
```

Supported targets: `int`, `float`, `bool` (`str` is a no-op). If the column
is missing from a row, the cast is skipped. If the cast fails (for example
`int("abc")`), a `ValueError` is raised.

## FilterRows { #filterrows }

Keeps only the rows that match a condition.

### Constant filter { #filterrows-constant }

Compare a column to a literal value with `==` (equal) or `!=` (not equal):

```python
from crxml import CrystalXMLSource, FilterRows, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

sales = src | FilterRows(field="Department", op="==", value="Sales")
print(len(collect(sales)))  # 6

not_active = src | FilterRows(field="Status", op="!=", value="Active")
print(len(collect(not_active)))  # 3
```

### Column comparison { #filterrows-compare }

Compare two columns in the same row with `field_a`, `op`, and `field_b`.
All six operators work here: `==`, `!=`, `>`, `<`, `>=`, `<=`.

```python
from crxml import CrystalXMLSource, FilterRows, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

# Data-quality check: rows where Name and Department are identical
suspect = src | FilterRows(field_a="Name", op="==", field_b="Department")
print(len(collect(suspect)))  # 0
```

This is handy for validating data (two columns that should always match, or
never match) and for row-level arithmetic relationships, like
`field_a="revenue", op=">", field_b="cost"`.

!!! warning

    Untyped columns are compared as strings, so `"9" > "100"` is true.
    Add a `CastTypes` stage (or `field_types` on the source) before
    comparing numbers.

### Null and type checks { #filterrows-null-type }

Two more keyword forms check a column's presence and type, no value needed:

```python
from crxml import CrystalXMLSource, FilterRows, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

missing_status = src | FilterRows(field="Status", is_null=True)
print(len(collect(missing_status)))  # 0, every row has a Status

strings = src | FilterRows(field="Name", is_type="string")
print(len(collect(strings)))  # 15
```

`is_null=True` keeps rows where the field is null or missing; `is_null=False`
does the opposite, keeping only rows where the field has a value, so it is
the way to drop rows with nulls. `is_type`
keeps rows where the field has the given type; valid types are `string`,
`int64`, `float64`, `bool`, `dictionary`, `date32`, `timestamp`, and
`decimal128`. Anything else raises a `ValueError`.

### Combining filters: Any, All, Not { #filterrows-combinators }

Three combinators build OR, AND, and NOT logic out of the keyword-form
filters above:

```python
from crxml import CrystalXMLSource, FilterRows, collect
from crxml import FilterRowsAny, FilterRowsAll, FilterRowsNot

src = CrystalXMLSource("report.xml", row_tag="Details")

# OR: Sales department or Inactive status
either = src | FilterRowsAny(
    FilterRows(field="Department", op="==", value="Sales"),
    FilterRows(field="Status", op="==", value="Inactive"),
)
print(len(collect(either)))  # 6

# AND: Sales department and Active status
both = src | FilterRowsAll(
    FilterRows(field="Department", op="==", value="Sales"),
    FilterRows(field="Status", op="==", value="Active"),
)
print(len(collect(both)))  # 3

# NOT: everything except Active rows
not_active = src | FilterRowsNot(FilterRows(field="Status", op="==", value="Active"))
print(len(collect(not_active)))  # 3
```

`FilterRowsAny` and `FilterRowsAll` take two or more filters; `FilterRowsNot`
takes exactly one. They nest, so
`FilterRowsAny(A, FilterRowsAll(B, FilterRowsNot(C)))` expresses
`A or (B and not C)`. The inner filters must use a keyword form (constant,
column comparison, `is_null`, or `is_type`) or an expression predicate; a
plain callable cannot be combined this way and raises `ValueError`.

!!! tip

    Chaining two `FilterRows` stages with `|` is already an implicit AND,
    so reach for `FilterRowsAll` mainly when nesting inside `FilterRowsAny`
    or `FilterRowsNot`.

All three combinators are fusable: the whole tree is pushed into the Rust
parse loop, exactly like a single keyword filter.

### Regex filter { #filterrows-regex }

Match a column against a regular expression with `op="regex"`. The pattern
is searched (not anchored) against the string form of the value, and rows
with a missing or null field are dropped:

```python
from crxml import CrystalXMLSource, FilterRows, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

# Names that start with "A"
a_names = src | FilterRows(field="Name", op="regex", value=r"^A")
print(len(collect(a_names)))  # 1
```

An invalid pattern raises at construction time.

### Expression predicates { #filterrows-expr }

For anything beyond a single keyword comparison, build the predicate with
`col()` from the expression API. Expressions compose with `&` (and), `|`
(or), and `~` (not), and the whole tree fuses into the Rust parse loop:

```python
from crxml import CrystalXMLSource, FilterRows, col, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

active_sales = src | FilterRows(
    (col("Department") == "Sales") & (col("Status") == "Active")
)
print(len(collect(active_sales)))  # 3

# Inclusive range and regex helpers
mid = src | FilterRows(col("Amount").between(5000, 15000))
errors = src | FilterRows(col("Name").matches(r"^A"))
```

Available methods: `startswith`, `endswith`, `contains`, `matches` (regex
search), `between` (inclusive range), `isin`, `not_in`, `is_null`,
`is_not_null`, `is_type`, plus the six comparison operators, which also work
column-to-column (`col("a") > col("b")`).

### Callable predicate { #filterrows-callable }

For arbitrary logic, pass a function that receives the row dict and
returns `True` to keep the row:

```python
from crxml import CrystalXMLSource, FilterRows, collect

src = CrystalXMLSource("report.xml", row_tag="Details")

a_names = src | FilterRows(lambda r: r["Name"].startswith("A"))
print([r["Name"] for r in collect(a_names)])
# ['Alice Johnson']
```

Plain lambdas and functions always run in Python, row by row, after the
table is parsed; they are never fused into the Rust parse loop. Prefer the
keyword forms or `col()` expressions when throughput matters. (The old
in-tree lambda bytecode compiler was removed; see the standalone
[lambda-compiler](https://github.com/emiliano-go/lambda-compiler) package if
you need that.)

For the full filter-spec format (including column-to-column comparison),
see the [Python API reference](../reference/python-api.md#filterrows).

## Combining stages { #combining-stages }

Stages compose freely, and order matters: they run left to right. Chaining
two `FilterRows` keeps rows matching **both** (logical AND):

```python
from crxml import CrystalXMLSource
from crxml import RenameFields, DropFields, CastTypes, FilterRows, to_pandas

src = CrystalXMLSource("report.xml", row_tag="Details")

df = to_pandas(
    src
    | RenameFields({"Name": "name", "Amount": "amount"})
    | DropFields(["Date"])
    | CastTypes({"amount": float})
    | FilterRows(field="Status", op="==", value="Active")
    | FilterRows(field="Department", op="!=", value="Sales")
)
print(df.head(3))
```

```console
          name   Department    amount  Status
0    Bob Smith  Engineering    8500.0  Active
1  Carol White    Marketing  12000.75  Active
2    Eve Davis  Engineering   11200.0  Active
```

## Recap { #recap }

* `RenameFields` renames columns; `DropFields` removes them.
* `CastTypes` converts strings to `int`, `float`, or `bool`.
* `FilterRows` keeps matching rows: constant (`field`/`op`/`value`,
  including `op="regex"`), column comparison (`field_a`/`op`/`field_b`),
  null check (`is_null=True`), type check (`is_type="..."`), an expression
  predicate (`col("x") > 1`, `col("n").between(a, b)`), or a lambda. Keyword
  forms and expressions compile into Rust filter operations; plain lambdas
  run in Python.
* `FilterRowsAny` / `FilterRowsAll` / `FilterRowsNot` combine keyword-form
  filters into OR / AND / NOT trees, and nest.
* Full parameter reference: [Python API](../reference/python-api.md#stages).

**Next:** [Sinks](sinks.md#sinks), getting data out of sources and
pipelines.
