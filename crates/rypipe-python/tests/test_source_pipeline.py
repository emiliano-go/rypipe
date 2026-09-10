"""Tests for the rypipe Source/Pipeline/stages/sinks API."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

import rypipe
from rypipe import (
    Adapter,
    CastTypes,
    DropFields,
    FilterRows,
    FilterRowsAll,
    FilterRowsAny,
    FilterRowsNot,
    RenameFields,
    Source,
    collect,
    to_arrow,
    to_csv,
    to_pandas,
    to_polars,
)
from rypipe.fusion import plan_split


_TYPE_MAP = {
    "int64": pa.int64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
}


def _apply_plan(table: pa.Table, plan: dict):
    """Apply simple pushdown plan kwargs to a pyarrow Table."""
    import pyarrow.compute as pc

    # Rename.
    mapping = plan.get("field_mapping") or {}
    if mapping:
        table = table.rename_columns([mapping.get(n, n) for n in table.column_names])

    # Drop.
    drop_fields = plan.get("drop_fields") or []
    if drop_fields:
        keep = [i for i, n in enumerate(table.column_names) if n not in drop_fields]
        table = pa.table(
            [table.column(i) for i in keep], names=[table.column_names[i] for i in keep]
        )

    # Cast.
    field_types = plan.get("field_types") or {}
    if field_types:
        columns = []
        names = []
        for name in table.column_names:
            col = table.column(name)
            if name in field_types:
                target = _TYPE_MAP.get(field_types[name])
                if target is not None:
                    col = pc.cast(col, target)
            columns.append(col)
            names.append(name)
        table = pa.table(columns, names=names)

    # Filter: recurses into and/or/not trees so the mock matches Rust Check semantics.
    def _mask_for(spec) -> "pa.Array":
        if "and" in spec:
            masks = [_mask_for(s) for s in spec["and"]]
            out = masks[0]
            for m in masks[1:]:
                out = pc.and_(out, m)
            return out
        if "or" in spec:
            masks = [_mask_for(s) for s in spec["or"]]
            out = masks[0]
            for m in masks[1:]:
                out = pc.or_(out, m)
            return out
        if "not" in spec:
            inner = _mask_for(spec["not"])
            return pc.invert(inner)
        if "always" in spec:
            val = spec["always"]
            return pa.array([val] * table.num_rows)
        if "not_field" in spec:
            col = table.column(spec["not_field"])
            m = pc.or_(pc.equal(col, ""), pc.is_null(col))
            return pc.fill_null(m, True)
        if "old" in spec and "new" in spec and "field" in spec:
            field = spec["field"]
            col = pc.replace_substring(table.column(field), spec["old"], spec["new"])
            cmp_op = spec.get("cmp_op", "==")
            cmp_fn = {
                "==": lambda a, b: pc.equal(a, b),
                "!=": lambda a, b: pc.not_equal(a, b),
                ">": lambda a, b: pc.greater(a, b),
                "<": lambda a, b: pc.less(a, b),
                ">=": lambda a, b: pc.greater_equal(a, b),
                "<=": lambda a, b: pc.less_equal(a, b),
            }[cmp_op]
            m = cmp_fn(col, spec["value"])
            return pc.fill_null(m, False)
        if "field" in spec and "op" in spec:
            field, op = spec["field"], spec["op"]
            if "values" in spec:
                values = spec["values"]
                m = pc.is_in(table.column(field), pa.array(values))
                if op == "not_in":
                    m = pc.invert(m)
                return pc.fill_null(m, False)
            if op == "starts_with":
                m = pc.starts_with(table.column(field), spec["value"])
            elif op == "ends_with":
                m = pc.ends_with(table.column(field), spec["value"])
            elif op == "contains":
                m = pc.match_substring(table.column(field), spec["value"])
            elif op in ("strip", "lstrip", "rstrip", "lower", "upper"):
                col = table.column(field)
                transforms = {
                    "strip": lambda c: pc.utf8_trim_whitespace(c),
                    "lstrip": lambda c: pc.utf8_ltrim_whitespace(c),
                    "rstrip": lambda c: pc.utf8_rtrim_whitespace(c),
                    "lower": lambda c: pc.utf8_lower(c),
                    "upper": lambda c: pc.utf8_upper(c),
                }
                col = transforms[op](col)
                cmp_op = spec.get("cmp_op", "==")
                cmp_fn = {
                    "==": lambda a, b: pc.equal(a, b),
                    "!=": lambda a, b: pc.not_equal(a, b),
                    ">": lambda a, b: pc.greater(a, b),
                    "<": lambda a, b: pc.less(a, b),
                    ">=": lambda a, b: pc.greater_equal(a, b),
                    "<=": lambda a, b: pc.less_equal(a, b),
                }[cmp_op]
                m = cmp_fn(col, spec["value"])
            elif op == "length":
                col = pc.utf8_length(table.column(field))
                cmp_op = spec.get("cmp_op", ">")
                cmp_val = pa.array([int(spec["value"])] * table.num_rows)
                cmp_fn = {
                    "==": lambda a, b: pc.equal(a, b),
                    "!=": lambda a, b: pc.not_equal(a, b),
                    ">": lambda a, b: pc.greater(a, b),
                    "<": lambda a, b: pc.less(a, b),
                    ">=": lambda a, b: pc.greater_equal(a, b),
                    "<=": lambda a, b: pc.less_equal(a, b),
                }[cmp_op]
                m = cmp_fn(col, cmp_val)
            else:
                fn_name = {
                    ">": "greater", "gt": "greater",
                    "<": "less", "lt": "less",
                    ">=": "greater_equal", "ge": "greater_equal",
                    "<=": "less_equal", "le": "less_equal",
                    "==": "equal", "eq": "equal",
                    "!=": "not_equal", "ne": "not_equal",
                }[op]
                m = getattr(pc, fn_name)(table.column(field), spec["value"])
            return pc.fill_null(m, False)
        if "field_a" in spec and "op" in spec:
            field_a, op, field_b = spec["field_a"], spec["op"], spec["field_b"]
            fn_name = {
                ">": "greater", "gt": "greater",
                "<": "less", "lt": "less",
                ">=": "greater_equal", "ge": "greater_equal",
                "<=": "less_equal", "le": "less_equal",
                "==": "equal", "eq": "equal",
                "!=": "not_equal", "ne": "not_equal",
            }[op]
            return pc.fill_null(getattr(pc, fn_name)(table.column(field_a), table.column(field_b)), False)
        raise ValueError(f"Unknown filter spec: {spec}")

    spec = plan.get("filter")
    if spec:
        table = table.filter(_mask_for(spec))

    return table


class _MockSource(Source):
    """In-memory source that returns a fixed table."""

    __slots__ = ("_table",)

    def __init__(self, table: pa.Table, **kwargs):
        # Bypass path validation by not calling super().__init__ directly.
        self._path = Path("mock")
        self._field_mapping = kwargs.get("field_mapping", {})
        self._drop_fields = kwargs.get("drop_fields", [])
        self._filter = kwargs.get("filter", None)
        self._field_types = kwargs.get("field_types", {})
        self._dictionary_columns = kwargs.get("dictionary_columns", [])
        self._schema = kwargs.get("schema", [])
        self._auto_dict = kwargs.get("auto_dict", False)
        self._strict_types = kwargs.get("strict_types", False)
        self._use_mmap = kwargs.get("use_mmap", True)
        self._batch_size = kwargs.get("batch_size", 1024)
        self._observer = kwargs.get("observer")
        self._cached_arrow = None
        self._table = table

    def _read_arrow(self, plan_overrides=None):
        plan = self._build_plan_kwargs()
        if plan_overrides:
            plan.update(plan_overrides)
        return _apply_plan(self._table, plan)


@pytest.fixture
def sample_table():
    return pa.table(
        {
            "name": ["Alice", "Bob", "Carol"],
            "age": ["30", "25", "35"],
            "city": ["NYC", "LA", "CHI"],
        }
    )


def test_source_iteration(sample_table):
    src = _MockSource(sample_table)
    rows = list(src)
    assert rows == [
        {"name": "Alice", "age": "30", "city": "NYC"},
        {"name": "Bob", "age": "25", "city": "LA"},
        {"name": "Carol", "age": "35", "city": "CHI"},
    ]


def test_source_to_arrow(sample_table):
    src = _MockSource(sample_table)
    assert src.to_arrow().equals(sample_table)


def test_source_schema(sample_table):
    src = _MockSource(sample_table)
    assert src.schema() == ["name", "age", "city"]


def test_pipeline_rename(sample_table):
    src = _MockSource(sample_table)
    pipe = src | RenameFields({"name": "full_name"})
    rows = collect(pipe)
    assert rows[0] == {"full_name": "Alice", "age": "30", "city": "NYC"}


def test_pipeline_drop(sample_table):
    src = _MockSource(sample_table)
    pipe = src | DropFields(["age"])
    rows = collect(pipe)
    assert "age" not in rows[0]
    assert "name" in rows[0]


def test_pipeline_filter_constant(sample_table):
    src = _MockSource(sample_table)
    pipe = src | FilterRows(field="city", op="==", value="LA")
    rows = collect(pipe)
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_pipeline_filter_compare(sample_table):
    src = _MockSource(sample_table)
    # Compare filters run after the table is assembled.
    pipe = src | FilterRows(field_a="age", op=">", field_b="age")
    rows = collect(pipe)
    assert len(rows) == 0


def test_pipeline_cast(sample_table):
    src = _MockSource(sample_table)
    pipe = src | CastTypes({"age": int})
    rows = collect(pipe)
    assert rows[0]["age"] == 30


def test_pipeline_chain_fusion(sample_table):
    src = _MockSource(sample_table)
    pipe = (
        src
        | RenameFields({"name": "full_name"})
        | DropFields(["city"])
        | FilterRows(field="full_name", op="!=", value="Bob")
    )
    rows = collect(pipe)
    assert len(rows) == 2
    assert "city" not in rows[0]
    assert rows[0]["full_name"] == "Alice"


def test_pipeline_to_pandas(sample_table, pandas):
    src = _MockSource(sample_table)
    pipe = src | RenameFields({"name": "full_name"})
    df = to_pandas(pipe)
    assert list(df.columns) == ["full_name", "age", "city"]


def test_source_to_polars(sample_table, polars):
    src = _MockSource(sample_table)
    df = src.to_polars()
    assert df.columns == ["name", "age", "city"]
    assert df.height == 3
    assert df["name"].to_list() == ["Alice", "Bob", "Carol"]


def test_pipeline_to_polars(sample_table, polars):
    src = _MockSource(sample_table)
    pipe = src | RenameFields({"name": "full_name"}) | DropFields(["city"])
    df = to_polars(pipe)
    assert df.columns == ["full_name", "age"]
    assert df.height == 3


def test_pipeline_to_polars_after_filter(sample_table, polars):
    src = _MockSource(sample_table)
    pipe = src | FilterRows(field="city", op="==", value="LA")
    df = to_polars(pipe)
    assert df.height == 1
    assert df["name"].to_list() == ["Bob"]


def test_to_polars_from_records(polars):
    # Plain iterables take the pa.Table.from_pylist path in to_arrow.
    df = to_polars([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    assert df.columns == ["a", "b"]
    assert df["a"].to_list() == [1, 2]


def test_pipeline_to_arrow(sample_table):
    src = _MockSource(sample_table)
    pipe = src | DropFields(["age"])
    table = to_arrow(pipe)
    assert table.column_names == ["name", "city"]


def test_pipeline_to_csv(sample_table, tmp_path):
    src = _MockSource(sample_table)
    pipe = src | DropFields(["city"])
    out = tmp_path / "out.csv"
    to_csv(pipe, out)
    text = out.read_text()
    assert "name,age" in text
    assert "Alice,30" in text


def test_source_to_parquet(sample_table, tmp_path):
    src = _MockSource(sample_table)
    out = tmp_path / "out.parquet"
    src.to_parquet(out)
    assert out.exists()
    assert out.stat().st_size > 0


# --- Streaming sink tests (memory= parameter) ---


def test_source_to_pandas_memory(sample_table):
    src = _MockSource(sample_table)
    df = src.to_pandas(memory="1MB")
    assert list(df.columns) == ["name", "age", "city"]
    assert len(df) == 3


def test_source_to_polars_memory(sample_table, polars):
    src = _MockSource(sample_table)
    df = src.to_polars(memory="1MB")
    assert df.columns == ["name", "age", "city"]
    assert df.height == 3


def test_source_to_parquet_memory(sample_table, tmp_path):
    src = _MockSource(sample_table)
    out = tmp_path / "out.parquet"
    src.to_parquet(out, memory="1MB")
    assert out.exists()
    assert out.stat().st_size > 0


def test_pipeline_to_pandas_memory(sample_table):
    src = _MockSource(sample_table)
    pipe = src | RenameFields({"name": "full_name"})
    df = pipe.to_pandas(memory="1MB")
    assert list(df.columns) == ["full_name", "age", "city"]
    assert len(df) == 3


def test_pipeline_to_polars_memory(sample_table, polars):
    src = _MockSource(sample_table)
    pipe = src | RenameFields({"name": "full_name"}) | DropFields(["city"])
    df = pipe.to_polars(memory="1MB")
    assert df.columns == ["full_name", "age"]
    assert df.height == 3


def test_pipeline_to_parquet_memory(sample_table, tmp_path):
    src = _MockSource(sample_table)
    pipe = src | DropFields(["city"])
    out = tmp_path / "out.parquet"
    pipe.to_parquet(out, memory="1MB")
    assert out.exists()
    assert out.stat().st_size > 0


def test_to_pandas_function_memory(sample_table):
    src = _MockSource(sample_table)
    df = to_pandas(src, memory="1MB")
    assert list(df.columns) == ["name", "age", "city"]
    assert len(df) == 3


def test_to_polars_function_memory(sample_table, polars):
    src = _MockSource(sample_table)
    df = to_polars(src, memory="1MB")
    assert df.columns == ["name", "age", "city"]
    assert df.height == 3


def test_collect_memory(sample_table):
    src = _MockSource(sample_table)
    rows = collect(src, memory="1MB")
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"


def test_source_clear_cache(sample_table):
    src = _MockSource(sample_table)
    first = src.to_arrow()
    src.clear_cache()
    second = src.to_arrow()
    assert first.equals(second)


def test_filter_rows_invalid_constant_op():
    with pytest.raises(ValueError):
        FilterRows(field="x", op="like", value="1")


def test_filter_rows_constant_ordering():
    """Constant filters now support ordering operators (>, <, >=, <=)."""
    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
        "age": ["30", "25", "35"],
    }))
    # age > "28" should keep Alice (30) and Carol (35)
    rows = collect(src | FilterRows(field="age", op=">", value="28"))
    assert len(rows) == 2
    assert {r["name"] for r in rows} == {"Alice", "Carol"}

    # age <= "25" should keep Bob (25)
    rows = collect(src | FilterRows(field="age", op="<=", value="25"))
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_filter_rows_constant_ordering_fusion():
    """Ordering constant filters should fuse into the Rust plan."""
    src = _MockSource(pa.table({
        "name": ["Alice", "Bob"],
        "age": ["30", "25"],
    }))
    pipe = src | FilterRows(field="age", op=">", value="28")
    # Should produce a fused result (2 rows where age > "28")
    rows = collect(pipe)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_filter_rows_invalid_compare_op():
    with pytest.raises(ValueError):
        FilterRows(field_a="x", op="like", field_b="y")


def test_expr_field_gt_literal():
    """col('age') > '28' should build a fusable spec."""
    from rypipe import col

    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
        "age": ["30", "25", "35"],
    }))
    f = FilterRows(col("age") > "28")
    assert f._filter_spec is not None
    assert f._filter_spec["op"] == ">"
    rows = collect(src | f)
    assert sorted([r["name"] for r in rows]) == ["Alice", "Carol"]


def test_expr_field_a_gt_field_b():
    """col('price') > col('cost') should compile to a column comparison."""
    from rypipe import col

    src = _MockSource(pa.table({
        "item": ["A", "B"],
        "price": ["700", "50"],
        "cost": ["60", "80"],
    }))
    f = FilterRows(col("price") > col("cost"))
    assert f._filter_spec is not None
    assert f._filter_spec["op"] == ">"
    rows = collect(src | f)
    assert len(rows) == 1
    assert rows[0]["item"] == "A"


def test_expr_startswith():
    from rypipe import col

    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
    }))
    f = FilterRows(col("name").startswith("A"))
    assert f._filter_spec is not None
    assert f._filter_spec["op"] == "starts_with"
    rows = collect(src | f)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_expr_compound_and():
    from rypipe import col

    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
        "age": ["30", "25", "35"],
    }))
    f = FilterRows((col("age") > "28") & col("name").startswith("A"))
    assert f._filter_spec is not None
    assert "and" in f._filter_spec
    rows = collect(src | f)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_lambda_plain_fallback():
    """Plain lambdas are no longer analyzed; they run as Python fallback."""
    threshold = 100
    f = FilterRows(lambda r: r["amount"] > threshold)
    assert f._filter_spec is None
    assert f._plan_kwargs() is None


def test_expr_not_startswith():
    from rypipe import col

    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
    }))
    f = FilterRows(~col("name").startswith("A"))
    assert f._filter_spec is not None
    assert "not" in f._filter_spec
    rows = collect(src | f)
    assert len(rows) == 2
    assert {r["name"] for r in rows} == {"Bob", "Carol"}


def test_expr_nested_compound_or_and():
    from rypipe import col

    src = _MockSource(pa.table({
        "a": ["1", "3", "1", "5"],
        "b": ["3", "1", "5", "1"],
        "c": ["x", "x", "y", "x"],
    }))
    f = FilterRows(((col("a") > "1") | (col("b") < "2")) & (col("c") == "x"))
    assert f._filter_spec is not None
    assert "and" in f._filter_spec
    rows = collect(src | f)
    assert len(rows) == 2
    assert {r["a"] for r in rows} == {"3", "5"}


def test_expr_nested_compound_and_or():
    from rypipe import col

    src = _MockSource(pa.table({
        "a": ["1", "3", "5", "3"],
        "b": ["3", "1", "3", "3"],
        "c": ["y", "y", "x", "x"],
    }))
    f = FilterRows((col("a") > "1") & ((col("b") < "2") | (col("c") == "x")))
    assert f._filter_spec is not None
    assert "and" in f._filter_spec
    rows = collect(src | f)
    assert len(rows) == 3
    assert {r["a"] for r in rows} == {"3", "5"}


def test_expr_contains():
    from rypipe import col

    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
    }))
    f = FilterRows(col("name").contains("li"))
    assert f._filter_spec is not None
    assert f._filter_spec["op"] == "contains"
    rows = collect(src | f)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_expr_isin():
    from rypipe import col

    src = _MockSource(pa.table({
        "status": ["active", "inactive", "pending"],
    }))
    f = FilterRows(col("status").isin(["active", "pending"]))
    assert f._filter_spec is not None
    assert f._filter_spec["op"] == "in"
    rows = collect(src | f)
    assert len(rows) == 2
    assert {r["status"] for r in rows} == {"active", "pending"}


def test_expr_matches_regex():
    from rypipe import col

    f = FilterRows(col("code").matches(r"^ERR\d+$"))
    assert f._filter_spec == {"field": "code", "op": "regex", "value": r"^ERR\d+$"}
    # Python fallback semantics
    assert f._predicate({"code": "ERR42"}) is True
    assert f._predicate({"code": "WARN42"}) is False
    assert f._predicate({}) is False
    assert f._predicate({"code": None}) is False
    with pytest.raises(Exception):
        col("code").matches("(")


def test_expr_between():
    from rypipe import col

    f = FilterRows(col("age").between(20, 30))
    assert f._filter_spec == {
        "and": [
            {"field": "age", "op": ">=", "value": "20"},
            {"field": "age", "op": "<=", "value": "30"},
        ]
    }
    src = _MockSource(pa.table({
        "name": ["Alice", "Bob", "Carol"],
        "age": ["30", "25", "35"],
    }))
    rows = collect(src | f)
    assert sorted([r["name"] for r in rows]) == ["Alice", "Bob"]


def test_expr_is_null_and_not_null():
    from rypipe import col

    f = FilterRows(col("city").is_null())
    assert f._filter_spec == {"field": "city", "op": "is_null"}
    f = FilterRows(col("city").is_not_null())
    assert f._filter_spec == {"not": {"field": "city", "op": "is_null"}}


def test_filter_rows_regex_keyword_form():
    f = FilterRows(field="code", op="regex", value=r"^ERR")
    assert f._filter_spec == {"field": "code", "op": "regex", "value": r"^ERR"}
    assert f._predicate({"code": "ERR1"}) is True
    assert f._predicate({"code": "OK"}) is False
    assert f._predicate({}) is False
    with pytest.raises(Exception):
        FilterRows(field="code", op="regex", value="(")


def test_expr_fusion_round_trip():
    """Expression predicates must survive plan_split into plan_overrides."""
    from rypipe import col

    overrides, remaining = plan_split([FilterRows(col("x") >= 18)])
    assert remaining == []
    assert overrides["filter"] == {"field": "x", "op": ">=", "value": "18"}


def test_drop_fields_rejects_string():
    with pytest.raises(TypeError):
        DropFields("name")


class _TableAdapter(Adapter):
    """Adapter that returns a fixed table from ``read``."""

    def __init__(self, table: pa.Table, **kwargs):
        self._table = table
        # Skip Source.__init__ path validation.
        self._path = Path("mock")
        self._field_mapping = kwargs.get("field_mapping", {})
        self._drop_fields = kwargs.get("drop_fields", [])
        self._filter = kwargs.get("filter", None)
        self._field_types = kwargs.get("field_types", {})
        self._dictionary_columns = kwargs.get("dictionary_columns", [])
        self._schema = kwargs.get("schema", [])
        self._auto_dict = kwargs.get("auto_dict", False)
        self._strict_types = kwargs.get("strict_types", False)
        self._use_mmap = kwargs.get("use_mmap", True)
        self._batch_size = kwargs.get("batch_size", 1024)
        self._observer = kwargs.get("observer")
        self._cached_arrow = None

    def read(self, path: str, **kwargs):
        return _apply_plan(self._table, kwargs)


def test_adapter_subclass_read(sample_table):
    src = _TableAdapter(sample_table)
    rows = collect(src | RenameFields({"name": "full_name"}))
    assert rows[0]["full_name"] == "Alice"


def test_adapter_subclass_filter(sample_table):
    src = _TableAdapter(sample_table)
    rows = collect(src | FilterRows(field="city", op="==", value="LA"))
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_source_iter_arrow_batches(sample_table):
    src = _MockSource(sample_table)
    batches = list(src.iter_arrow_batches(batch_size=2))
    assert len(batches) == 2
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 3
    combined = pa.Table.from_batches(batches, schema=sample_table.schema)
    assert combined.equals(sample_table)


def test_pipeline_iter_arrow_batches(sample_table):
    pipe = _MockSource(sample_table) | DropFields(["city"])
    batches = list(pipe.iter_arrow_batches(batch_size=2))
    combined = pa.Table.from_batches(batches)
    assert combined.column_names == ["name", "age"]
    assert combined.num_rows == 3


def test_read_batches_module_level(sample_table, monkeypatch):
    class _FakeAdapter:
        def read(self, path, **kwargs):
            return sample_table

    monkeypatch.setitem(rypipe._ADAPTERS, "mockfmt", _FakeAdapter())
    batches = list(rypipe.read_batches("data.mockfmt", format="mockfmt", batch_size=2))
    assert len(batches) == 2
    combined = pa.Table.from_batches(batches, schema=sample_table.schema)
    assert combined.equals(sample_table)


def test_read_stream_still_collects(sample_table, monkeypatch):
    class _FakeAdapter:
        def read(self, path, **kwargs):
            return sample_table

    monkeypatch.setitem(rypipe._ADAPTERS, "mockfmt", _FakeAdapter())
    table = rypipe.read_stream("data.mockfmt", format="mockfmt")
    assert table.equals(sample_table)


# ---------------------------------------------------------------------------
# v1.1: Boolean combinators and multi-filter fusion

def test_filter_rows_any_or(sample_table):
    # name == Alice OR city == LA → 2 rows
    src = _MockSource(sample_table)
    pipe = src | FilterRowsAny(
        FilterRows(field="name", op="==", value="Alice"),
        FilterRows(field="city", op="==", value="LA"),
    )
    rows = collect(pipe)
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert names == {"Alice", "Bob"}


def test_filter_rows_all_and(sample_table):
    # Alice is in NYC (both match); only row 1
    src = _MockSource(sample_table)
    pipe = src | FilterRowsAll(
        FilterRows(field="name", op="==", value="Alice"),
        FilterRows(field="city", op="==", value="NYC"),
    )
    rows = collect(pipe)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_filter_rows_not(sample_table):
    src = _MockSource(sample_table)
    pipe = src | FilterRowsNot(FilterRows(field="city", op="==", value="LA"))
    rows = collect(pipe)
    assert len(rows) == 2
    assert all(r["city"] != "LA" for r in rows)


def test_chained_filter_rows_implicit_and(sample_table):
    # Chaining FilterRows is now implicit AND (fusion bugfix).
    src = _MockSource(sample_table)
    pipe = (
        src
        | FilterRows(field="city", op="==", value="NYC")
        | FilterRows(field="name", op="==", value="Alice")
    )
    rows = collect(pipe)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
    # Verify fusion produced a single and-spec rather than dropping the first filter.
    overrides, _ = plan_split([FilterRows(field="a", op="==", value="1"), FilterRows(field="b", op="==", value="2")])
    assert overrides["filter"] == {"and": [{"field": "a", "op": "==", "value": "1"}, {"field": "b", "op": "==", "value": "2"}]}


def test_nested_combinator_pipeline(sample_table):
    # (city==NYC OR city==LA) AND name != Carol → 2 rows
    src = _MockSource(sample_table)
    ors = FilterRowsAny(
        FilterRows(field="city", op="==", value="NYC"),
        FilterRows(field="city", op="==", value="LA"),
    )
    not_carol = FilterRowsNot(FilterRows(field="name", op="==", value="Carol"))
    # Verify per-row .apply: ors keeps 2 rows, not_carol keeps 2, conjunction keeps 2.
    assert len(collect(src | ors)) == 2
    assert len(collect(src | not_carol)) == 2
    # Piping ors then not_carol is an AND of the two trees
    rows = collect(src | ors | not_carol)
    assert len(rows) == 2
    assert all(r["name"] in {"Alice", "Bob"} for r in rows)


def test_filter_rows_any_rejects_callable_inner():
    """Callable predicates that can't be compiled are rejected by combinators."""
    uncompileable = lambda r: r["name"].strip().lower() == "alice"
    with pytest.raises(ValueError, match="only accepts fusable"):
        FilterRowsAny(FilterRows(predicate=uncompileable), FilterRows(field="x", op="==", value="1"))


def test_filter_rows_not_requires_leaf():
    """Callable predicates that can't be compiled are rejected by NOT."""
    uncompileable = lambda r: r["name"].strip().lower() == "alice"
    with pytest.raises(ValueError, match="only accepts fusable"):
        FilterRowsNot(FilterRows(predicate=uncompileable))


def test_filter_rows_any_needs_two():
    with pytest.raises(ValueError):
        FilterRowsAny(FilterRows(field="x", op="==", value="1"))


def test_filter_compare_inside_or(sample_table):
    a = FilterRows(field_a="city", op="==", field_b="city")  # self-compare always true
    b = FilterRows(field="name", op="==", value="Nobody")
    rows = collect(_MockSource(sample_table) | FilterRowsAny(a, b))
    assert len(rows) == 3  # all rows satisfy self-compare


def test_filter_rows_is_null_spec_and_fallback():
    f = FilterRows(field="city", is_null=True)
    assert f._filter_spec == {"field": "city", "op": "is_null"}
    assert f._predicate({"city": None}) is True
    assert f._predicate({}) is True
    assert f._predicate({"city": "LA"}) is False


def test_filter_rows_is_null_false_spec():
    f = FilterRows(field="city", is_null=False)
    assert f._filter_spec == {"not": {"field": "city", "op": "is_null"}}
    assert f._plan_kwargs() == {"filter": {"not": {"field": "city", "op": "is_null"}}}


def test_filter_rows_is_null_false_python_fallback():
    stage = FilterRows(field="city", is_null=False)
    rows = [{"city": "LA"}, {"city": None}, {"other": 1}]
    assert list(stage(rows)) == [{"city": "LA"}]


def test_filter_rows_field_alone_still_errors():
    with pytest.raises(ValueError, match="is_null"):
        FilterRows(field="city")


def test_filter_rows_not_null_pipeline_fusion(sample_table):
    """The not-null spec must survive the fusion path into plan_overrides."""
    overrides, _remaining = plan_split([FilterRows(field="city", is_null=False)])
    assert overrides["filter"] == {"not": {"field": "city", "op": "is_null"}}


def test_source_strict_types_kwarg_forwarded(sample_table):
    src = _MockSource(sample_table, strict_types=True)
    assert src._build_plan_kwargs()["strict_types"] is True
    src = _MockSource(sample_table)
    assert "strict_types" not in src._build_plan_kwargs()
