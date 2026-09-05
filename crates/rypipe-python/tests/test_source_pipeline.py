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
    to_dataframe,
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
        if "field" in spec:
            field, op, value = spec["field"], spec["op"], spec["value"]
            fn_name = {
                ">": "greater", "gt": "greater",
                "<": "less", "lt": "less",
                ">=": "greater_equal", "ge": "greater_equal",
                "<=": "less_equal", "le": "less_equal",
                "==": "equal", "eq": "equal",
                "!=": "not_equal", "ne": "not_equal",
            }[op]
            m = getattr(pc, fn_name)(table.column(field), value)
            return pc.fill_null(m, False)
        field_a, op, field_b = spec["field_a"], spec["op"], spec["field_b"]
        fn_name = {
            ">": "greater",
            "gt": "greater",
            "<": "less",
            "lt": "less",
            ">=": "greater_equal",
            "ge": "greater_equal",
            "<=": "less_equal",
            "le": "less_equal",
            "==": "equal",
            "eq": "equal",
            "!=": "not_equal",
            "ne": "not_equal",
        }[op]
        return pc.fill_null(getattr(pc, fn_name)(table.column(field_a), table.column(field_b)), False)

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
        self._use_mmap = kwargs.get("use_mmap", True)
        self._batch_size = kwargs.get("batch_size", 1024)
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


def test_pipeline_to_dataframe(sample_table, pandas):
    src = _MockSource(sample_table)
    pipe = src | RenameFields({"name": "full_name"})
    df = to_dataframe(pipe)
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
        self._use_mmap = kwargs.get("use_mmap", True)
        self._batch_size = kwargs.get("batch_size", 1024)
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
    with pytest.raises(ValueError, match="only accepts fusable"):
        FilterRowsAny(FilterRows(predicate=lambda r: True), FilterRows(field="x", op="==", value="1"))


def test_filter_rows_not_requires_leaf():
    with pytest.raises(ValueError, match="only accepts fusable"):
        FilterRowsNot(FilterRows(predicate=lambda r: False))


def test_filter_rows_any_needs_two():
    with pytest.raises(ValueError):
        FilterRowsAny(FilterRows(field="x", op="==", value="1"))


def test_filter_compare_inside_or(sample_table):
    a = FilterRows(field_a="city", op="==", field_b="city")  # self-compare always true
    b = FilterRows(field="name", op="==", value="Nobody")
    rows = collect(_MockSource(sample_table) | FilterRowsAny(a, b))
    assert len(rows) == 3  # all rows satisfy self-compare
