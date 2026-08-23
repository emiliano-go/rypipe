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
    RenameFields,
    Source,
    collect,
    to_arrow,
    to_csv,
    to_dataframe,
    to_polars,
)


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

    # Filter.
    spec = plan.get("filter")
    if spec:
        if "field" in spec:
            field, op, value = spec["field"], spec["op"], spec["value"]
            mask = pc.equal(table.column(field), value)
            if op not in ("==", "eq"):
                mask = pc.invert(mask)
            mask = pc.fill_null(mask, op in ("==", "eq"))
        else:
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
            mask = getattr(pc, fn_name)(table.column(field_a), table.column(field_b))
            mask = pc.fill_null(mask, False)
        table = table.filter(mask)

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
        FilterRows(field="x", op=">", value="1")


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
