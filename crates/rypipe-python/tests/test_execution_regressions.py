import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rypipe import CastTypes, DropFields, FilterRows, RenameFields, col, collect, to_arrow, to_parquet
from test_source_pipeline import _MockSource


@pytest.mark.parametrize("stages, expected", [
    ([lambda rows: ({**row, "x": "changed"} for row in rows),
      FilterRows(field="x", op="==", value="changed")], [{"x": "changed", "y": "2"}]),
    ([DropFields(["x"]), DropFields(["y"])], [{}]),
    ([RenameFields({"x": "middle"}), RenameFields({"middle": "final"})],
     [{"final": "1", "y": "2"}]),
    ([CastTypes({"x": int, "y": lambda value: value + "!"})], [{"x": 1, "y": "2!"}]),
    ([CastTypes({"x": int}), CastTypes({"x": str})], [{"x": "1", "y": "2"}]),
])
def test_pipeline_stage_order(stages, expected):
    pipeline = _MockSource(pa.table({"x": ["1"], "y": ["2"]}))
    for stage in stages:
        pipeline = pipeline | stage
    assert collect(pipeline) == expected


def test_source_filter_survives_pipeline_filter():
    source = _MockSource(pa.table({"x": ["1", "2"]}),
                         filter={"field": "x", "op": "==", "value": "1"})
    assert collect(source | FilterRows(field="x", op="!=", value="1")) == []


def test_filter_before_cast_preserves_string_comparison():
    pipeline = (_MockSource(pa.table({"x": ["2", "10"]}))
                | FilterRows(field="x", op=">", value="1") | CastTypes({"x": int}))
    assert collect(pipeline) == [{"x": 2}, {"x": 10}]


def test_empty_pipeline_keeps_schema():
    source = _MockSource(pa.table({"x": pa.array([], type=pa.int64())}))
    assert to_arrow(source | RenameFields({"x": "renamed"})).schema == pa.schema([
        ("renamed", pa.int64())
    ])


def test_filtered_pipeline_keeps_schema():
    source = _MockSource(pa.table({"x": [1]}))
    result = to_arrow(source | FilterRows(field="x", op=">", value=10))
    assert result.schema == source.to_arrow().schema
    assert result.num_rows == 0


def test_compound_filter_preserves_arrow_schema_and_metadata():
    from decimal import Decimal

    schema = pa.schema([
        pa.field("amount", pa.decimal128(38, 2), metadata={b"unit": b"USD"}),
        pa.field("label", pa.large_string()),
    ], metadata={b"source": b"test"})
    table = pa.Table.from_pylist([
        {"amount": Decimal("1.25"), "label": "keep"},
        {"amount": Decimal("0.00"), "label": "drop"},
    ], schema=schema)
    source = _MockSource(table)
    source.to_arrow()
    result = (source | FilterRows((col("amount") > 0) & col("label").contains("keep"))).to_arrow()
    assert result.schema.equals(schema, check_metadata=True)
    assert result.to_pylist() == [{"amount": Decimal("1.25"), "label": "keep"}]


def test_generic_pipeline_reads_once():
    class CountedSource(_MockSource):
        reads = 0

        def _read_arrow(self, plan_overrides=None):
            self.reads += 1
            return super()._read_arrow(plan_overrides)

    source = CountedSource(pa.table({"x": [1]}))
    assert collect(source | (lambda rows: rows)) == [{"x": 1}]
    assert source.reads == 1


def test_stream_failure_is_not_replayed():
    class FailingSource(_MockSource):
        def iter_record_batches(self, **kwargs):
            yield self._table.to_batches()[0]
            raise ValueError("broken input after first batch")

    batches = (FailingSource(pa.table({"x": [1]})) | DropFields([])).iter_record_batches()
    assert next(batches).to_pylist() == [{"x": 1}]
    with pytest.raises(ValueError, match="broken input"):
        next(batches)


@pytest.mark.parametrize("method", [False, True])
def test_streaming_parquet_write_options(tmp_path, method):
    source = _MockSource(pa.table({"x": [1, 2, 3]}))
    path = tmp_path / "rows.parquet"
    if method:
        source.to_parquet(path, memory="1MiB", row_group_size=1, write_page_index=True)
    else:
        to_parquet(source, path, memory="1MiB", row_group_size=1, write_page_index=True)
    assert pq.read_table(path).equals(source.to_arrow())
    assert pq.ParquetFile(path).metadata.num_row_groups == 3


def test_pipeline_to_arrow_method():
    pipeline = _MockSource(pa.table({"x": [1]})) | RenameFields({"x": "y"})
    assert pipeline.to_arrow().to_pylist() == [{"y": 1}]


def test_pipeline_sinks_share_cache_and_clear(tmp_path, pandas, polars):
    class CountedSource(_MockSource):
        reads = 0

        def _read_arrow(self, plan_overrides=None):
            self.reads += 1
            return super()._read_arrow(plan_overrides)

    source = CountedSource(pa.table({"x": [1, 2]}))
    pipeline = source | RenameFields({"x": "y"})
    frame = pipeline.to_pandas(chunksize=1)
    assert frame["y"].tolist() == [1, 2]
    assert frame["y"].dtype == pandas.ArrowDtype(pa.int64())
    table = pipeline.to_arrow()
    assert pipeline.to_arrow() is table
    assert to_arrow(pipeline) is table
    assert pipeline.to_pandas().shape == (2, 1)
    assert pipeline.to_polars().height == 2
    assert collect(pipeline) == table.to_pylist()
    assert list(pipeline) == table.to_pylist()
    pipeline.to_parquet(tmp_path / "cached.parquet")
    assert sum(b.num_rows for b in pipeline.iter_record_batches()) == 2
    assert source.reads == 1
    assert source._cached_arrow is None
    pipeline.clear_cache()
    assert pipeline.to_arrow().equals(table)
    assert source.reads == 2


def test_pipeline_reuses_source_cache_without_changing_it():
    class CountedSource(_MockSource):
        reads = 0

        def _read_arrow(self, plan_overrides=None):
            self.reads += 1
            return super()._read_arrow(plan_overrides)

    source = CountedSource(pa.table({"x": [1, 2]}))
    original = source.to_arrow()
    pipeline = source | RenameFields({"x": "y"})
    assert pipeline.to_arrow().column_names == ["y"]
    assert source.to_arrow() is original
    pipeline.clear_cache()
    assert pipeline.to_arrow().column_names == ["y"]
    assert source.reads == 1


@pytest.mark.parametrize("stage", [lambda rows: rows, FilterRows(lambda row: False)])
def test_empty_fallback_preserves_known_schema(stage):
    source = _MockSource(pa.table({"x": pa.array([], type=pa.int64())}))
    assert (source | stage).to_arrow().schema == source.to_arrow().schema


def test_empty_streaming_sinks_keep_schema(tmp_path, pandas, polars):
    source = _MockSource(pa.table({"x": pa.array([], type=pa.int64())}))
    assert source.to_pandas(memory="1MiB").columns.tolist() == ["x"]
    assert source.to_polars(memory="1MiB").columns == ["x"]
    path = tmp_path / "empty.parquet"
    source.to_parquet(path, memory="1MiB")
    assert pq.read_table(path).schema == source.to_arrow().schema


def test_public_errors_are_shared_python_classes():
    import rypipe
    from _rypipe import errors

    for name in ("ParseError", "XmlError", "PlanError", "MergeError", "ParserError"):
        assert getattr(rypipe, name) is getattr(errors, name)


def test_cast_results_match_when_source_is_cached():
    original = pa.table({"enabled": ["false", "true"]})
    source = _MockSource(original)
    expected = (source | CastTypes({"enabled": bool})).to_arrow()
    source.to_arrow()
    assert (source | CastTypes({"enabled": bool})).to_arrow().equals(expected)
    assert collect(source | CastTypes({"enabled": bool})) == expected.to_pylist()


def test_cached_source_filter_on_dropped_column():
    source = _MockSource(pa.table({"x": ["1"], "y": ["2"]}))
    source.to_arrow()
    assert collect(source | DropFields(["x"]) | FilterRows(col("x") == "1")) == []


def test_cast_semantics_do_not_depend_on_execution_path():
    from datetime import date, datetime
    from decimal import Decimal

    values = {"enabled": "false", "count": "12", "ratio": "1.25",
              "day": "2026-09-14", "at": "2026-09-14T12:30:00", "price": "12.50"}
    casts = {"enabled": bool, "count": int, "ratio": float,
             "day": date, "at": datetime, "price": Decimal}
    stage = CastTypes(casts)
    source = _MockSource(pa.Table.from_pylist([values, dict.fromkeys(values)]))
    expected = [{"enabled": False, "count": 12, "ratio": 1.25,
                 "day": date(2026, 9, 14), "at": datetime(2026, 9, 14, 12, 30),
                 "price": Decimal("12.50")}, dict.fromkeys(values)]
    assert list(stage([values.copy(), dict.fromkeys(values)])) == expected
    source.to_arrow()
    assert collect(source | stage) == expected
    mixed = CastTypes({**casts, "label": lambda v: v.upper()})
    assert mixed.apply({**values, "label": "ok"}) == {**expected[0], "label": "OK"}
