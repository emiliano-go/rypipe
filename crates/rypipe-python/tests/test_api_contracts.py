import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rypipe


@pytest.fixture
def source_factory(tmp_path):
    path = tmp_path / "input"
    path.write_text("input", encoding="utf-8")

    class RecordingSource(rypipe.Adapter):
        def read(self, path, **kwargs):
            self.calls.append(kwargs)
            return self.table

    def make(table=None, **kwargs):
        source = RecordingSource(path, **kwargs)
        source.calls = []
        source.table = table if table is not None else pa.table({"x": [1, 2]})
        return source

    return make


def test_empty_source_schema_uses_table_metadata(source_factory):
    source = source_factory(pa.table({"x": pa.array([], type=pa.int64())}))
    assert source.schema() == ["x"]
    assert source.schema() == ["x"]
    assert len(source.calls) == 1
    pipeline = source | rypipe.RenameFields({"x": "renamed"})
    assert pipeline.schema() == ["renamed"]
    assert len(source.calls) == 1


def test_source_forwards_adapter_options(source_factory):
    source = source_factory(row_tag="Record", auto_dict_threshold=0.25,
                            auto_dict_max_size=128, max_split_chunks=4, prefault=True)
    source.to_arrow()
    assert source.calls[0] | {"use_mmap": True, "auto_dict": False} == {
        "row_tag": "Record", "auto_dict_threshold": 0.25,
        "auto_dict_max_size": 128, "max_split_chunks": 4, "prefault": True,
        "use_mmap": True, "auto_dict": False,
    }


def test_source_options_are_snapshots(source_factory):
    mapping = {"x": "y"}
    drop = ["z"]
    spec = {"and": [{"field": "x", "op": "==", "value": "1"}]}
    source = source_factory(field_mapping=mapping, drop_fields=drop, filter=spec)
    mapping["x"] = "changed"
    drop.append("x")
    spec["and"][0]["value"] = "2"
    source.to_arrow()
    assert source.calls[0]["field_mapping"] == {"x": "y"}
    assert source.calls[0]["drop_fields"] == ["z"]
    assert source.calls[0]["filter"]["and"][0]["value"] == "1"


def test_pipeline_stage_configuration_is_a_snapshot():
    mapping = {"x": "y"}
    stages = [rypipe.RenameFields(mapping)]
    pipeline = rypipe.Pipeline([{"x": 1}], stages)
    mapping["x"] = "changed"
    stages.append(rypipe.DropFields(["y"]))
    assert pipeline.to_arrow().to_pylist() == [{"y": 1}]


@pytest.mark.parametrize("size", [0, -1, True, 1.5, "2"])
def test_invalid_batch_size_fails_before_read(source_factory, size):
    source = source_factory()
    with pytest.raises(ValueError, match="batch_size"):
        list(source.iter_arrow_batches(batch_size=size))
    assert source.calls == []
    with pytest.raises(ValueError, match="batch_size"):
        rypipe.Pipeline(source, batch_size=size)


def test_invalid_pandas_options_fail_before_read(source_factory, pandas):
    source = source_factory()
    with pytest.raises(ValueError, match="dtype_backend"):
        source.to_pandas(dtype_backend="typo")
    with pytest.raises(ValueError, match="chunksize"):
        source.to_pandas(chunksize=0)
    assert source.calls == []


def test_invalid_stage_fails_during_construction(source_factory):
    source = source_factory()
    with pytest.raises(TypeError, match="stage"):
        source | 123
    assert source.calls == []


def test_adapter_contract_error_is_explicit(source_factory):
    source = source_factory()
    source.table = pa.record_batch({"x": [1]})
    with pytest.raises(TypeError, match="pyarrow.Table"):
        source.to_arrow()
    assert source._cached_arrow is None


def test_register_single_extension_and_validate_adapter():
    class Reader:
        def read(self, path, **kwargs):
            return pa.table({"path": [path]})

    with pytest.raises(TypeError, match="read"):
        rypipe.register_adapter("invalid_contract", object())
    rypipe.register_adapter("contract", Reader(), extensions=".contract")
    assert rypipe.read("data.contract")["path"].to_pylist() == ["data.contract"]


def test_stream_validation_precedes_read(source_factory):
    source = source_factory()
    with pytest.raises(ValueError, match="batch_size"):
        list(rypipe.read_batches("input", adapter=source, batch_size=0))
    assert source.calls == []


def test_untransformed_pipeline_reuses_table_identity(source_factory):
    source = source_factory()
    original = source.to_arrow()
    assert rypipe.Pipeline(source).to_arrow() is original
    assert len(source.calls) == 1


def test_stream_contract_error_is_explicit(source_factory):
    source = source_factory()
    source.iter_record_batches = lambda path, **kwargs: iter([source.table])
    with pytest.raises(TypeError, match="pyarrow.RecordBatch"):
        list(rypipe.read_batches("input", adapter=source))


def test_source_stream_contract_error_is_explicit(source_factory):
    class StreamingSource(rypipe.Adapter):
        def read(self, path, **kwargs):
            return pa.table({"x": [1]})

        def _iter_record_batches_stream(self, memory, batch_size, **kwargs):
            yield pa.table({"x": [1]})

    source = StreamingSource(source_factory()._path)
    with pytest.raises(TypeError, match="pyarrow.RecordBatch"):
        list(source.iter_record_batches())


@pytest.mark.parametrize("chunksize", [None, 1])
def test_generic_to_pandas_honors_dtype_backend(chunksize):
    frame = rypipe.to_pandas(
        [{"x": 1}, {"x": 2}], chunksize=chunksize, dtype_backend="pyarrow"
    )
    assert str(frame.dtypes["x"]) == "int64[pyarrow]"
    frame = rypipe.to_pandas(
        [{"x": 1}, {"x": 2}], chunksize=chunksize, dtype_backend="numpy"
    )
    assert str(frame.dtypes["x"]) == "int64"


def test_parquet_stream_routes_reader_options(source_factory, tmp_path):
    source = source_factory()
    output = tmp_path / "rows.parquet"
    source.to_parquet(
        output, memory="1MiB", prefault=True,
        filesystem=pa.fs.LocalFileSystem(),
    )
    assert pq.read_table(output).to_pylist() == [{"x": 1}, {"x": 2}]
    assert source.calls[0]["prefault"] is True
    assert "filesystem" not in source.calls[0]


def test_parquet_stream_row_group_size(source_factory, tmp_path):
    source = source_factory(pa.table({"x": list(range(10))}))
    output = tmp_path / "groups.parquet"
    source.to_parquet(output, memory="1MiB", batch_size=10, row_group_size=3)
    assert pq.ParquetFile(output).metadata.num_row_groups == 4
