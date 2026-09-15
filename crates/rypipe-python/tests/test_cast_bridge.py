import pyarrow as pa
import pytest

from _rypipe._rypipe import _cast_strings
from rypipe.stages.cast import CastTypes, _cast_column


@pytest.mark.parametrize("type_", [pa.string(), pa.large_string()])
def test_cast_bridge_matches_arrow_for_string_types(type_):
    column = pa.array(["1", None, "2", "42"], type=type_)
    result = _cast_column(column, "int64")
    assert result.equals(pa.array([1, None, 2, 42], type=pa.int64()))


def test_cast_bridge_accepts_chunked_array():
    column = pa.chunked_array([["1", None], ["2", "3"]], type=pa.string())
    assert _cast_column(column, "int64").equals(pa.array([1, None, 2, 3], type=pa.int64()))


def test_cast_bridge_preserves_empty_input():
    result = _cast_column(pa.array([], type=pa.string()), "int64")
    assert result.equals(pa.array([], type=pa.int64()))


def test_cast_bridge_rejects_invalid_private_input():
    with pytest.raises(TypeError):
        _cast_strings(pa.table({"value": [1]}), "int64")
    with pytest.raises(ValueError):
        _cast_strings(pa.table({"left": ["1"], "right": ["2"]}), "int64")


def test_cast_types_copies_mapping():
    mapping = {"value": int}
    stage = CastTypes(mapping)
    mapping["value"] = str
    assert list(stage([{"value": "7"}])) == [{"value": 7}]
