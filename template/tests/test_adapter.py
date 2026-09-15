import pyarrow as pa
import pytest

from {{crate_name}} import AdapterSource, CastTypes, FilterRows, ParseError, PlanError, col, read


def test_read_and_pipeline(tmp_path):
    path = tmp_path / "data.{{crate_name}}"
    path.write_text("answer=42\nother=7\n", encoding="utf-8")
    assert isinstance(read(path), pa.Table)
    table = (AdapterSource(path) | CastTypes({"value": int})
             | FilterRows((col("value") > 10) & (col("key") == "answer"))).to_arrow()
    assert table.to_pylist() == [{"key": "answer", "value": 42}]
    with pytest.raises(PlanError, match="unknown field type"):
        read(path, field_types={"value": "bogus"})
    path.write_bytes(b"bad=\xff\n")
    with pytest.raises(ParseError, match="UTF-8"):
        read(path)
