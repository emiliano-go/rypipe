"""Smoke tests for the rypipe-python extension.

These tests require the `_rypipe` extension to be built/importable (for example
via `maturin develop`) and `pyarrow` to be installed. They are not run by
`cargo test` directly because `_rypipe` is a `cdylib` extension module.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

try:
    import _rypipe
except ImportError as exc:  # pragma: no cover
    pytest.skip(f"_rypipe extension not importable: {exc}", allow_module_level=True)


XML_SIMPLE = b"""<Rows>
<Row A="1" B="hello"/>
<Row A="2" B="world"/>
<Row A="3" B="hello"/>
</Rows>"""


def _tmp_xml(data: bytes) -> Path:
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        f.write(data)
        return Path(f.name)


def test_read_to_columnar():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar(str(path))
    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert set(table.column_names) == {"A", "B"}


def test_read_to_columnar_multi():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar_multi(str(path), num_chunks=2)
    assert table.num_rows == 3


def test_read_to_columnar_par():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar_par(str(path), num_chunks=2)
    assert table.num_rows == 3


def test_read_to_columnar_bounded():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar_bounded(str(path), memory=4096)
    assert table.num_rows == 3


def test_field_mapping_and_drop():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar(
        str(path),
        field_mapping={"A": "Alpha"},
        drop_fields=["B"],
    )
    assert table.column_names == ["Alpha"]
    assert table.column("Alpha").to_pylist() == ["1", "2", "3"]


def test_field_types_and_filter():
    xml = b"""<Rows>
    <Row A="10" B="5"/>
    <Row A="20" B="25"/>
    <Row A="30" B="30"/>
    </Rows>"""
    path = _tmp_xml(xml)
    table = _rypipe.read_to_columnar(
        str(path),
        field_types={"A": "int64", "B": "int64"},
        filter={"op": ">", "field_a": "A", "field_b": "B"},
    )
    assert table.num_rows == 1
    assert table.column("A").to_pylist() == [10]


def test_per_row_filter():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar(
        str(path),
        filter={"op": "==", "field": "B", "value": "hello"},
    )
    assert table.num_rows == 2


def test_auto_dict_promotion():
    path = _tmp_xml(XML_SIMPLE)
    table = _rypipe.read_to_columnar_par(
        str(path),
        num_chunks=2,
        auto_dict=True,
    )
    # auto_dict may promote B to dictionary; the table should still expose B.
    assert table.num_rows == 3
    assert "B" in table.column_names


def test_custom_row_tag():
    xml = b"""<Items><Item X="a"/><Item X="b"/></Items>"""
    path = _tmp_xml(xml)
    table = _rypipe.read_to_columnar(str(path), row_tag="Item")
    assert table.num_rows == 2
    assert table.column("X").to_pylist() == ["a", "b"]


def test_exceptions():
    with pytest.raises(FileNotFoundError):
        _rypipe.read_to_columnar("/nonexistent/path.xml")

    path = _tmp_xml(b"\xff\xfe")
    with pytest.raises(_rypipe.XmlError):
        _rypipe.read_to_columnar(str(path))

    with pytest.raises(_rypipe.PlanError):
        _rypipe.read_to_columnar(str(_tmp_xml(XML_SIMPLE)), field_types={"A": "unknown"})
