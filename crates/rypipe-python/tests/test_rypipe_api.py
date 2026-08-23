"""Tests for the public ``rypipe`` Python API.

These tests exercise the developer-friendly wrapper over the low-level
``_rypipe`` extension.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

import rypipe

XML_SIMPLE = b"""<Rows>
<Row A="1" B="hello"/>
<Row A="2" B="world"/>
<Row A="3" B="hello"/>
</Rows>"""


def _tmp_xml(data: bytes) -> Path:
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        f.write(data)
        return Path(f.name)


def test_read_auto_format():
    path = _tmp_xml(XML_SIMPLE)
    table = rypipe.read(str(path))
    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert set(table.column_names) == {"A", "B"}


def test_read_explicit_format():
    path = _tmp_xml(XML_SIMPLE)
    table = rypipe.read(str(path), format="xml")
    assert table.num_rows == 3


def test_read_rename_and_drop():
    path = _tmp_xml(XML_SIMPLE)
    table = rypipe.read(
        str(path),
        rename={"A": "Alpha"},
        drop=["B"],
    )
    assert table.column_names == ["Alpha"]


def test_read_field_types_and_filter():
    xml = b"""<Rows>
    <Row A="10" B="5"/>
    <Row A="20" B="25"/>
    <Row A="30" B="30"/>
    </Rows>"""
    path = _tmp_xml(xml)
    table = rypipe.read(
        str(path),
        fields={"A": "int64", "B": "int64"},
        filter={"op": ">", "field_a": "A", "field_b": "B"},
    )
    assert table.num_rows == 1
    assert table.column("A").to_pylist() == [10]


def test_read_per_row_filter():
    path = _tmp_xml(XML_SIMPLE)
    table = rypipe.read(
        str(path),
        filter={"op": "==", "field": "B", "value": "hello"},
    )
    assert table.num_rows == 2


def test_read_par():
    path = _tmp_xml(XML_SIMPLE)
    table = rypipe.read_par(str(path), chunks=2)
    assert table.num_rows == 3


def test_read_stream():
    path = _tmp_xml(XML_SIMPLE)
    table = rypipe.read_stream(str(path), memory="1MiB")
    assert table.num_rows == 3


def test_read_custom_row_tag():
    xml = b"""<Items><Item X="a"/><Item X="b"/></Items>"""
    path = _tmp_xml(xml)
    table = rypipe.read(str(path), format="xml", row_tag="Item")
    assert table.num_rows == 2
    assert table.column("X").to_pylist() == ["a", "b"]


def test_read_unsupported_format():
    path = _tmp_xml(XML_SIMPLE)
    with pytest.raises(rypipe.PlanError):
        rypipe.read(str(path), format="not-a-format")


def test_read_unknown_extension():
    with tempfile.NamedTemporaryFile(suffix=".unknown", delete=False) as f:
        f.write(XML_SIMPLE)
        path = Path(f.name)
    with pytest.raises(rypipe.RypipeError):
        rypipe.read(str(path))


def test_parse_memory():
    from rypipe import _parse_memory  # type: ignore[attr-defined]

    assert _parse_memory(1024) == 1024
    assert _parse_memory("1KB") == 1000
    assert _parse_memory("1KiB") == 1024
    assert _parse_memory("2MB") == 2_000_000
    assert _parse_memory("1.5GiB") == int(1.5 * 1024**3)
