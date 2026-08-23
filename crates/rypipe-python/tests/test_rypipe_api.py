"""Tests for the public ``rypipe`` Python API.

``rypipe`` is a pure engine package with no built-in format adapters. These
tests exercise the adapter registry, plan construction, and helper utilities
using a mock adapter.
"""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import rypipe


def test_register_adapter_and_read():
    mock_table = pa.table({"a": [1, 2, 3]})
    adapter = MagicMock()
    adapter.read = MagicMock(return_value=mock_table)

    rypipe.register_adapter("mock", adapter, extensions=[".mock"])
    result = rypipe.read("data.mock", option="value")

    adapter.read.assert_called_once_with("data.mock", option="value")
    assert result is mock_table


def test_read_explicit_format():
    mock_table = pa.table({"b": ["x", "y"]})
    adapter = MagicMock()
    adapter.read = MagicMock(return_value=mock_table)

    rypipe.register_adapter("mock2", adapter)
    result = rypipe.read("data.unknown", format="mock2")

    adapter.read.assert_called_once_with("data.unknown")
    assert result is mock_table


def test_read_explicit_adapter_object():
    mock_table = pa.table({"c": [True]})
    adapter = MagicMock()
    adapter.read = MagicMock(return_value=mock_table)

    result = rypipe.read("data.anything", adapter=adapter, foo=42)

    adapter.read.assert_called_once_with("data.anything", foo=42)
    assert result is mock_table


def test_read_unknown_extension():
    with pytest.raises(rypipe.RypipeError):
        rypipe.read("data.unknown")


def test_read_unregistered_format():
    with pytest.raises(rypipe.RypipeError):
        rypipe.read("data.xml", format="xml")


def test_exceptions():
    assert issubclass(rypipe.ParseError, Exception)
    assert issubclass(rypipe.XmlError, rypipe.ParseError)
    assert issubclass(rypipe.PlanError, Exception)
    assert issubclass(rypipe.MergeError, Exception)


def test_parse_memory():
    from rypipe import _parse_memory  # type: ignore[attr-defined]

    assert _parse_memory(1024) == 1024
    assert _parse_memory("1KB") == 1000
    assert _parse_memory("1KiB") == 1024
    assert _parse_memory("2MB") == 2_000_000
    assert _parse_memory("1.5GiB") == int(1.5 * 1024**3)
