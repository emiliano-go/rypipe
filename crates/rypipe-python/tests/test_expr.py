"""Tests for the expression API (spec generation only, no engine)."""

import re

import pytest

from rypipe.expr import col


def test_constant_comparison():
    assert (col("age") >= 18)._to_spec() == {"field": "age", "op": ">=", "value": "18"}


def test_string_value():
    assert (col("status") == "active")._to_spec() == {
        "field": "status",
        "op": "==",
        "value": "active",
    }


def test_bool_value_lowercase():
    assert (col("active") == True)._to_spec() == {  # noqa: E712
        "field": "active",
        "op": "==",
        "value": "true",
    }


def test_column_comparison():
    assert (col("start") < col("end"))._to_spec() == {
        "field_a": "start",
        "op": "<",
        "field_b": "end",
    }


def test_and_or_not():
    spec = ((col("age") >= 18) & col("name").startswith("A"))._to_spec()
    assert spec == {
        "and": [
            {"field": "age", "op": ">=", "value": "18"},
            {"field": "name", "op": "starts_with", "value": "A"},
        ]
    }
    spec = ((col("a") == 1) | (col("b") == 2))._to_spec()
    assert spec["or"][0] == {"field": "a", "op": "==", "value": "1"}
    spec = (~(col("x") == 1))._to_spec()
    assert spec == {"not": {"field": "x", "op": "==", "value": "1"}}


def test_membership():
    assert col("s").isin(["a", "b"])._to_spec() == {
        "field": "s",
        "op": "in",
        "values": ["a", "b"],
    }
    assert col("s").not_in([1, 2])._to_spec() == {
        "field": "s",
        "op": "not_in",
        "values": ["1", "2"],
    }
    with pytest.raises(ValueError):
        col("s").isin([])


def test_string_methods():
    assert col("n").endswith(".log")._to_spec() == {
        "field": "n",
        "op": "ends_with",
        "value": ".log",
    }
    assert col("n").contains("err")._to_spec() == {
        "field": "n",
        "op": "contains",
        "value": "err",
    }


def test_null_and_type_checks():
    assert col("x").is_null()._to_spec() == {"field": "x", "op": "is_null"}
    assert col("x").is_not_null()._to_spec() == {
        "not": {"field": "x", "op": "is_null"}
    }
    assert col("x").is_type("int64")._to_spec() == {
        "field": "x",
        "op": "is_type",
        "value": "int64",
    }


def test_bad_inputs_raise_early():
    with pytest.raises(TypeError):
        col("x") == object()
    with pytest.raises(TypeError):
        col(123)
    with pytest.raises(TypeError):
        (col("x") == 1) & 42


def test_composition_is_data():
    p = (col("amount") > 100) & ~(col("status") == "void")
    assert p._to_spec() == {
        "and": [
            {"field": "amount", "op": ">", "value": "100"},
            {"not": {"field": "status", "op": "==", "value": "void"}},
        ]
    }


def test_matches():
    assert col("code").matches(r"^ERR\d+$")._to_spec() == {
        "field": "code",
        "op": "regex",
        "value": r"^ERR\d+$",
    }
    with pytest.raises(re.error):
        col("code").matches("(")
    with pytest.raises(TypeError):
        col("code").matches(123)


def test_between():
    assert col("n").between(2, 6)._to_spec() == {
        "and": [
            {"field": "n", "op": ">=", "value": "2"},
            {"field": "n", "op": "<=", "value": "6"},
        ]
    }
    with pytest.raises(TypeError):
        col("n").between(object(), 6)
