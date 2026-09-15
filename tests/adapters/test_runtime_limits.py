import math

import pytest

import rypipe
from rypipe import _parse_memory
from rypipe_ldif import LdifSource


@pytest.fixture
def ldif_path(tmp_path):
    path = tmp_path / "limits.ldif"
    path.write_text("dn: one\nvalue: a\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("depth, should_raise", [(127, False), (128, True)])
def test_filter_nesting_limit(ldif_path, depth, should_raise):
    spec = {"field": "value", "op": "==", "value": "a"}
    for _ in range(depth):
        spec = {"not": spec}
    if should_raise:
        with pytest.raises(rypipe.PlanError, match="nesting exceeds maximum depth"):
            LdifSource(ldif_path, filter=spec).to_arrow()
    else:
        LdifSource(ldif_path, filter=spec).to_arrow()


@pytest.mark.parametrize("op, size, should_raise", [
    ("in", 100_000, False), ("in", 100_001, True),
    ("not_in", 100_000, False), ("not_in", 100_001, True),
])
def test_filter_membership_limit(ldif_path, op, size, should_raise):
    spec = {"field": "value", "op": op, "values": [str(i) for i in range(size)]}
    if should_raise:
        with pytest.raises(rypipe.PlanError, match="values|100,000|100000"):
            LdifSource(ldif_path, filter=spec).to_arrow()
    else:
        expected = 0 if op == "in" else 1
        assert LdifSource(ldif_path, filter=spec).to_arrow().num_rows == expected


def test_regex_limit_and_supported_pattern(ldif_path):
    assert LdifSource(ldif_path, filter={
        "field": "value", "op": "regex", "value": "a+"
    }).to_arrow().num_rows == 1
    pattern = "a" * (1024 * 1024 + 1)
    with pytest.raises(rypipe.PlanError, match="regex|pattern|size|length"):
        LdifSource(ldif_path, filter={
            "field": "value", "op": "regex", "value": pattern
        }).to_arrow()


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, -0.1, 1.1])
def test_auto_dict_threshold_rejects_invalid(ldif_path, value):
    with pytest.raises(rypipe.PlanError, match="auto_dict_threshold"):
        LdifSource(ldif_path, auto_dict_threshold=value).to_arrow()


def test_auto_dict_max_size_rejects_zero(ldif_path):
    with pytest.raises(rypipe.PlanError, match="auto_dict_max_size"):
        LdifSource(ldif_path, auto_dict_max_size=0).to_arrow()


@pytest.mark.parametrize("value", ["", "NaN", "inf", "-1MiB", "1XB"])
def test_memory_parser_rejects_invalid(value):
    with pytest.raises(rypipe.RypipeError, match="invalid memory"):
        _parse_memory(value)


def test_resolve_engine_rejects_zero_threads():
    with pytest.raises((ValueError, rypipe.PlanError), match="threads"):
        rypipe.resolve_engine(100, threads=0)
