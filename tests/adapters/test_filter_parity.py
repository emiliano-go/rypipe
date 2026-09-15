import pytest

from rypipe import FilterRows, col
from rypipe.expr import Predicate
from rypipe_ldif import LdifSource


@pytest.mark.parametrize("predicate, typed", [
    (col("n") > 10, False),
    (col("n").between(3, 15), False),
    ((col("n") > 10) & col("label").contains("x"), False),
    (col("n").isin([2, 20]), True),
    (col("n").not_in([2, 20]), True),
    (col("n") > 10, True),
    (col("n").between(3, 15), True),
    ((col("n") > 10) & col("label").contains("x"), True),
    (col("label").contains("None"), False),
    (col("label").length("==", 0), False),
    (col("label").lower("==", "none"), False),
    (col("label").upper("==", "NONE"), False),
    (col("label").strip("==", "None"), False),
    ((col("n") == col("other")) | (col("label") == "x"), False),
])
def test_filters_match_fresh_cached_and_direct(tmp_path, predicate, typed):
    path = tmp_path / "rows.ldif"
    path.write_text("id: a\nn: 2\nother: 2\nlabel: x\n\n"
                    "id: b\nn: 10\nlabel: y\n\n"
                    "id: c\nn: 20\nlabel: x\n\n"
                    "id: d\n", encoding="utf-8")
    source = LdifSource(path, field_types={"n": "int64"} if typed else None)
    stage = FilterRows(predicate)
    fresh = (source | stage).to_arrow().to_pylist()
    source.to_arrow()
    assert (source | stage).to_arrow().to_pylist() == fresh
    assert list(stage(source.to_arrow().to_pylist())) == fresh


def test_filter_rejects_accidental_python_truth_values():
    with pytest.raises(TypeError, match="predicate"):
        FilterRows(False)
    with pytest.raises(TypeError, match="truth value"):
        bool(col("n") > 10)
    with pytest.raises(TypeError, match="truth value"):
        bool(col("n"))
    with pytest.raises(ValueError, match="comparison operator"):
        FilterRows(col("n").strip("bad", ""))


@pytest.mark.parametrize("predicate, field_types", [
    (col("n") == 9007199254740993, {"n": "int64"}),
    (col("n") > col("other"), {"n": "float64", "other": "int64"}),
    (col("n") != col("other"), {"n": "int64"}),
    (col("n") > col("other"), {"n": "int64"}),
    (col("flag").isin([True]), {"flag": "bool"}),
    (col("flag") == True, {"flag": "bool"}),
    ((col("flag") == True) | (col("label") == "y"), {"flag": "bool"}),
    (col("label").length("==", 1), {}),
    (col("label").lstrip("==", "x "), {}),
    (col("label").rstrip("==", " x"), {}),
    (col("n") > "bad", {"n": "int64"}),
    (col("n") != 2, {"n": "float64"}),
    (Predicate({"field": "n", "op": ">", "arith_op": "/", "arith_value": 0, "value": "1"}), {}),
    (Predicate({"field": "n", "op": ">", "arith_op": "*", "arith_value": 2, "value": "1"}), {}),
    (col("n").is_type("int64"), {}),
    (col("flag").is_type("bool"), {}),
    (Predicate({"always": True}), {}),
    (Predicate({"always": False}), {}),
    (Predicate({"not_field": "n"}), {}),
])
def test_typed_and_text_filter_edge_parity(tmp_path, predicate, field_types):
    path = tmp_path / "edge.ldif"
    path.write_text("id: a\nn: 9007199254740992\nother: 9007199254740993\nflag: true\nlabel: é\n\n"
                    "id: b\nn: 9007199254740993\nother: 2\nflag: false\nlabel:  x \n\n"
                    "id: c\nn: NaN\nother: 3\nlabel: y\n\n"
                    "id: d\n", encoding="utf-8")
    source = LdifSource(path, field_types=field_types)
    stage = FilterRows(predicate)
    fresh = (source | stage).to_arrow()
    source.to_arrow()
    assert (source | stage).to_arrow().column("id").to_pylist() == fresh.column("id").to_pylist()
    assert [r["id"] for r in stage(source.to_arrow().to_pylist())] == fresh.column("id").to_pylist()


@pytest.mark.parametrize("predicate, expected", [
    (col("n").isin([2]), ["a"]),
    (col("n").isin(["bad"]), []),
    (col("n") == 2, ["a"]),
    (col("n").is_type("string"), ["a"]),
])
def test_filters_use_cast_values(tmp_path, predicate, expected):
    path = tmp_path / "cast.ldif"
    path.write_text("id: a\nn: 002\n\nid: b\nn: bad\n\nid: c\n", encoding="utf-8")
    source = LdifSource(path, field_types={"n": "int64"})
    stage = FilterRows(predicate)
    assert (source | stage).to_arrow().column("id").to_pylist() == expected
    source.to_arrow()
    assert (source | stage).to_arrow().column("id").to_pylist() == expected


def test_keyword_filter_matches_expression_literals(tmp_path):
    path = tmp_path / "keyword.ldif"
    path.write_text("id: a\nn: 2\n\nid: b\nn: 3\n", encoding="utf-8")
    source = LdifSource(path, field_types={"n": "int64"})
    stage = FilterRows(field="n", op=">", value=2)
    assert (source | stage).to_arrow().column("id").to_pylist() == ["b"]
    source.to_arrow()
    assert (source | stage).to_arrow().column("id").to_pylist() == ["b"]


@pytest.mark.parametrize("predicate, field_types, expected", [
    (col("a") > col("b"), {"a": "decimal128(0)", "b": "decimal128(3)"}, ["a"]),
    (col("a") == col("b"), {"a": "decimal128(0)", "b": "decimal128(3)"}, ["b"]),
    (col("a") > "18446744073709551615", {"a": "decimal128(0)"}, ["a"]),
    (col("when").is_null(), {"when": "date32"}, ["b", "c"]),
    (col("when").contains("bad"), {"when": "date32"}, []),
])
def test_decimal_and_date_filters(tmp_path, predicate, field_types, expected):
    path = tmp_path / "typed.ldif"
    path.write_text("id: a\na: 18446744073709551616\nb: 0.001\nwhen: 2026-09-14\n\n"
                    "id: b\na: 0\nb: 0\nwhen: bad\n\n"
                    "id: c\n", encoding="utf-8")
    source = LdifSource(path, field_types=field_types)
    stage = FilterRows(predicate)
    assert (source | stage).to_arrow().column("id").to_pylist() == expected
    source.to_arrow()
    assert (source | stage).to_arrow().column("id").to_pylist() == expected
