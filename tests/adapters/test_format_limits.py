import pytest

import rypipe
from rypipe_ldif import LdifSource
from rypipe_properties import PropertiesSource


def test_properties_plain_values(tmp_path):
    path = tmp_path / "values.properties"
    path.write_text(
        "  # ignored \\\n! ignored\n\n"
        "first: a=b  \nsecond  = :x\nthird\nfourth\t: y\nempty=\n",
        encoding="utf-8",
    )
    assert PropertiesSource(path).to_arrow().to_pylist() == [
        {"key": "first", "value": "a=b  "},
        {"key": "second", "value": ":x"},
        {"key": "third", "value": ""},
        {"key": "fourth", "value": "y"},
        {"key": "empty", "value": ""},
    ]


def test_ldif_comments_and_plain_values(tmp_path):
    path = tmp_path / "values.ldif"
    path.write_text("# header\n\ndn: one\n# detail\nname: Example  \n", encoding="utf-8")
    assert LdifSource(path).to_arrow().to_pylist() == [{"dn": "one", "name": "Example  "}]


@pytest.mark.parametrize("source, text, message", [
    (LdifSource, "dn: one\n continuation\n", "folded continuation"),
    (LdifSource, "dn:: b25l\n", "base64"),
    (LdifSource, "dn:< file:///value\n", "URL values"),
    (PropertiesSource, "name=escaped\\ value\n", "escapes and continuations"),
    (PropertiesSource, "name=first\\\n second\n", "escapes and continuations"),
])
def test_unsupported_forms_raise(tmp_path, source, text, message):
    path = tmp_path / "input"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(rypipe.ParserError, match=message):
        source(path).to_arrow()
