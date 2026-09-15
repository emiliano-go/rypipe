import importlib

import pyarrow as pa
import pytest

import rypipe
from rypipe import CastTypes, FilterRows, RenameFields, col


@pytest.mark.parametrize("name, source_name, text, field", [
    ("ini", "IniSource", "[main]\namount=42\namount=7\n", "amount"),
    ("ldif", "LdifSource", "dn: one\namount: 42\n\ndn: two\namount: 7\n", "amount"),
    ("properties", "PropertiesSource", "answer=42\nother=7\n", "value"),
])
def test_native_adapter_contract(tmp_path, name, source_name, text, field):
    adapter = importlib.import_module(f"rypipe_{name}")
    path = tmp_path / f"rows.{name}"
    path.write_text(text, encoding="utf-8")
    source = getattr(adapter, source_name)(path)
    pipeline = (source | CastTypes({field: int})
                | FilterRows((col(field) >= 10) & (col(field) != 100))
                | RenameFields({field: "amount_out"}))
    table = pipeline.to_arrow()
    assert isinstance(table, pa.Table)
    assert table["amount_out"].to_pylist() == [42]
    assert pipeline.to_arrow() is table
    with pytest.warns(RuntimeWarning, match="no streaming reader"):
        batches = list(rypipe.read_batches(path, format=name, batch_size=1))
    assert len(batches) == 2
    with pytest.raises(rypipe.PlanError, match="unknown field type"):
        rypipe.read(path, format=name, field_types={field: "invalid"})
    path.write_bytes(b"bad=\xff\n")
    with pytest.raises(rypipe.ParseError, match="UTF-8"):
        rypipe.read(path, format=name)


@pytest.mark.parametrize("price, day", [
    ("12.50", "2026-09-14"),
    ("1.1234567890123456789", " 2026-09-14 "),
    (" 1.2 ", "2026-9-1"),
])
def test_native_casts_match_cached_and_direct_execution(tmp_path, price, day):
    from datetime import date, datetime
    from decimal import Decimal
    from rypipe_ldif import LdifSource

    values = {"enabled": "false", "count": "12", "ratio": "1.25",
              "day": day, "at": "2026-09-14T12:30:00", "price": price}
    stage = CastTypes({"enabled": bool, "count": int, "ratio": float,
                       "day": date, "at": datetime, "price": Decimal})
    path = tmp_path / "typed.ldif"
    path.write_text("\n".join(f"{key}: {value}" for key, value in values.items()) + "\n",
                    encoding="utf-8")
    source = LdifSource(path)
    fresh = (source | stage).to_arrow()
    source.to_arrow()
    cached = (source | stage).to_arrow()
    assert cached.equals(fresh)
    assert [stage.apply(values.copy())] == fresh.to_pylist()
