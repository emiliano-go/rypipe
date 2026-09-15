from datetime import date, datetime
from decimal import Decimal
from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc

_PY_TO_RUST_TYPE = {
    int: "int64",
    float: "float64",
    str: None,
    bool: "bool",
    date: "date32",
    datetime: "timestamp",
    Decimal: "decimal128",
}

_ARROW_TYPES = {
    "int64": pa.int64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "date32": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "decimal128": pa.decimal128(38, 18),
}


def _cast_column(column, kind):
    if pa.types.is_dictionary(column.type):
        column = pc.dictionary_decode(column)
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        from _rypipe._rypipe import _cast_strings

        return _cast_strings(pa.Table.from_arrays([column], names=["value"]), kind)
    return pc.cast(column, _ARROW_TYPES[kind])


class CastTypes:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, Callable]):
        self._mapping = dict(mapping)

    def apply(self, record: dict) -> dict:
        mapping = self._mapping
        if not mapping:
            return record
        for field, cast_fn in mapping.items():
            try:
                kind = _PY_TO_RUST_TYPE.get(cast_fn)
                value = record[field]
                record[field] = (_cast_column(pa.array([value]), kind)[0].as_py()
                                 if kind is not None else cast_fn(value))
            except KeyError:
                pass
            except (ValueError, TypeError) as e:
                val = record[field]
                raise ValueError(
                    f"CastTypes: cannot cast field '{field}' "
                    f"value {val!r}: {e}"
                ) from e
        return record

    def __call__(self, stream):
        return map(self.apply, stream)

    def _plan_kwargs(self) -> dict | None:
        ft = {}
        for field, fn in self._mapping.items():
            rust_type = _PY_TO_RUST_TYPE.get(fn)
            if rust_type is None:
                return None
            ft[field] = rust_type
        if not ft:
            return None
        return {"field_types": ft, "strict_types": True}
