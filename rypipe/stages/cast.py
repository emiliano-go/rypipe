from datetime import date, datetime
from typing import Callable

_PY_TO_RUST_TYPE = {
    int: "int64",
    float: "float64",
    str: None,
    bool: "bool",
    date: "date32",
    datetime: "timestamp",
}


class CastTypes:
    __slots__ = ("_mapping",)

    def __init__(self, mapping: dict[str, Callable]):
        self._mapping = mapping

    def apply(self, record: dict) -> dict:
        mapping = self._mapping
        if not mapping:
            return record
        for field, cast_fn in mapping.items():
            try:
                record[field] = cast_fn(record[field])
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
                if fn is str:
                    continue
                return None
            ft[field] = rust_type
        if not ft:
            return None
        return {"field_types": ft}
