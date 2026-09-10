"""Polars-style expression API for fusable filters.

Expressions build the same filter spec dicts that the Rust engine fuses,
without lambda bytecode analysis:

    from rypipe.expr import col

    src | FilterRows(col("amount") > 100)
    src | FilterRows((col("age") >= 18) & col("name").startswith("A"))

An expression compares to a literal or another column; the result is a
`Predicate` whose `_to_spec()` produces the plan spec. Anything the spec
language cannot express raises at construction time, so there is no silent
fallback to per-row Python.
"""

from __future__ import annotations

import re
from typing import Any

__all__ = ["col", "Expr", "Predicate"]

_CMP_OPS = {
    "==": "==",
    "!=": "!=",
    ">": ">",
    "<": "<",
    ">=": ">=",
    "<=": "<=",
}


def _spec_value(value: Any) -> str:
    """Coerce a Python literal to the string form specs carry."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    raise TypeError(
        f"unsupported literal {value!r} in filter expression; "
        "use int, float, str, or bool"
    )


class Expr:
    """A column reference; combinators produce `Predicate` objects."""

    __slots__ = ("_field",)

    def __init__(self, field: str):
        if not isinstance(field, str) or not field:
            raise TypeError(f"col() expects a non-empty column name, got {field!r}")
        self._field = field

    # -- comparisons -----------------------------------------------------
    def _compare(self, op: str, other: Any) -> "Predicate":
        if isinstance(other, Expr):
            return Predicate(
                {"field_a": self._field, "op": op, "field_b": other._field}
            )
        return Predicate({"field": self._field, "op": op, "value": _spec_value(other)})

    def __eq__(self, other: Any) -> "Predicate":  # type: ignore[override]
        return self._compare("==", other)

    def __ne__(self, other: Any) -> "Predicate":  # type: ignore[override]
        return self._compare("!=", other)

    def __gt__(self, other: Any) -> "Predicate":
        return self._compare(">", other)

    def __lt__(self, other: Any) -> "Predicate":
        return self._compare("<", other)

    def __ge__(self, other: Any) -> "Predicate":
        return self._compare(">=", other)

    def __le__(self, other: Any) -> "Predicate":
        return self._compare("<=", other)

    __hash__ = None  # type: ignore[assignment]

    # -- membership -------------------------------------------------------
    def isin(self, values) -> "Predicate":
        vals = [_spec_value(v) for v in values]
        if not vals:
            raise ValueError("isin() requires at least one value")
        return Predicate({"field": self._field, "op": "in", "values": vals})

    def not_in(self, values) -> "Predicate":
        vals = [_spec_value(v) for v in values]
        if not vals:
            raise ValueError("not_in() requires at least one value")
        return Predicate({"field": self._field, "op": "not_in", "values": vals})

    # -- string methods ---------------------------------------------------
    def startswith(self, prefix: str) -> "Predicate":
        return Predicate(
            {"field": self._field, "op": "starts_with", "value": _spec_value(prefix)}
        )

    def endswith(self, suffix: str) -> "Predicate":
        return Predicate(
            {"field": self._field, "op": "ends_with", "value": _spec_value(suffix)}
        )

    def contains(self, needle: str) -> "Predicate":
        return Predicate(
            {"field": self._field, "op": "contains", "value": _spec_value(needle)}
        )

    def matches(self, pattern: str) -> "Predicate":
        """Regex search against the string form of the column value."""
        if not isinstance(pattern, str):
            raise TypeError(f"matches() expects a str pattern, got {pattern!r}")
        re.compile(pattern)
        return Predicate({"field": self._field, "op": "regex", "value": pattern})

    def between(self, lo: Any, hi: Any) -> "Predicate":
        """Inclusive range check: lo <= column <= hi."""
        return Predicate(
            {
                "and": [
                    {"field": self._field, "op": ">=", "value": _spec_value(lo)},
                    {"field": self._field, "op": "<=", "value": _spec_value(hi)},
                ]
            }
        )

    # -- null and type checks ---------------------------------------------
    def is_null(self) -> "Predicate":
        return Predicate({"field": self._field, "op": "is_null"})

    def is_not_null(self) -> "Predicate":
        return Predicate({"not": {"field": self._field, "op": "is_null"}})

    def is_type(self, type_name: str) -> "Predicate":
        return Predicate(
            {"field": self._field, "op": "is_type", "value": type_name}
        )

    def __repr__(self) -> str:
        return f"col({self._field!r})"


class Predicate:
    """A boolean expression; composes with `&`, `|`, `~`."""

    __slots__ = ("_spec",)

    def __init__(self, spec: dict):
        self._spec = spec

    def __and__(self, other: "Predicate") -> "Predicate":
        if not isinstance(other, Predicate):
            return NotImplemented
        return Predicate({"and": [self._spec, other._spec]})

    def __or__(self, other: "Predicate") -> "Predicate":
        if not isinstance(other, Predicate):
            return NotImplemented
        return Predicate({"or": [self._spec, other._spec]})

    def __invert__(self) -> "Predicate":
        return Predicate({"not": self._spec})

    def _to_spec(self) -> dict:
        return self._spec

    def __repr__(self) -> str:
        return f"Predicate({self._spec!r})"


def col(field: str) -> Expr:
    """Reference a column by name."""
    return Expr(field)
