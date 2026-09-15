from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from math import isinf


def _filter_string(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if isinf(value):
            return str(value)
        return format(Decimal(str(value)), "f").removesuffix(".0")
    if isinstance(value, datetime):
        return str(value)
    return str(value)


def _literal_scalar(value, dtype):
    import pyarrow as pa
    import pyarrow.compute as pc

    if pa.types.is_dictionary(dtype):
        dtype = dtype.value_type
    value = _filter_string(value)
    if pa.types.is_temporal(dtype):
        raw_type = pa.int32() if pa.types.is_date32(dtype) else pa.int64()
        return pc.cast(pa.scalar(int(value), type=raw_type), dtype)
    return pc.cast(pa.scalar(value), dtype)


class _ConstantPredicate:
    __slots__ = ("_field", "_op", "_value", "_literals")

    _VALID_OPS = frozenset({
        "==", "eq", "!=", "ne",
        ">", "gt", "<", "lt",
        ">=", "ge", "<=", "le",
    })

    _OPS = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "gt": lambda a, b: a > b,
        "lt": lambda a, b: a < b,
        "ge": lambda a, b: a >= b,
        "le": lambda a, b: a <= b,
    }

    def __init__(self, field: str, op: str, value: str):
        if op not in self._VALID_OPS:
            raise ValueError(
                f"FilterRows: unsupported operator {op!r} for constant filter; "
                f"valid operators: {', '.join(sorted(self._VALID_OPS))}"
            )
        self._field = field
        self._op = op
        self._value = value
        self._literals = {}

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        if actual is None:
            return False
        if isinstance(actual, str):
            return self._OPS[self._op](actual, _filter_string(self._value))
        import pyarrow as pa

        key = (type(actual), actual.as_tuple().exponent if isinstance(actual, Decimal) else None)
        if key not in self._literals:
            try:
                dtype = pa.scalar(actual).type
                if pa.types.is_decimal(dtype):
                    dtype = pa.decimal128(38, dtype.scale)
                value = _literal_scalar(self._value, dtype).as_py()
            except (ValueError, TypeError, pa.ArrowException):
                value = None
            self._literals[key] = value
        value = self._literals[key]
        if value is None:
            return False
        if actual != actual or value != value:
            return False
        return self._OPS[self._op](actual, value)


class _ComparePredicate:
    __slots__ = ("_field_a", "_op", "_field_b")

    _OPS = _ConstantPredicate._OPS

    def __init__(self, field_a: str, op: str, field_b: str):
        if op not in self._OPS:
            valid = ", ".join(sorted(self._OPS))
            raise ValueError(
                f"FilterRows: unsupported operator {op!r} for column comparison; "
                f"valid operators: {valid}"
            )
        self._field_a = field_a
        self._op = op
        self._field_b = field_b

    def __call__(self, record: dict) -> bool:
        a, b = record.get(self._field_a), record.get(self._field_b)
        if a is None or b is None or a != a or b != b:
            return False
        if type(a) is not type(b) and not (type(a) in (int, float) and type(b) in (int, float)):
            return False
        if type(a) is not type(b) and type(a) in (int, float) and type(b) in (int, float):
            a, b = float(a), float(b)
        return bool(self._OPS[self._op](a, b))


def _comparison(op):
    try:
        return _ConstantPredicate._OPS[op]
    except KeyError:
        raise ValueError(f"unsupported comparison operator {op!r}") from None


class _StartsWithPredicate:
    """Fusable predicate: r["field"].startswith(value)"""
    __slots__ = ("_field", "_value")

    def __init__(self, field: str, value: str):
        self._field = field
        self._value = value

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        if actual is None:
            return False
        return _filter_string(actual).startswith(self._value)


class _EndsWithPredicate:
    """Fusable predicate: r["field"].endswith(value)"""
    __slots__ = ("_field", "_value")

    def __init__(self, field: str, value: str):
        self._field = field
        self._value = value

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        if actual is None:
            return False
        return _filter_string(actual).endswith(self._value)


class _RegexPredicate:
    """Fusable predicate: regex search against str(r["field"])

    Warning: user-supplied regex patterns can cause catastrophic backtracking
    (ReDoS) on adversarial input. Use simple patterns for untrusted data.
    """
    __slots__ = ("_field", "_value", "_compiled")

    def __init__(self, field: str, value: str):
        self._field = field
        self._value = value
        self._compiled = re.compile(value)

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        if actual is None:
            return False
        return bool(self._compiled.search(_filter_string(actual)))


class _InPredicate:
    """Fusable predicate: r["field"] in values or r["field"] not in values"""
    __slots__ = ("_field", "_values", "_negate")

    def __init__(self, field: str, values: tuple, negate: bool = False):
        self._field = field
        self._values = values
        self._negate = negate

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        result = actual is not None and _filter_string(actual) in self._values
        return not result if self._negate else result


def _build_predicate_from_spec(spec: dict):
    """Build a predicate callable from a filter spec dict (used for Python fallback)."""
    if "always" in spec:
        val = spec["always"]
        return lambda r: val
    if "field" in spec and "op" in spec:
        op = spec["op"]
        if op == "is_null":
            return _IsNullPredicate(spec["field"])
        if op == "is_type":
            return _IsTypePredicate(spec["field"], spec["value"])
        if "value" in spec:
            if op == "regex":
                return _RegexPredicate(spec["field"], spec["value"])
            if op == "starts_with":
                return _StartsWithPredicate(spec["field"], spec["value"])
            if op == "ends_with":
                return _EndsWithPredicate(spec["field"], spec["value"])
            if op == "contains":
                field, value = spec["field"], spec["value"]
                return lambda r: r.get(field) is not None and value in _filter_string(r[field])
            if op in ("strip", "lstrip", "rstrip", "lower", "upper"):
                field, value, cmp_op = spec["field"], spec["value"], spec.get("cmp_op", "==")
                cmp_fn = _comparison(cmp_op)
                transforms = {
                    "strip": lambda s: s.strip(), "lstrip": lambda s: s.lstrip(),
                    "rstrip": lambda s: s.rstrip(), "lower": lambda s: s.lower(),
                    "upper": lambda s: s.upper(),
                }
                transform = transforms[op]
                return lambda r: r.get(field) is not None and cmp_fn(transform(_filter_string(r[field])), value)
            if op == "length":
                field, value, cmp_op = spec["field"], spec["value"], spec.get("cmp_op", ">")
                cmp_fn = _comparison(cmp_op)
                return lambda r: r.get(field) is not None and cmp_fn(float(len(_filter_string(r[field]))), float(value))
            if "arith_op" in spec:
                # Arithmetic compare: field * arith_value op cmp_value
                arith_op = spec["arith_op"]
                arith_val = float(spec["arith_value"])
                cmp_val = spec["value"]
                arith_fns = {
                    "+": lambda a, b: a + b,
                    "-": lambda a, b: a - b,
                    "*": lambda a, b: a * b,
                    "/": lambda a, b: a / b,
                }
                arith_fn = arith_fns[arith_op]
                cmp_fn = _comparison(op)
                field = spec["field"]
                def arithmetic(r):
                    actual = r.get(field)
                    if not isinstance(actual, (str, int, float)) or isinstance(actual, bool):
                        return False
                    try:
                        result = arith_fn(float(actual), arith_val)
                        target = float(cmp_val)
                    except (ValueError, TypeError, ZeroDivisionError, OverflowError):
                        return False
                    return result == result and target == target and cmp_fn(result, target)
                return arithmetic
            if "old" in spec and "new" in spec:
                field, old, new, cmp_op, value = spec["field"], spec["old"], spec["new"], spec.get("cmp_op", "=="), spec["value"]
                cmp_fn = _comparison(cmp_op)
                return lambda r: r.get(field) is not None and cmp_fn(_filter_string(r[field]).replace(old, new), value)
            return _ConstantPredicate(spec["field"], op, spec["value"])
        if "values" in spec:
            return _InPredicate(spec["field"], spec["values"], negate=(op == "not_in"))
        return _ComparePredicate(spec["field_a"], op, spec["field_b"])
    if "field_a" in spec and "op" in spec and "field_b" in spec:
        return _ComparePredicate(spec["field_a"], spec["op"], spec["field_b"])
    if "not_field" in spec:
        return lambda r: r.get(spec["not_field"]) is None or _filter_string(r[spec["not_field"]]) == ""
    if "and" in spec:
        predicates = [_build_predicate_from_spec(s) for s in spec["and"]]
        return lambda r: all(p(r) for p in predicates)
    if "or" in spec:
        predicates = [_build_predicate_from_spec(s) for s in spec["or"]]
        return lambda r: any(p(r) for p in predicates)
    if "not" in spec:
        inner = _build_predicate_from_spec(spec["not"])
        return lambda r: not inner(r)
    return None


class _IsNullPredicate:
    """Fusable predicate: r["field"] is None or missing"""
    __slots__ = ("_field",)

    def __init__(self, field: str):
        self._field = field

    def __call__(self, record: dict) -> bool:
        return record.get(self._field) is None


class _NotNullPredicate:
    """Fusable predicate: r["field"] is not None and present"""
    __slots__ = ("_field",)

    def __init__(self, field: str):
        self._field = field

    def __call__(self, record: dict) -> bool:
        return record.get(self._field) is not None


class _IsTypePredicate:
    """Fusable predicate: r["field"] is of the given type"""
    __slots__ = ("_field", "_field_type")

    _VALID_TYPES = frozenset({
        "string", "int64", "float64", "bool", "boolean",
        "dictionary", "date32", "timestamp", "decimal128",
    })

    def __init__(self, field: str, field_type: str):
        if field_type.lower() not in self._VALID_TYPES:
            raise ValueError(
                f"FilterRows: unsupported type {field_type!r}; "
                f"valid types: {', '.join(sorted(self._VALID_TYPES))}"
            )
        self._field = field
        self._field_type = field_type.lower()

    def __call__(self, record: dict) -> bool:
        val = record.get(self._field)
        if val is None:
            return False
        kind = self._field_type
        if kind == "string":
            return isinstance(val, (str, int, float, date, Decimal))
        if isinstance(val, str):
            if kind == "dictionary":
                return True
            if kind in ("bool", "boolean"):
                return val.lower() in {"true", "false", "1", "0", "yes", "no"}
            if kind in ("int64", "timestamp") and re.fullmatch(r"[+-]?[0-9]+", val):
                return -(1 << 63) <= int(val) < (1 << 63)
            try:
                if kind in ("date32", "timestamp"):
                    date.fromisoformat(val.strip())
                    return True
                if kind in ("float64", "decimal128") and val == val.strip() and "_" not in val:
                    float(val)
                    return True
            except (ValueError, OverflowError):
                pass
            return False
        if kind == "int64":
            if isinstance(val, (int, date, Decimal)):
                return True
            return isinstance(val, float) and val.is_integer() and -(1 << 63) <= val < (1 << 63)
        if kind == "float64":
            return isinstance(val, (int, float, Decimal))
        if kind in ("bool", "boolean"):
            return isinstance(val, (int, float)) and val in (0, 1)
        if kind == "date32":
            return type(val) is date
        if kind == "timestamp":
            return isinstance(val, datetime)
        if kind == "decimal128":
            return isinstance(val, Decimal)
        return False


class FilterRows:
    __slots__ = ("_predicate", "_filter_spec")

    def __init__(
        self,
        predicate=None,
        *,
        field=None,
        op=None,
        value=None,
        field_a=None,
        field_b=None,
        is_null=None,
        is_type=None,
    ):
        if predicate is not None:
            to_spec = getattr(predicate, "_to_spec", None)
            if callable(to_spec):
                # Expression API predicate (rypipe.expr.Predicate); fusable
                spec = to_spec()
                self._filter_spec = spec
                self._predicate = _build_predicate_from_spec(spec)
                if self._predicate is None:
                    raise ValueError(
                        f"FilterRows: cannot build a predicate from spec {spec!r}"
                    )
            else:
                # Plain callable; Python fallback execution, not fusable
                if not callable(predicate):
                    raise TypeError("FilterRows predicate must be an expression or callable")
                self._predicate = predicate
                self._filter_spec = None
        elif is_null is not None and field is not None:
            if is_null:
                self._filter_spec = {"field": field, "op": "is_null"}
                self._predicate = _IsNullPredicate(field)
            else:
                self._filter_spec = {"not": {"field": field, "op": "is_null"}}
                self._predicate = _NotNullPredicate(field)
        elif is_type is not None and field is not None:
            self._filter_spec = {"field": field, "op": "is_type", "value": is_type}
            self._predicate = _IsTypePredicate(field, is_type)
        elif field is not None and op is not None and value is not None:
            from ..expr import _spec_value

            value = _spec_value(value)
            self._filter_spec = {"field": field, "op": op, "value": value}
            _EXPRESSION_ONLY_OPS = frozenset({
                "strip", "lstrip", "rstrip", "lower", "upper", "length",
            })
            if op in _EXPRESSION_ONLY_OPS:
                raise ValueError(
                    f"FilterRows keyword args do not support op={op!r}; "
                    f"use the expression API instead: col({field!r}).{op}(...)"
                )
            if op == "regex":
                self._predicate = _RegexPredicate(field, value)
            else:
                self._predicate = _ConstantPredicate(field, op, value)
        elif field_a is not None and op is not None and field_b is not None:
            self._filter_spec = {"field_a": field_a, "op": op, "field_b": field_b}
            self._predicate = _ComparePredicate(field_a, op, field_b)
        else:
            raise ValueError(
                "FilterRows requires either a callable predicate, an "
                "expression predicate (see rypipe.expr), or "
                "keyword arguments (field+op+value for constant filter, "
                "field_a+op+field_b for column comparison, "
                "field+is_null=True for a null check or field+is_null=False "
                "for a not-null check, or field+is_type for type check). "
                f"Got predicate={predicate!r}, field={field!r}, op={op!r}, "
                f"value={value!r}, field_a={field_a!r}, field_b={field_b!r}, "
                f"is_null={is_null!r}, is_type={is_type!r}"
            )

    def apply(self, record: dict) -> dict | None:
        return record if self._predicate(record) else None

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        if self._filter_spec is not None:
            return {"filter": self._filter_spec}
        return None


# ---------------------------------------------------------------------------
# Boolean combinators over fusable FilterRows
# ---------------------------------------------------------------------------

def _require_filter_spec(obj, label: str) -> dict:
    """Extract a fusable spec or raise with a helpful message."""
    if isinstance(obj, FilterRows):
        if obj._filter_spec is None:
            raise ValueError(
                f"{label} only accepts fusable filters: FilterRows with field/op/value "
                f"or field_a/op/field_b keyword form, or an expression predicate "
                f"(col(...)). Plain lambdas/Callables cannot be combined."
            )
        return obj._filter_spec
    if isinstance(obj, (FilterRowsAny, FilterRowsAll, FilterRowsNot)):
        return obj._combined_spec()
    raise TypeError(
        f"{label} expects FilterRows or combinator instances, got {type(obj).__name__!r}"
    )


def _matches(obj, record: dict) -> bool:
    """Uniform row test for FilterRows and combinators."""
    if isinstance(obj, FilterRows):
        return obj._predicate(record)
    return obj.apply(record) is not None


class FilterRowsAny:
    """Keep rows that satisfy **any** of the given fusable filters (OR).

    Each argument must be a :class:`FilterRows` built with the keyword form
    (``field``/``field_a``) or another combinator, so the whole tree can be
    pushed into the Rust parse loop. Combinators nest:
    ``FilterRowsAny(A, FilterRowsAll(B, FilterRowsNot(C)))`` is
    ``A or (B and not C)``.

    Example::

        FilterRowsAny(
            FilterRows(field="dept", op="==", value="sales"),
            FilterRows(field="tenure", op="==", value="junior"),
        )
        # keeps rows where dept == 'sales' OR tenure == 'junior'
    """

    __slots__ = ("_filters", "_specs")

    def __init__(self, *filters: FilterRows):
        if len(filters) < 2:
            raise ValueError("FilterRowsAny requires at least two filters")
        self._filters = filters
        self._specs = [_require_filter_spec(f, "FilterRowsAny") for f in filters]

    def apply(self, record: dict) -> dict | None:
        for f in self._filters:
            if _matches(f, record):
                return record
        return None

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _combined_spec(self) -> dict:
        return {"or": self._specs}

    def _plan_kwargs(self) -> dict | None:
        return {"filter": self._combined_spec()}


class FilterRowsAll:
    """Keep rows that satisfy **all** of the given fusable filters (AND).

    Chaining plain ``FilterRows`` stages with ``|`` already implies AND; this
    class makes an explicit conjunction useful when combining inside another
    combinator or when the stage order matters.

    Example::

        FilterRowsAll(
            FilterRows(field="status", op="==", value="active"),
            FilterRows(field_a="price", op=">", field_b="cost"),
        )
    """

    __slots__ = ("_filters", "_specs")

    def __init__(self, *filters: FilterRows):
        if len(filters) < 2:
            raise ValueError("FilterRowsAll requires at least two filters")
        self._filters = filters
        self._specs = [_require_filter_spec(f, "FilterRowsAll") for f in filters]

    def apply(self, record: dict) -> dict | None:
        for f in self._filters:
            if not _matches(f, record):
                return None
        return record

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _combined_spec(self) -> dict:
        return {"and": self._specs}

    def _plan_kwargs(self) -> dict | None:
        return {"filter": self._combined_spec()}


class FilterRowsNot:
    """Negate a single fusable filter.

    Example::

        FilterRowsNot(FilterRows(field="status", op="==", value="deleted"))
        # keeps rows where status != 'deleted'
    """

    __slots__ = ("_inner", "_spec")

    def __init__(self, inner: FilterRows):
        self._inner = inner
        self._spec = _require_filter_spec(inner, "FilterRowsNot")

    def apply(self, record: dict) -> dict | None:
        return None if _matches(self._inner, record) else record

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _combined_spec(self) -> dict:
        return {"not": self._spec}

    def _plan_kwargs(self) -> dict | None:
        return {"filter": self._combined_spec()}
