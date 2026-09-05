from __future__ import annotations

from .lambda_compiler import _analyze_lambda


class _ConstantPredicate:
    __slots__ = ("_field", "_op", "_value")

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

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        if actual is None:
            return False
        return self._OPS[self._op](actual, self._value)


class _ComparePredicate:
    __slots__ = ("_field_a", "_op", "_field_b")

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
        fn = self._OPS.get(self._op)
        return bool(fn(record.get(self._field_a), record.get(self._field_b)))


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
        return str(actual).startswith(self._value)


class _InPredicate:
    """Fusable predicate: r["field"] in values"""
    __slots__ = ("_field", "_values")

    def __init__(self, field: str, values: tuple):
        self._field = field
        self._values = values

    def __call__(self, record: dict) -> bool:
        actual = record.get(self._field)
        return actual in self._values


def _build_predicate_from_spec(spec: dict):
    """Build a predicate callable from a filter spec dict (used for Python fallback)."""
    if "field" in spec and "op" in spec and "value" in spec:
        op = spec["op"]
        if op == "starts_with":
            return _StartsWithPredicate(spec["field"], spec["value"])
        return _ConstantPredicate(spec["field"], op, spec["value"])
    if "field_a" in spec and "op" in spec and "field_b" in spec:
        return _ComparePredicate(spec["field_a"], spec["op"], spec["field_b"])
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
    ):
        if predicate is not None:
            # Try to compile the lambda into a fusable filter spec
            spec = _analyze_lambda(predicate)
            if spec is not None:
                # Lambda was compiled successfully — use the spec for fusion
                self._filter_spec = spec
                self._predicate = _build_predicate_from_spec(spec)
            else:
                # Unknown pattern — fall back to Python execution
                self._predicate = predicate
                self._filter_spec = None
        elif field is not None and op is not None and value is not None:
            self._filter_spec = {"field": field, "op": op, "value": value}
            self._predicate = _ConstantPredicate(field, op, value)
        elif field_a is not None and op is not None and field_b is not None:
            self._filter_spec = {"field_a": field_a, "op": op, "field_b": field_b}
            self._predicate = _ComparePredicate(field_a, op, field_b)
        else:
            raise ValueError(
                "FilterRows requires either a callable predicate or "
                "keyword arguments (field+op+value for constant filter, "
                "or field_a+op+field_b for column comparison). "
                f"Got predicate={predicate!r}, field={field!r}, op={op!r}, "
                f"value={value!r}, field_a={field_a!r}, field_b={field_b!r}"
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
    if not isinstance(obj, FilterRows):
        raise TypeError(f"{label} expects FilterRows instances, got {type(obj).__name__!r}")
    if obj._filter_spec is None:
        raise ValueError(
            f"{label} only accepts fusable filters: FilterRows with field/op/value "
            f"or field_a/op/field_b keyword form. Pass FilterRows(..., field=..., op=..., "
            f"value=...) or field_a/field_b instead of a plain lambda/Callable."
        )
    return obj._filter_spec


class FilterRowsAny:
    """Keep rows that satisfy **any** of the given fusable filters (OR).

    Each argument must be a :class:`FilterRows` built with the keyword form
    (``field``/``field_a``) so it can be pushed into the Rust parse loop.

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
            if f._predicate(record):
                return record
        return None

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        return {"filter": {"or": self._specs}}


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
            if not f._predicate(record):
                return None
        return record

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        return {"filter": {"and": self._specs}}


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
        return None if self._inner._predicate(record) else record

    def __call__(self, stream):
        return (r for r in map(self.apply, stream) if r is not None)

    def _plan_kwargs(self) -> dict | None:
        return {"filter": {"not": self._spec}}
