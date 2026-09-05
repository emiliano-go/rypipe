"""Bytecode pattern analyzer for compiling lambdas into fusable filter specs.

Detects common lambda patterns (field comparisons, string methods, membership,
truthiness, compound logic) and converts them to filter spec dicts that the
Rust engine can fuse. Unknown patterns fall back to Python execution.

Supported patterns:
    - r["field"] op literal          → CompareLiteral / Equal / NotEqual
    - r["field_a"] op r["field_b"]  → Compare
    - r["field"].startswith("x")    → StartsWith
    - r["field"].endswith("x")      → EndsWith
    - r["field"] in (...)           → InPredicate
    - r["field"] not in (...)       → NotInPredicate
    - not r["field"]                → Not(TruthyPredicate)
    - lambda r: True / False        → AlwaysTrue / AlwaysFalse
    - a and b                       → And(left, right)
    - a or b                        → Or(left, right)

Unsupported (fall back to Python):
    - Closures (value from outer scope)
    - Nested function calls: int(r["x"]), float(r["x"])
    - Arithmetic: r["a"] * 2 > 100
    - Method chains: r["name"].strip().lower()
    - Complex compound: a and b or c
"""

from __future__ import annotations

import dis
from typing import Optional


def _analyze_lambda(fn) -> Optional[dict]:
    """Try to compile a lambda into a fusable filter spec.

    Returns a filter spec dict if the lambda matches a known pattern,
    or None if the pattern is unknown (fall back to Python execution).
    """
    try:
        bytecode = list(dis.get_instructions(fn))
    except TypeError:
        return None

    # Must be a simple lambda: one argument
    if len(fn.__code__.co_varnames) != 1:
        return None

    # Filter to meaningful instructions (skip RESUME, CACHE, etc.)
    ops = [i for i in bytecode if i.opname not in (
        "RESUME", "PUSH_NULL", "PRECALL", "CACHE",
        "COPY", "TO_BOOL", "POP_JUMP_IF_FALSE", "POP_JUMP_IF_TRUE",
        "POP_TOP", "NOT_TAKEN",
    )]

    # Try each pattern detector (compound first, then simple)
    for detector in [
        _match_bool_constant,
        _match_compound_or,
        _match_compound_and,
        _match_cast_and_compare,
        _match_arithmetic_compare,
        _match_not_field,
        _match_field_in_collection,
        _match_field_op_literal,
        _match_field_op_field,
        _match_field_method_literal,
    ]:
        result = detector(bytecode, ops)
        if result is not None:
            return result

    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COMP_OP_MAP = {
    "<": "<",
    "lt": "<",
    "<=": "<=",
    "le": "<=",
    "==": "==",
    "eq": "==",
    "!=": "!=",
    "ne": "!=",
    ">": ">",
    "gt": ">",
    ">=": ">=",
    "ge": ">=",
    "not in": "not in",
    "in": "in",
    "is": "==",
    "is not": "!=",
    "LessThan": "<",
    "LessEqual": "<=",
    "Equal": "==",
    "NotEqual": "!=",
    "GreaterThan": ">",
    "GreaterEqual": ">=",
}


def _normalize_op(op) -> Optional[str]:
    """Normalize a comparison operator string to a standard form."""
    return _COMP_OP_MAP.get(op)


def _is_load_fast(instr) -> bool:
    return instr.opname in ("LOAD_FAST", "LOAD_FAST_BORROW")


def _is_load_const_str(instr) -> bool:
    return instr.opname == "LOAD_CONST" and isinstance(instr.argval, str)


def _is_load_const_value(instr) -> bool:
    return instr.opname in ("LOAD_CONST", "LOAD_SMALL_INT")


def _field_access(ops: list, i: int) -> Optional[str]:
    """Check for field access pattern: LOAD_FAST LOAD_CONST BINARY_OP.
    Returns the field name if matched, None otherwise."""
    if i + 2 >= len(ops):
        return None
    if not _is_load_fast(ops[i]):
        return None
    if not _is_load_const_str(ops[i + 1]):
        return None
    if ops[i + 2].opname != "BINARY_OP":
        return None
    return ops[i + 1].argval


# ---------------------------------------------------------------------------
# Pattern: lambda r: True / lambda r: False
# Bytecode: LOAD_CONST(True/False) RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_bool_constant(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: lambda r: True or lambda r: False"""
    if len(ops) == 2:
        if ops[0].opname == "LOAD_CONST" and ops[0].argval is True:
            if ops[1].opname == "RETURN_VALUE":
                return {"always": True}
        if ops[0].opname == "LOAD_CONST" and ops[0].argval is False:
            if ops[1].opname == "RETURN_VALUE":
                return {"always": False}
    return None


# ---------------------------------------------------------------------------
# Pattern: int(r["field"]) op literal
# Bytecode: LOAD_GLOBAL(int/float/str/bool) LOAD_FAST LOAD_CONST BINARY_OP
#           CALL LOAD_CONST COMPARE_OP RETURN_VALUE
# ---------------------------------------------------------------------------

_CAST_FUNCTIONS = {"int", "float", "str", "bool"}


def _match_cast_and_compare(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: int(r["field"]) > 100, float(r["field"]) >= 0, etc."""
    for i in range(len(ops) - 6):
        # LOAD_GLOBAL for cast function
        if ops[i].opname != "LOAD_GLOBAL":
            continue
        cast_fn = ops[i].argval
        if cast_fn not in _CAST_FUNCTIONS:
            continue

        # Field access: LOAD_FAST LOAD_CONST BINARY_OP
        field = _field_access(ops, i + 1)
        if field is None:
            continue

        # CALL
        if ops[i + 4].opname != "CALL":
            continue

        # Value
        if not _is_load_const_value(ops[i + 5]):
            continue

        # COMPARE_OP
        if ops[i + 6].opname != "COMPARE_OP":
            continue
        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue

        # RETURN_VALUE
        if i + 7 >= len(ops) or ops[i + 7].opname != "RETURN_VALUE":
            continue

        value = ops[i + 5].argval

        # Cast the value to the target type for correct comparison
        if cast_fn == "int":
            try:
                value = int(value)
            except (ValueError, TypeError):
                continue
        elif cast_fn == "float":
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue
        elif cast_fn == "bool":
            if value in ("True", "true", "1"):
                value = True
            elif value in ("False", "false", "0"):
                value = False
            else:
                continue

        return {
            "field": field,
            "op": op,
            "value": str(value),
        }

    return None


_ARITH_OPS = {
    0: "+",   # ADD
    1: "-",   # SUBTRACT
    5: "*",   # MULTIPLY
    6: "/",   # TRUE_DIVIDE
}


# ---------------------------------------------------------------------------
# Pattern: r["field"] * 2 > 100 (arithmetic then compare)
# Bytecode: LOAD_FAST LOAD_CONST BINARY_OP([]) LOAD_CONST(arith_val)
#           BINARY_OP(arith) LOAD_CONST(cmp_val) COMPARE_OP RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_arithmetic_compare(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: r["field"] <op> <constant> <arith_op> <constant>"""
    for i in range(len(ops) - 6):
        field = _field_access(ops, i)
        if field is None:
            continue

        # After field access: LOAD_CONST(arith_val) BINARY_OP(arith) LOAD_CONST(cmp_val) COMPARE_OP
        if ops[i + 3].opname not in ("LOAD_CONST", "LOAD_SMALL_INT"):
            continue
        if ops[i + 4].opname != "BINARY_OP":
            continue
        if ops[i + 5].opname not in ("LOAD_CONST", "LOAD_SMALL_INT"):
            continue
        if ops[i + 6].opname != "COMPARE_OP":
            continue
        if i + 7 >= len(ops) or ops[i + 7].opname != "RETURN_VALUE":
            continue

        arith_op_code = ops[i + 4].argval
        arith_val = ops[i + 3].argval
        cmp_val = ops[i + 5].argval
        cmp_op = _normalize_op(ops[i + 6].argval)
        if cmp_op is None:
            continue

        arith_symbol = _ARITH_OPS.get(arith_op_code)
        if arith_symbol is None:
            continue

        return {
            "field": field,
            "op": cmp_op,
            "value": str(cmp_val),
            "arith_op": arith_symbol,
            "arith_value": str(arith_val),
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"] op literal_value
# Filtered ops: LOAD_FAST, LOAD_CONST, BINARY_OP, LOAD_CONST/LOAD_SMALL_INT,
#               COMPARE_OP, RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_field_op_literal(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: r["field"] op literal_value"""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue

        if not _is_load_const_value(ops[i + 3]):
            continue
        if ops[i + 4].opname != "COMPARE_OP":
            continue
        if i + 5 >= len(ops) or ops[i + 5].opname != "RETURN_VALUE":
            continue

        op = _normalize_op(ops[i + 4].argval)
        if op is None:
            continue

        return {
            "field": field,
            "op": op,
            "value": str(ops[i + 3].argval),
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field_a"] op r["field_b"]
# ---------------------------------------------------------------------------

def _match_field_op_field(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: r["field_a"] op r["field_b"]"""
    for i in range(len(ops) - 5):
        field_a = _field_access(ops, i)
        if field_a is None:
            continue

        field_b = _field_access(ops, i + 3)
        if field_b is None:
            continue

        if i + 6 >= len(ops) or ops[i + 6].opname != "COMPARE_OP":
            continue
        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue
        if i + 7 >= len(ops) or ops[i + 7].opname != "RETURN_VALUE":
            continue

        return {
            "field_a": field_a,
            "op": op,
            "field_b": field_b,
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"].method(arg)
# ---------------------------------------------------------------------------

def _match_field_method_literal(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: r["field"].startswith/endswith literal"""
    for i in range(len(ops) - 5):
        field = _field_access(ops, i)
        if field is None:
            continue

        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        method = ops[i + 3].argval
        if method not in ("startswith", "endswith"):
            continue
        if ops[i + 4].opname != "LOAD_CONST":
            continue
        if ops[i + 5].opname != "CALL":
            continue

        value = ops[i + 4].argval

        if method == "startswith":
            return {"field": field, "op": "starts_with", "value": str(value)}
        elif method == "endswith":
            return {"field": field, "op": "ends_with", "value": str(value)}

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"] in (tuple/list)
# Bytecode: LOAD_FAST LOAD_CONST BINARY_OP LOAD_CONST(tup) CONTAINS_OP RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_field_in_collection(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: r["field"] in (...) or r["field"] not in (...)"""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue

        # Collection must be a tuple or list constant
        if ops[i + 3].opname != "LOAD_CONST":
            continue
        coll = ops[i + 3].argval
        if not isinstance(coll, (tuple, list)):
            continue

        # CONTAINS_OP
        if ops[i + 4].opname != "CONTAINS_OP":
            continue
        # arg=0 means "in", arg=1 means "not in"
        is_not = ops[i + 4].argval == 1

        if i + 5 >= len(ops) or ops[i + 5].opname != "RETURN_VALUE":
            continue

        values = tuple(str(v) for v in coll)
        op = "not_in" if is_not else "in"
        return {"field": field, "op": op, "values": values}

    return None


# ---------------------------------------------------------------------------
# Pattern: not r["field"]
# Bytecode: LOAD_FAST LOAD_CONST BINARY_OP TO_BOOL UNARY_NOT RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_not_field(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: not r["field"] (truthiness negation)"""
    # Check for UNARY_NOT in raw bytecode
    for i, instr in enumerate(raw_bytecode):
        if instr.opname == "UNARY_NOT":
            # Everything before UNARY_NOT should be a field access
            before = [op for op in ops if op.offset < instr.offset]
            field = _field_access(before, 0) if len(before) >= 3 else None
            if field is None:
                continue
            # RETURN_VALUE should follow
            after = [op for op in ops if op.offset > instr.offset]
            if not after or after[0].opname != "RETURN_VALUE":
                continue
            return {"not_field": field}

    return None


# ---------------------------------------------------------------------------
# Pattern: compound AND (a and b)
# Uses raw bytecode to find POP_JUMP_IF_FALSE
# ---------------------------------------------------------------------------

def _match_compound_and(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: simple_expression and simple_expression"""
    marker_idx = _find_logic_marker(raw_bytecode, "POP_JUMP_IF_FALSE")
    if marker_idx is None:
        return None

    and_offset = raw_bytecode[marker_idx].offset
    left_ops = [op for op in ops if op.offset < and_offset]
    right_ops = [op for op in ops if op.offset > and_offset]

    left_spec = _match_simple(left_ops)
    right_spec = _match_simple(right_ops)

    if left_spec is None or right_spec is None:
        return None

    return {"and": [left_spec, right_spec]}


# ---------------------------------------------------------------------------
# Pattern: compound OR (a or b)
# Uses raw bytecode to find POP_JUMP_IF_TRUE
# ---------------------------------------------------------------------------

def _match_compound_or(raw_bytecode: list, ops: list) -> Optional[dict]:
    """Detect: simple_expression or simple_expression"""
    marker_idx = _find_logic_marker(raw_bytecode, "POP_JUMP_IF_TRUE")
    if marker_idx is None:
        return None

    or_offset = raw_bytecode[marker_idx].offset
    left_ops = [op for op in ops if op.offset < or_offset]
    right_ops = [op for op in ops if op.offset > or_offset]

    left_spec = _match_simple(left_ops)
    right_spec = _match_simple(right_ops)

    if left_spec is None or right_spec is None:
        return None

    return {"or": [left_spec, right_spec]}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _find_logic_marker(raw_bytecode: list, jump_op: str) -> Optional[int]:
    """Find COPY → TO_BOOL → POP_JUMP_IF_xxx pattern. Returns index or None."""
    for i, instr in enumerate(raw_bytecode):
        if instr.opname == "COPY" and i + 2 < len(raw_bytecode):
            if raw_bytecode[i + 1].opname == "TO_BOOL":
                if raw_bytecode[i + 2].opname == jump_op:
                    return i
    return None


def _match_simple(ops: list) -> Optional[dict]:
    """Match a single expression (no RETURN_VALUE required)."""
    for detector in [
        _match_field_op_literal_simple,
        _match_field_op_field_simple,
        _match_field_method_literal_simple,
        _match_field_in_collection_simple,
        _match_not_field_simple,
    ]:
        result = detector(ops)
        if result is not None:
            return result
    return None


def _match_field_op_literal_simple(ops: list) -> Optional[dict]:
    """Match field op literal (no RETURN_VALUE check)."""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue
        if not _is_load_const_value(ops[i + 3]):
            continue
        if ops[i + 4].opname != "COMPARE_OP":
            continue
        op = _normalize_op(ops[i + 4].argval)
        if op is None:
            continue
        return {"field": field, "op": op, "value": str(ops[i + 3].argval)}
    return None


def _match_field_op_field_simple(ops: list) -> Optional[dict]:
    """Match field_a op field_b (no RETURN_VALUE check)."""
    for i in range(len(ops) - 5):
        field_a = _field_access(ops, i)
        if field_a is None:
            continue
        field_b = _field_access(ops, i + 3)
        if field_b is None:
            continue
        if i + 6 >= len(ops) or ops[i + 6].opname != "COMPARE_OP":
            continue
        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue
        return {"field_a": field_a, "op": op, "field_b": field_b}
    return None


def _match_field_method_literal_simple(ops: list) -> Optional[dict]:
    """Match field.startswith/endswith (no RETURN_VALUE check)."""
    for i in range(len(ops) - 5):
        field = _field_access(ops, i)
        if field is None:
            continue
        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        method = ops[i + 3].argval
        if method not in ("startswith", "endswith"):
            continue
        if ops[i + 4].opname != "LOAD_CONST":
            continue
        if ops[i + 5].opname != "CALL":
            continue
        value = ops[i + 4].argval
        if method == "startswith":
            return {"field": field, "op": "starts_with", "value": str(value)}
        elif method == "endswith":
            return {"field": field, "op": "ends_with", "value": str(value)}
    return None


def _match_field_in_collection_simple(ops: list) -> Optional[dict]:
    """Match field in tuple/list (no RETURN_VALUE check)."""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue
        if ops[i + 3].opname != "LOAD_CONST":
            continue
        coll = ops[i + 3].argval
        if not isinstance(coll, (tuple, list)):
            continue
        if ops[i + 4].opname != "CONTAINS_OP":
            continue
        is_not = ops[i + 4].argval == 1
        values = tuple(str(v) for v in coll)
        op = "not_in" if is_not else "in"
        return {"field": field, "op": op, "values": values}
    return None


def _match_not_field_simple(ops: list) -> Optional[dict]:
    """Match not field (no RETURN_VALUE check)."""
    # Look for UNARY_NOT in the ops list
    for i, instr in enumerate(ops):
        if instr.opname == "UNARY_NOT":
            # Everything before UNARY_NOT should be a field access
            before = ops[:i]
            field = _field_access(before, 0) if len(before) >= 3 else None
            if field is not None:
                return {"not_field": field}
    return None
