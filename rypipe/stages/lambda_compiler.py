"""Bytecode pattern analyzer for compiling lambdas into fusable filter specs.

Detects common lambda patterns (field comparisons, string methods, compound
AND) and converts them to filter spec dicts that the Rust engine can fuse.
Unknown patterns fall back to Python execution.
"""

from __future__ import annotations

import dis
from typing import Any, Optional


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
    result = _match_compound_and(bytecode, ops)
    if result is not None:
        return result

    result = _match_field_op_literal(ops)
    if result is not None:
        return result

    result = _match_field_op_field(ops)
    if result is not None:
        return result

    result = _match_field_method_literal(ops)
    if result is not None:
        return result

    return None


def _get_compare_op(argval) -> Optional[str]:
    """Extract comparison operator from COMPARE_OP argval."""
    if isinstance(argval, str):
        return argval  # Python < 3.12
    # Python 3.12+: argval is the operator string
    return None


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


# ---------------------------------------------------------------------------
# Pattern: r["field"] op literal_value
# Filtered ops: LOAD_FAST, LOAD_CONST, BINARY_OP, LOAD_CONST/LOAD_SMALL_INT,
#               COMPARE_OP, RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_field_op_literal(ops: list, require_return: bool = True) -> Optional[dict]:
    """Detect: r["field"] op literal_value"""
    for i in range(len(ops) - 4):
        # LOAD_FAST(r) LOAD_CONST(field) BINARY_OP LOAD_CONST(value) COMPARE_OP
        if ops[i].opname not in ("LOAD_FAST", "LOAD_FAST_BORROW"):
            continue
        if ops[i + 1].opname != "LOAD_CONST" or not isinstance(ops[i + 1].argval, str):
            continue
        if ops[i + 2].opname != "BINARY_OP":
            continue
        if ops[i + 3].opname not in ("LOAD_CONST", "LOAD_SMALL_INT"):
            continue
        if ops[i + 4].opname != "COMPARE_OP":
            continue
        # Check that RETURN_VALUE follows (optional for compound expressions)
        if require_return:
            if i + 5 >= len(ops) or ops[i + 5].opname != "RETURN_VALUE":
                continue

        op = _normalize_op(ops[i + 4].argval)
        if op is None:
            continue

        return {
            "field": ops[i + 1].argval,
            "op": op,
            "value": str(ops[i + 3].argval),
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field_a"] op r["field_b"]
# Filtered ops: LOAD_FAST, LOAD_CONST, BINARY_OP (×2), COMPARE_OP, RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_field_op_field(ops: list, require_return: bool = True) -> Optional[dict]:
    """Detect: r["field_a"] op r["field_b"]"""
    for i in range(len(ops) - 5):
        # First field: LOAD_FAST LOAD_CONST BINARY_OP
        if ops[i].opname not in ("LOAD_FAST", "LOAD_FAST_BORROW"):
            continue
        if ops[i + 1].opname != "LOAD_CONST" or not isinstance(ops[i + 1].argval, str):
            continue
        if ops[i + 2].opname != "BINARY_OP":
            continue

        # Second field: LOAD_FAST LOAD_CONST BINARY_OP
        if ops[i + 3].opname not in ("LOAD_FAST", "LOAD_FAST_BORROW"):
            continue
        if ops[i + 4].opname != "LOAD_CONST" or not isinstance(ops[i + 4].argval, str):
            continue
        if ops[i + 5].opname != "BINARY_OP":
            continue

        # Comparison
        if i + 6 >= len(ops) or ops[i + 6].opname != "COMPARE_OP":
            continue
        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue

        # Return (optional for compound expressions)
        if require_return:
            if i + 7 >= len(ops) or ops[i + 7].opname != "RETURN_VALUE":
                continue

        return {
            "field_a": ops[i + 1].argval,
            "op": op,
            "field_b": ops[i + 4].argval,
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"].method(arg)
# Filtered ops: LOAD_FAST, LOAD_CONST, BINARY_OP, LOAD_ATTR, LOAD_CONST,
#               CALL, RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_field_method_literal(ops: list) -> Optional[dict]:
    """Detect: r["field"].startswith/endswith literal"""
    for i in range(len(ops) - 5):
        if ops[i].opname not in ("LOAD_FAST", "LOAD_FAST_BORROW"):
            continue
        if ops[i + 1].opname != "LOAD_CONST" or not isinstance(ops[i + 1].argval, str):
            continue
        if ops[i + 2].opname != "BINARY_OP":
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

        field = ops[i + 1].argval
        value = ops[i + 4].argval

        if method == "startswith":
            return {"field": field, "op": "starts_with", "value": str(value)}
        elif method == "endswith":
            return {"field": field, "op": "ends_with", "value": str(value)}

    return None


# ---------------------------------------------------------------------------
# Pattern: r["a"] > x and r["b"] < y (compound AND)
# Uses raw bytecode to find the AND short-circuit pattern
# ---------------------------------------------------------------------------

def _match_compound_and(raw_bytecode: list, filtered_ops: list) -> Optional[dict]:
    """Detect: simple_expression and simple_expression"""
    # Find the AND short-circuit pattern in raw bytecode
    and_marker_idx = None
    for i, instr in enumerate(raw_bytecode):
        if instr.opname == "COPY" and i + 2 < len(raw_bytecode):
            if raw_bytecode[i + 1].opname == "TO_BOOL":
                if raw_bytecode[i + 2].opname in ("POP_JUMP_IF_FALSE", "POP_JUMP_FORWARD_IF_FALSE"):
                    and_marker_idx = i
                    break

    if and_marker_idx is None:
        return None

    # Get the offset of the AND marker
    and_offset = raw_bytecode[and_marker_idx].offset

    # Split filtered ops into left and right based on offset
    left_ops = [op for op in filtered_ops if op.offset < and_offset]
    right_ops = [op for op in filtered_ops if op.offset > and_offset]

    left_spec = _match_simple_comparison(left_ops)
    right_spec = _match_simple_comparison(right_ops)

    if left_spec is None or right_spec is None:
        return None

    return {"and": [left_spec, right_spec]}


def _match_simple_comparison(ops: list) -> Optional[dict]:
    """Match a single comparison expression (no RETURN_VALUE required)."""
    result = _match_field_op_literal(ops, require_return=False)
    if result is not None:
        return result
    result = _match_field_op_field(ops, require_return=False)
    if result is not None:
        return result
    result = _match_field_method_literal(ops)
    if result is not None:
        return result
    return None
