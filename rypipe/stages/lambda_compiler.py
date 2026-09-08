"""Bytecode pattern analyzer for compiling lambdas into fusable filter specs.

Detects common lambda patterns (field comparisons, string methods, membership,
truthiness, compound logic) and converts them to filter spec dicts that the
Rust engine can fuse. Unknown patterns fall back to Python execution.

Supported patterns:
    - r["field"] op literal          → CompareLiteral / Equal / NotEqual
    - r["field_a"] op r["field_b"]  → Compare
    - r["field"].startswith("x")    → StartsWith
    - r["field"].endswith("x")      → EndsWith
    - r["field"].contains("x")      → Contains
    - r["field"].strip() op "x"     → Strip
    - r["field"].lower() op "x"     → Lower
    - r["field"].upper() op "x"     → Upper
    - r["field"].replace(a,b) op "x" → Replace
    - len(r["field"]) op N          → Length
    - r["field"] in (...)           → InPredicate
    - r["field"] not in (...)       → NotInPredicate
    - not r["field"]                → Not(TruthyPredicate)
    - not r["field"].startswith()   → Not(StartsWith)
    - not r["field"].endswith()     → Not(EndsWith)
    - lambda r: True / False        → AlwaysTrue / AlwaysFalse
    - a and b                       → And(left, right)
    - a or b                        → Or(left, right)
    - nested compound               → And(Or(...), ...) etc.
    - closures with constants       → resolved at construction time
"""

from __future__ import annotations

import builtins
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
    for detector in [
        _match_bool_constant,
        _match_compound_and,
        _match_compound_or,
        _match_len_field_compare,
        _match_cast_and_compare,
        _match_arithmetic_compare,
        _match_field_in_collection,
        _match_field_op_literal,
        _match_field_op_field,
        _match_field_method_replace,
        _match_field_method_noarg_literal,
        _match_field_method_literal,
        _match_not_field,
    ]:
        result = detector(bytecode, ops, fn)
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
    """Check for field access pattern: LOAD_FAST LOAD_CONST BINARY_SUBSCR.
    Returns the field name if matched, None otherwise."""
    if i + 2 >= len(ops):
        return None
    if not _is_load_fast(ops[i]):
        return None
    if not _is_load_const_str(ops[i + 1]):
        return None
    if ops[i + 2].opname not in ("BINARY_SUBSCR", "BINARY_OP"):
        return None
    return ops[i + 1].argval


def _resolve_closure_value(fn, instr) -> Optional[Any]:
    """Try to resolve a LOAD_GLOBAL/LOAD_DEREF value from the lambda's scope.

    Only resolves constant types (int, float, str, bool). Returns None if
    the value cannot be resolved or is not a constant type.
    """
    if instr.opname == "LOAD_GLOBAL":
        name = instr.argval
        # Check function globals first
        val = fn.__globals__.get(name)
        if val is None:
            # Check builtins
            val = getattr(builtins, name, None)
        if val is not None and type(val) in (int, float, str, bool):
            return val
    elif instr.opname == "LOAD_DEREF":
        name = instr.argval
        # Check closure cells
        for cell in fn.__closure__ or ():
            try:
                if cell.cell_contents is not None:
                    val = cell.cell_contents
                    if type(val) in (int, float, str, bool):
                        return val
            except ValueError:
                pass
        # Also check __globals__ for free vars that might be there
        val = fn.__globals__.get(name)
        if val is not None and type(val) in (int, float, str, bool):
            return val
    return None


# ---------------------------------------------------------------------------
# Pattern: lambda r: True / lambda r: False
# Bytecode: LOAD_CONST(True/False) RETURN_VALUE
# ---------------------------------------------------------------------------

def _match_bool_constant(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
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


def _match_cast_and_compare(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
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

        # Value: try LOAD_CONST first, then closure resolution
        value = None
        if _is_load_const_value(ops[i + 5]):
            value = ops[i + 5].argval
        elif fn is not None and ops[i + 5].opname in ("LOAD_GLOBAL", "LOAD_DEREF"):
            value = _resolve_closure_value(fn, ops[i + 5])
        if value is None:
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

def _match_arithmetic_compare(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
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
# ---------------------------------------------------------------------------

def _match_field_op_literal(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: r["field"] op literal_value"""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue

        # Try LOAD_CONST first, then closure resolution
        value = None
        if _is_load_const_value(ops[i + 3]):
            value = ops[i + 3].argval
        elif fn is not None and ops[i + 3].opname in ("LOAD_GLOBAL", "LOAD_DEREF"):
            value = _resolve_closure_value(fn, ops[i + 3])
        if value is None:
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
            "value": str(value),
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field_a"] op r["field_b"]
# ---------------------------------------------------------------------------

def _match_field_op_field(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
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
# Pattern: r["field"].method(arg) (startswith, endswith, contains)
# ---------------------------------------------------------------------------

_MATCHED_METHODS = {"startswith", "endswith", "contains"}


def _match_field_method_literal(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: r["field"].startswith/endswith/contains literal"""
    for i in range(len(ops) - 5):
        field = _field_access(ops, i)
        if field is None:
            continue

        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        method = ops[i + 3].argval
        if method not in _MATCHED_METHODS:
            continue
        if ops[i + 4].opname != "LOAD_CONST":
            continue
        if ops[i + 5].opname != "CALL":
            continue

        value = ops[i + 4].argval

        # Check for negation (UNARY_NOT after CALL)
        is_negated = False
        for instr in raw_bytecode:
            if instr.offset > ops[i + 5].offset and instr.opname == "UNARY_NOT":
                is_negated = True
                break

        spec = {"field": field, "op": f"starts_with" if method == "startswith" else f"ends_with" if method == "endswith" else "contains", "value": str(value)}

        if is_negated:
            return {"not": spec}
        return spec

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"].strip/lower/upper() op literal (no-arg methods)
# ---------------------------------------------------------------------------

_NOARG_METHODS = {"strip", "lstrip", "rstrip", "lower", "upper"}


def _match_field_method_noarg_literal(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: r["field"].strip()/lower()/upper() op literal"""
    for i in range(len(ops) - 5):
        field = _field_access(ops, i)
        if field is None:
            continue

        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        method = ops[i + 3].argval
        if method not in _NOARG_METHODS:
            continue
        if ops[i + 4].opname != "CALL":
            continue

        # Value and comparison
        if not _is_load_const_value(ops[i + 5]):
            continue
        if ops[i + 6].opname != "COMPARE_OP":
            continue
        if i + 7 >= len(ops) or ops[i + 7].opname != "RETURN_VALUE":
            continue

        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue

        # Check for negation
        is_negated = False
        for instr in raw_bytecode:
            if instr.offset > ops[i + 6].offset and instr.opname == "UNARY_NOT":
                is_negated = True
                break

        spec = {"field": field, "op": method, "value": str(ops[i + 5].argval), "cmp_op": op}

        if is_negated:
            return {"not": spec}
        return spec

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"].replace(old, new) op literal
# ---------------------------------------------------------------------------

def _match_field_method_replace(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: r["field"].replace("old", "new") op literal"""
    for i in range(len(ops) - 6):
        field = _field_access(ops, i)
        if field is None:
            continue

        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        if ops[i + 3].argval != "replace":
            continue

        # Two args to replace: old, new
        if ops[i + 4].opname != "LOAD_CONST":
            continue
        if ops[i + 5].opname != "LOAD_CONST":
            continue
        if ops[i + 6].opname != "CALL":
            continue

        old_val = str(ops[i + 4].argval)
        new_val = str(ops[i + 5].argval)

        # Value and comparison
        if not _is_load_const_value(ops[i + 7]):
            continue
        if ops[i + 8].opname != "COMPARE_OP":
            continue
        if i + 9 >= len(ops) or ops[i + 9].opname != "RETURN_VALUE":
            continue

        op = _normalize_op(ops[i + 8].argval)
        if op is None:
            continue

        # Check for negation
        is_negated = False
        for instr in raw_bytecode:
            if instr.offset > ops[i + 8].offset and instr.opname == "UNARY_NOT":
                is_negated = True
                break

        spec = {"field": field, "old": old_val, "new": new_val, "cmp_op": op, "value": str(ops[i + 7].argval)}

        if is_negated:
            return {"not": spec}
        return spec

    return None


# ---------------------------------------------------------------------------
# Pattern: len(r["field"]) op literal
# ---------------------------------------------------------------------------

def _match_len_field_compare(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: len(r["field"]) > 5, len(r["name"]) == 0, etc."""
    for i in range(len(ops) - 5):
        if ops[i].opname != "LOAD_GLOBAL":
            continue
        if ops[i].argval != "len":
            continue

        # Field access inside len()
        field = _field_access(ops, i + 1)
        if field is None:
            continue

        # CALL to len()
        if ops[i + 4].opname != "CALL":
            continue

        # Value and comparison
        if not _is_load_const_value(ops[i + 5]):
            continue
        if ops[i + 6].opname != "COMPARE_OP":
            continue
        if i + 7 >= len(ops) or ops[i + 7].opname != "RETURN_VALUE":
            continue

        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue

        return {
            "field": field,
            "op": "length",
            "value": str(ops[i + 5].argval),
            "cmp_op": op,
        }

    return None


# ---------------------------------------------------------------------------
# Pattern: r["field"] in (tuple/list/frozenset/set)
# ---------------------------------------------------------------------------

def _match_field_in_collection(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: r["field"] in (...) or r["field"] not in (...)"""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue

        # Path 1: Collection as a constant (tuple/list)
        if ops[i + 3].opname == "LOAD_CONST":
            coll = ops[i + 3].argval
            if isinstance(coll, (tuple, list, frozenset, set)):
                if ops[i + 4].opname == "CONTAINS_OP":
                    is_not = ops[i + 4].argval == 1
                    if i + 5 >= len(ops) or ops[i + 5].opname != "RETURN_VALUE":
                        continue
                    values = tuple(str(v) for v in coll)
                    op = "not_in" if is_not else "in"
                    return {"field": field, "op": op, "values": values}

        # Path 2: frozenset/set constructor: LOAD_GLOBAL(frozenset/set) + constants + BUILD_SET + CALL
        if ops[i + 3].opname == "LOAD_GLOBAL" and ops[i + 3].argval in ("frozenset", "set"):
            # Collect constants between LOAD_GLOBAL and BUILD_SET
            j = i + 4
            values_list = []
            while j < len(ops) and ops[j].opname in ("LOAD_CONST", "LOAD_SMALL_INT"):
                values_list.append(str(ops[j].argval))
                j += 1
            if j < len(ops) and ops[j].opname == "BUILD_SET" and ops[j].argval == len(values_list):
                if j + 1 < len(ops) and ops[j + 1].opname == "CALL":
                    if j + 2 < len(ops) and ops[j + 2].opname == "CONTAINS_OP":
                        is_not = ops[j + 2].argval == 1
                        if j + 3 >= len(ops) or ops[j + 3].opname != "RETURN_VALUE":
                            continue
                        op = "not_in" if is_not else "in"
                        return {"field": field, "op": op, "values": tuple(values_list)}

    return None


# ---------------------------------------------------------------------------
# Pattern: not r["field"]
# ---------------------------------------------------------------------------

def _match_not_field(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: not r["field"] (truthiness negation)"""
    for i, instr in enumerate(raw_bytecode):
        if instr.opname == "UNARY_NOT":
            before = [op for op in ops if op.offset < instr.offset]
            field = _field_access(before, 0) if len(before) >= 3 else None
            if field is None:
                continue
            after = [op for op in ops if op.offset > instr.offset]
            if not after or after[0].opname != "RETURN_VALUE":
                continue
            return {"not_field": field}

    return None


# ---------------------------------------------------------------------------
# Pattern: compound AND (a and b) (flat, single level)
# ---------------------------------------------------------------------------

def _match_compound_and(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: simple_expression and simple_expression"""
    marker_idx = _find_logic_marker(raw_bytecode, "POP_JUMP_IF_FALSE")
    if marker_idx is None:
        return None

    and_offset = raw_bytecode[marker_idx].offset
    left_ops = [op for op in ops if op.offset < and_offset]
    right_ops = [op for op in ops if op.offset > and_offset]

    left_spec = _match_compound_or_only(left_ops, raw_bytecode, fn) or _match_simple(left_ops)
    right_spec = _match_compound_or_only(right_ops, raw_bytecode, fn) or _match_simple(right_ops)

    if left_spec is None or right_spec is None:
        return None

    return {"and": [left_spec, right_spec]}


# ---------------------------------------------------------------------------
# Pattern: compound OR (a or b) (flat, single level)
# ---------------------------------------------------------------------------

def _match_compound_or(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: simple_expression or simple_expression"""
    marker_idx = _find_logic_marker(raw_bytecode, "POP_JUMP_IF_TRUE")
    if marker_idx is None:
        return None

    or_offset = raw_bytecode[marker_idx].offset
    left_ops = [op for op in ops if op.offset < or_offset]
    right_ops = [op for op in ops if op.offset > or_offset]

    left_spec = _match_compound_and_only(left_ops, raw_bytecode, fn) or _match_simple(left_ops)
    right_spec = _match_compound_and_only(right_ops, raw_bytecode, fn) or _match_simple(right_ops)

    if left_spec is None or right_spec is None:
        return None

    return {"or": [left_spec, right_spec]}


def _match_compound_or_only(ops: list, raw_bytecode: list, fn=None) -> Optional[dict]:
    """Match only OR compound (used by AND detector to avoid recursion)."""
    for i in range(len(raw_bytecode) - 1, -1, -1):
        if raw_bytecode[i].opname == "POP_JUMP_IF_TRUE":
            or_offset = raw_bytecode[i].offset
            left_ops = [op for op in ops if op.offset < or_offset]
            right_ops = [op for op in ops if op.offset > or_offset]
            left_spec = _match_simple(left_ops)
            right_spec = _match_simple(right_ops)
            if left_spec is not None and right_spec is not None:
                return {"or": [left_spec, right_spec]}
    return None


def _match_compound_and_only(ops: list, raw_bytecode: list, fn=None) -> Optional[dict]:
    """Match only AND compound (used by OR detector to avoid recursion)."""
    for i in range(len(raw_bytecode) - 1, -1, -1):
        if raw_bytecode[i].opname == "POP_JUMP_IF_FALSE":
            and_offset = raw_bytecode[i].offset
            left_ops = [op for op in ops if op.offset < and_offset]
            right_ops = [op for op in ops if op.offset > and_offset]
            left_spec = _match_simple(left_ops)
            right_spec = _match_simple(right_ops)
            if left_spec is not None and right_spec is not None:
                return {"and": [left_spec, right_spec]}
    return None


# ---------------------------------------------------------------------------
# Pattern: nested compound AND (searches reverse for outermost)
# ---------------------------------------------------------------------------

def _match_compound_and_nested(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: (a or b) and c, a and (b or c), etc. (nested compound)"""
    marker_idx = _find_logic_marker_reverse(raw_bytecode, "POP_JUMP_IF_FALSE")
    if marker_idx is None:
        return None

    and_offset = raw_bytecode[marker_idx].offset
    left_ops = [op for op in ops if op.offset < and_offset]
    right_ops = [op for op in ops if op.offset > and_offset]

    # Try simple first, then compound recursively
    left_spec = _match_simple(left_ops) or _match_compound_recursive(left_ops, fn)
    right_spec = _match_simple(right_ops) or _match_compound_recursive(right_ops, fn)

    if left_spec is None or right_spec is None:
        return None

    return {"and": [left_spec, right_spec]}


# ---------------------------------------------------------------------------
# Pattern: nested compound OR (searches reverse for outermost)
# ---------------------------------------------------------------------------

def _match_compound_or_nested(raw_bytecode: list, ops: list, fn=None) -> Optional[dict]:
    """Detect: (a and b) or c, a or (b and c), etc. (nested compound)"""
    marker_idx = _find_logic_marker_reverse(raw_bytecode, "POP_JUMP_IF_TRUE")
    if marker_idx is None:
        return None

    or_offset = raw_bytecode[marker_idx].offset
    left_ops = [op for op in ops if op.offset < or_offset]
    right_ops = [op for op in ops if op.offset > or_offset]

    # Try simple first, then compound recursively
    left_spec = _match_simple(left_ops) or _match_compound_recursive(left_ops, fn)
    right_spec = _match_simple(right_ops) or _match_compound_recursive(right_ops, fn)

    if left_spec is None or right_spec is None:
        return None

    return {"or": [left_spec, right_spec]}


def _match_compound_recursive(ops: list, fn=None, depth: int = 0) -> Optional[dict]:
    """Recursively match compound expressions. Limited to depth 3."""
    if depth >= 3:
        return None
    if not ops:
        return None

    # Try simple match
    simple = _match_simple(ops)
    if simple is not None:
        return simple

    # Try compound AND
    and_spec = _match_compound_and_recursive(ops, fn, depth)
    if and_spec is not None:
        return and_spec

    # Try compound OR
    or_spec = _match_compound_or_recursive(ops, fn, depth)
    if or_spec is not None:
        return or_spec

    return None


def _match_compound_and_recursive(ops: list, fn=None, depth: int = 0) -> Optional[dict]:
    """Match compound AND within a subset of ops."""
    # Reconstruct raw_bytecode-like structure for the subset
    # Use offsets to find the outermost POP_JUMP_IF_FALSE
    for i in range(len(ops) - 1, -1, -1):
        if ops[i].opname == "POP_JUMP_IF_FALSE":
            and_offset = ops[i].offset
            left_ops = [op for op in ops if op.offset < and_offset]
            right_ops = [op for op in ops if op.offset > and_offset]

            left_spec = _match_simple(left_ops) or _match_compound_recursive(left_ops, fn, depth + 1)
            right_spec = _match_simple(right_ops) or _match_compound_recursive(right_ops, fn, depth + 1)

            if left_spec is not None and right_spec is not None:
                return {"and": [left_spec, right_spec]}
    return None


def _match_compound_or_recursive(ops: list, fn=None, depth: int = 0) -> Optional[dict]:
    """Match compound OR within a subset of ops."""
    for i in range(len(ops) - 1, -1, -1):
        if ops[i].opname == "POP_JUMP_IF_TRUE":
            or_offset = ops[i].offset
            left_ops = [op for op in ops if op.offset < or_offset]
            right_ops = [op for op in ops if op.offset > or_offset]

            left_spec = _match_simple(left_ops) or _match_compound_recursive(left_ops, fn, depth + 1)
            right_spec = _match_simple(right_ops) or _match_compound_recursive(right_ops, fn, depth + 1)

            if left_spec is not None and right_spec is not None:
                return {"or": [left_spec, right_spec]}
    return None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _find_logic_marker(raw_bytecode: list, jump_op: str) -> Optional[int]:
    """Find COPY → TO_BOOL → POP_JUMP_IF_xxx pattern. Returns index of the jump instruction or None."""
    for i, instr in enumerate(raw_bytecode):
        if instr.opname == "COPY" and i + 2 < len(raw_bytecode):
            if raw_bytecode[i + 1].opname == "TO_BOOL":
                if raw_bytecode[i + 2].opname == jump_op:
                    return i + 2  # Return the jump instruction index, not the COPY
    return None


def _find_logic_marker_reverse(raw_bytecode: list, jump_op: str) -> Optional[int]:
    """Find COPY → TO_BOOL → POP_JUMP_IF_xxx pattern (last occurrence, for nesting)."""
    result = None
    for i, instr in enumerate(raw_bytecode):
        if instr.opname == "COPY" and i + 2 < len(raw_bytecode):
            if raw_bytecode[i + 1].opname == "TO_BOOL":
                if raw_bytecode[i + 2].opname == jump_op:
                    result = i + 2  # Return the jump instruction index, not the COPY
    return result


def _match_simple(ops: list) -> Optional[dict]:
    """Match a single expression (no RETURN_VALUE required)."""
    for detector in [
        _match_field_op_literal_simple,
        _match_field_op_field_simple,
        _match_field_method_literal_simple,
        _match_field_method_noarg_literal_simple,
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
    """Match field.startswith/endswith/contains (no RETURN_VALUE check)."""
    for i in range(len(ops) - 5):
        field = _field_access(ops, i)
        if field is None:
            continue
        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        method = ops[i + 3].argval
        if method not in _MATCHED_METHODS:
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
        elif method == "contains":
            return {"field": field, "op": "contains", "value": str(value)}
    return None


def _match_field_method_noarg_literal_simple(ops: list) -> Optional[dict]:
    """Match field.strip()/lower()/upper() (no RETURN_VALUE check)."""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue
        if ops[i + 3].opname != "LOAD_ATTR":
            continue
        method = ops[i + 3].argval
        if method not in _NOARG_METHODS:
            continue
        if i + 4 >= len(ops) or ops[i + 4].opname != "CALL":
            continue
        # In compound context, the value and compare come after
        if i + 6 >= len(ops):
            continue
        if not _is_load_const_value(ops[i + 5]):
            continue
        if ops[i + 6].opname != "COMPARE_OP":
            continue
        op = _normalize_op(ops[i + 6].argval)
        if op is None:
            continue
        return {"field": field, "op": method, "value": str(ops[i + 5].argval), "cmp_op": op}
    return None


def _match_field_in_collection_simple(ops: list) -> Optional[dict]:
    """Match field in tuple/list/frozenset/set (no RETURN_VALUE check)."""
    for i in range(len(ops) - 4):
        field = _field_access(ops, i)
        if field is None:
            continue
        # Path 1: Collection as a constant
        if ops[i + 3].opname == "LOAD_CONST":
            coll = ops[i + 3].argval
            if isinstance(coll, (tuple, list, frozenset, set)):
                if i + 4 < len(ops) and ops[i + 4].opname == "CONTAINS_OP":
                    is_not = ops[i + 4].argval == 1
                    values = tuple(str(v) for v in coll)
                    op = "not_in" if is_not else "in"
                    return {"field": field, "op": op, "values": values}
        # Path 2: frozenset/set constructor
        if ops[i + 3].opname == "LOAD_GLOBAL" and ops[i + 3].argval in ("frozenset", "set"):
            j = i + 4
            values_list = []
            while j < len(ops) and ops[j].opname in ("LOAD_CONST", "LOAD_SMALL_INT"):
                values_list.append(str(ops[j].argval))
                j += 1
            if j < len(ops) and ops[j].opname == "BUILD_SET" and ops[j].argval == len(values_list):
                if j + 1 < len(ops) and ops[j + 1].opname == "CALL":
                    if j + 2 < len(ops) and ops[j + 2].opname == "CONTAINS_OP":
                        is_not = ops[j + 2].argval == 1
                        op = "not_in" if is_not else "in"
                        return {"field": field, "op": op, "values": tuple(values_list)}
    return None


def _match_not_field_simple(ops: list) -> Optional[dict]:
    """Match not field (no RETURN_VALUE check)."""
    for i, instr in enumerate(ops):
        if instr.opname == "UNARY_NOT":
            before = ops[:i]
            field = _field_access(before, 0) if len(before) >= 3 else None
            if field is not None:
                return {"not_field": field}
    return None
