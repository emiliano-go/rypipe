"""Shared fixtures for the rypipe Python test suite."""

from __future__ import annotations

import importlib
import os

import pytest


def _optional(name: str):
    """Import an optional dependency.

    Missing dependencies skip the test locally. When
    ``RYPIPE_REQUIRE_OPTIONAL_DEPS`` is set (CI does), a missing dependency is
    a hard failure instead, so optional-path coverage cannot quietly vanish
    from a green run.
    """
    if os.environ.get("RYPIPE_REQUIRE_OPTIONAL_DEPS"):
        return importlib.import_module(name)
    return pytest.importorskip(name)


@pytest.fixture
def pandas():
    return _optional("pandas")


@pytest.fixture
def polars():
    return _optional("polars")
