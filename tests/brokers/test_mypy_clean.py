"""FEAT-712 regression gate: the brokers layer stays mypy-clean (issue:b12a71083ca3).

``uv run mypy src/`` is a CI step, but it is red for files outside the brokers
layer; this test keeps ``brokers/`` and ``hooks/brokers/`` clean on their own.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("src/navigator_eventbus/brokers", "src/navigator_eventbus/hooks/brokers")


def test_brokers_packages_are_mypy_clean() -> None:
    """``mypy`` over both packages exits 0 under the repo's ``[tool.mypy]`` config."""
    pytest.importorskip("mypy")
    result = subprocess.run(
        # --follow-imports=silent: imported modules outside the targets are still
        # analysed for types but their own errors are not reported here.
        [sys.executable, "-m", "mypy", "--follow-imports=silent", *TARGETS],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
