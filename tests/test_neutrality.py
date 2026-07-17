"""Neutrality guard — cero imports de `parrot.*` en `src/navigator_eventbus/`.

FEAT-312, TASK-1805 (spec §5 acceptance criterion, §4 test spec). Lazy
imports of ``navigator.brokers.*`` and ``gmqtt`` (hooks/brokers/*) are
explicitly permitted in this phase — phase 3 (``eventbus-brokers-port``)
recables them.
"""
import re
from pathlib import Path

SRC = Path(__file__).parent.parent / "src" / "navigator_eventbus"
FORBIDDEN = re.compile(r"^\s*(from|import)\s+parrot(\.|\s|$)", re.MULTILINE)


def test_no_parrot_imports():
    offenders = [
        p for p in SRC.rglob("*.py") if FORBIDDEN.search(p.read_text())
    ]
    assert not offenders, f"parrot imports found: {offenders}"


def test_navigator_brokers_lazy_imports_are_confined_to_hooks_brokers():
    """navigator.brokers.* references are permitted, but ONLY inside the
    hooks/brokers/ sub-package (phase-1 scope) — nowhere else in src/."""
    pattern = re.compile(r"^\s*(from|import)\s+navigator\.brokers", re.MULTILINE)
    offenders = [
        p
        for p in SRC.rglob("*.py")
        if pattern.search(p.read_text())
        and "hooks/brokers" not in str(p.relative_to(SRC))
    ]
    assert not offenders, f"navigator.brokers imports outside hooks/brokers/: {offenders}"
