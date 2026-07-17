"""Lazy Import Utility for navigator-eventbus.

Local replica of ``parrot._imports.lazy_import`` (ai-parrot) — provides a
canonical pattern for lazily importing optional dependencies (e.g. the
``[scheduler]``/``[watchdog]``/``[mqtt]`` extras) with a clear, actionable
error message when the dependency is missing.

This module uses only Python stdlib — no external dependencies.
"""
import importlib
from types import ModuleType


def lazy_import(
    module_path: str,
    package_name: str | None = None,
    extra: str | None = None,
) -> ModuleType:
    """Import a module lazily, raising a clear error if not installed.

    Imports ``module_path`` using ``importlib.import_module`` and returns the
    module object on success. If the module is not installed, raises an
    ``ImportError`` with an actionable install instruction.

    This function is thread-safe because ``importlib.import_module`` is
    thread-safe (it uses the module import lock internally).

    Args:
        module_path: Dotted Python module path to import, e.g. ``"gmqtt"``.
        package_name: Human-readable pip package name. If omitted, the first
            segment of ``module_path`` is used. Use this when the pip name
            differs from the module name.
        extra: navigator-eventbus extras group name. When provided, the
            error message will suggest ``pip install
            navigator-eventbus[<extra>]``. When omitted, the error message
            will suggest ``pip install <package_name>`` directly.

    Returns:
        The imported module object.

    Raises:
        ImportError: If ``module_path`` cannot be imported, with a message
            that includes the install instruction.

    Examples:
        >>> import json
        >>> mod = lazy_import("json")
        >>> mod.dumps({"key": "value"})
        '{"key": "value"}'

        >>> lazy_import("gmqtt", extra="mqtt")  # if not installed
        ImportError: 'gmqtt' is required but not installed.
                     Install it with: pip install navigator-eventbus[mqtt]
    """
    try:
        return importlib.import_module(module_path)
    except ImportError as exc:
        pkg = package_name or module_path.split(".")[0]
        if extra:
            msg = (
                f"'{pkg}' is required but not installed. "
                f"Install it with: pip install navigator-eventbus[{extra}]"
            )
        else:
            msg = (
                f"'{pkg}' is required but not installed. "
                f"Install it with: pip install {pkg}"
            )
        raise ImportError(msg) from exc


def require_extra(extra: str, *modules: str) -> None:
    """Verify that all required modules for an extras group are importable.

    Args:
        extra: navigator-eventbus extras group name, e.g. ``"mqtt"``.
        *modules: One or more dotted Python module paths to check.

    Raises:
        ImportError: If any of the listed modules cannot be imported, with
            a message directing the user to install the extras group.
    """
    for mod in modules:
        lazy_import(mod, extra=extra)
