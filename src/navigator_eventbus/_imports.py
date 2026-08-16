"""Lazy Import Utility for navigator-eventbus.

Local replica of ``parrot._imports.lazy_import`` (ai-parrot) — provides a
canonical pattern for lazily importing optional dependencies (e.g. the
``[scheduler]``/``[watchdog]``/``[mqtt]`` extras) with a clear, actionable
error message when the dependency is missing.

This module uses only Python stdlib — no external dependencies.
"""
import importlib
import inspect
from collections.abc import Callable
from types import ModuleType
from typing import Any


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


def resolve_callable(ref: str) -> Callable[..., Any]:
    """Resolve a dotted import string to a live callable.

    Accepts two spellings, the first preferred because it is unambiguous
    about where the module ends and the attribute begins:

    - ``"pkg.mod:attr"`` — colon-separated (preferred)
    - ``"pkg.mod.attr"`` — the last dotted segment is the attribute

    Attribute paths after the colon are supported, so classmethod or
    staticmethod factories work: ``"pkg.mod:Class.build"``.

    This exists so configuration loaded from YAML/JSON can name a Python
    callable. Import failures are raised as ``ValueError`` rather than
    ``ImportError`` so a Pydantic validator surfaces them as a clean
    ``ValidationError`` instead of letting an ``ImportError`` escape.

    .. warning::
       Resolving an import string executes module-level code in the named
       module. Treat any configuration that reaches this function as
       privileged, trusted-operator input — never resolve a reference taken
       from an HTTP request or from a user-writable store.

    Args:
        ref: The import reference to resolve.

    Returns:
        The resolved callable.

    Raises:
        ValueError: The reference is empty or malformed, the module cannot
            be imported, or the attribute path does not exist.
        TypeError: The reference resolved to a non-callable object.

    Examples:
        >>> fn = resolve_callable("json:dumps")
        >>> fn({"key": "value"})
        '{"key": "value"}'
    """
    if not isinstance(ref, str) or not ref.strip():
        raise ValueError(
            f"resolve_callable() requires a non-empty str reference; got {ref!r}"
        )
    ref = ref.strip()
    if ":" in ref:
        module_path, _, attr_path = ref.partition(":")
    else:
        module_path, _, attr_path = ref.rpartition(".")
    module_path, attr_path = module_path.strip(), attr_path.strip()
    if not module_path or not attr_path:
        raise ValueError(
            f"Invalid callable reference {ref!r}; expected 'pkg.mod:func' "
            "or 'pkg.mod.func'."
        )

    try:
        obj: Any = importlib.import_module(module_path)
    except ImportError as exc:
        raise ValueError(
            f"Cannot import module {module_path!r} from reference {ref!r}: {exc}"
        ) from exc

    for part in attr_path.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError as exc:
            raise ValueError(
                f"Module {module_path!r} has no attribute path {attr_path!r} "
                f"(failed at {part!r}) for reference {ref!r}"
            ) from exc

    if not callable(obj):
        raise TypeError(
            f"Reference {ref!r} resolved to a non-callable "
            f"{type(obj).__name__} object"
        )
    return obj


def accepts_two_positionals(fn: Callable[..., Any]) -> bool:
    """Whether *fn* can be called with two positional arguments.

    Used to decide, **once** at configuration time, whether a webhook
    preprocessor wants the optional context argument. Deciding it up front
    rather than per call matters: a ``try: fn(a, b) except TypeError: fn(a)``
    fallback would swallow a genuine ``TypeError`` raised from inside the
    function body and silently downgrade a real bug to an arity mismatch.

    Callables whose signature cannot be introspected (some C builtins) are
    reported as single-argument, the safer default.

    ``inspect.signature`` is applied to *fn* directly rather than to
    ``fn.__call__``. It already resolves ``functools.partial`` (reporting the
    post-binding signature) and callable instances (following
    ``type(obj).__call__`` and dropping ``self``). Unwrapping to ``__call__``
    by hand breaks partials, whose ``__call__`` is a ``(*args, **kwargs)``
    method-wrapper that would always look variadic.

    Args:
        fn: The callable to inspect.

    Returns:
        True when *fn* accepts a second positional argument.
    """
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):
        return False
    if any(p.kind is p.VAR_POSITIONAL for p in params):
        return True
    positional = [
        p
        for p in params
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    return len(positional) >= 2
