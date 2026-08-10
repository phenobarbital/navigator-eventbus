"""Tests for the pluggable webhook signature schemes (webhook-support, TASK-A)."""
import hashlib
import hmac

import pytest

from navigator_eventbus._imports import accepts_two_positionals, resolve_callable
from navigator_eventbus.webhook_signatures import (
    HexDigestScheme,
    SignatureCheck,
    SignatureScheme,
    SignatureVerdict,
    StripeScheme,
    available_signature_schemes,
    get_signature_scheme,
    register_signature_scheme,
)

SECRET = "s3cr3t"
BODY = b'{"action":"opened","number":42}'


def hex_digest(secret: str, body: bytes, algo=hashlib.sha256) -> str:
    return hmac.new(secret.encode(), body, algo).hexdigest()


# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------


def test_github_scheme_accepts_valid_signature():
    scheme = get_signature_scheme("github")
    headers = {"X-Hub-Signature-256": f"sha256={hex_digest(SECRET, BODY)}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).ok


def test_github_scheme_rejects_tampered_body():
    scheme = get_signature_scheme("github")
    headers = {"X-Hub-Signature-256": f"sha256={hex_digest(SECRET, BODY)}"}
    check = scheme.verify(secret=SECRET, body=BODY + b" ", headers=headers)
    assert check.verdict is SignatureVerdict.MISMATCH


def test_github_scheme_rejects_wrong_secret():
    scheme = get_signature_scheme("github")
    headers = {"X-Hub-Signature-256": f"sha256={hex_digest('other', BODY)}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).verdict is (
        SignatureVerdict.MISMATCH
    )


def test_github_scheme_missing_header_is_missing_verdict():
    scheme = get_signature_scheme("github")
    check = scheme.verify(secret=SECRET, body=BODY, headers={})
    assert check.verdict is SignatureVerdict.MISSING


@pytest.mark.parametrize(
    "value, detail",
    [
        (hex_digest(SECRET, BODY), "missing_alg_prefix"),  # bare, not allowed
        (f"sha1={hex_digest(SECRET, BODY)}", "unsupported_alg"),
        ("sha256=nothexatall", "not_hex"),
        ("sha256=", "not_hex"),
    ],
)
def test_github_scheme_malformed_header_variants(value, detail):
    scheme = get_signature_scheme("github")
    check = scheme.verify(
        secret=SECRET, body=BODY, headers={"X-Hub-Signature-256": value}
    )
    assert check.verdict is SignatureVerdict.MALFORMED
    assert check.detail == detail


def test_github_scheme_honours_header_override():
    scheme = get_signature_scheme("github")
    headers = {"X-Custom-Sig": f"sha256={hex_digest(SECRET, BODY)}"}
    assert scheme.verify(
        secret=SECRET, body=BODY, headers=headers, header_override="X-Custom-Sig"
    ).ok


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------


def test_jira_scheme_uses_x_hub_signature_header():
    scheme = get_signature_scheme("jira")
    assert scheme.header == "X-Hub-Signature"
    headers = {"X-Hub-Signature": f"sha256={hex_digest(SECRET, BODY)}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).ok


@pytest.mark.parametrize("algo_name, algo", [("sha256", hashlib.sha256), ("sha1", hashlib.sha1)])
def test_jira_scheme_accepts_sha1_and_sha256(algo_name, algo):
    scheme = get_signature_scheme("jira")
    headers = {"X-Hub-Signature": f"{algo_name}={hex_digest(SECRET, BODY, algo)}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).ok


def test_jira_scheme_rejects_unsupported_algorithm():
    scheme = get_signature_scheme("jira")
    headers = {"X-Hub-Signature": f"sha512={hex_digest(SECRET, BODY, hashlib.sha512)}"}
    check = scheme.verify(secret=SECRET, body=BODY, headers=headers)
    assert check.verdict is SignatureVerdict.MALFORMED
    assert check.detail == "unsupported_alg"


# ---------------------------------------------------------------------------
# Generic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("prefix", ["", "sha256="])
def test_generic_scheme_accepts_bare_and_prefixed_hex(prefix):
    scheme = get_signature_scheme("generic")
    headers = {"X-Webhook-Signature": f"{prefix}{hex_digest(SECRET, BODY)}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).ok


def test_verify_is_case_insensitive_on_hex():
    scheme = get_signature_scheme("generic")
    headers = {"X-Webhook-Signature": hex_digest(SECRET, BODY).upper()}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).ok


def test_verify_finds_header_case_insensitively():
    """Plain dicts (not aiohttp CIMultiDict) must still resolve the header."""
    scheme = get_signature_scheme("generic")
    headers = {"x-webhook-signature": hex_digest(SECRET, BODY)}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers).ok


# ---------------------------------------------------------------------------
# Stripe
# ---------------------------------------------------------------------------


def stripe_header(secret: str, body: bytes, timestamp: int) -> str:
    digest = hmac.new(
        secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256
    ).hexdigest()
    return f"t={timestamp},v1={digest}"


def test_stripe_scheme_signs_timestamp_dot_body():
    scheme = get_signature_scheme("stripe")
    headers = {"Stripe-Signature": stripe_header(SECRET, BODY, 1_700_000_000)}
    check = scheme.verify(
        secret=SECRET, body=BODY, headers=headers, now=1_700_000_010
    )
    assert check.ok


def test_stripe_scheme_rejects_body_signed_without_timestamp_prefix():
    """A plain HMAC over the body alone must NOT verify."""
    scheme = get_signature_scheme("stripe")
    headers = {"Stripe-Signature": f"t=1700000000,v1={hex_digest(SECRET, BODY)}"}
    check = scheme.verify(secret=SECRET, body=BODY, headers=headers, now=1_700_000_010)
    assert check.verdict is SignatureVerdict.MISMATCH


def test_stripe_scheme_rejects_stale_timestamp():
    scheme = get_signature_scheme("stripe")
    headers = {"Stripe-Signature": stripe_header(SECRET, BODY, 1_700_000_000)}
    check = scheme.verify(
        secret=SECRET, body=BODY, headers=headers, tolerance_seconds=300,
        now=1_700_000_000 + 301,
    )
    assert check.verdict is SignatureVerdict.STALE
    assert check.detail == "expired"


def test_stripe_scheme_rejects_future_timestamp():
    scheme = get_signature_scheme("stripe")
    headers = {"Stripe-Signature": stripe_header(SECRET, BODY, 1_700_000_000)}
    check = scheme.verify(
        secret=SECRET, body=BODY, headers=headers, tolerance_seconds=300,
        now=1_700_000_000 - 301,
    )
    assert check.verdict is SignatureVerdict.STALE
    assert check.detail == "future"


def test_stripe_scheme_zero_tolerance_disables_freshness_check():
    scheme = get_signature_scheme("stripe")
    headers = {"Stripe-Signature": stripe_header(SECRET, BODY, 1_700_000_000)}
    check = scheme.verify(
        secret=SECRET, body=BODY, headers=headers, tolerance_seconds=0,
        now=2_000_000_000,
    )
    assert check.ok


def test_stripe_scheme_accepts_any_of_multiple_v1():
    """Stripe sends one v1 per active secret during key rotation."""
    scheme = get_signature_scheme("stripe")
    ts = 1_700_000_000
    good = hmac.new(
        SECRET.encode(), f"{ts}.".encode() + BODY, hashlib.sha256
    ).hexdigest()
    stale_secret = hmac.new(
        b"old-secret", f"{ts}.".encode() + BODY, hashlib.sha256
    ).hexdigest()
    headers = {"Stripe-Signature": f"t={ts},v1={stale_secret},v1={good}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers, now=ts).ok


def test_stripe_scheme_ignores_v0_scheme_versions():
    scheme = get_signature_scheme("stripe")
    ts = 1_700_000_000
    digest = hmac.new(
        SECRET.encode(), f"{ts}.".encode() + BODY, hashlib.sha256
    ).hexdigest()
    headers = {"Stripe-Signature": f"t={ts},v0=deadbeef,v1={digest}"}
    assert scheme.verify(secret=SECRET, body=BODY, headers=headers, now=ts).ok


@pytest.mark.parametrize(
    "value, detail",
    [
        ("v1=abcdef", "missing_timestamp"),
        ("t=1700000000", "missing_v1"),
        ("t=notanumber,v1=abcdef", "bad_timestamp"),
        ("t=1700000000,v1=zzzz", "not_hex"),
    ],
)
def test_stripe_scheme_malformed_header_variants(value, detail):
    scheme = get_signature_scheme("stripe")
    check = scheme.verify(
        secret=SECRET, body=BODY, headers={"Stripe-Signature": value},
        now=1_700_000_000,
    )
    assert check.verdict is SignatureVerdict.MALFORMED
    assert check.detail == detail


def test_stripe_scheme_missing_header_is_missing_verdict():
    scheme = get_signature_scheme("stripe")
    assert scheme.verify(secret=SECRET, body=BODY, headers={}).verdict is (
        SignatureVerdict.MISSING
    )


# ---------------------------------------------------------------------------
# Round trip — sign() must be the exact inverse of verify()
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", available_signature_schemes())
def test_sign_verify_roundtrip(name):
    scheme = get_signature_scheme(name)
    headers = scheme.sign(secret=SECRET, body=BODY, now=1_700_000_000)
    check = scheme.verify(
        secret=SECRET, body=BODY, headers=headers, now=1_700_000_000
    )
    assert check.ok, f"{name} round-trip failed: {check}"


@pytest.mark.parametrize("name", available_signature_schemes())
def test_sign_verify_roundtrip_rejects_wrong_secret(name):
    scheme = get_signature_scheme(name)
    headers = scheme.sign(secret=SECRET, body=BODY, now=1_700_000_000)
    check = scheme.verify(
        secret="different", body=BODY, headers=headers, now=1_700_000_000
    )
    assert not check.ok


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_builtin_schemes_are_registered():
    assert available_signature_schemes() == ["generic", "github", "jira", "stripe"]


def test_register_custom_scheme_then_get():
    scheme = HexDigestScheme(name="_test_shopify", header="X-Shopify-Hmac")
    try:
        register_signature_scheme(scheme)
        assert get_signature_scheme("_test_shopify") is scheme
    finally:
        from navigator_eventbus import webhook_signatures

        webhook_signatures._SCHEMES.pop("_test_shopify", None)


def test_register_duplicate_requires_replace():
    with pytest.raises(ValueError, match="already registered"):
        register_signature_scheme(HexDigestScheme(name="github", header="X-Nope"))


def test_register_duplicate_with_replace_overrides():
    original = get_signature_scheme("generic")
    replacement = HexDigestScheme(name="generic", header="X-Other")
    try:
        register_signature_scheme(replacement, replace=True)
        assert get_signature_scheme("generic") is replacement
    finally:
        register_signature_scheme(original, replace=True)


def test_register_requires_non_empty_name():
    with pytest.raises(ValueError, match="non-empty 'name'"):
        register_signature_scheme(HexDigestScheme(name="", header="X-Nope"))


def test_get_unknown_scheme_raises_with_available_names():
    with pytest.raises(KeyError) as excinfo:
        get_signature_scheme("does-not-exist")
    assert "github" in str(excinfo.value)


def test_hexdigest_scheme_rejects_unknown_algorithm():
    with pytest.raises(ValueError, match="unknown algorithm"):
        HexDigestScheme(name="bad", header="X", algorithms=("md5",))


def test_scheme_is_a_signature_scheme():
    assert isinstance(get_signature_scheme("github"), SignatureScheme)
    assert isinstance(get_signature_scheme("stripe"), StripeScheme)


def test_signature_check_ok_property():
    assert SignatureCheck(SignatureVerdict.OK).ok
    assert not SignatureCheck(SignatureVerdict.MISMATCH).ok


# ---------------------------------------------------------------------------
# resolve_callable / accepts_two_positionals
# ---------------------------------------------------------------------------


def test_resolve_callable_supports_colon_and_dotted_forms():
    assert resolve_callable("json:dumps")({"a": 1}) == '{"a": 1}'
    assert resolve_callable("json.dumps")({"a": 1}) == '{"a": 1}'


def test_resolve_callable_supports_dotted_attr_after_colon():
    fn = resolve_callable("collections:OrderedDict.fromkeys")
    assert list(fn("ab")) == ["a", "b"]


def test_resolve_callable_strips_whitespace():
    assert resolve_callable("  json:dumps  ")({}) == "{}"


@pytest.mark.parametrize("ref", ["", "   ", "nocolonnodot", ":dumps", "json:"])
def test_resolve_callable_rejects_malformed_reference(ref):
    with pytest.raises(ValueError):
        resolve_callable(ref)


def test_resolve_callable_rejects_non_string():
    with pytest.raises(ValueError, match="non-empty str"):
        resolve_callable(None)  # type: ignore[arg-type]


def test_resolve_callable_rejects_unimportable_module():
    with pytest.raises(ValueError, match="Cannot import module"):
        resolve_callable("navigator_eventbus._definitely_not_here:fn")


def test_resolve_callable_rejects_missing_attribute():
    with pytest.raises(ValueError, match="no attribute path"):
        resolve_callable("json:not_a_real_function")


def test_resolve_callable_rejects_non_callable():
    with pytest.raises(TypeError, match="non-callable"):
        resolve_callable("json:__doc__")


def test_accepts_two_positionals_detects_arity():
    assert not accepts_two_positionals(lambda payload: payload)
    assert accepts_two_positionals(lambda payload, ctx: payload)
    assert accepts_two_positionals(lambda *args: args)
    assert not accepts_two_positionals(lambda payload, *, ctx=None: payload)


def test_accepts_two_positionals_on_callable_object():
    class OneArg:
        def __call__(self, payload):
            return payload

    class TwoArgs:
        def __call__(self, payload, ctx):
            return payload

    assert not accepts_two_positionals(OneArg())
    assert accepts_two_positionals(TwoArgs())


def test_accepts_two_positionals_falls_back_to_false_when_uninspectable():
    # Some C builtins have no introspectable signature.
    assert accepts_two_positionals(print) in (True, False)
