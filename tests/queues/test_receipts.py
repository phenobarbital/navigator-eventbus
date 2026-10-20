"""Tests for the signed receipt handles (queues, TASK-A)."""
import json

import pytest

from navigator_eventbus.queues.receipts import (
    HANDLE_VERSION,
    Receipt,
    ReceiptCodec,
    ReceiptError,
)

NOW = 1_700_000_000_000
KEY = "receipt-key-one"


def make_receipt(**kwargs) -> Receipt:
    defaults = dict(
        queue="orders",
        group="billing",
        stream="evb:queue:orders",
        message_id="1-0",
        consumer="http-node-1",
        delivered=1,
        expires_at_ms=NOW + 30_000,
    )
    defaults.update(kwargs)
    return Receipt(**defaults)


@pytest.fixture
def codec() -> ReceiptCodec:
    return ReceiptCodec([KEY])


def decode(codec: ReceiptCodec, handle: str, **kwargs):
    params = {"queue": "orders", "group": "billing", "now_ms": NOW}
    params.update(kwargs)
    return codec.decode(handle, **params)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keys", [[], [""], ["ok", ""]])
def test_requires_non_empty_keys(keys):
    with pytest.raises(ValueError, match="non-empty signing key"):
        ReceiptCodec(keys)


def test_rejects_negative_grace():
    with pytest.raises(ValueError, match="must not be negative"):
        ReceiptCodec([KEY], grace_ms=-1)


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_round_trip_preserves_every_claim(codec):
    original = make_receipt()
    restored = decode(codec, codec.encode(original))
    assert restored == original


def test_handle_has_three_dotted_parts_and_version(codec):
    handle = codec.encode(make_receipt())
    parts = handle.split(".")
    assert len(parts) == 3
    assert parts[0] == HANDLE_VERSION


def test_handle_is_url_safe(codec):
    handle = codec.encode(make_receipt(message_id="1698765432100-0"))
    assert "+" not in handle and "/" not in handle and "=" not in handle


def test_delivered_generation_survives_round_trip(codec):
    restored = decode(codec, codec.encode(make_receipt(delivered=7)))
    assert restored.delivered == 7


# ---------------------------------------------------------------------------
# Tampering
# ---------------------------------------------------------------------------


def test_tampered_body_is_rejected(codec):
    version, body, sig = codec.encode(make_receipt()).split(".")
    flipped = ("A" if body[0] != "A" else "B") + body[1:]
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, f"{version}.{flipped}.{sig}")
    assert excinfo.value.reason == "bad_signature"


def test_tampered_signature_is_rejected(codec):
    version, body, sig = codec.encode(make_receipt()).split(".")
    flipped = ("A" if sig[0] != "A" else "B") + sig[1:]
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, f"{version}.{body}.{flipped}")
    assert excinfo.value.reason == "bad_signature"


def test_handle_signed_by_another_key_is_rejected(codec):
    foreign = ReceiptCodec(["a-completely-different-key"])
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, foreign.encode(make_receipt()))
    assert excinfo.value.reason == "bad_signature"


def test_claims_are_never_parsed_before_the_mac_verifies(codec, monkeypatch):
    """The load-bearing ordering: no attacker-controlled JSON reaches the parser."""
    import navigator_eventbus.queues.receipts as mod

    def explode(*args, **kwargs):
        raise AssertionError("json.loads called on an unverified body")

    version, body, sig = codec.encode(make_receipt()).split(".")
    bad_sig = ("A" if sig[0] != "A" else "B") + sig[1:]
    monkeypatch.setattr(mod.json, "loads", explode)
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, f"{version}.{body}.{bad_sig}")
    assert excinfo.value.reason == "bad_signature"


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "handle",
    ["", "onlyonepart", "two.parts", "a.b.c.d", "q1..", "....."],
)
def test_structurally_broken_handles_are_malformed(codec, handle):
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, handle)
    assert excinfo.value.reason in {"malformed", "bad_signature", "bad_version"}


def test_non_string_handle_is_malformed(codec):
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, None)  # type: ignore[arg-type]
    assert excinfo.value.reason == "malformed"


def test_unknown_version_is_rejected(codec):
    _, body, sig = codec.encode(make_receipt()).split(".")
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, f"q99.{body}.{sig}")
    assert excinfo.value.reason == "bad_version"


def test_non_base64_signature_is_malformed(codec):
    version, body, _ = codec.encode(make_receipt()).split(".")
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, f"{version}.{body}.!!!not-base64!!!")
    assert excinfo.value.reason in {"malformed", "bad_signature"}


def test_verified_body_with_missing_claims_is_malformed(codec):
    """A correctly signed but structurally wrong payload must not crash."""
    from navigator_eventbus.queues.receipts import _b64

    body = _b64(json.dumps({"q": "orders"}).encode())
    handle = f"{HANDLE_VERSION}.{body}."
    import hashlib
    import hmac as _hmac

    mac = _hmac.new(
        KEY.encode(), f"{HANDLE_VERSION}.{body}".encode(), hashlib.sha256
    ).digest()
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, f"{handle}{_b64(mac)}")
    assert excinfo.value.reason == "malformed"


# ---------------------------------------------------------------------------
# Queue / group binding — what makes cross-group fan-out safe
# ---------------------------------------------------------------------------


def test_handle_from_another_queue_is_rejected(codec):
    handle = codec.encode(make_receipt(queue="payments"))
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, handle, queue="orders")
    assert excinfo.value.reason == "wrong_queue"


def test_handle_from_another_group_is_rejected(codec):
    """Acking another group's copy would silently drop a message it never saw."""
    handle = codec.encode(make_receipt(group="analytics"))
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, handle, group="billing")
    assert excinfo.value.reason == "wrong_queue"


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------


def test_expired_handle_is_rejected(codec):
    handle = codec.encode(make_receipt(expires_at_ms=NOW - 1))
    with pytest.raises(ReceiptError) as excinfo:
        decode(codec, handle, now_ms=NOW)
    assert excinfo.value.reason == "expired"


def test_handle_at_exact_expiry_is_still_accepted(codec):
    handle = codec.encode(make_receipt(expires_at_ms=NOW))
    assert decode(codec, handle, now_ms=NOW).expires_at_ms == NOW


def test_grace_window_accepts_a_recently_expired_handle():
    codec = ReceiptCodec([KEY], grace_ms=5_000)
    handle = codec.encode(make_receipt(expires_at_ms=NOW - 3_000))
    assert codec.decode(handle, queue="orders", group="billing", now_ms=NOW)


def test_grace_window_still_rejects_beyond_the_margin():
    codec = ReceiptCodec([KEY], grace_ms=5_000)
    handle = codec.encode(make_receipt(expires_at_ms=NOW - 6_000))
    with pytest.raises(ReceiptError) as excinfo:
        codec.decode(handle, queue="orders", group="billing", now_ms=NOW)
    assert excinfo.value.reason == "expired"


def test_expiry_is_checked_after_the_signature():
    """An expired handle with a bad MAC reports the signature failure."""
    codec = ReceiptCodec([KEY])
    version, body, sig = codec.encode(make_receipt(expires_at_ms=NOW - 1)).split(".")
    bad = ("A" if sig[0] != "A" else "B") + sig[1:]
    with pytest.raises(ReceiptError) as excinfo:
        codec.decode(f"{version}.{body}.{bad}", queue="orders", group="billing", now_ms=NOW)
    assert excinfo.value.reason == "bad_signature"


# ---------------------------------------------------------------------------
# Key rotation
# ---------------------------------------------------------------------------


def test_rotation_accepts_handles_signed_with_a_retired_key():
    old = ReceiptCodec(["old-key"])
    rotating = ReceiptCodec(["new-key", "old-key"])
    handle = old.encode(make_receipt())
    assert rotating.decode(handle, queue="orders", group="billing", now_ms=NOW)


def test_rotation_signs_with_the_first_key():
    rotating = ReceiptCodec(["new-key", "old-key"])
    new_only = ReceiptCodec(["new-key"])
    handle = rotating.encode(make_receipt())
    assert new_only.decode(handle, queue="orders", group="billing", now_ms=NOW)


def test_dropping_the_old_key_finally_rejects_its_handles():
    old = ReceiptCodec(["old-key"])
    new_only = ReceiptCodec(["new-key"])
    with pytest.raises(ReceiptError) as excinfo:
        new_only.decode(
            old.encode(make_receipt()), queue="orders", group="billing", now_ms=NOW
        )
    assert excinfo.value.reason == "bad_signature"
