"""HMAC signatures for webhook request bodies."""

import hashlib
import hmac


def sign_body(body: bytes, secret: str) -> str:
    """Sign the exact bytes sent in a webhook."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(body: bytes, signature: str | None, secret: str) -> bool:
    """Compare a webhook signature without timing-sensitive equality checks."""
    if signature is None:
        return False
    try:
        return hmac.compare_digest(sign_body(body, secret), signature)
    except TypeError:
        return False
