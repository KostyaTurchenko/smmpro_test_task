"""Check HTTP validation and signed, idempotent payment callbacks."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from conftest import Harness

pytestmark = pytest.mark.asyncio


async def test_twenty_simultaneous_payment_callbacks_credit_once(harness: Harness):
    payment_id = str(uuid4())
    payload = {
        "payment_id": payment_id,
        "user_id": harness.user_id,
        "tokens": 100,
        "status": "completed",
    }

    responses = await asyncio.gather(
        *[harness.webhook("/webhooks/payments", payload) for _ in range(20)]
    )

    assert all(response.status_code == 200 for response in responses)
    balance = await harness.balance()
    operations = await harness.ledger()
    assert balance["balance"] == 100
    assert len(operations) == 1
    assert operations[0]["kind"] == "payment"
    assert operations[0]["amount"] == 100
    assert str(operations[0]["payment_id"]) == payment_id


async def test_no_eligible_provider_rejects_before_debit(harness: Harness):
    await harness.credit()

    response = await harness.api.post(
        "/generate",
        json={
            "user_id": harness.user_id,
            "model_slug": "image-fast",
            "prompt": "A mountain",
            "ref_images": [f"https://example.com/{i}.png" for i in range(11)],
        },
    )

    assert response.status_code == 400, response.text
    assert (await harness.balance())["balance"] == 100
    assert len(await harness.ledger()) == 1
    assert (
        await harness.db.fetchval("SELECT count(*) FROM tasks WHERE user_id = $1", harness.user_id)
        == 0
    )


async def test_insufficient_balance_rejects_without_charge(harness: Harness):
    await harness.credit(tokens=1)

    response = await harness.api.post(
        "/generate",
        json={
            "user_id": harness.user_id,
            "model_slug": "image-fast",
            "prompt": "A mountain",
            "ref_images": [],
        },
    )

    assert response.status_code == 402, response.text
    assert (await harness.balance())["balance"] == 1
    assert len(await harness.ledger()) == 1


@pytest.mark.parametrize("signature", [None, "0" * 64])
async def test_payment_signature_is_required(harness: Harness, signature):
    response = await harness.webhook(
        "/webhooks/payments",
        {
            "payment_id": str(uuid4()),
            "user_id": harness.user_id,
            "tokens": 100,
            "status": "completed",
        },
        signature=signature,
    )

    assert response.status_code == 401, response.text
    assert (
        await harness.db.fetchval("SELECT count(*) FROM ledger WHERE user_id = $1", harness.user_id)
        == 0
    )


@pytest.mark.parametrize("signature", [None, "0" * 64])
async def test_provider_signature_is_required(harness: Harness, signature):
    response = await harness.webhook(
        "/webhooks/providers/alpha",
        {
            "attempt_id": str(uuid4()),
            "external_id": str(uuid4()),
            "status": "completed",
            "result_url": "https://example.com/result.png",
            "error_type": None,
        },
        signature=signature,
    )

    assert response.status_code == 401, response.text


async def test_failed_payment_is_ignored(harness: Harness):
    await harness.credit(tokens=7)

    response = await harness.webhook(
        "/webhooks/payments",
        {
            "payment_id": str(uuid4()),
            "user_id": harness.user_id,
            "tokens": 100,
            "status": "failed",
        },
    )

    assert response.status_code == 200, response.text
    assert (await harness.balance())["balance"] == 7
    assert len(await harness.ledger()) == 1
