"""Exercise dispatch, fallback and financial races through the running API."""

from __future__ import annotations

import asyncio
import os
import time
from uuid import UUID, uuid4

import pytest
from conftest import Harness

from generation_service.adapters.postgres import PostgresStore

pytestmark = pytest.mark.asyncio


def provider_result(attempt, *, status="failed", error_type="technical"):
    return {
        "attempt_id": str(attempt["id"]),
        "external_id": attempt["external_id"],
        "status": status,
        "error_type": error_type,
        "result_url": "https://example.com/result.png" if status == "completed" else None,
    }


async def wait_fake_delivery(harness: Harness, attempt_id: str, *, max_wait: float = 12):
    deadline = time.monotonic() + max_wait
    while True:
        status = await harness.db.fetchval(
            "SELECT status FROM fake_provider_tasks WHERE attempt_id = $1",
            UUID(attempt_id),
        )
        if status == "delivered":
            return
        assert time.monotonic() < deadline, status
        await asyncio.sleep(0.05)


async def test_ten_concurrent_last_leg_failures_refund_once(harness: Harness):
    await harness.credit()
    await harness.settings("beta", drop_webhook=True)
    task_id = await harness.generate(refs=5)
    attempt = await harness.wait_attempt(task_id)

    responses = await asyncio.gather(
        *[harness.webhook("/webhooks/providers/beta", provider_result(attempt)) for _ in range(10)]
    )

    assert all(response.status_code == 200 for response in responses)
    task = await harness.wait_task(task_id, "failed")
    operations = await harness.ledger()
    assert len(task["attempts"]) == 1
    assert sum(operation["kind"] == "charge" for operation in operations) == 1
    assert sum(operation["kind"] == "refund" for operation in operations) == 1
    assert (await harness.balance())["balance"] == 100


async def test_technical_failure_falls_back_to_second_provider(harness: Harness):
    await harness.credit()
    await harness.settings("alpha", failure_rate=1.0)
    task_id = await harness.generate()

    task = await harness.wait_task(task_id, "completed")

    assert [attempt["provider"] for attempt in task["attempts"]] == ["alpha", "beta"]
    assert [attempt["status"] for attempt in task["attempts"]] == ["failed", "completed"]
    assert task["result_url"]
    assert (await harness.balance())["balance"] == 98
    assert [operation["kind"] for operation in await harness.ledger()].count("refund") == 0


async def test_late_completed_cannot_reverse_timeout_and_refund(harness: Harness):
    await harness.credit()
    await harness.settings("beta", drop_webhook=True)
    task_id = await harness.generate(refs=5)
    attempt = await harness.wait_attempt(task_id)
    await harness.db.execute(
        "UPDATE attempts SET deadline_at = now() - interval '1 second' WHERE id = $1",
        attempt["id"],
    )
    await harness.wait_task(task_id, "failed")

    response = await harness.webhook(
        "/webhooks/providers/beta",
        provider_result(attempt, status="completed", error_type=None),
    )

    assert response.status_code == 200, response.text
    task = await harness.task(task_id)
    operations = await harness.ledger()
    assert task["status"] == "failed"
    assert task["result_url"] is None
    assert len(task["attempts"]) == 1
    assert sum(operation["kind"] == "refund" for operation in operations) == 1
    assert (await harness.balance())["balance"] == 100


async def test_concurrent_generations_enforce_three_active_tasks(harness: Harness):
    await harness.credit()
    await harness.settings("beta", drop_webhook=True)
    payload = {
        "user_id": harness.user_id,
        "model_slug": "image-fast",
        "prompt": "An apple",
        "ref_images": [f"https://example.com/{i}.png" for i in range(5)],
    }

    responses = await asyncio.gather(
        *[harness.api.post("/generate", json=payload) for _ in range(10)]
    )

    accepted = [response.json()["task_id"] for response in responses if response.status_code == 202]
    assert len(accepted) == 3, [response.status_code for response in responses]
    assert sum(response.status_code == 429 for response in responses) == 7
    assert (await harness.balance())["balance"] == 94
    operations = await harness.ledger()
    assert sum(operation["kind"] == "charge" for operation in operations) == 3
    assert (
        await harness.db.fetchval(
            "SELECT count(*) FROM tasks WHERE user_id = $1 AND status IN ('pending', 'running')",
            harness.user_id,
        )
        == 3
    )

    for task_id in accepted:
        attempt = await harness.wait_attempt(task_id)
        response = await harness.webhook(
            "/webhooks/providers/beta",
            provider_result(attempt, status="completed", error_type=None),
        )
        assert response.status_code == 200, response.text
        await harness.wait_task(task_id, "completed")


async def test_video_moderation_stops_chain_when_configured(harness: Harness):
    await harness.credit()
    await harness.settings("beta", failure_rate=1.0, error_type="moderation")
    task_id = await harness.generate(model="video-long")

    task = await harness.wait_task(task_id, "failed")

    assert [attempt["provider"] for attempt in task["attempts"]] == ["beta"]
    assert task["error_type"] == "moderation"
    assert (await harness.balance())["balance"] == 100


async def test_payload_failure_stops_chain(harness: Harness):
    await harness.credit()
    await harness.settings("alpha", failure_rate=1.0, error_type="payload")
    task_id = await harness.generate()

    task = await harness.wait_task(task_id, "failed")

    assert [attempt["provider"] for attempt in task["attempts"]] == ["alpha"]
    assert task["error_type"] == "payload"
    assert (await harness.balance())["balance"] == 100


async def test_synchronous_503_falls_back(harness: Harness):
    await harness.credit()
    await harness.settings("alpha", sync_error="503")
    task_id = await harness.generate()

    task = await harness.wait_task(task_id, "completed")

    assert [attempt["provider"] for attempt in task["attempts"]] == ["alpha", "beta"]
    assert task["attempts"][0]["status"] == "failed"
    assert task["attempts"][0]["error_type"] == "technical"
    assert (await harness.balance())["balance"] == 98


async def test_balance_matches_signed_journal(harness: Harness):
    await harness.credit()
    successful_id = await harness.generate()
    await harness.wait_task(successful_id, "completed")
    await harness.settings("beta", failure_rate=1.0)
    failed_id = await harness.generate(refs=5)
    await harness.wait_task(failed_id, "failed")

    balance = await harness.balance()
    operations = await harness.ledger()

    assert balance["balance"] == sum(operation["amount"] for operation in operations) == 98
    assert balance["balance"] == sum(operation["amount"] for operation in balance["operations"])
    assert sorted(operation["amount"] for operation in operations) == [-2, -2, 2, 100]
    assert {operation["task_id"] for operation in operations if operation["kind"] == "charge"} == {
        UUID(successful_id),
        UUID(failed_id),
    }


async def test_concurrent_completed_callbacks_charge_once(harness: Harness):
    await harness.credit()
    await harness.settings("alpha", duplicate_count=10, concurrent_duplicates=True)
    task_id = await harness.generate()
    task = await harness.wait_task(task_id, "completed")
    await wait_fake_delivery(harness, task["attempts"][0]["id"])

    operations = await harness.ledger()

    assert len(task["attempts"]) == 1
    assert task["attempts"][0]["status"] == "completed"
    assert sum(operation["kind"] == "charge" for operation in operations) == 1
    assert sum(operation["kind"] == "refund" for operation in operations) == 0
    assert (await harness.balance())["balance"] == 98


async def test_http_timeout_falls_back_and_ignores_late_success(harness: Harness):
    await harness.credit()
    await harness.settings("alpha", sync_error="timeout", sync_timeout_seconds=6, delay_seconds=6)
    task_id = await harness.generate()
    completed = await harness.wait_task(task_id, "completed")
    assert [attempt["provider"] for attempt in completed["attempts"]] == ["alpha", "beta"]
    await wait_fake_delivery(harness, completed["attempts"][0]["id"])

    after_late_success = await harness.task(task_id)
    operations = await harness.ledger()

    assert after_late_success["status"] == "completed"
    assert after_late_success["result_url"] == completed["result_url"]
    assert [attempt["status"] for attempt in after_late_success["attempts"]] == [
        "failed",
        "completed",
    ]
    assert sum(operation["kind"] == "charge" for operation in operations) == 1
    assert sum(operation["kind"] == "refund" for operation in operations) == 0
    assert (await harness.balance())["balance"] == 98


async def test_first_leg_timeout_falls_back_and_ignores_late_success(harness: Harness):
    await harness.credit()
    await harness.settings("alpha", drop_webhook=True)
    task_id = await harness.generate()
    attempt = await harness.wait_attempt(task_id)
    assert attempt["provider"] == "alpha"
    await harness.db.execute(
        "UPDATE attempts SET deadline_at = now() - interval '1 second' WHERE id = $1",
        attempt["id"],
    )
    completed = await harness.wait_task(task_id, "completed")

    response = await harness.webhook(
        "/webhooks/providers/alpha",
        provider_result(attempt, status="completed", error_type=None),
    )

    assert response.status_code == 200, response.text
    after_late_success = await harness.task(task_id)
    assert after_late_success["status"] == "completed"
    assert after_late_success["result_url"] == completed["result_url"]
    assert [attempt["provider"] for attempt in after_late_success["attempts"]] == ["alpha", "beta"]
    assert [attempt["status"] for attempt in after_late_success["attempts"]] == [
        "timed_out",
        "completed",
    ]
    assert [operation["kind"] for operation in await harness.ledger()].count("refund") == 0
    assert (await harness.balance())["balance"] == 98


async def test_ledger_and_balance_roll_back_together(harness: Harness):
    """An interrupted transaction must not retain either financial write."""
    await harness.credit()
    payment_id = str(uuid4())
    database_url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    store = await PostgresStore.connect(database_url)
    try:
        with pytest.raises(RuntimeError, match="interrupted before commit"):
            async with store.transaction() as repository:
                await repository.lock_user(harness.user_id)
                assert await repository.post_ledger(
                    harness.user_id, "payment", 17, payment_id=payment_id
                )
                raise RuntimeError("interrupted before commit")
    finally:
        await store.close()

    assert (await harness.balance())["balance"] == 100
    operations = await harness.ledger()
    assert len(operations) == 1
    assert all(operation["payment_id"] != payment_id for operation in operations)


async def test_expired_submission_lease_reuses_provider_job(harness: Harness):
    """Recover the persisted state left after acceptance but before saving its ID."""
    await harness.credit()
    await harness.settings("alpha", drop_webhook=True)
    task_id = await harness.generate()
    accepted = await harness.wait_attempt(task_id)
    await harness.db.execute(
        """UPDATE attempts
           SET status = 'submitting', external_id = NULL, claim_token = $2,
               lease_until = now() - interval '1 second',
               deadline_at = now() + interval '30 seconds'
           WHERE id = $1""",
        accepted["id"],
        uuid4(),
    )

    recovered = await harness.wait_attempt(task_id)

    assert recovered["id"] == accepted["id"]
    assert recovered["external_id"] == accepted["external_id"]
    assert recovered["status"] == "running"
    assert (
        await harness.db.fetchval(
            "SELECT count(*) FROM fake_provider_tasks WHERE attempt_id = $1",
            accepted["id"],
        )
        == 1
    )
    response = await harness.webhook(
        "/webhooks/providers/alpha",
        provider_result(recovered, status="completed", error_type=None),
    )
    assert response.status_code == 200, response.text
    task = await harness.wait_task(task_id, "completed")
    assert len(task["attempts"]) == 1
    assert [operation["kind"] for operation in await harness.ledger()].count("charge") == 1
    assert (await harness.balance())["balance"] == 98
