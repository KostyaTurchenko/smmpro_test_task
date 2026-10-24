"""HTTP integration fixtures shared by all four API worker tests."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio

DEFAULT_SETTINGS = {
    "failure_rate": 0.0,
    "error_type": "technical",
    "sync_error": "none",
    "delay_seconds": 0.05,
    "duplicate_count": 1,
    "concurrent_duplicates": False,
    "drop_webhook": False,
    "sync_timeout_seconds": 10.0,
}


@dataclass
class Harness:
    """Drive the running API and inspect persisted financial effects."""

    api: httpx.AsyncClient
    providers: dict[str, httpx.AsyncClient]
    db: asyncpg.Connection
    secret: str
    user_id: int

    async def settings(self, provider: str, **overrides: Any) -> None:
        response = await self.providers[provider].post(
            "/settings", json={**DEFAULT_SETTINGS, **overrides}
        )
        assert response.status_code == 200, response.text

    async def webhook(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        signature: str | None = "valid",
    ) -> httpx.Response:
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json"}
        if signature is not None:
            headers["X-Signature"] = (
                hmac.new(self.secret.encode(), body, hashlib.sha256).hexdigest()
                if signature == "valid"
                else signature
            )
        return await self.api.post(path, content=body, headers=headers)

    async def credit(self, tokens: int = 100) -> str:
        payment_id = str(uuid4())
        response = await self.webhook(
            "/webhooks/payments",
            {
                "payment_id": payment_id,
                "user_id": self.user_id,
                "tokens": tokens,
                "status": "completed",
            },
        )
        assert response.status_code == 200, response.text
        return payment_id

    async def generate(self, *, model: str = "image-fast", refs: int = 0) -> str:
        response = await self.api.post(
            "/generate",
            json={
                "user_id": self.user_id,
                "model_slug": model,
                "prompt": "A quiet street in watercolor",
                "ref_images": [f"https://example.com/{i}.png" for i in range(refs)],
            },
        )
        assert response.status_code == 202, response.text
        return response.json()["task_id"]

    async def task(self, task_id: str) -> dict[str, Any]:
        response = await self.api.get(f"/tasks/{task_id}")
        assert response.status_code == 200, response.text
        return response.json()

    async def wait_task(self, task_id: str, status: str, *, max_wait: float = 15) -> dict[str, Any]:
        deadline = time.monotonic() + max_wait
        while True:
            task = await self.task(task_id)
            if task["status"] == status:
                return task
            assert time.monotonic() < deadline, task
            await asyncio.sleep(0.05)

    async def wait_attempt(self, task_id: str) -> asyncpg.Record:
        deadline = time.monotonic() + 15
        while True:
            attempt = await self.db.fetchrow(
                """SELECT id, external_id, provider, status, deadline_at
                   FROM attempts
                   WHERE task_id = $1 AND external_id IS NOT NULL
                   ORDER BY started_at DESC LIMIT 1""",
                UUID(task_id),
            )
            if attempt is not None:
                return attempt
            assert time.monotonic() < deadline, await self.task(task_id)
            await asyncio.sleep(0.05)

    async def balance(self) -> dict[str, Any]:
        response = await self.api.get(f"/users/{self.user_id}/balance")
        assert response.status_code == 200, response.text
        return response.json()

    async def ledger(self) -> list[asyncpg.Record]:
        return await self.db.fetch(
            "SELECT kind, amount, payment_id, task_id FROM ledger WHERE user_id = $1",
            self.user_id,
        )


@pytest_asyncio.fixture
async def harness() -> Harness:
    database_url = os.environ.get("DATABASE_URL")
    webhook_secret = os.environ.get("WEBHOOK_SECRET")
    if not database_url or not webhook_secret:
        pytest.fail("Integration tests require DATABASE_URL and WEBHOOK_SECRET")
    database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
    db = await asyncpg.connect(database_url)
    async with (
        httpx.AsyncClient(
            base_url=os.environ.get("TEST_API_URL", "http://api:8000"), timeout=20
        ) as api,
        httpx.AsyncClient(
            base_url=os.environ.get("ALPHA_URL", "http://alpha:8000"), timeout=20
        ) as alpha,
        httpx.AsyncClient(
            base_url=os.environ.get("BETA_URL", "http://beta:8000"), timeout=20
        ) as beta,
    ):
        current = Harness(
            api=api,
            providers={"alpha": alpha, "beta": beta},
            db=db,
            secret=webhook_secret,
            user_id=secrets.randbelow(2**62 - 1) + 1,
        )
        original_settings = {}
        try:
            responses = await asyncio.gather(
                *[provider.get("/settings") for provider in current.providers.values()]
            )
            for name, response in zip(current.providers, responses, strict=True):
                assert response.status_code == 200, response.text
                original_settings[name] = response.json()
            await asyncio.gather(current.settings("alpha"), current.settings("beta"))
            yield current
        finally:
            try:
                responses = await asyncio.gather(
                    *[
                        current.providers[name].post("/settings", json=settings)
                        for name, settings in original_settings.items()
                    ],
                    return_exceptions=True,
                )
                for response in responses:
                    if isinstance(response, BaseException):
                        raise response
                    assert response.status_code == 200, response.text
            finally:
                await db.close()
