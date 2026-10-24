"""A persistent fake provider with configurable failures and webhook delivery."""

import asyncio
import json
import logging
import os
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

import asyncpg
import httpx
from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from generation_service.adapters.signatures import sign_body

logger = logging.getLogger(__name__)


class ProviderSettings(BaseModel):
    """Behavior applied to newly accepted provider requests."""

    model_config = ConfigDict(extra="forbid")

    failure_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    error_type: Literal["technical", "moderation", "payload"] = "technical"
    sync_error: Literal["none", "503", "timeout"] = "none"
    delay_seconds: float | None = Field(default=None, ge=0.0)
    duplicate_count: int = Field(default=1, ge=1)
    concurrent_duplicates: bool = False
    drop_webhook: bool = False
    sync_timeout_seconds: float = Field(default=10.0, gt=0.0)


class TaskRequest(BaseModel):
    """Generation request received from the orchestration service."""

    model_config = ConfigDict(extra="forbid")

    attempt_id: UUID
    model_slug: str
    prompt: str
    ref_images: list[str]
    callback_url: AnyHttpUrl


class TaskAccepted(BaseModel):
    external_id: UUID


def _decode_json(value: Any) -> dict[str, Any]:
    return json.loads(value) if isinstance(value, str) else dict(value)


async def _initialize_settings(pool: asyncpg.Pool, provider: str) -> None:
    await pool.execute(
        """
        INSERT INTO fake_provider_settings (provider, settings)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (provider) DO NOTHING
        """,
        provider,
        ProviderSettings().model_dump_json(),
    )


async def _claim_callback(
    pool: asyncpg.Pool,
    provider: str,
    callback_timeout: float,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        """
        WITH candidate AS (
            SELECT id
            FROM fake_provider_tasks
            WHERE provider = $1
                AND status = 'pending'
                AND next_callback_at <= clock_timestamp()
                AND (claimed_until IS NULL OR claimed_until < clock_timestamp())
            ORDER BY next_callback_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE fake_provider_tasks AS task
        SET claim_token = $2,
            claimed_until = clock_timestamp() + make_interval(
                secs => GREATEST(
                    30.0,
                    $3::double precision * (settings->>'duplicate_count')::integer + 5.0
                )
            ),
            callback_attempts = callback_attempts + 1
        FROM candidate
        WHERE task.id = candidate.id
        RETURNING task.*
        """,
        provider,
        uuid4(),
        callback_timeout,
    )


async def _deliver_callback(
    client: httpx.AsyncClient,
    task: asyncpg.Record,
    secret: str,
) -> None:
    settings = ProviderSettings.model_validate(_decode_json(task["settings"]))
    request = _decode_json(task["request"])
    body = json.dumps(_decode_json(task["outcome"]), separators=(",", ":")).encode()
    headers = {
        "Content-Type": "application/json",
        "X-Signature": sign_body(body, secret),
    }

    async def send_once() -> None:
        response = await client.post(request["callback_url"], content=body, headers=headers)
        response.raise_for_status()

    if settings.concurrent_duplicates:
        results = await asyncio.gather(
            *(send_once() for _ in range(settings.duplicate_count)),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
    else:
        for _ in range(settings.duplicate_count):
            await send_once()


async def _run_callback_worker(app: FastAPI) -> None:
    """Deliver saved callbacks and reclaim work after interrupted deliveries."""
    while True:
        try:
            task = await _claim_callback(
                app.state.pool,
                app.state.provider,
                app.state.callback_timeout,
            )
            if task is None:
                await asyncio.sleep(app.state.poll_interval)
                continue

            try:
                await _deliver_callback(app.state.client, task, app.state.secret)
            except (httpx.HTTPError, OSError) as exc:
                logger.warning(
                    "Callback delivery failed for provider %s task %s: %s",
                    app.state.provider,
                    task["id"],
                    exc,
                )
                retry_delay = min(30.0, float(2 ** min(task["callback_attempts"], 5)))
                await app.state.pool.execute(
                    """
                    UPDATE fake_provider_tasks
                    SET claimed_until = NULL, claim_token = NULL,
                        next_callback_at = clock_timestamp() + make_interval(
                            secs => $3::double precision
                        )
                    WHERE id = $1 AND claim_token = $2
                    """,
                    task["id"],
                    task["claim_token"],
                    retry_delay,
                )
            else:
                await app.state.pool.execute(
                    """
                    UPDATE fake_provider_tasks
                    SET status = 'delivered', delivered_at = clock_timestamp(),
                        claimed_until = NULL, claim_token = NULL
                    WHERE id = $1 AND claim_token = $2
                    """,
                    task["id"],
                    task["claim_token"],
                )
        except (asyncpg.PostgresError, OSError):
            logger.exception("Provider callback worker failed")
            await asyncio.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.provider = os.getenv("PROVIDER_NAME", "alpha")
    app.state.secret = os.getenv("WEBHOOK_SECRET", "development-provider-secret")
    app.state.callback_timeout = float(os.getenv("CALLBACK_HTTP_TIMEOUT", "5"))
    app.state.poll_interval = float(os.getenv("PROVIDER_POLL_INTERVAL", "0.2"))
    pool = await asyncpg.create_pool(
        os.getenv("DATABASE_URL", "postgresql://generation:generation@db:5432/generation"),
        min_size=1,
        max_size=5,
    )
    async with httpx.AsyncClient(timeout=app.state.callback_timeout) as client:
        app.state.pool = pool
        app.state.client = client
        await _initialize_settings(pool, app.state.provider)
        worker = asyncio.create_task(_run_callback_worker(app))
        try:
            yield
        finally:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker
            await pool.close()


app = FastAPI(title="Fake generation provider", lifespan=lifespan)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    await request.app.state.pool.fetchval("SELECT 1")
    return {"status": "ok", "provider": request.app.state.provider}


@app.get("/settings", response_model=ProviderSettings)
async def get_settings(request: Request) -> ProviderSettings:
    value = await request.app.state.pool.fetchval(
        "SELECT settings FROM fake_provider_settings WHERE provider = $1",
        request.app.state.provider,
    )
    return ProviderSettings.model_validate(_decode_json(value))


@app.post("/settings", response_model=ProviderSettings)
async def update_settings(settings: ProviderSettings, request: Request) -> ProviderSettings:
    """Update supplied settings without changing already accepted tasks."""
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            current = await connection.fetchval(
                """
                SELECT settings FROM fake_provider_settings
                WHERE provider = $1 FOR UPDATE
                """,
                request.app.state.provider,
            )
            updated = ProviderSettings.model_validate(
                _decode_json(current) | settings.model_dump(exclude_unset=True)
            )
            await connection.execute(
                """
                UPDATE fake_provider_settings
                SET settings = $2::jsonb, updated_at = clock_timestamp()
                WHERE provider = $1
                """,
                request.app.state.provider,
                updated.model_dump_json(),
            )
    return updated


@app.post("/tasks", response_model=TaskAccepted, status_code=status.HTTP_202_ACCEPTED)
async def submit_task(
    task: TaskRequest,
    request: Request,
    idempotency_key: Annotated[UUID, Header(alias="Idempotency-Key")],
) -> TaskAccepted:
    """Persist a provider job once for each idempotency key."""
    if idempotency_key != task.attempt_id:
        raise HTTPException(status_code=400, detail="Idempotency key must match attempt_id")

    provider = request.app.state.provider
    payload = task.model_dump(mode="json")
    async with request.app.state.pool.acquire() as connection:
        async with connection.transaction():
            saved = await connection.fetchrow(
                """
                SELECT * FROM fake_provider_tasks
                WHERE provider = $1 AND attempt_id = $2
                """,
                provider,
                task.attempt_id,
            )
            if saved is None:
                value = await connection.fetchval(
                    "SELECT settings FROM fake_provider_settings WHERE provider = $1",
                    provider,
                )
                settings = ProviderSettings.model_validate(_decode_json(value))
                external_id = uuid4()
                failed = random.random() < settings.failure_rate
                extension = "mp4" if task.model_slug.startswith("video") else "png"
                outcome = {
                    "attempt_id": str(task.attempt_id),
                    "external_id": str(external_id),
                    "status": "failed" if failed else "completed",
                    "result_url": (
                        None
                        if failed
                        else f"https://results.example/{provider}/{external_id}.{extension}"
                    ),
                    "error_type": settings.error_type if failed else None,
                }
                delay = settings.delay_seconds
                if delay is None:
                    delay = random.uniform(2.0, 20.0)
                job_status = "pending"
                if settings.sync_error == "503":
                    job_status = "rejected"
                elif settings.drop_webhook:
                    job_status = "dropped"
                saved = await connection.fetchrow(
                    """
                    INSERT INTO fake_provider_tasks (
                        id, provider, attempt_id, request, settings, outcome,
                        status, due_at, next_callback_at
                    )
                    VALUES (
                        $1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7,
                        clock_timestamp() + make_interval(secs => $8::double precision),
                        clock_timestamp() + make_interval(secs => $8::double precision)
                    )
                    ON CONFLICT (provider, attempt_id) DO NOTHING
                    RETURNING *
                    """,
                    external_id,
                    provider,
                    task.attempt_id,
                    json.dumps(payload),
                    settings.model_dump_json(),
                    json.dumps(outcome),
                    job_status,
                    delay,
                )
                if saved is None:
                    saved = await connection.fetchrow(
                        """
                        SELECT * FROM fake_provider_tasks
                        WHERE provider = $1 AND attempt_id = $2
                        """,
                        provider,
                        task.attempt_id,
                    )

    if _decode_json(saved["request"]) != payload:
        raise HTTPException(status_code=409, detail="Idempotency key has another payload")
    settings = ProviderSettings.model_validate(_decode_json(saved["settings"]))
    if settings.sync_error == "503":
        raise HTTPException(status_code=503, detail="Provider is temporarily unavailable")
    if settings.sync_error == "timeout":
        await asyncio.sleep(settings.sync_timeout_seconds)
    return TaskAccepted(external_id=saved["id"])
