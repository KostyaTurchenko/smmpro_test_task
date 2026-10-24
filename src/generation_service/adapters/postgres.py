import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from generation_service.domain import (
    Attempt,
    AttemptIdentity,
    AttemptStatus,
    DispatchAttempt,
    ErrorType,
    ExpiredAttempt,
    GenerationTask,
    Model,
    ModelLeg,
    TaskStatus,
)


def _task(row: asyncpg.Record) -> GenerationTask:
    return GenerationTask(
        id=row["id"],
        user_id=row["user_id"],
        model_slug=row["model_slug"],
        price=row["price"],
        prompt=row["prompt"],
        ref_images=row["ref_images"],
        status=TaskStatus(row["status"]),
        current_attempt_id=row["current_attempt_id"],
        result_url=row["result_url"],
        error_type=ErrorType(row["error_type"]) if row["error_type"] else None,
    )


def _attempt(row: asyncpg.Record) -> Attempt:
    return Attempt(
        id=row["id"],
        task_id=row["task_id"],
        leg_id=row["leg_id"],
        provider=row["provider"],
        priority=row["priority"],
        status=AttemptStatus(row["status"]),
        external_id=row["external_id"],
        error_type=ErrorType(row["error_type"]) if row["error_type"] else None,
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        deadline_at=row["deadline_at"],
        claim_token=row["claim_token"],
    )


def _identity(row: asyncpg.Record | None) -> AttemptIdentity | None:
    if row is None:
        return None
    return AttemptIdentity(
        user_id=row["user_id"], task_id=row["task_id"], attempt_id=row["attempt_id"]
    )


class PostgresRepository:
    """Persist generation changes on a single transaction connection."""

    def __init__(self, connection: asyncpg.Connection) -> None:
        self.connection = connection

    async def ensure_user(self, user_id: int) -> None:
        await self.connection.execute(
            "INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", user_id
        )

    async def lock_user(self, user_id: int) -> int:
        balance = await self.connection.fetchval(
            "SELECT balance FROM users WHERE id = $1 FOR UPDATE", user_id
        )
        if balance is None:
            raise LookupError(f"User {user_id} does not exist")
        return balance

    async def get_model(self, slug: str) -> Model | None:
        row = await self.connection.fetchrow("SELECT * FROM models WHERE slug = $1", slug)
        if row is None:
            return None
        legs = await self.connection.fetch(
            """
            SELECT id, provider, priority, max_ref_images, skip_on_moderation
            FROM model_legs WHERE model_slug = $1 ORDER BY priority
            """,
            slug,
        )
        return Model(
            slug=row["slug"], price=row["price"], legs=[ModelLeg(**dict(leg)) for leg in legs]
        )

    async def active_task_count(self, user_id: int) -> int:
        return await self.connection.fetchval(
            """
            SELECT count(*) FROM tasks
            WHERE user_id = $1 AND status IN ('pending', 'running')
            """,
            user_id,
        )

    async def insert_task(self, task: GenerationTask) -> None:
        await self.connection.execute(
            """
            INSERT INTO tasks (
                id, user_id, model_slug, price, prompt, ref_images, status,
                current_attempt_id, result_url, error_type
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            task.id,
            task.user_id,
            task.model_slug,
            task.price,
            task.prompt,
            task.ref_images,
            task.status.value,
            task.current_attempt_id,
            task.result_url,
            task.error_type.value if task.error_type else None,
        )

    async def insert_attempt(self, attempt: Attempt) -> None:
        await self.connection.execute(
            """
            INSERT INTO attempts (
                id, task_id, leg_id, provider, priority, status, external_id,
                error_type, started_at, finished_at, deadline_at, claim_token
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            attempt.id,
            attempt.task_id,
            attempt.leg_id,
            attempt.provider,
            attempt.priority,
            attempt.status.value,
            attempt.external_id,
            attempt.error_type.value if attempt.error_type else None,
            attempt.started_at,
            attempt.finished_at,
            attempt.deadline_at,
            attempt.claim_token,
        )

    async def update_task(self, task: GenerationTask) -> None:
        await self.connection.execute(
            """
            UPDATE tasks SET status = $2, current_attempt_id = $3,
                result_url = $4, error_type = $5, updated_at = clock_timestamp()
            WHERE id = $1
            """,
            task.id,
            task.status.value,
            task.current_attempt_id,
            task.result_url,
            task.error_type.value if task.error_type else None,
        )

    async def update_attempt(self, attempt: Attempt) -> None:
        await self.connection.execute(
            """
            UPDATE attempts SET status = $2, external_id = $3, error_type = $4,
                started_at = $5, finished_at = $6, deadline_at = $7, claim_token = $8,
                lease_until = CASE WHEN $8::uuid IS NULL THEN NULL ELSE lease_until END
            WHERE id = $1
            """,
            attempt.id,
            attempt.status.value,
            attempt.external_id,
            attempt.error_type.value if attempt.error_type else None,
            attempt.started_at,
            attempt.finished_at,
            attempt.deadline_at,
            attempt.claim_token,
        )

    async def lock_task(self, task_id: UUID) -> GenerationTask | None:
        row = await self.connection.fetchrow(
            "SELECT * FROM tasks WHERE id = $1 FOR UPDATE", task_id
        )
        return _task(row) if row else None

    async def lock_attempt(self, attempt_id: UUID) -> Attempt | None:
        row = await self.connection.fetchrow(
            "SELECT * FROM attempts WHERE id = $1 FOR UPDATE", attempt_id
        )
        return _attempt(row) if row else None

    async def identify_attempt(self, attempt_id: UUID) -> AttemptIdentity | None:
        row = await self.connection.fetchrow(
            """
            SELECT t.user_id, a.task_id, a.id AS attempt_id
            FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.id = $1
            """,
            attempt_id,
        )
        return _identity(row)

    async def find_attempt(self, provider: str, external_id: str) -> AttemptIdentity | None:
        row = await self.connection.fetchrow(
            """
            SELECT t.user_id, a.task_id, a.id AS attempt_id
            FROM attempts a JOIN tasks t ON t.id = a.task_id
            WHERE a.provider = $1 AND a.external_id = $2
            """,
            provider,
            external_id,
        )
        return _identity(row)

    async def post_ledger(
        self,
        user_id: int,
        kind: str,
        amount: int,
        *,
        task_id: UUID | None = None,
        payment_id: str | None = None,
    ) -> bool:
        """Change the balance only when its ledger operation is newly inserted."""
        operation_id = await self.connection.fetchval(
            """
            INSERT INTO ledger (user_id, kind, amount, task_id, payment_id)
            VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING id
            """,
            user_id,
            kind,
            amount,
            task_id,
            payment_id,
        )
        if operation_id is None:
            return False
        await self.connection.execute(
            "UPDATE users SET balance = balance + $2 WHERE id = $1", user_id, amount
        )
        return True


class PostgresStore:
    """Coordinate transactions and recoverable worker claims in PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    @classmethod
    async def connect(cls, database_url: str) -> "PostgresStore":
        async def initialize(connection: asyncpg.Connection) -> None:
            await connection.set_type_codec(
                "jsonb", schema="pg_catalog", encoder=json.dumps, decoder=json.loads
            )

        pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10, init=initialize)
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[PostgresRepository]:
        async with self.pool.acquire() as connection, connection.transaction():
            yield PostgresRepository(connection)

    async def claim_dispatches(
        self,
        *,
        timeout_seconds: float,
        lease_seconds: float,
        limit: int,
        task_id: UUID | None = None,
    ) -> list[DispatchAttempt]:
        async with self.pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                SELECT a.id, a.task_id, a.provider, t.model_slug, t.prompt, t.ref_images
                FROM attempts a JOIN tasks t ON t.id = a.task_id
                WHERE t.current_attempt_id = a.id AND t.status IN ('pending', 'running')
                  AND ($1::uuid IS NULL OR t.id = $1)
                  AND (
                    a.status = 'queued' OR (
                        a.status = 'submitting' AND a.lease_until <= clock_timestamp()
                        AND a.deadline_at > clock_timestamp()
                    )
                  )
                ORDER BY a.created_at, a.id
                LIMIT $2 FOR UPDATE OF a SKIP LOCKED
                """,
                task_id,
                limit,
            )
            claims = [DispatchAttempt(**dict(row), claim_token=uuid4()) for row in rows]
            await connection.executemany(
                """
                UPDATE attempts SET status = 'submitting', claim_token = $2,
                    started_at = coalesce(started_at, clock_timestamp()),
                    deadline_at = coalesce(
                        deadline_at, clock_timestamp() + make_interval(secs => $3)
                    ),
                    lease_until = least(
                        clock_timestamp() + make_interval(secs => $4),
                        coalesce(deadline_at, clock_timestamp() + make_interval(secs => $3))
                    )
                WHERE id = $1
                """,
                [(claim.id, claim.claim_token, timeout_seconds, lease_seconds) for claim in claims],
            )
            return claims

    async def claim_timeouts(self, *, lease_seconds: float, limit: int) -> list[ExpiredAttempt]:
        async with self.pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                SELECT a.id FROM attempts a JOIN tasks t ON t.id = a.task_id
                WHERE t.current_attempt_id = a.id AND t.status IN ('pending', 'running')
                  AND a.status IN ('submitting', 'running')
                  AND a.deadline_at <= clock_timestamp()
                  AND (a.lease_until IS NULL OR a.lease_until <= clock_timestamp())
                ORDER BY a.deadline_at, a.id
                LIMIT $1 FOR UPDATE OF a SKIP LOCKED
                """,
                limit,
            )
            claims = [ExpiredAttempt(id=row["id"], claim_token=uuid4()) for row in rows]
            await connection.executemany(
                """
                UPDATE attempts SET claim_token = $2,
                    lease_until = clock_timestamp() + make_interval(secs => $3)
                WHERE id = $1
                """,
                [(claim.id, claim.claim_token, lease_seconds) for claim in claims],
            )
            return claims

    async def get_task(self, task_id: UUID) -> dict[str, Any] | None:
        async with (
            self.pool.acquire() as connection,
            connection.transaction(isolation="repeatable_read", readonly=True),
        ):
            row = await connection.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)
            if row is None:
                return None
            current_leg = await connection.fetchrow(
                """
                SELECT l.provider, l.priority, l.max_ref_images, l.skip_on_moderation
                FROM attempts a JOIN model_legs l ON l.id = a.leg_id WHERE a.id = $1
                """,
                row["current_attempt_id"],
            )
            attempts = await connection.fetch(
                """
                SELECT id, leg_id, provider, priority, status, external_id, error_type,
                    started_at, finished_at, deadline_at, created_at
                FROM attempts WHERE task_id = $1 ORDER BY priority
                """,
                task_id,
            )
            return {
                **dict(row),
                "current_leg": dict(current_leg) if current_leg else None,
                "attempts": [dict(attempt) for attempt in attempts],
            }

    async def get_balance(self, user_id: int) -> dict[str, Any] | None:
        async with (
            self.pool.acquire() as connection,
            connection.transaction(isolation="repeatable_read", readonly=True),
        ):
            balance = await connection.fetchval("SELECT balance FROM users WHERE id = $1", user_id)
            if balance is None:
                return None
            rows = await connection.fetch(
                """
                SELECT id, kind, amount, task_id, payment_id, created_at
                FROM ledger WHERE user_id = $1 ORDER BY id DESC LIMIT 25
                """,
                user_id,
            )
            return {
                "user_id": user_id,
                "balance": balance,
                "operations": [dict(row) for row in rows],
            }
