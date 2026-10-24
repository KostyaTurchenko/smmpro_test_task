from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol
from uuid import UUID

from generation_service.domain import (
    Attempt,
    AttemptIdentity,
    DispatchAttempt,
    ExpiredAttempt,
    GenerationTask,
    Model,
    Submission,
)


class Repository(Protocol):
    """Read and change generation state within one transaction."""

    async def ensure_user(self, user_id: int) -> None: ...

    async def lock_user(self, user_id: int) -> int: ...

    async def get_model(self, slug: str) -> Model | None: ...

    async def active_task_count(self, user_id: int) -> int: ...

    async def insert_task(self, task: GenerationTask) -> None: ...

    async def insert_attempt(self, attempt: Attempt) -> None: ...

    async def update_task(self, task: GenerationTask) -> None: ...

    async def update_attempt(self, attempt: Attempt) -> None: ...

    async def lock_task(self, task_id: UUID) -> GenerationTask | None: ...

    async def lock_attempt(self, attempt_id: UUID) -> Attempt | None: ...

    async def identify_attempt(self, attempt_id: UUID) -> AttemptIdentity | None: ...

    async def find_attempt(self, provider: str, external_id: str) -> AttemptIdentity | None: ...

    async def post_ledger(
        self,
        user_id: int,
        kind: str,
        amount: int,
        *,
        task_id: UUID | None = None,
        payment_id: str | None = None,
    ) -> bool: ...


class Store(Protocol):
    """Provide atomic state changes and persistent worker claims."""

    def transaction(self) -> AbstractAsyncContextManager[Repository]: ...

    async def claim_dispatches(
        self,
        *,
        timeout_seconds: float,
        lease_seconds: float,
        limit: int,
        task_id: UUID | None = None,
    ) -> list[DispatchAttempt]: ...

    async def claim_timeouts(self, *, lease_seconds: float, limit: int) -> list[ExpiredAttempt]: ...

    async def get_task(self, task_id: UUID) -> dict[str, Any] | None: ...

    async def get_balance(self, user_id: int) -> dict[str, Any] | None: ...


class ProviderGateway(Protocol):
    """Submit an attempt using its ID as an idempotency key."""

    async def submit(self, attempt: DispatchAttempt) -> Submission: ...
