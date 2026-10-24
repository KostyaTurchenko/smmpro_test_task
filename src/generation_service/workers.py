import asyncio
import logging
from uuid import UUID

from generation_service.config import Settings
from generation_service.domain import DispatchAttempt, SubmissionError
from generation_service.ports import ProviderGateway, Store
from generation_service.services import GenerationService

logger = logging.getLogger(__name__)


class GenerationWorker:
    """Dispatch durable attempts and advance expired ones across worker processes."""

    def __init__(
        self,
        store: Store,
        gateway: ProviderGateway,
        service: GenerationService,
        settings: Settings,
    ) -> None:
        self.store = store
        self.gateway = gateway
        self.service = service
        self.settings = settings

    async def dispatch_pending(self, task_id: UUID | None = None) -> None:
        attempts = await self.store.claim_dispatches(
            timeout_seconds=self.settings.provider_timeout,
            lease_seconds=self.settings.dispatch_lease_seconds,
            limit=self.settings.worker_batch_size,
            task_id=task_id,
        )
        await asyncio.gather(*(self._submit(attempt) for attempt in attempts))

    async def _submit(self, attempt: DispatchAttempt) -> None:
        try:
            submission = await self.gateway.submit(attempt)
        except SubmissionError as error:
            await self.service.submission_failed(attempt, error.error_type)
        else:
            await self.service.mark_submitted(attempt, submission)

    async def expire_pending(self) -> None:
        attempts = await self.store.claim_timeouts(
            lease_seconds=self.settings.dispatch_lease_seconds,
            limit=self.settings.worker_batch_size,
        )
        await asyncio.gather(
            *(self.service.timeout(attempt.id, attempt.claim_token) for attempt in attempts)
        )

    async def run_dispatcher(self) -> None:
        while True:
            try:
                await self.dispatch_pending()
            except Exception:
                logger.exception("Dispatch iteration failed; unacknowledged leases will expire")
            await asyncio.sleep(self.settings.worker_poll_interval)

    async def run_watchdog(self) -> None:
        while True:
            try:
                await self.expire_pending()
            except Exception:
                logger.exception("Watchdog iteration failed; unacknowledged leases will expire")
            await asyncio.sleep(self.settings.worker_poll_interval)
