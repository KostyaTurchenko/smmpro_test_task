from datetime import UTC, datetime
from uuid import UUID, uuid4

from generation_service.domain import (
    Attempt,
    AttemptIdentity,
    AttemptStatus,
    DispatchAttempt,
    ErrorType,
    GenerationTask,
    ModelLeg,
    ProviderResult,
    ServiceError,
    Submission,
    TaskStatus,
)
from generation_service.ports import Repository, Store

ACTIVE_ATTEMPTS = {AttemptStatus.SUBMITTING, AttemptStatus.RUNNING}
TERMINAL_TASKS = {TaskStatus.COMPLETED, TaskStatus.FAILED}


class GenerationService:
    """Coordinate generation attempts, token charges and refunds."""

    def __init__(self, store: Store) -> None:
        self.store = store

    async def generate(
        self, user_id: int, model_slug: str, prompt: str, ref_images: list[str]
    ) -> UUID:
        async with self.store.transaction() as repository:
            model = await repository.get_model(model_slug)
            if model is None:
                raise ServiceError("Unknown model", 404)
            leg = self._next_leg(model.legs, len(ref_images))
            if leg is None:
                raise ServiceError("No provider accepts this number of reference images", 400)

            await repository.ensure_user(user_id)
            balance = await repository.lock_user(user_id)
            if await repository.active_task_count(user_id) >= 3:
                raise ServiceError("User already has three active tasks", 429)
            if balance < model.price:
                raise ServiceError("Insufficient tokens", 402)

            task = GenerationTask(uuid4(), user_id, model.slug, model.price, prompt, ref_images)
            await repository.insert_task(task)
            await repository.post_ledger(user_id, "charge", -model.price, task_id=task.id)
            await self._queue_attempt(repository, task, leg)
            return task.id

    async def apply_result(self, result: ProviderResult) -> bool:
        """Apply a signed result only to the current live attempt."""
        async with self.store.transaction() as repository:
            if result.attempt_id is not None:
                identity = await repository.identify_attempt(result.attempt_id)
            else:
                identity = await repository.find_attempt(result.provider, result.external_id)
            if identity is None:
                return False
            task, attempt = await self._lock_current(repository, identity)
            if task is None or attempt is None:
                return False
            if attempt.provider != result.provider:
                raise ServiceError("Provider does not match the attempt", 400)
            if attempt.external_id is not None and attempt.external_id != result.external_id:
                raise ServiceError("External ID does not match the attempt", 400)

            attempt.external_id = result.external_id
            attempt.claim_token = None
            if result.status == "completed":
                attempt.status = AttemptStatus.COMPLETED
                attempt.finished_at = datetime.now(UTC)
                task.status = TaskStatus.COMPLETED
                task.result_url = result.result_url
                task.error_type = None
                await repository.update_attempt(attempt)
                await repository.update_task(task)
            else:
                await self._fail(
                    repository, task, attempt, result.error_type or ErrorType.TECHNICAL
                )
            return True

    async def mark_submitted(self, dispatch: DispatchAttempt, submission: Submission) -> None:
        async with self.store.transaction() as repository:
            identity = await repository.identify_attempt(dispatch.id)
            if identity is None:
                return
            task, attempt = await self._lock_current(repository, identity)
            if task is None or attempt is None or attempt.claim_token != dispatch.claim_token:
                return
            attempt.external_id = submission.external_id
            attempt.status = AttemptStatus.RUNNING
            attempt.claim_token = None
            task.status = TaskStatus.RUNNING
            await repository.update_attempt(attempt)
            await repository.update_task(task)

    async def submission_failed(self, dispatch: DispatchAttempt, error_type: ErrorType) -> None:
        await self._fail_claimed(dispatch.id, dispatch.claim_token, error_type, timed_out=False)

    async def timeout(self, attempt_id: UUID, claim_token: UUID) -> None:
        await self._fail_claimed(attempt_id, claim_token, ErrorType.TECHNICAL, timed_out=True)

    async def _fail_claimed(
        self, attempt_id: UUID, claim_token: UUID, error_type: ErrorType, *, timed_out: bool
    ) -> None:
        async with self.store.transaction() as repository:
            identity = await repository.identify_attempt(attempt_id)
            if identity is None:
                return
            task, attempt = await self._lock_current(repository, identity)
            if task is None or attempt is None or attempt.claim_token != claim_token:
                return
            await self._fail(repository, task, attempt, error_type, timed_out=timed_out)

    async def _lock_current(
        self, repository: Repository, identity: AttemptIdentity
    ) -> tuple[GenerationTask | None, Attempt | None]:
        # All balance-changing paths acquire locks in the same order.
        await repository.lock_user(identity.user_id)
        task = await repository.lock_task(identity.task_id)
        attempt = await repository.lock_attempt(identity.attempt_id)
        if (
            task is None
            or attempt is None
            or task.status in TERMINAL_TASKS
            or task.current_attempt_id != attempt.id
            or attempt.status not in ACTIVE_ATTEMPTS
        ):
            return None, None
        return task, attempt

    async def _fail(
        self,
        repository: Repository,
        task: GenerationTask,
        attempt: Attempt,
        error_type: ErrorType,
        *,
        timed_out: bool = False,
    ) -> None:
        """Advance a failed attempt or finish with one atomic refund."""
        attempt.status = AttemptStatus.TIMED_OUT if timed_out else AttemptStatus.FAILED
        attempt.error_type = error_type
        attempt.finished_at = datetime.now(UTC)
        attempt.claim_token = None
        await repository.update_attempt(attempt)

        model = await repository.get_model(task.model_slug)
        leg = None
        if error_type != ErrorType.PAYLOAD and model is not None:
            leg = self._next_leg(
                model.legs, len(task.ref_images), after_priority=attempt.priority, error=error_type
            )
        if leg is not None:
            await self._queue_attempt(repository, task, leg)
            return

        task.status = TaskStatus.FAILED
        task.error_type = error_type
        await repository.post_ledger(task.user_id, "refund", task.price, task_id=task.id)
        await repository.update_task(task)

    @staticmethod
    async def _queue_attempt(repository: Repository, task: GenerationTask, leg: ModelLeg) -> None:
        attempt = Attempt(uuid4(), task.id, leg.id, leg.provider, leg.priority)
        await repository.insert_attempt(attempt)
        task.current_attempt_id = attempt.id
        task.status = TaskStatus.PENDING
        await repository.update_task(task)

    @staticmethod
    def _next_leg(
        legs: list[ModelLeg],
        reference_count: int,
        *,
        after_priority: int = 0,
        error: ErrorType | None = None,
    ) -> ModelLeg | None:
        return next(
            (
                leg
                for leg in sorted(legs, key=lambda item: item.priority)
                if leg.priority > after_priority
                and leg.max_ref_images >= reference_count
                and not (error == ErrorType.MODERATION and leg.skip_on_moderation)
            ),
            None,
        )


class PaymentService:
    """Credit each successful payment once, together with its ledger entry."""

    def __init__(self, store: Store) -> None:
        self.store = store

    async def process(self, payment_id: str, user_id: int, tokens: int, status: str) -> bool:
        if status != "completed":
            return False
        async with self.store.transaction() as repository:
            await repository.ensure_user(user_id)
            await repository.lock_user(user_id)
            return await repository.post_ledger(user_id, "payment", tokens, payment_id=payment_id)
