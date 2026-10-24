from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class ErrorType(StrEnum):
    TECHNICAL = "technical"
    MODERATION = "moderation"
    PAYLOAD = "payload"


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class AttemptStatus(StrEnum):
    QUEUED = "queued"
    SUBMITTING = "submitting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"


@dataclass(frozen=True)
class ModelLeg:
    id: int
    provider: str
    priority: int
    max_ref_images: int
    skip_on_moderation: bool


@dataclass(frozen=True)
class Model:
    slug: str
    price: int
    legs: list[ModelLeg]


@dataclass
class GenerationTask:
    """Track a generation and its current provider attempt."""

    id: UUID
    user_id: int
    model_slug: str
    price: int
    prompt: str
    ref_images: list[str]
    status: TaskStatus = TaskStatus.PENDING
    current_attempt_id: UUID | None = None
    result_url: str | None = None
    error_type: ErrorType | None = None


@dataclass
class Attempt:
    """Record a single execution through a model leg."""

    id: UUID
    task_id: UUID
    leg_id: int
    provider: str
    priority: int
    status: AttemptStatus = AttemptStatus.QUEUED
    external_id: str | None = None
    error_type: ErrorType | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    deadline_at: datetime | None = None
    claim_token: UUID | None = None


@dataclass(frozen=True)
class DispatchAttempt:
    id: UUID
    task_id: UUID
    provider: str
    model_slug: str
    prompt: str
    ref_images: list[str]
    claim_token: UUID


@dataclass(frozen=True)
class Submission:
    external_id: str


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    external_id: str
    status: str
    attempt_id: UUID | None = None
    result_url: str | None = None
    error_type: ErrorType | None = None


@dataclass(frozen=True)
class AttemptIdentity:
    user_id: int
    task_id: UUID
    attempt_id: UUID


@dataclass(frozen=True)
class ExpiredAttempt:
    id: UUID
    claim_token: UUID


class ServiceError(Exception):
    """Represent a rejected application operation."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class SubmissionError(Exception):
    """Describe a provider refusing a task submission."""

    def __init__(
        self, error_type: ErrorType, message: str = "Provider rejected submission"
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
