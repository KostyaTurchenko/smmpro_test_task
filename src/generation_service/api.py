import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from generation_service.adapters.postgres import PostgresStore
from generation_service.adapters.providers import HttpProviderGateway
from generation_service.adapters.signatures import verify_signature
from generation_service.config import Settings
from generation_service.domain import ErrorType, ProviderResult, ServiceError
from generation_service.services import GenerationService, PaymentService
from generation_service.workers import GenerationWorker

logger = logging.getLogger(__name__)
UserId = Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]


class GenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UserId
    model_slug: str = Field(min_length=1)
    prompt: str = Field(min_length=1)
    ref_images: list[str] = Field(default_factory=list)


class GenerateResponse(BaseModel):
    task_id: UUID


class PaymentWebhook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: str = Field(min_length=1)
    user_id: UserId
    tokens: int = Field(gt=0, le=2**63 - 1, strict=True)
    status: Literal["completed", "failed", "pending"]


class ProviderWebhook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attempt_id: UUID | None = None
    external_id: str = Field(min_length=1)
    status: Literal["completed", "failed"]
    result_url: str | None = None
    error_type: ErrorType | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> "ProviderWebhook":
        if self.status == "completed" and (not self.result_url or self.error_type is not None):
            raise ValueError("Completed result requires a URL and no error")
        if self.status == "failed" and (self.error_type is None or self.result_url is not None):
            raise ValueError("Failed result requires an error and no result URL")
        return self


async def require_signature(
    request: Request, x_signature: Annotated[str | None, Header()] = None
) -> None:
    if not verify_signature(
        await request.body(), x_signature or "", request.app.state.settings.webhook_secret
    ):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    store = await PostgresStore.connect(settings.database_url)
    async with httpx.AsyncClient(timeout=settings.provider_http_timeout) as client:
        gateway = HttpProviderGateway(
            client,
            {"alpha": settings.alpha_url, "beta": settings.beta_url},
            settings.callback_base_url,
        )
        service = GenerationService(store)
        worker = GenerationWorker(store, gateway, service, settings)
        app.state.settings = settings
        app.state.store = store
        app.state.generations = service
        app.state.payments = PaymentService(store)
        app.state.worker = worker
        background = [
            asyncio.create_task(worker.run_dispatcher()),
            asyncio.create_task(worker.run_watchdog()),
        ]
        try:
            yield
        finally:
            for job in background:
                job.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await store.close()


app = FastAPI(title="Generation service", lifespan=lifespan)


@app.exception_handler(ServiceError)
async def service_error_handler(request: Request, error: ServiceError) -> JSONResponse:
    return JSONResponse(status_code=error.status_code, content={"detail": str(error)})


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    async with request.app.state.store.transaction() as repository:
        await repository.get_model("image-fast")
    return {"status": "ok"}


@app.post("/generate", status_code=202, response_model=GenerateResponse)
async def generate(payload: GenerateRequest, request: Request) -> GenerateResponse:
    task_id = await request.app.state.generations.generate(
        payload.user_id, payload.model_slug, payload.prompt, payload.ref_images
    )
    try:
        await request.app.state.worker.dispatch_pending(task_id)
    except Exception:
        # The committed attempt remains in the queue for a background worker.
        logger.exception("Immediate dispatch failed for task %s", task_id)
    return GenerateResponse(task_id=task_id)


@app.post("/webhooks/payments", dependencies=[Depends(require_signature)])
async def payment_webhook(payload: PaymentWebhook, request: Request) -> dict[str, bool]:
    applied = await request.app.state.payments.process(
        payload.payment_id, payload.user_id, payload.tokens, payload.status
    )
    return {"applied": applied}


@app.post("/webhooks/providers/{name}", dependencies=[Depends(require_signature)])
async def provider_webhook(
    name: Literal["alpha", "beta"], payload: ProviderWebhook, request: Request
) -> dict[str, bool]:
    result = ProviderResult(provider=name, **payload.model_dump())
    return {"applied": await request.app.state.generations.apply_result(result)}


@app.get("/tasks/{task_id}")
async def get_task(task_id: UUID, request: Request) -> dict[str, Any]:
    task = await request.app.state.store.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.get("/users/{user_id}/balance")
async def get_balance(
    user_id: Annotated[int, Path(gt=0, le=2**63 - 1)], request: Request
) -> dict[str, Any]:
    balance = await request.app.state.store.get_balance(user_id)
    if balance is None:
        raise HTTPException(status_code=404, detail="User not found")
    return balance
