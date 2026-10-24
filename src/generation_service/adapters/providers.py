"""HTTP implementation of the generation provider port."""

import httpx

from generation_service.domain import (
    DispatchAttempt,
    ErrorType,
    Submission,
    SubmissionError,
)


class HttpProviderGateway:
    """Submit generation attempts using the providers' shared HTTP protocol."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_urls: dict[str, str],
        callback_base_url: str,
    ) -> None:
        self._client = client
        self._base_urls = base_urls
        self._callback_base_url = callback_base_url.rstrip("/")

    async def submit(self, attempt: DispatchAttempt) -> Submission:
        """Submit one idempotent attempt and normalize provider errors."""
        try:
            response = await self._client.post(
                f"{self._base_urls[attempt.provider].rstrip('/')}/tasks",
                headers={"Idempotency-Key": str(attempt.id)},
                json={
                    "attempt_id": str(attempt.id),
                    "model_slug": attempt.model_slug,
                    "prompt": attempt.prompt,
                    "ref_images": attempt.ref_images,
                    "callback_url": (
                        f"{self._callback_base_url}/webhooks/providers/{attempt.provider}"
                    ),
                },
            )
        except httpx.HTTPError as exc:
            raise SubmissionError(ErrorType.TECHNICAL) from exc

        if not response.is_success:
            error_type = ErrorType.TECHNICAL
            if 400 <= response.status_code < 500:
                error_type = ErrorType.PAYLOAD
                try:
                    body = response.json()
                    provider_error = body.get("error_type")
                    if provider_error is None and isinstance(body.get("detail"), dict):
                        provider_error = body["detail"].get("error_type")
                    if provider_error == ErrorType.MODERATION.value:
                        error_type = ErrorType.MODERATION
                except (ValueError, AttributeError, TypeError):
                    pass
            raise SubmissionError(error_type)

        try:
            external_id = response.json()["external_id"]
            if not isinstance(external_id, str) or not external_id:
                raise ValueError("Provider response is missing an external ID")
        except (ValueError, KeyError, TypeError) as exc:
            raise SubmissionError(ErrorType.TECHNICAL) from exc
        return Submission(external_id=external_id)
