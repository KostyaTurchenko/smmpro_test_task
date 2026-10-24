from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Load application settings from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://generation:generation@db:5432/generation"
    webhook_secret: str = "local-development-secret"
    alpha_url: str = "http://alpha:8000"
    beta_url: str = "http://beta:8000"
    callback_base_url: str = "http://api:8000"
    provider_timeout: float = Field(default=60, gt=0)
    provider_http_timeout: float = Field(default=5, gt=0)
    dispatch_lease_seconds: float = Field(default=15, gt=0)
    worker_poll_interval: float = Field(default=0.2, gt=0)
    worker_batch_size: int = Field(default=10, ge=1, le=100)

    @model_validator(mode="after")
    def validate_lease(self) -> "Settings":
        if self.dispatch_lease_seconds <= self.provider_http_timeout:
            raise ValueError("Dispatch lease must exceed the provider HTTP timeout")
        return self
