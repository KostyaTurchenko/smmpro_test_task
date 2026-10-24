FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --create-home --uid 10001 app
USER app

FROM base AS runtime
CMD ["uvicorn", "generation_service.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]

FROM base AS tests
USER root
RUN pip install --no-cache-dir '.[dev]'
COPY tests ./tests
USER app
ENV PYTEST_ADDOPTS="-p no:cacheprovider"
CMD ["pytest", "-q"]
