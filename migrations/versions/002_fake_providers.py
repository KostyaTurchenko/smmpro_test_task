"""Persist fake provider settings and callback jobs."""

from alembic import op

revision = "002_fake_providers"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE fake_provider_settings (
            provider TEXT PRIMARY KEY,
            settings JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );
        """
    )
    op.execute(
        """
        CREATE TABLE fake_provider_tasks (
            id UUID PRIMARY KEY,
            provider TEXT NOT NULL,
            attempt_id UUID NOT NULL,
            request JSONB NOT NULL,
            settings JSONB NOT NULL,
            outcome JSONB NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN ('pending', 'delivered', 'dropped', 'rejected')
            ),
            due_at TIMESTAMPTZ NOT NULL,
            next_callback_at TIMESTAMPTZ NOT NULL,
            claimed_until TIMESTAMPTZ,
            claim_token UUID,
            callback_attempts INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            delivered_at TIMESTAMPTZ,
            UNIQUE (provider, attempt_id)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX fake_provider_tasks_pending_idx
            ON fake_provider_tasks (provider, next_callback_at)
            WHERE status = 'pending';
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE fake_provider_tasks")
    op.execute("DROP TABLE fake_provider_settings")
