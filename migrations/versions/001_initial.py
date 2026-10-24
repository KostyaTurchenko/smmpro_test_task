"""Create generation state, model catalog and token ledger."""

from alembic import op

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE models (
            slug TEXT PRIMARY KEY,
            price BIGINT NOT NULL CHECK (price > 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE model_legs (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            model_slug TEXT NOT NULL REFERENCES models (slug),
            provider TEXT NOT NULL,
            priority INTEGER NOT NULL CHECK (priority > 0),
            max_ref_images INTEGER NOT NULL CHECK (max_ref_images >= 0),
            skip_on_moderation BOOLEAN NOT NULL DEFAULT FALSE,
            UNIQUE (model_slug, priority)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE tasks (
            id UUID PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users (id),
            model_slug TEXT NOT NULL REFERENCES models (slug),
            price BIGINT NOT NULL CHECK (price > 0),
            prompt TEXT NOT NULL,
            ref_images JSONB NOT NULL CHECK (jsonb_typeof(ref_images) = 'array'),
            status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
            current_attempt_id UUID,
            result_url TEXT,
            error_type TEXT CHECK (error_type IN ('technical', 'moderation', 'payload')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE attempts (
            id UUID PRIMARY KEY,
            task_id UUID NOT NULL REFERENCES tasks (id),
            leg_id BIGINT NOT NULL REFERENCES model_legs (id),
            provider TEXT NOT NULL,
            priority INTEGER NOT NULL CHECK (priority > 0),
            status TEXT NOT NULL CHECK (
                status IN ('queued', 'submitting', 'running', 'completed', 'failed', 'timed_out')
            ),
            external_id TEXT,
            error_type TEXT CHECK (error_type IN ('technical', 'moderation', 'payload')),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            deadline_at TIMESTAMPTZ,
            claim_token UUID,
            lease_until TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            UNIQUE (task_id, leg_id)
        )
        """
    )
    op.execute(
        """
        ALTER TABLE tasks ADD CONSTRAINT tasks_current_attempt_fk
        FOREIGN KEY (current_attempt_id) REFERENCES attempts (id) DEFERRABLE INITIALLY DEFERRED
        """
    )
    op.execute(
        """
        CREATE TABLE ledger (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users (id),
            kind TEXT NOT NULL,
            amount BIGINT NOT NULL,
            task_id UUID REFERENCES tasks (id),
            payment_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CHECK (
                (kind = 'payment' AND amount > 0 AND payment_id IS NOT NULL AND task_id IS NULL)
                OR (kind = 'charge' AND amount < 0 AND task_id IS NOT NULL AND payment_id IS NULL)
                OR (kind = 'refund' AND amount > 0 AND task_id IS NOT NULL AND payment_id IS NULL)
            )
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX ledger_payment_idx ON ledger (payment_id) WHERE payment_id IS NOT NULL"
    )
    op.execute(
        """
        CREATE UNIQUE INDEX ledger_task_kind_idx ON ledger (task_id, kind)
        WHERE task_id IS NOT NULL
        """
    )
    op.execute("CREATE INDEX ledger_user_idx ON ledger (user_id, id DESC)")
    op.execute(
        """
        CREATE UNIQUE INDEX attempts_external_idx ON attempts (provider, external_id)
        WHERE external_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX tasks_active_user_idx ON tasks (user_id)
        WHERE status IN ('pending', 'running')
        """
    )
    op.execute(
        """
        CREATE INDEX attempts_dispatch_idx ON attempts (created_at)
        WHERE status IN ('queued', 'submitting')
        """
    )
    op.execute(
        """
        CREATE INDEX attempts_timeout_idx ON attempts (deadline_at)
        WHERE status IN ('submitting', 'running')
        """
    )
    op.execute("INSERT INTO models (slug, price) VALUES ('image-fast', 2), ('video-long', 25)")
    op.execute(
        """
        INSERT INTO model_legs (
            model_slug, provider, priority, max_ref_images, skip_on_moderation
        ) VALUES
            ('image-fast', 'alpha', 1, 4, FALSE),
            ('image-fast', 'beta', 2, 10, FALSE),
            ('video-long', 'beta', 1, 2, FALSE),
            ('video-long', 'alpha', 2, 1, TRUE)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE ledger")
    op.execute("ALTER TABLE tasks DROP CONSTRAINT tasks_current_attempt_fk")
    op.execute("DROP TABLE attempts")
    op.execute("DROP TABLE tasks")
    op.execute("DROP TABLE model_legs")
    op.execute("DROP TABLE models")
    op.execute("DROP TABLE users")
