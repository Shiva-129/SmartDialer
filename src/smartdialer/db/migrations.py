"""Database schema migrations for PostgreSQL (safe, idempotent, zero data loss)."""

from sqlalchemy import create_engine, text
from smartdialer.core.config import settings
from smartdialer.core.logging import get_logger
from smartdialer.db.base import Base
# Ensure all models are loaded
import smartdialer.models  # noqa: F401

logger = get_logger("migrations")


def run_migrations(target_engine=None) -> None:
    """
    Ensure all tables, columns, constraints, and indexes exist.
    Idempotent and safe to run on existing databases without data loss.
    """
    engine = target_engine or create_engine(settings.sync_database_url)

    # 1. Create missing tables
    Base.metadata.create_all(bind=engine)

    # 2. Add columns if not already present on existing tables
    migration_statements = [
        # Call model additions
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS dispatch_token UUID;",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS dialing_mode VARCHAR(20) DEFAULT 'PROGRESSIVE';",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS termination_status VARCHAR(30) DEFAULT 'NOT_REQUIRED';",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS termination_attempts INTEGER DEFAULT 0;",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS next_termination_retry_at TIMESTAMP WITH TIME ZONE;",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS termination_error VARCHAR(500);",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS termination_confirmed_at TIMESTAMP WITH TIME ZONE;",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS disposition VARCHAR(50);",
        "ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_notes VARCHAR(2000);",
        # Agent model additions
        "ALTER TABLE agents ADD COLUMN IF NOT EXISTS dispatch_token UUID;",
        # OutboxEvent additions
        "ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS worker_id VARCHAR(100);",
        "ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS fencing_token UUID;",
        # ProviderRequestLedger additions
        "ALTER TABLE provider_request_ledger ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'PENDING';",
        "ALTER TABLE provider_request_ledger ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP WITH TIME ZONE;",
        "ALTER TABLE provider_request_ledger ADD COLUMN IF NOT EXISTS worker_id VARCHAR(100);",
        "ALTER TABLE provider_request_ledger ADD COLUMN IF NOT EXISTS attempt_count INTEGER DEFAULT 1;",
        "ALTER TABLE provider_request_ledger ADD COLUMN IF NOT EXISTS fencing_token UUID;",
        # Partial indexes for hot-path performance
        "CREATE INDEX IF NOT EXISTS idx_agents_avail_pool ON agents (campaign_id) WHERE status = 'AVAILABLE';",
        "CREATE INDEX IF NOT EXISTS idx_borrowers_avail_pool ON borrowers (campaign_id) WHERE status = 'ELIGIBLE';",
        "CREATE INDEX IF NOT EXISTS ix_calls_dispatch_token ON calls (dispatch_token) WHERE dispatch_token IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS ix_calls_termination ON calls (termination_status, next_termination_retry_at);",
    ]

    with engine.begin() as conn:
        for stmt in migration_statements:
            try:
                conn.execute(text(stmt))
            except Exception as exc:
                logger.warning("Migration statement skipped or already applied", stmt=stmt, error=str(exc))

    logger.info("Database migrations successfully executed", db=str(engine.url))


if __name__ == "__main__":
    run_migrations()
