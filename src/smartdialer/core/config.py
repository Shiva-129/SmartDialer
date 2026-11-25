"""Application configuration using Pydantic Settings."""

import os
from typing import Optional
from urllib.parse import quote_plus
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration settings for the SmartDialer application."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Environment
    ENVIRONMENT: str = Field(default="development", description="Runtime environment")
    DEBUG: bool = Field(default=False, description="Debug mode")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")

    # Database
    POSTGRES_USER: str = Field(default="postgres", description="PostgreSQL user")
    POSTGRES_PASSWORD: str = Field(default="", description="PostgreSQL password")
    POSTGRES_HOST: str = Field(default="127.0.0.1", description="PostgreSQL host")
    POSTGRES_PORT: int = Field(default=5432, description="PostgreSQL port")
    POSTGRES_DB: str = Field(default="smartdialer", description="Default database name")
    POSTGRES_TEST_DB: str = Field(default="smartdialer_test", description="Test database name")

    # Explicit URLs (if provided, take precedence)
    DATABASE_URL: Optional[str] = Field(default=None, description="Explicit sync database URL")
    ASYNC_DATABASE_URL: Optional[str] = Field(default=None, description="Explicit async database URL")

    # Database connection pool settings
    DB_POOL_SIZE: int = Field(default=20, description="Database connection pool size")
    DB_MAX_OVERFLOW: int = Field(default=10, description="Database max overflow connections")
    DB_POOL_TIMEOUT: int = Field(default=30, description="Connection pool checkout timeout")

    # Safety & Dialing Limits
    SAFETY_MAX_ABANDONMENT_RATE: float = Field(
        default=0.03,
        description="Regulatory ceiling for abandoned call rate (3%)",
    )
    AGENT_RESERVATION_TIMEOUT_SECONDS: int = Field(
        default=30,
        description="Max seconds an agent can stay reserved before being auto-reclaimed",
    )
    BORROWER_RESERVATION_TIMEOUT_SECONDS: int = Field(
        default=45,
        description="Max seconds a borrower can stay reserved before being auto-reclaimed",
    )
    CALL_RINGING_TIMEOUT_SECONDS: int = Field(
        default=60,
        description="Max seconds a call may ring without provider answer before marked failed",
    )
    OUTBOX_POLL_INTERVAL_SECONDS: float = Field(
        default=0.25,
        description="Polling interval for outbox processor loop",
    )

    # Optional Twilio PSTN Credentials (never required for local simulation)
    TWILIO_ACCOUNT_SID: Optional[str] = Field(default=None, description="Twilio Account SID")
    TWILIO_AUTH_TOKEN: Optional[str] = Field(default=None, description="Twilio Auth Token")
    TWILIO_FROM_NUMBER: Optional[str] = Field(default=None, description="Twilio caller ID phone number (E.164)")
    TWILIO_WEBHOOK_BASE_URL: Optional[str] = Field(
        default=None,
        description="Public HTTPS base URL (e.g. ngrok) for Twilio status callbacks",
    )

    def _format_auth(self) -> str:
        """URL-encode database credentials to handle special characters safely (FIND-LOW-01)."""
        user = quote_plus(self.POSTGRES_USER)
        if self.POSTGRES_PASSWORD:
            pwd = quote_plus(self.POSTGRES_PASSWORD)
            return f"{user}:{pwd}"
        return user

    @property
    def sync_database_url(self) -> str:
        """Construct or return synchronous PostgreSQL connection URL with psycopg3 driver."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        auth = self._format_auth()
        return f"postgresql+psycopg://{auth}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def async_database_url(self) -> str:
        """Construct or return asynchronous PostgreSQL connection URL with asyncpg driver."""
        if self.ASYNC_DATABASE_URL:
            return self.ASYNC_DATABASE_URL
        auth = self._format_auth()
        return f"postgresql+asyncpg://{auth}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    def get_test_sync_url(self) -> str:
        """Return sync test database connection URL."""
        auth = self._format_auth()
        return f"postgresql+psycopg://{auth}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_TEST_DB}"

    def get_test_async_url(self) -> str:
        """Return async test database connection URL."""
        auth = self._format_auth()
        return f"postgresql+asyncpg://{auth}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_TEST_DB}"


settings = Settings()
