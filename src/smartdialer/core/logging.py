"""Structured logging configuration with keyword arguments support."""

import logging
import sys
from typing import Any, Dict


class CustomFormatter(logging.Formatter):
    """Clean formatted output with extra key-values."""

    def format(self, record: logging.LogRecord) -> str:
        base_message = super().format(record)
        extras = getattr(record, "structured_data", {})
        if extras:
            extra_str = " ".join(f"{k}={v}" for k, v in extras.items())
            return f"{base_message} | {extra_str}"
        return base_message


class StructuredLoggerAdapter(logging.LoggerAdapter):
    """Adapter allowing kwargs to be passed directly as structured key-values."""

    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        if not self.isEnabledFor(level):
            return
        extra = kwargs.pop("extra", {}) or {}
        exc_info = kwargs.pop("exc_info", None)
        stack_info = kwargs.pop("stack_info", False)
        # Any remaining kwargs are structured fields
        structured_data = {**(self.extra or {}), **extra, **kwargs}
        self.logger._log(
            level,
            msg,
            args,
            exc_info=exc_info,
            extra={"structured_data": structured_data},
            stack_info=stack_info,
        )


def get_logger(name: str = "smartdialer") -> StructuredLoggerAdapter:
    """Retrieve or initialize a structured logger instance."""
    base_logger = logging.getLogger(name)
    if not base_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = CustomFormatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        base_logger.addHandler(handler)
        base_logger.setLevel(logging.INFO)
    return StructuredLoggerAdapter(base_logger, {})
