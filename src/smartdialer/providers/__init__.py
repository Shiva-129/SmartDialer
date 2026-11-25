"""Telecom providers package."""

from smartdialer.providers.base import (
    ProviderInitiateResult,
    SimulatedWebhookEvent,
    TelecomProvider,
)
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.providers.registry import ProviderRegistry, provider_registry

__all__ = [
    "TelecomProvider",
    "ProviderInitiateResult",
    "SimulatedWebhookEvent",
    "ProviderA",
    "ProviderB",
    "ProviderRegistry",
    "provider_registry",
]
