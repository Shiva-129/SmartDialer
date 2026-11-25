"""Registry of configured telecom providers."""

from typing import Dict, Optional
from smartdialer.providers.base import TelecomProvider
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.providers.twilio_provider import TwilioProvider


class ProviderRegistry:
    """Singleton directory of active telecom providers."""

    def __init__(self) -> None:
        self._providers: Dict[str, TelecomProvider] = {
            "provider_a": ProviderA(),
            "provider_b": ProviderB(),
            "twilio": TwilioProvider(),
        }

    def get(self, name: str) -> Optional[TelecomProvider]:
        """Retrieve a provider instance by name."""
        return self._providers.get(name)

    def register(self, provider: TelecomProvider) -> None:
        """Register a custom or configured provider instance."""
        self._providers[provider.name] = provider


provider_registry = ProviderRegistry()
