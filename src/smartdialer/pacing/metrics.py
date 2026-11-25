"""Statistical metrics tracker for answer rates and call durations."""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Optional, Tuple


@dataclass
class CampaignTelemetry:
    """Maintains statistical trackers for a campaign's answer rates and call durations."""
    window_size: int = 100
    alpha_ema: float = 0.20  # Exponential smoothing factor
    
    # Completed call outcomes: (answered: bool, duration_sec: float, timestamp: datetime)
    history: Deque[Tuple[bool, float, datetime]] = field(default_factory=lambda: deque(maxlen=100))
    
    # Exponential moving average of answer rate
    _ema_answer_rate: Optional[float] = None
    default_answer_rate: float = 0.30
    default_talk_duration_sec: float = 90.0

    def record_call_result(
        self,
        answered: bool,
        duration_sec: float = 0.0,
    ) -> None:
        """Record an outbound call result and update statistical models."""
        now = datetime.now(timezone.utc)
        self.history.append((answered, duration_sec, now))

        val = 1.0 if answered else 0.0
        if self._ema_answer_rate is None:
            self._ema_answer_rate = val
        else:
            self._ema_answer_rate = (self.alpha_ema * val) + ((1.0 - self.alpha_ema) * self._ema_answer_rate)

    @property
    def answer_rate_ema(self) -> float:
        """Return smoothed exponential moving average answer rate."""
        if self._ema_answer_rate is None:
            return self.default_answer_rate
        # Clamp to realistic bounds [0.05, 0.95]
        return max(0.05, min(0.95, self._ema_answer_rate))

    @property
    def historical_answer_rate(self) -> float:
        """Return windowed empirical answer rate."""
        if not self.history:
            return self.default_answer_rate
        answered_count = sum(1 for ans, _, _ in self.history if ans)
        rate = answered_count / len(self.history)
        return max(0.05, min(0.95, rate))

    @property
    def average_talk_duration_seconds(self) -> float:
        """Return empirical average talk duration of answered calls."""
        answered_durations = [dur for ans, dur, _ in self.history if ans and dur > 0]
        if not answered_durations:
            return self.default_talk_duration_sec
        return sum(answered_durations) / len(answered_durations)

    def set_baseline(self, answer_rate: float, talk_duration_sec: float = 90.0) -> None:
        """Manually seed or override baseline parameters (e.g. for simulation scenarios)."""
        self.default_answer_rate = max(0.05, min(0.95, answer_rate))
        self._ema_answer_rate = self.default_answer_rate
        self.default_talk_duration_sec = max(1.0, talk_duration_sec)


class TelemetryRegistry:
    """Registry of campaign telemetry trackers."""

    def __init__(self) -> None:
        self._registry: dict[str, CampaignTelemetry] = {}

    def get(self, campaign_id: str) -> CampaignTelemetry:
        if campaign_id not in self._registry:
            self._registry[campaign_id] = CampaignTelemetry()
        return self._registry[campaign_id]

    def reset(self) -> None:
        self._registry.clear()


# Global registry instance
telemetry_registry = TelemetryRegistry()
