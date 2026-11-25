"""Dialer strategy implementations."""

from smartdialer.dialer.predictive import PredictiveDialResult, PredictiveDialer
from smartdialer.dialer.progressive import ProgressiveDialResult, ProgressiveDialer

__all__ = [
    "ProgressiveDialer",
    "ProgressiveDialResult",
    "PredictiveDialer",
    "PredictiveDialResult",
]
