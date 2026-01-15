"""
Social media data collectors.
"""
from .base import BaseCollector, CollectorResult
from .instagram import InstagramCollector
from .twitter import TwitterCollector
from .facebook import FacebookCollector

__all__ = [
    "BaseCollector",
    "CollectorResult",
    "InstagramCollector",
    "TwitterCollector",
    "FacebookCollector",
]
