"""
Base collector class voor social media data verzameling.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Optional, AsyncGenerator
import asyncio
import random
import logging

from ..config.settings import settings, RateLimitConfig
from ..database.models import Post, FollowerSnapshot, Account

logger = logging.getLogger(__name__)


@dataclass
class CollectorResult:
    """Resultaat van een collectie run."""
    success: bool
    posts_collected: int = 0
    followers: Optional[int] = None
    following: Optional[int] = None
    error: Optional[str] = None
    posts: list[Post] = field(default_factory=list)


class RateLimiter:
    """
    Token bucket rate limiter voor API/scraping requests.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._tokens = config.requests_per_minute
        self._last_update = datetime.now()
        self._daily_count = 0
        self._daily_reset = date.today()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """
        Wacht tot een token beschikbaar is.
        Raised exception als daily limit bereikt.
        """
        async with self._lock:
            # Reset daily counter indien nodig
            if date.today() != self._daily_reset:
                self._daily_count = 0
                self._daily_reset = date.today()

            # Check daily limit
            if self._daily_count >= self.config.daily_max:
                raise RateLimitExceededError(
                    f"Dagelijkse limiet bereikt ({self.config.daily_max})"
                )

            # Refill tokens based on elapsed time
            now = datetime.now()
            elapsed = (now - self._last_update).total_seconds()
            tokens_to_add = elapsed * (self.config.requests_per_minute / 60)
            self._tokens = min(self.config.requests_per_minute, self._tokens + tokens_to_add)
            self._last_update = now

            # Wait if no tokens available
            if self._tokens < 1:
                wait_time = (1 - self._tokens) * (60 / self.config.requests_per_minute)
                # Add jitter
                wait_time += random.uniform(0, self.config.min_delay_seconds)
                logger.debug(f"Rate limit: wacht {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
                self._tokens = 1

            # Consume token
            self._tokens -= 1
            self._daily_count += 1

            # Minimum delay between requests
            await asyncio.sleep(self.config.min_delay_seconds)


class RateLimitExceededError(Exception):
    """Daily rate limit exceeded."""
    pass


class PlatformBlockedError(Exception):
    """Platform has blocked our requests."""
    pass


class BaseCollector(ABC):
    """
    Abstract base class voor platform-specifieke collectors.
    """

    platform: str = "unknown"

    def __init__(self):
        config = settings.rate_limits.get(self.platform)
        if config:
            self.rate_limiter = RateLimiter(config)
        else:
            # Default rate limiter
            self.rate_limiter = RateLimiter(RateLimitConfig(
                requests_per_minute=5,
                daily_max=100,
                min_delay_seconds=5.0
            ))

    @abstractmethod
    async def collect_profile(self, handle: str) -> tuple[Optional[int], Optional[int]]:
        """
        Verzamel profiel informatie.
        Returns: (followers, following)
        """
        pass

    @abstractmethod
    async def collect_posts(
        self,
        handle: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100
    ) -> AsyncGenerator[Post, None]:
        """
        Verzamel posts van een account.
        Yields Post objecten.
        """
        pass

    async def collect(
        self,
        account: Account,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100
    ) -> CollectorResult:
        """
        Verzamel alle data voor een account.
        """
        try:
            # Rate limit check
            await self.rate_limiter.acquire()

            # Collect profile
            followers, following = await self.collect_profile(account.handle)

            # Collect posts
            posts = []
            async for post in self.collect_posts(account.handle, since, until, limit):
                post.account_id = account.id
                posts.append(post)

                # Rate limit between posts
                if len(posts) % 10 == 0:
                    await self.rate_limiter.acquire()

            return CollectorResult(
                success=True,
                posts_collected=len(posts),
                followers=followers,
                following=following,
                posts=posts
            )

        except RateLimitExceededError as e:
            logger.warning(f"Rate limit: {e}")
            return CollectorResult(success=False, error=str(e))

        except PlatformBlockedError as e:
            logger.error(f"Geblokkeerd door platform: {e}")
            return CollectorResult(success=False, error=str(e))

        except Exception as e:
            logger.error(f"Collectie fout voor {account.handle}: {e}", exc_info=True)
            return CollectorResult(success=False, error=str(e))

    async def collect_historical(
        self,
        account: Account,
        months: int = 12
    ) -> CollectorResult:
        """
        Verzamel historische data voor de afgelopen X maanden.
        """
        until = datetime.now()
        since = until - timedelta(days=months * 30)

        logger.info(
            f"Historische collectie voor {account.handle}: "
            f"{since.date()} tot {until.date()}"
        )

        return await self.collect(
            account,
            since=since,
            until=until,
            limit=months * 50  # ~50 posts per maand max
        )

    def _parse_count(self, text: str) -> int:
        """
        Parse follower/like counts van tekst.
        Handles: '1.2K', '1.5M', '12,345', etc.
        """
        if not text:
            return 0

        text = text.strip().upper().replace(",", "").replace(".", "")

        multiplier = 1
        if text.endswith("K"):
            multiplier = 1000
            text = text[:-1]
        elif text.endswith("M"):
            multiplier = 1000000
            text = text[:-1]
        elif text.endswith("B"):
            multiplier = 1000000000
            text = text[:-1]

        try:
            # Handle decimal values like "1.2K"
            value = float(text.replace(",", "."))
            return int(value * multiplier)
        except ValueError:
            return 0
