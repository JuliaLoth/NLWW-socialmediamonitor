"""
Twitter/X data collector via Nitter instances scraping.
Geen API key nodig.
"""
import asyncio
import re
from datetime import datetime
from typing import Optional, AsyncGenerator
import logging
import uuid
import random

try:
    import httpx
    from bs4 import BeautifulSoup
except ImportError:
    httpx = None
    BeautifulSoup = None

from .base import BaseCollector, PlatformBlockedError
from ..database.models import Post, ContentType
from ..config.settings import settings

logger = logging.getLogger(__name__)


class TwitterCollector(BaseCollector):
    """
    Twitter/X collector via Nitter instances.
    Scraped publieke Nitter mirrors die geen API nodig hebben.
    """

    platform = "twitter"

    def __init__(self):
        super().__init__()

        if httpx is None or BeautifulSoup is None:
            raise ImportError(
                "httpx en beautifulsoup4 zijn niet geinstalleerd. "
                "Installeer met: pip install httpx beautifulsoup4"
            )

        self.nitter_instances = settings.nitter_instances.copy()
        self._current_instance_idx = 0

        # HTTP client met headers die lijken op browser
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

    def _get_nitter_url(self, path: str) -> str:
        """Get URL voor huidige Nitter instance."""
        instance = self.nitter_instances[self._current_instance_idx]
        return f"{instance}/{path}"

    def _rotate_instance(self):
        """Roteer naar volgende Nitter instance."""
        self._current_instance_idx = (self._current_instance_idx + 1) % len(self.nitter_instances)
        logger.debug(f"Roteer naar Nitter instance: {self.nitter_instances[self._current_instance_idx]}")

    async def _fetch_page(self, path: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """
        Fetch en parse een Nitter pagina.
        Probeert meerdere instances bij fouten.
        """
        for attempt in range(retries * len(self.nitter_instances)):
            url = self._get_nitter_url(path)

            try:
                await self.rate_limiter.acquire()
                response = await self.client.get(url)

                if response.status_code == 200:
                    return BeautifulSoup(response.text, "lxml")

                elif response.status_code == 429:
                    logger.warning(f"Nitter rate limit op {url}")
                    self._rotate_instance()
                    await asyncio.sleep(random.uniform(5, 15))

                elif response.status_code in (403, 503):
                    logger.warning(f"Nitter instance niet beschikbaar: {url}")
                    self._rotate_instance()

                else:
                    logger.warning(f"Nitter HTTP {response.status_code}: {url}")
                    self._rotate_instance()

            except httpx.TimeoutException:
                logger.warning(f"Nitter timeout: {url}")
                self._rotate_instance()

            except Exception as e:
                logger.error(f"Nitter fout: {e}")
                self._rotate_instance()

        raise PlatformBlockedError("Alle Nitter instances gefaald")

    async def collect_profile(self, handle: str) -> tuple[Optional[int], Optional[int]]:
        """
        Verzamel Twitter profiel informatie via Nitter.
        """
        try:
            soup = await self._fetch_page(handle)
            if not soup:
                return None, None

            # Parse profile stats
            followers = None
            following = None

            # Zoek naar stats in de profile header
            stats = soup.select(".profile-stat-num")

            # Nitter layout: tweets, following, followers
            if len(stats) >= 3:
                following = self._parse_stat(stats[1].text)
                followers = self._parse_stat(stats[2].text)

            # Alternatieve selector
            if followers is None:
                followers_elem = soup.select_one(".followers .profile-stat-num")
                if followers_elem:
                    followers = self._parse_stat(followers_elem.text)

            if following is None:
                following_elem = soup.select_one(".following .profile-stat-num")
                if following_elem:
                    following = self._parse_stat(following_elem.text)

            return followers, following

        except PlatformBlockedError:
            raise
        except Exception as e:
            logger.error(f"Fout bij ophalen Twitter profiel {handle}: {e}")
            return None, None

    def _parse_stat(self, text: str) -> int:
        """Parse een statistiek van Nitter pagina."""
        text = text.strip().replace(",", "").replace(".", "")

        # Handle K/M suffixes
        multiplier = 1
        if text.endswith("K"):
            multiplier = 1000
            text = text[:-1]
        elif text.endswith("M"):
            multiplier = 1000000
            text = text[:-1]

        try:
            return int(float(text) * multiplier)
        except ValueError:
            return 0

    async def collect_posts(
        self,
        handle: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100
    ) -> AsyncGenerator[Post, None]:
        """
        Verzamel tweets via Nitter scraping.
        """
        cursor = None
        count = 0

        while count < limit:
            # Build URL with cursor for pagination
            path = f"{handle}"
            if cursor:
                path += f"?cursor={cursor}"

            try:
                soup = await self._fetch_page(path)
                if not soup:
                    break

                # Find tweet items
                tweets = soup.select(".timeline-item")

                if not tweets:
                    break

                for tweet_elem in tweets:
                    if count >= limit:
                        break

                    # Parse tweet
                    post = self._parse_tweet(tweet_elem, handle)

                    if post is None:
                        continue

                    # Check date bounds
                    if until and post.posted_at > until:
                        continue

                    if since and post.posted_at < since:
                        # Tweets zijn chronologisch
                        return

                    yield post
                    count += 1

                # Find next page cursor
                show_more = soup.select_one(".show-more a")
                if show_more and "cursor=" in show_more.get("href", ""):
                    cursor = show_more["href"].split("cursor=")[-1]
                else:
                    break

            except PlatformBlockedError:
                raise
            except Exception as e:
                logger.error(f"Fout bij ophalen Twitter posts {handle}: {e}")
                break

        logger.info(f"Twitter: {count} tweets verzameld voor {handle}")

    def _parse_tweet(self, elem, handle: str) -> Optional[Post]:
        """Parse een tweet element naar Post object."""
        try:
            # Skip retweets en replies
            if elem.select_one(".retweet-header"):
                return None
            if elem.select_one(".replying-to"):
                return None

            # Tweet ID/link
            link_elem = elem.select_one(".tweet-link")
            if not link_elem:
                return None

            tweet_url = link_elem.get("href", "")
            tweet_id = tweet_url.split("/")[-1].split("#")[0]

            # Timestamp
            time_elem = elem.select_one(".tweet-date a")
            if time_elem:
                timestamp_str = time_elem.get("title", "")
                try:
                    # Format: "Jan 13, 2026 · 10:30 AM UTC"
                    posted_at = datetime.strptime(
                        timestamp_str.split(" · ")[0],
                        "%b %d, %Y"
                    )
                except ValueError:
                    posted_at = datetime.now()
            else:
                posted_at = datetime.now()

            # Content
            content_elem = elem.select_one(".tweet-content")
            caption = content_elem.text.strip()[:200] if content_elem else None

            # Stats
            likes = self._parse_tweet_stat(elem, ".icon-heart")
            retweets = self._parse_tweet_stat(elem, ".icon-retweet")
            replies = self._parse_tweet_stat(elem, ".icon-comment")
            quotes = self._parse_tweet_stat(elem, ".icon-quote")

            # Content type
            content_type = ContentType.TEXT.value
            if elem.select_one(".attachment.video-container"):
                content_type = ContentType.VIDEO.value
            elif elem.select_one(".attachment.image"):
                content_type = ContentType.IMAGE.value

            # Hashtags
            hashtags = re.findall(r"#(\w+)", caption or "")

            return Post(
                id=str(uuid.uuid4()),
                account_id="",  # Set by caller
                platform_post_id=tweet_id,
                posted_at=posted_at,
                content_type=content_type,
                likes=likes,
                comments=replies,
                shares=retweets + quotes,
                views=None,  # Niet beschikbaar via Nitter
                url=f"https://twitter.com/{handle}/status/{tweet_id}",
                caption_snippet=caption,
                hashtags=hashtags if hashtags else None,
                collected_at=datetime.now(),
            )

        except Exception as e:
            logger.debug(f"Kon tweet niet parsen: {e}")
            return None

    def _parse_tweet_stat(self, elem, icon_class: str) -> int:
        """Parse een tweet statistiek."""
        stat_elem = elem.select_one(f"{icon_class}")
        if stat_elem:
            parent = stat_elem.parent
            if parent:
                text = parent.text.strip()
                return self._parse_stat(text)
        return 0

    async def close(self):
        """Sluit HTTP client."""
        await self.client.aclose()
