"""
Facebook data collector via Playwright scraping.
Scraped publieke Facebook pagina's.
"""
import asyncio
import re
from datetime import datetime, timedelta
from typing import Optional, AsyncGenerator
import logging
import uuid

try:
    from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeout
except ImportError:
    async_playwright = None

from .base import BaseCollector, PlatformBlockedError
from ..database.models import Post, ContentType

logger = logging.getLogger(__name__)


class FacebookCollector(BaseCollector):
    """
    Facebook collector via Playwright browser automation.
    Scraped publieke Facebook pagina's zonder login.
    """

    platform = "facebook"

    def __init__(self):
        super().__init__()

        if async_playwright is None:
            raise ImportError(
                "playwright is niet geinstalleerd. "
                "Installeer met: pip install playwright && playwright install chromium"
            )

        self._playwright = None
        self._browser: Optional[Browser] = None

    async def _ensure_browser(self):
        """Start browser als nog niet gestart."""
        if self._browser is None:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ]
            )
            logger.debug("Playwright browser gestart")

    async def _get_page(self) -> Page:
        """Krijg een nieuwe browser page."""
        await self._ensure_browser()
        context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="nl-NL",
        )
        page = await context.new_page()
        return page

    async def collect_profile(self, handle: str) -> tuple[Optional[int], Optional[int]]:
        """
        Verzamel Facebook pagina informatie.
        """
        page = None
        try:
            await self.rate_limiter.acquire()

            page = await self._get_page()
            url = f"https://www.facebook.com/{handle}"

            await page.goto(url, wait_until="networkidle", timeout=30000)

            # Wacht even voor JS rendering
            await asyncio.sleep(2)

            # Sluit eventuele popups
            await self._close_popups(page)

            # Zoek naar follower count
            followers = await self._extract_followers(page)

            # Facebook pagina's hebben geen 'following'
            return followers, None

        except PlaywrightTimeout:
            logger.warning(f"Facebook timeout voor {handle}")
            return None, None

        except Exception as e:
            logger.error(f"Fout bij ophalen Facebook profiel {handle}: {e}")
            return None, None

        finally:
            if page:
                await page.context.close()

    async def _close_popups(self, page: Page):
        """Sluit Facebook login/cookie popups."""
        try:
            # Cookie consent
            cookie_btns = [
                '[data-cookiebanner="accept_button"]',
                'button[title="Alle cookies toestaan"]',
                'button[title="Allow all cookies"]',
            ]
            for selector in cookie_btns:
                btn = page.locator(selector)
                if await btn.count() > 0:
                    await btn.first.click()
                    await asyncio.sleep(0.5)
                    break

            # Login popup close
            close_btns = [
                '[aria-label="Sluiten"]',
                '[aria-label="Close"]',
                'div[role="dialog"] [aria-label="Close"]',
            ]
            for selector in close_btns:
                btn = page.locator(selector)
                if await btn.count() > 0:
                    await btn.first.click()
                    await asyncio.sleep(0.5)
                    break

        except Exception:
            pass  # Popups zijn optioneel

    async def _extract_followers(self, page: Page) -> Optional[int]:
        """Extract follower count van pagina."""
        try:
            # Verschillende selectors proberen
            selectors = [
                # Nederlandse tekst
                'a[href*="followers"] span',
                # Engelse tekst
                '[href*="/followers"] span',
                # Alternatieve locatie
                'span:has-text("volgers")',
                'span:has-text("followers")',
                'span:has-text("likes")',
            ]

            page_content = await page.content()

            # Regex patterns voor follower counts
            patterns = [
                r'([\d,.]+)\s*(?:volgers|followers)',
                r'([\d,.]+)\s*(?:vind-ik-leuks|likes)',
                r'(\d[\d,.]*[KMB]?)\s*(?:volgers|followers|likes)',
            ]

            for pattern in patterns:
                match = re.search(pattern, page_content, re.IGNORECASE)
                if match:
                    return self._parse_count(match.group(1))

            # Probeer selectors
            for selector in selectors:
                try:
                    elem = page.locator(selector).first
                    if await elem.count() > 0:
                        text = await elem.text_content()
                        count = self._parse_count(text)
                        if count > 0:
                            return count
                except Exception:
                    continue

            return None

        except Exception as e:
            logger.debug(f"Kon followers niet extracten: {e}")
            return None

    async def collect_posts(
        self,
        handle: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100
    ) -> AsyncGenerator[Post, None]:
        """
        Verzamel Facebook posts.
        """
        page = None
        try:
            await self.rate_limiter.acquire()

            page = await self._get_page()
            url = f"https://www.facebook.com/{handle}/posts"

            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            await self._close_popups(page)

            count = 0
            seen_ids = set()
            scroll_attempts = 0
            max_scroll_attempts = 50  # Verhoogd voor historische data

            while count < limit and scroll_attempts < max_scroll_attempts:
                # Find post elements
                posts = await page.query_selector_all('[data-pagelet*="FeedUnit"], [role="article"]')

                for post_elem in posts:
                    if count >= limit:
                        break

                    post = await self._parse_post(post_elem, handle)

                    if post is None:
                        continue

                    if post.platform_post_id in seen_ids:
                        continue

                    seen_ids.add(post.platform_post_id)

                    # Check date bounds
                    if until and post.posted_at > until:
                        continue

                    if since and post.posted_at < since:
                        # Stop als we te ver terug zijn
                        return

                    yield post
                    count += 1

                # Scroll voor meer posts
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                scroll_attempts += 1

            logger.info(f"Facebook: {count} posts verzameld voor {handle}")

        except PlaywrightTimeout:
            logger.warning(f"Facebook timeout voor {handle}")

        except Exception as e:
            logger.error(f"Fout bij ophalen Facebook posts {handle}: {e}")

        finally:
            if page:
                await page.context.close()

    async def _parse_post(self, elem, handle: str) -> Optional[Post]:
        """Parse een Facebook post element."""
        try:
            # Post ID uit link
            links = await elem.query_selector_all('a[href*="/posts/"], a[href*="story_fbid"]')
            post_id = None
            post_url = None

            for link in links:
                href = await link.get_attribute("href")
                if href and "/posts/" in href:
                    post_id = href.split("/posts/")[-1].split("?")[0].split("/")[0]
                    post_url = f"https://www.facebook.com/{handle}/posts/{post_id}"
                    break
                elif href and "story_fbid" in href:
                    match = re.search(r'story_fbid=(\d+)', href)
                    if match:
                        post_id = match.group(1)
                        post_url = href
                        break

            if not post_id:
                post_id = str(uuid.uuid4())[:12]  # Fallback ID

            # Timestamp - probeer te parsen uit relative time
            time_elem = await elem.query_selector('a[href*="/posts/"] span, [data-utime]')
            posted_at = datetime.now() - timedelta(days=1)  # Default: gisteren

            if time_elem:
                time_text = await time_elem.text_content()
                posted_at = self._parse_fb_time(time_text)

            # Content
            content_elem = await elem.query_selector('[data-ad-preview="message"], [data-ad-comet-preview="message"]')
            caption = None
            if content_elem:
                caption = await content_elem.text_content()
                caption = caption[:200] if caption else None

            # Engagement stats
            likes = await self._get_reaction_count(elem)
            comments = await self._get_comment_count(elem)
            shares = await self._get_share_count(elem)

            # Content type
            content_type = ContentType.TEXT.value
            if await elem.query_selector('video'):
                content_type = ContentType.VIDEO.value
            elif await elem.query_selector('img[src*="scontent"]'):
                content_type = ContentType.IMAGE.value

            # Hashtags
            hashtags = re.findall(r"#(\w+)", caption or "")

            return Post(
                id=str(uuid.uuid4()),
                account_id="",
                platform_post_id=post_id,
                posted_at=posted_at,
                content_type=content_type,
                likes=likes,
                comments=comments,
                shares=shares,
                views=None,
                url=post_url,
                caption_snippet=caption,
                hashtags=hashtags if hashtags else None,
                collected_at=datetime.now(),
            )

        except Exception as e:
            logger.debug(f"Kon Facebook post niet parsen: {e}")
            return None

    def _parse_fb_time(self, text: str) -> datetime:
        """Parse Facebook relative time naar datetime."""
        text = text.lower().strip()
        now = datetime.now()

        if "just now" in text or "zojuist" in text:
            return now

        # "X uur" / "X hours"
        match = re.search(r'(\d+)\s*(?:uur|hour|u)', text)
        if match:
            return now - timedelta(hours=int(match.group(1)))

        # "X min" / "X minuten"
        match = re.search(r'(\d+)\s*(?:min)', text)
        if match:
            return now - timedelta(minutes=int(match.group(1)))

        # "gisteren" / "yesterday"
        if "gisteren" in text or "yesterday" in text:
            return now - timedelta(days=1)

        # "X dagen" / "X days"
        match = re.search(r'(\d+)\s*(?:dag|day)', text)
        if match:
            return now - timedelta(days=int(match.group(1)))

        # "X weken" / "X weeks"
        match = re.search(r'(\d+)\s*(?:week|wek)', text)
        if match:
            return now - timedelta(weeks=int(match.group(1)))

        # Probeer datum format
        for fmt in ["%d %B", "%B %d", "%d %b", "%b %d"]:
            try:
                parsed = datetime.strptime(text, fmt)
                parsed = parsed.replace(year=now.year)
                if parsed > now:
                    parsed = parsed.replace(year=now.year - 1)
                return parsed
            except ValueError:
                continue

        return now - timedelta(days=1)

    async def _get_reaction_count(self, elem) -> int:
        """Get aantal reactions/likes."""
        try:
            selectors = [
                '[aria-label*="reaction"], [aria-label*="reactie"]',
                'span[data-hover*="reaction"]',
            ]
            for selector in selectors:
                count_elem = await elem.query_selector(selector)
                if count_elem:
                    text = await count_elem.get_attribute("aria-label") or await count_elem.text_content()
                    return self._parse_count(text)
        except Exception:
            pass
        return 0

    async def _get_comment_count(self, elem) -> int:
        """Get aantal comments."""
        try:
            comment_elem = await elem.query_selector('span:has-text("comment"), span:has-text("reactie")')
            if comment_elem:
                text = await comment_elem.text_content()
                match = re.search(r'(\d+)', text)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return 0

    async def _get_share_count(self, elem) -> int:
        """Get aantal shares."""
        try:
            share_elem = await elem.query_selector('span:has-text("share"), span:has-text("gedeeld")')
            if share_elem:
                text = await share_elem.text_content()
                match = re.search(r'(\d+)', text)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return 0

    async def close(self):
        """Sluit browser."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
