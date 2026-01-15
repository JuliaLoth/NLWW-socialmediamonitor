"""
Instagram data collector met instaloader.
"""
import asyncio
from datetime import datetime
from typing import Optional, AsyncGenerator
import logging
import uuid

try:
    import instaloader
except ImportError:
    instaloader = None

from .base import BaseCollector, PlatformBlockedError
from ..database.models import Post, PostComment, ContentType

logger = logging.getLogger(__name__)


class InstagramCollector(BaseCollector):
    """
    Instagram collector via instaloader library.
    Ondersteunt zowel anonieme als ingelogde sessies.
    Ingelogde sessies hebben hogere rate limits.
    """

    platform = "instagram"

    def __init__(self, session_file: Optional[str] = None, username: Optional[str] = None):
        """
        Initialize Instagram collector.

        Args:
            session_file: Pad naar bestaand sessie bestand (optioneel)
            username: Instagram username voor sessie (optioneel)
        """
        super().__init__()

        if instaloader is None:
            raise ImportError(
                "instaloader is niet geinstalleerd. "
                "Installeer met: pip install instaloader"
            )

        # Initialize instaloader met langzamere rate limiting
        self.loader = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            quiet=True,
            max_connection_attempts=3,
            request_timeout=60,
        )

        # Probeer sessie te laden voor hogere rate limits
        self._logged_in = False
        if session_file and username:
            try:
                self.loader.load_session_from_file(username, session_file)
                self._logged_in = True
                logger.info(f"Instagram sessie geladen voor {username}")
            except Exception as e:
                logger.warning(f"Kon sessie niet laden: {e}")
        elif username:
            # Probeer standaard sessie bestand
            try:
                self.loader.load_session_from_file(username)
                self._logged_in = True
                logger.info(f"Instagram sessie geladen voor {username}")
            except Exception as e:
                logger.debug(f"Geen sessie gevonden voor {username}: {e}")

    async def collect_profile(self, handle: str) -> tuple[Optional[int], Optional[int]]:
        """
        Verzamel Instagram profiel informatie.
        """
        try:
            # Run in thread pool (instaloader is sync)
            profile = await asyncio.to_thread(
                instaloader.Profile.from_username,
                self.loader.context,
                handle
            )

            return profile.followers, profile.followees

        except instaloader.exceptions.ProfileNotExistsException:
            logger.warning(f"Instagram profiel niet gevonden: {handle}")
            return None, None

        except instaloader.exceptions.ConnectionException as e:
            if "429" in str(e) or "rate" in str(e).lower():
                raise PlatformBlockedError(f"Instagram rate limit: {e}")
            raise

        except Exception as e:
            logger.error(f"Fout bij ophalen Instagram profiel {handle}: {e}")
            return None, None

    async def collect_posts(
        self,
        handle: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100
    ) -> AsyncGenerator[Post, None]:
        """
        Verzamel Instagram posts.
        """
        try:
            # Get profile
            profile = await asyncio.to_thread(
                instaloader.Profile.from_username,
                self.loader.context,
                handle
            )

            count = 0

            # Iterate over posts
            for post in profile.get_posts():
                # Check date bounds
                post_date = post.date_utc

                if until and post_date > until:
                    continue

                if since and post_date < since:
                    # Posts zijn chronologisch, stop als we voorbij since zijn
                    break

                # Check limit
                if count >= limit:
                    break

                # Determine content type
                if post.is_video:
                    content_type = ContentType.VIDEO.value
                elif post.typename == "GraphSidecar":
                    content_type = ContentType.CAROUSEL.value
                else:
                    content_type = ContentType.IMAGE.value

                # Extract hashtags from caption
                hashtags = []
                if post.caption_hashtags:
                    hashtags = list(post.caption_hashtags)

                # Create Post object
                yield Post(
                    id=str(uuid.uuid4()),
                    account_id="",  # Will be set by caller
                    platform_post_id=post.shortcode,
                    posted_at=post_date,
                    content_type=content_type,
                    likes=post.likes,
                    comments=post.comments,
                    shares=0,  # Instagram doesn't expose shares
                    views=post.video_view_count if post.is_video else None,
                    url=f"https://www.instagram.com/p/{post.shortcode}/",
                    caption_snippet=post.caption[:200] if post.caption else None,
                    hashtags=hashtags,
                    collected_at=datetime.now(),
                )

                count += 1

                # Yield control periodically
                if count % 5 == 0:
                    await asyncio.sleep(0.1)

            logger.info(f"Instagram: {count} posts verzameld voor {handle}")

        except instaloader.exceptions.ProfileNotExistsException:
            logger.warning(f"Instagram profiel niet gevonden: {handle}")
            return

        except instaloader.exceptions.ConnectionException as e:
            if "429" in str(e) or "rate" in str(e).lower():
                raise PlatformBlockedError(f"Instagram rate limit: {e}")
            raise

        except Exception as e:
            logger.error(f"Fout bij ophalen Instagram posts {handle}: {e}")
            raise

    async def collect_comments(
        self,
        shortcode: str,
        account_handle: str,
        limit: int = 50
    ) -> AsyncGenerator[PostComment, None]:
        """
        Verzamel comments voor een specifieke post.

        Args:
            shortcode: Instagram post shortcode (bijv. 'ABC123')
            account_handle: Handle van het account (om te detecteren of comment van account is)
            limit: Max aantal comments om op te halen
        """
        try:
            # Get post by shortcode
            post = await asyncio.to_thread(
                instaloader.Post.from_shortcode,
                self.loader.context,
                shortcode
            )

            count = 0
            account_handle_lower = account_handle.lower()

            # Iterate over comments
            for comment in post.get_comments():
                if count >= limit:
                    break

                # Check if comment is from the account itself
                comment_owner = comment.owner.username.lower() if comment.owner else ""
                is_from_account = comment_owner == account_handle_lower

                yield PostComment(
                    id=str(uuid.uuid4()),
                    post_id="",  # Will be set by caller
                    comment_id=str(comment.id),
                    author_handle=comment.owner.username if comment.owner else None,
                    comment_text=comment.text,
                    is_from_account=is_from_account,
                    parent_comment_id=None,  # Instagram API doesn't easily expose this
                    posted_at=comment.created_at_utc,
                    likes=comment.likes_count if hasattr(comment, 'likes_count') else 0,
                    collected_at=datetime.now(),
                )

                count += 1

                # Yield control periodically
                if count % 10 == 0:
                    await asyncio.sleep(0.1)

            logger.info(f"Instagram: {count} comments verzameld voor post {shortcode}")

        except instaloader.exceptions.ConnectionException as e:
            if "429" in str(e) or "rate" in str(e).lower():
                raise PlatformBlockedError(f"Instagram rate limit: {e}")
            logger.warning(f"Kon comments niet ophalen voor {shortcode}: {e}")
            return

        except Exception as e:
            logger.error(f"Fout bij ophalen Instagram comments {shortcode}: {e}")
            return
