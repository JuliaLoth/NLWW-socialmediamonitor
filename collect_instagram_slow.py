#!/usr/bin/env python3
"""
Instagram data collectie met LANGZAME rate limiting.

Strategie gebaseerd op instaloader documentatie:
- Custom RateController met langere wachttijden
- 30-60 seconden tussen requests
- Batches van 3 accounts met 5 minuten pauze ertussen

Bronnen:
- https://instaloader.github.io/module/instaloadercontext.html
- https://github.com/instaloader/instaloader/issues/1922
"""
import asyncio
import sys
import os
import time
import random
from pathlib import Path
from datetime import datetime

# Fix Windows encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

import instaloader
from instaloader import RateController

from src.database.connection import get_connection
from src.database.queries import AccountQueries, PostQueries
from src.database.models import Post
import uuid


class SlowRateController(RateController):
    """
    Custom RateController met veel langere wachttijden.
    Dit voorkomt 429/401 rate limit errors.
    """

    def __init__(self, context):
        super().__init__(context)
        self._last_request = 0
        self._min_wait = 20  # Minimaal 20 seconden tussen requests
        self._max_wait = 40  # Maximaal 40 seconden

    def sleep(self, secs: float):
        """Sleep met extra random delay."""
        # Voeg random jitter toe
        jitter = random.uniform(0, 5)
        actual_sleep = secs + jitter
        print(f"      [Rate limit: wachten {actual_sleep:.1f}s]", end="\r")
        time.sleep(actual_sleep)

    def wait_before_query(self, query_type: str) -> None:
        """Wacht langer voor elke query."""
        # Bereken tijd sinds laatste request
        now = time.time()
        elapsed = now - self._last_request

        # Bepaal minimum wachttijd
        min_wait = random.uniform(self._min_wait, self._max_wait)

        if elapsed < min_wait:
            wait_time = min_wait - elapsed
            self.sleep(wait_time)

        # Update laatste request tijd
        self._last_request = time.time()

        # Roep ook de parent aan voor extra veiligheid
        super().wait_before_query(query_type)

    def handle_429(self, query_type: str) -> None:
        """Bij 429 error: wacht veel langer."""
        print("      [429 Rate Limit! Wachten 5 minuten...]")
        self.sleep(300)  # 5 minuten wachten
        super().handle_429(query_type)


# Configuratie
SINCE = datetime(2025, 1, 1)
UNTIL = datetime(2026, 1, 14)
POSTS_PER_ACCOUNT = 100
ACCOUNTS_PER_BATCH = 2  # Maar 2 accounts per batch
BATCH_PAUSE = 180  # 3 minuten pauze tussen batches

SESSION_FILE = str(Path(__file__).parent / "data" / "session-artbyloth")
SESSION_USER = "artbyloth"


async def collect_account(loader, account, db):
    """Verzamel posts voor één account."""
    try:
        profile = instaloader.Profile.from_username(loader.context, account.handle)

        count = 0
        for post in profile.get_posts():
            post_date = post.date_utc

            if post_date > UNTIL:
                continue
            if post_date < SINCE:
                break
            if count >= POSTS_PER_ACCOUNT:
                break

            # Content type
            if post.is_video:
                content_type = "video"
            elif post.typename == "GraphSidecar":
                content_type = "carousel"
            else:
                content_type = "image"

            # Sla op
            db_post = Post(
                id=str(uuid.uuid4()),
                account_id=account.id,
                platform_post_id=post.shortcode,
                posted_at=post_date,
                content_type=content_type,
                likes=post.likes,
                comments=post.comments,
                shares=0,
                views=post.video_view_count if post.is_video else None,
                url=f"https://www.instagram.com/p/{post.shortcode}/",
                caption_snippet=post.caption[:200] if post.caption else None,
                hashtags=list(post.caption_hashtags) if post.caption_hashtags else None,
                collected_at=datetime.now(),
            )

            PostQueries.upsert(db_post, db)
            count += 1

            if count % 10 == 0:
                print(f"    {count} posts...", end="\r")

        return count

    except instaloader.exceptions.ProfileNotExistsException:
        print(f"    Account niet gevonden")
        return 0
    except instaloader.exceptions.ConnectionException as e:
        print(f"    Connectie fout: {e}")
        return -1
    except Exception as e:
        print(f"    FOUT: {e}")
        return -1


async def main():
    print("=" * 60)
    print("INSTAGRAM - LANGZAME COLLECTIE")
    print("=" * 60)
    print(f"Periode: {SINCE.strftime('%d-%m-%Y')} t/m {UNTIL.strftime('%d-%m-%Y')}")
    print(f"Max posts per account: {POSTS_PER_ACCOUNT}")
    print(f"Accounts per batch: {ACCOUNTS_PER_BATCH}")
    print(f"Pauze tussen batches: {BATCH_PAUSE}s")
    print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)

    db = get_connection()

    # Maak loader met custom rate controller
    loader = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
        max_connection_attempts=3,
        request_timeout=120,
        rate_controller=lambda ctx: SlowRateController(ctx)
    )

    # Laad sessie
    try:
        loader.load_session_from_file(SESSION_USER, SESSION_FILE)
        print(f"Sessie geladen: {SESSION_USER}")
    except Exception as e:
        print(f"Sessie fout: {e}")

    # Haal accounts op
    accounts = AccountQueries.get_all(db)
    ig_accounts = [a for a in accounts if a.platform == "instagram"]

    print(f"Gevonden: {len(ig_accounts)} Instagram accounts")
    print("-" * 60)

    total_posts = 0
    successful = 0
    failed = 0

    # Verdeel in batches
    batches = [ig_accounts[i:i+ACCOUNTS_PER_BATCH]
               for i in range(0, len(ig_accounts), ACCOUNTS_PER_BATCH)]

    for batch_num, batch in enumerate(batches, 1):
        print(f"\n=== BATCH {batch_num}/{len(batches)} ===")

        for account in batch:
            print(f"\n  @{account.handle} ({account.country})")

            count = await collect_account(loader, account, db)

            if count > 0:
                print(f"    {count} posts verzameld")
                total_posts += count
                successful += 1
            elif count == 0:
                failed += 1
            else:  # -1 = error
                failed += 1
                # Bij error, langere pauze
                print(f"    Extra pauze na error...")
                await asyncio.sleep(60)

        # Pauze tussen batches
        if batch_num < len(batches):
            print(f"\n  Batch pauze: {BATCH_PAUSE} seconden...")
            await asyncio.sleep(BATCH_PAUSE)

    # Eindresultaat
    print("\n" + "=" * 60)
    print("COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Succesvol: {successful}")
    print(f"Mislukt: {failed}")
    print(f"Totaal posts: {total_posts}")
    print(f"Einde: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
