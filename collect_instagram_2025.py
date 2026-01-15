#!/usr/bin/env python3
"""
Instagram historische data collectie voor 2025 met sessie login.
"""
import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime

# Fix Windows encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import get_connection
from src.database.queries import AccountQueries, PostQueries

# Onderzoeksperiode
SINCE = datetime(2025, 1, 1)
UNTIL = datetime(2026, 1, 14)
POSTS_PER_ACCOUNT = 150

# Sessie configuratie
SESSION_FILE = str(Path(__file__).parent / "data" / "session-artbyloth")
SESSION_USER = "artbyloth"


async def main():
    print("=" * 60)
    print("INSTAGRAM - HISTORISCHE DATA 2025")
    print("=" * 60)
    print(f"Periode: {SINCE.strftime('%d-%m-%Y')} t/m {UNTIL.strftime('%d-%m-%Y')}")
    print(f"Max posts per account: {POSTS_PER_ACCOUNT}")
    print(f"Sessie: {SESSION_USER}")
    print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)

    # Import hier om sessie parameters door te geven
    import instaloader
    from src.database.models import Post, ContentType
    import uuid

    db = get_connection()

    # Huidige stand
    current_ig = db.fetchone("""
        SELECT COUNT(*) FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.platform = 'instagram'
    """)[0]
    print(f"\nHuidige Instagram posts: {current_ig}")

    accounts = AccountQueries.get_all(db)
    ig_accounts = [a for a in accounts if a.platform == "instagram"]

    print(f"Gevonden: {len(ig_accounts)} Instagram accounts\n")
    print("-" * 60)

    # Initialize instaloader met sessie
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
        request_timeout=60,
    )

    # Laad sessie
    try:
        loader.load_session_from_file(SESSION_USER, SESSION_FILE)
        test_user = loader.test_login()
        if test_user:
            print(f"Sessie geladen: ingelogd als {test_user}")
        else:
            print("WAARSCHUWING: Sessie geladen maar login test mislukt")
    except Exception as e:
        print(f"FOUT bij laden sessie: {e}")
        print("Doorgaan zonder sessie (lagere rate limits)")

    total_posts = 0
    successful = 0
    failed = 0

    for i, account in enumerate(ig_accounts, 1):
        print(f"\n[{i}/{len(ig_accounts)}] {account.country.upper()} - @{account.handle}")

        try:
            # Get profile
            profile = instaloader.Profile.from_username(loader.context, account.handle)

            count = 0
            for post in profile.get_posts():
                # Check date bounds
                post_date = post.date_utc

                if post_date > UNTIL:
                    continue

                if post_date < SINCE:
                    break  # Posts zijn chronologisch

                if count >= POSTS_PER_ACCOUNT:
                    break

                # Determine content type
                if post.is_video:
                    content_type = "video"
                elif post.typename == "GraphSidecar":
                    content_type = "carousel"
                else:
                    content_type = "image"

                # Create post
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

                if count % 20 == 0:
                    print(f"    {count} posts...", end="\r")

            print(f"    {count} posts verzameld          ")
            total_posts += count

            if count > 0:
                successful += 1
            else:
                failed += 1

            # Rate limiting - met sessie kunnen we iets sneller
            if i < len(ig_accounts):
                wait_time = 8
                print(f"    Wachten {wait_time} seconden...")
                await asyncio.sleep(wait_time)

        except instaloader.exceptions.ProfileNotExistsException:
            print(f"    Account niet gevonden")
            failed += 1
        except instaloader.exceptions.ConnectionException as e:
            if "429" in str(e) or "rate" in str(e).lower():
                print(f"    Rate limit! Wachten 60 seconden...")
                await asyncio.sleep(60)
            else:
                print(f"    Connectie fout: {e}")
            failed += 1
        except Exception as e:
            print(f"    FOUT: {e}")
            failed += 1
            await asyncio.sleep(15)
            continue

    # Eindstatistieken
    new_ig = db.fetchone("""
        SELECT COUNT(*) FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.platform = 'instagram'
    """)[0]

    print("\n" + "=" * 60)
    print("INSTAGRAM COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Accounts succesvol: {successful}")
    print(f"Accounts mislukt: {failed}")
    print(f"Posts verzameld deze run: {total_posts}")
    print(f"Totaal Instagram posts: {new_ig}")
    print(f"\nEinde: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
