#!/usr/bin/env python3
"""
Verzamel Instagram posts met LANGZAME requests om rate limiting te voorkomen.
Gebruikt langere pauzes tussen requests (5-10 sec) en tussen accounts (60 sec).
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
import random

os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

import instaloader
from src.database.connection import get_connection
from src.database.models import Post, generate_uuid

# Configuratie
CUTOFF_DATE = datetime.now() - timedelta(days=180)  # 6 maanden
MAX_POSTS_PER_ACCOUNT = 100  # Max posts per account
DELAY_BETWEEN_POSTS = 5  # Seconden tussen posts
DELAY_BETWEEN_ACCOUNTS = 60  # Seconden tussen accounts
MAX_RETRIES = 3


def collect_with_retry(L, handle, account_id, db, max_posts=MAX_POSTS_PER_ACCOUNT):
    """Verzamel posts met retry logic."""

    for attempt in range(MAX_RETRIES):
        try:
            profile = instaloader.Profile.from_username(L.context, handle)
            print(f"  Profiel: {profile.full_name} ({profile.followers:,} volgers)")

            new_posts = 0
            skipped = 0

            for post in profile.get_posts():
                # Stop als we genoeg posts hebben
                if new_posts + skipped >= max_posts:
                    print(f"  Max posts ({max_posts}) bereikt")
                    break

                # Stop als post ouder is dan cutoff
                if post.date_utc < CUTOFF_DATE:
                    print(f"  Gestopt bij post van {post.date_utc.strftime('%Y-%m-%d')} (ouder dan 6 maanden)")
                    break

                # Check of post al bestaat
                exists = db.fetchone(
                    "SELECT 1 FROM posts WHERE account_id = ? AND platform_post_id = ?",
                    [account_id, post.shortcode]
                )

                if exists:
                    skipped += 1
                    continue

                # Nieuwe post toevoegen
                post_data = Post(
                    id=generate_uuid(),
                    account_id=account_id,
                    platform_post_id=post.shortcode,
                    posted_at=post.date_utc,
                    content_type='post',
                    likes=post.likes,
                    comments=post.comments,
                    shares=None,
                    views=post.video_view_count if post.is_video else None,
                    url=f'https://instagram.com/p/{post.shortcode}',
                    caption_snippet=post.caption[:500] if post.caption else None,
                    hashtags=list(post.caption_hashtags) if post.caption_hashtags else None,
                    collected_at=datetime.now(),
                    last_updated=datetime.now()
                )

                db.execute('''
                    INSERT INTO posts (id, account_id, platform_post_id, posted_at, content_type,
                        likes, comments, shares, views, url, caption_snippet, hashtags, collected_at, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', [post_data.id, post_data.account_id, post_data.platform_post_id, post_data.posted_at,
                      post_data.content_type, post_data.likes, post_data.comments, post_data.shares,
                      post_data.views, post_data.url, post_data.caption_snippet,
                      str(post_data.hashtags) if post_data.hashtags else None,
                      post_data.collected_at, post_data.last_updated])

                new_posts += 1

                if new_posts % 10 == 0:
                    print(f"    {new_posts} nieuwe posts verzameld...")

                # LANGZAME delay met random jitter
                delay = DELAY_BETWEEN_POSTS + random.uniform(0, 3)
                time.sleep(delay)

            return new_posts, skipped

        except instaloader.exceptions.TooManyRequestsException:
            wait_time = 60 * (attempt + 1)  # 60, 120, 180 sec
            print(f"  RATE LIMITED! Wachten {wait_time} seconden... (poging {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait_time)

        except instaloader.exceptions.ConnectionException as e:
            if "403" in str(e):
                wait_time = 120 * (attempt + 1)
                print(f"  403 Forbidden - wachten {wait_time} sec... (poging {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                raise

        except Exception as e:
            print(f"  FOUT: {e}")
            raise

    print(f"  Max retries bereikt, account overslaan")
    return 0, 0


def main():
    print("=" * 60)
    print("INSTAGRAM COLLECTIE - LANGZAAM (rate limit safe)")
    print(f"Posts vanaf: {CUTOFF_DATE.strftime('%Y-%m-%d')}")
    print(f"Max per account: {MAX_POSTS_PER_ACCOUNT}")
    print(f"Delay tussen posts: {DELAY_BETWEEN_POSTS}+ sec")
    print(f"Delay tussen accounts: {DELAY_BETWEEN_ACCOUNTS} sec")
    print("=" * 60)

    db = get_connection()

    # Haal accounts op die nog niet volledig verzameld zijn
    accounts = db.fetchall("""
        SELECT a.id, a.country, a.handle, COUNT(p.id) as existing_posts
        FROM accounts a
        LEFT JOIN posts p ON a.id = p.account_id
        WHERE a.platform = 'instagram' AND a.status = 'active'
        GROUP BY a.id, a.country, a.handle
        HAVING existing_posts < ?
        ORDER BY existing_posts ASC
    """, [MAX_POSTS_PER_ACCOUNT])

    print(f"Accounts met minder dan {MAX_POSTS_PER_ACCOUNT} posts: {len(accounts)}")
    print("-" * 60)

    if not accounts:
        print("Alle accounts hebben al voldoende posts!")
        return

    # Instaloader setup
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_comments=False,
        save_metadata=False,
        post_metadata_txt_pattern='',
        request_timeout=60,
    )

    total_new = 0
    total_skipped = 0

    for i, (account_id, country, handle, existing) in enumerate(accounts, 1):
        needed = MAX_POSTS_PER_ACCOUNT - existing
        print(f"\n[{i}/{len(accounts)}] {country.upper()} - @{handle}")
        print(f"  Bestaand: {existing}, nodig: {needed}")

        try:
            new_posts, skipped = collect_with_retry(L, handle, account_id, db, max_posts=needed + existing)

            total_new += new_posts
            total_skipped += skipped
            print(f"  Resultaat: {new_posts} nieuw, {skipped} al in DB")

        except Exception as e:
            print(f"  FOUT: {e}")
            continue

        # Lange pauze tussen accounts
        if i < len(accounts):
            wait = DELAY_BETWEEN_ACCOUNTS + random.uniform(0, 30)
            print(f"  Wachten {wait:.0f} sec voor volgende account...")
            time.sleep(wait)

    print("\n" + "=" * 60)
    print("COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal nieuwe posts: {total_new}")
    print(f"Totaal geskipt (al in DB): {total_skipped}")


if __name__ == "__main__":
    main()
