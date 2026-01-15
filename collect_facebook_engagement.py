#!/usr/bin/env python3
"""
Facebook engagement data collectie met facebook-scraper library.
Haalt likes, comments en reactions op voor bestaande Facebook posts.

Bron: https://github.com/kevinzg/facebook-scraper
"""
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

from facebook_scraper import get_posts, set_cookies, set_user_agent
from src.database.connection import get_connection
from src.database.queries import AccountQueries, PostQueries
from src.database.models import Post
import uuid

# Configuratie
POSTS_PER_ACCOUNT = 50
SINCE = datetime(2025, 1, 1)

# Set English locale voor betere scraping
set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def main():
    print("=" * 60)
    print("FACEBOOK ENGAGEMENT - facebook-scraper")
    print("=" * 60)
    print()

    db = get_connection()

    # Haal Facebook accounts op
    accounts = AccountQueries.get_all(db)
    fb_accounts = [a for a in accounts if a.platform == "facebook"]

    print(f"Gevonden: {len(fb_accounts)} Facebook accounts")
    print("-" * 60)

    print("Cookies worden per request geladen vanuit browser")

    total_updated = 0

    for i, account in enumerate(fb_accounts, 1):
        print(f"\n[{i}/{len(fb_accounts)}] @{account.handle} ({account.country})")

        try:
            print(f"  Scraping {account.handle}...")

            posts_data = get_posts(
                account.handle,
                pages=3,
                options={
                    "posts_per_page": 20,
                    "allow_extra_requests": True,
                },
                cookies=str(Path(__file__).parent / "facebook_cookies_netscape.txt"),
            )

            count = 0
            for post_data in posts_data:
                # Debug eerste post
                if count == 0:
                    print(f"  Post gevonden: likes={post_data.get('likes')}, comments={post_data.get('comments')}")
                if count >= POSTS_PER_ACCOUNT:
                    break

                post_time = post_data.get("time")
                if post_time and post_time < SINCE:
                    continue

                # Extract engagement
                likes = post_data.get("likes", 0) or 0
                comments = post_data.get("comments", 0) or 0
                shares = post_data.get("shares", 0) or 0

                # Reactions breakdown indien beschikbaar
                reactions = post_data.get("reactions", {})
                if reactions and isinstance(reactions, dict):
                    likes = sum(reactions.values())

                post_id = post_data.get("post_id") or str(uuid.uuid4())
                post_url = post_data.get("post_url", "")
                text = post_data.get("text", "") or ""

                # Content type detectie
                if post_data.get("video"):
                    content_type = "video"
                elif post_data.get("images") and len(post_data.get("images", [])) > 1:
                    content_type = "carousel"
                elif post_data.get("image"):
                    content_type = "image"
                else:
                    content_type = "text"

                # Sla op in database
                db_post = Post(
                    id=str(uuid.uuid4()),
                    account_id=account.id,
                    platform_post_id=post_id,
                    posted_at=post_time or datetime.now(),
                    content_type=content_type,
                    likes=likes,
                    comments=comments,
                    shares=shares,
                    views=None,
                    url=post_url,
                    caption_snippet=text[:200] if text else None,
                    hashtags=None,
                    collected_at=datetime.now(),
                )

                PostQueries.upsert(db_post, db)
                count += 1

                if count % 10 == 0:
                    print(f"  {count} posts...", end="\r")

            print(f"  {count} posts verzameld (met engagement data)")
            total_updated += count

        except Exception as e:
            print(f"  FOUT: {e}")
            continue

    print("\n" + "=" * 60)
    print("COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal posts met engagement: {total_updated}")


if __name__ == "__main__":
    main()
