#!/usr/bin/env python3
"""
Verzamel 6 maanden posts voor alle actieve Instagram accounts.
Skipt posts die al in de database zitten.
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time

os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

import instaloader
from src.database.connection import get_connection
from src.database.models import Post, generate_uuid

# 6 maanden geleden
CUTOFF_DATE = datetime.now() - timedelta(days=180)


def main():
    print("=" * 60)
    print("INSTAGRAM COLLECTIE - 6 MAANDEN")
    print(f"Posts vanaf: {CUTOFF_DATE.strftime('%Y-%m-%d')}")
    print("=" * 60)

    db = get_connection()

    # Haal alle actieve Instagram accounts
    accounts = db.fetchall("""
        SELECT id, country, handle
        FROM accounts
        WHERE platform = 'instagram' AND status = 'active'
        ORDER BY country
    """)

    print(f"Accounts te verwerken: {len(accounts)}")
    print("-" * 60)

    # Instaloader setup
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_comments=False,
        save_metadata=False,
        post_metadata_txt_pattern='',
    )

    total_new = 0
    total_skipped = 0

    for i, (account_id, country, handle) in enumerate(accounts, 1):
        print(f"\n[{i}/{len(accounts)}] {country.upper()} - @{handle}")

        # Check hoeveel posts we al hebben
        existing_count = db.fetchone(
            "SELECT COUNT(*) FROM posts WHERE account_id = ?",
            [account_id]
        )[0]
        print(f"  Bestaande posts in DB: {existing_count}")

        try:
            profile = instaloader.Profile.from_username(L.context, handle)
            print(f"  Profiel: {profile.full_name} ({profile.followers} volgers)")

            new_posts = 0
            skipped = 0

            for post in profile.get_posts():
                # Stop als post ouder is dan 6 maanden
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
                total_new += 1

                if new_posts % 10 == 0:
                    print(f"    {new_posts} nieuwe posts verzameld...")

                # Rate limiting
                time.sleep(0.5)

            total_skipped += skipped
            print(f"  Resultaat: {new_posts} nieuw, {skipped} al in DB")

            # Pauze tussen accounts om rate limiting te voorkomen
            time.sleep(2)

        except Exception as e:
            print(f"  FOUT: {e}")
            continue

    print("\n" + "=" * 60)
    print("COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal nieuwe posts: {total_new}")
    print(f"Totaal geskipt (al in DB): {total_skipped}")


if __name__ == "__main__":
    main()
