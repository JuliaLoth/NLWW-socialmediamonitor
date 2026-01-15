#!/usr/bin/env python3
"""
Verzamel comments van Instagram posts voor sentiment analyse en response tracking.
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import time
import random

os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

import instaloader
from src.database.connection import get_connection
from src.database.models import generate_uuid

# Configuratie
MAX_COMMENTS_PER_POST = 50
DELAY_BETWEEN_POSTS = 3


def main():
    print("=" * 60)
    print("INSTAGRAM COMMENTS VERZAMELEN")
    print("=" * 60)

    db = get_connection()

    # Maak comments tabel als die niet bestaat
    db.execute('''
        CREATE TABLE IF NOT EXISTS post_comments (
            id VARCHAR PRIMARY KEY,
            post_id VARCHAR NOT NULL,
            username VARCHAR,
            text VARCHAR,
            likes INTEGER DEFAULT 0,
            is_account_reply BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (post_id) REFERENCES posts(id)
        )
    ''')

    # Haal posts op die nog geen comments hebben
    posts = db.fetchall("""
        SELECT p.id, p.platform_post_id, a.handle, a.country, p.comments
        FROM posts p
        JOIN accounts a ON p.account_id = a.id
        LEFT JOIN post_comments pc ON p.id = pc.post_id
        WHERE a.platform = 'instagram'
          AND a.status = 'active'
          AND p.comments > 0
          AND pc.id IS NULL
        GROUP BY p.id, p.platform_post_id, a.handle, a.country, p.comments
        ORDER BY a.country, p.comments DESC
        LIMIT 100
    """)

    print(f"Posts om comments te verzamelen: {len(posts)}")
    print("-" * 60)

    if not posts:
        print("Geen posts gevonden die comments nodig hebben.")
        return

    # Instaloader setup met sessie
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_comments=True,
        save_metadata=False,
        post_metadata_txt_pattern='',
        request_timeout=60,
    )

    # Gebruik opgeslagen sessie
    SESSION_ID = os.environ.get('INSTAGRAM_SESSION_ID', '42180448680:NsIbWhuYaQPwbz:13:AYizp59p_gShYprKM2Xf5CMqM6OAQMm8k4qOmSwtnA')
    L.context._session.cookies.set('sessionid', SESSION_ID, domain='instagram.com')
    print(f"Instagram sessie geladen")

    total_comments = 0

    for i, (post_id, shortcode, handle, country, comment_count) in enumerate(posts, 1):
        print(f"\n[{i}/{len(posts)}] {country} @{handle} - post {shortcode} ({comment_count} comments)")

        try:
            post = instaloader.Post.from_shortcode(L.context, shortcode)

            comment_count = 0
            for comment in post.get_comments():
                if comment_count >= MAX_COMMENTS_PER_POST:
                    break

                # Check of dit een reply is van het account zelf
                is_account_reply = comment.owner.username.lower() == handle.lower()

                comment_data = {
                    'id': generate_uuid(),
                    'post_id': post_id,
                    'username': comment.owner.username,
                    'text': comment.text[:500] if comment.text else None,
                    'likes': comment.likes_count or 0,
                    'is_account_reply': is_account_reply,
                    'created_at': comment.created_at_utc,
                    'collected_at': datetime.now()
                }

                db.execute('''
                    INSERT INTO post_comments (id, post_id, username, text, likes, is_account_reply, created_at, collected_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', [comment_data['id'], comment_data['post_id'], comment_data['username'],
                      comment_data['text'], comment_data['likes'], comment_data['is_account_reply'],
                      comment_data['created_at'], comment_data['collected_at']])

                comment_count += 1
                total_comments += 1

            print(f"  {comment_count} comments verzameld")

            # Delay tussen posts
            if i < len(posts):
                delay = DELAY_BETWEEN_POSTS + random.uniform(0, 2)
                time.sleep(delay)

        except instaloader.exceptions.TooManyRequestsException:
            print("  RATE LIMITED - stoppen")
            break
        except Exception as e:
            print(f"  FOUT: {e}")
            continue

    print("\n" + "=" * 60)
    print("VERZAMELING VOLTOOID")
    print("=" * 60)
    print(f"Totaal comments verzameld: {total_comments}")


if __name__ == "__main__":
    main()
