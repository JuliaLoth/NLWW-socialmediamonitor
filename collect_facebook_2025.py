#!/usr/bin/env python3
"""
Facebook historische data collectie voor 2025.
Verzamelt maximaal mogelijke posts per account via Playwright scraping.
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
from src.collectors.facebook import FacebookCollector

# Onderzoeksperiode
SINCE = datetime(2025, 1, 1)
UNTIL = datetime(2026, 1, 14)
POSTS_PER_ACCOUNT = 150  # Max posts per account


async def main():
    print("=" * 60)
    print("FACEBOOK - HISTORISCHE DATA 2025")
    print("=" * 60)
    print(f"Periode: {SINCE.strftime('%d-%m-%Y')} t/m {UNTIL.strftime('%d-%m-%Y')}")
    print(f"Max posts per account: {POSTS_PER_ACCOUNT}")
    print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)

    db = get_connection()

    # Huidige stand
    current_posts = db.fetchone("SELECT COUNT(*) FROM posts WHERE id IN (SELECT p.id FROM posts p JOIN accounts a ON p.account_id = a.id WHERE a.platform = 'facebook')")[0]
    print(f"\nHuidige Facebook posts: {current_posts}")

    accounts = AccountQueries.get_all(db)
    fb_accounts = [a for a in accounts if a.platform == "facebook"]

    print(f"Gevonden: {len(fb_accounts)} Facebook accounts\n")
    print("-" * 60)

    try:
        collector = FacebookCollector()
    except ImportError as e:
        print(f"FOUT: {e}")
        return

    total_posts = 0
    successful = 0
    failed = 0

    for i, account in enumerate(fb_accounts, 1):
        print(f"\n[{i}/{len(fb_accounts)}] {account.country.upper()} - @{account.handle}")

        try:
            count = 0
            async for post in collector.collect_posts(
                account.handle,
                since=SINCE,
                until=UNTIL,
                limit=POSTS_PER_ACCOUNT
            ):
                post.account_id = account.id
                PostQueries.upsert(post, db)
                count += 1

                if count % 10 == 0:
                    print(f"    {count} posts...", end="\r")

            print(f"    {count} posts verzameld          ")
            total_posts += count

            if count > 0:
                successful += 1
            else:
                failed += 1

            # Rate limiting
            if i < len(fb_accounts):
                wait_time = 8
                print(f"    Wachten {wait_time} seconden...")
                await asyncio.sleep(wait_time)

        except Exception as e:
            print(f"    FOUT: {e}")
            failed += 1
            await asyncio.sleep(15)
            continue

    await collector.close()

    # Eindstatistieken
    new_fb_posts = db.fetchone("SELECT COUNT(*) FROM posts WHERE id IN (SELECT p.id FROM posts p JOIN accounts a ON p.account_id = a.id WHERE a.platform = 'facebook')")[0]

    print("\n" + "=" * 60)
    print("FACEBOOK COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Accounts succesvol: {successful}")
    print(f"Accounts mislukt: {failed}")
    print(f"Posts verzameld deze run: {total_posts}")
    print(f"Totaal Facebook posts: {new_fb_posts}")
    print(f"\nEinde: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
