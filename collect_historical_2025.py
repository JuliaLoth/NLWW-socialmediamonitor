#!/usr/bin/env python3
"""
Historische data collectie voor heel 2025 (jan 2025 - jan 2026).
Verzamelt Instagram en Facebook posts voor alle accounts.
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
from src.collectors.instagram import InstagramCollector
from src.collectors.facebook import FacebookCollector

# Onderzoeksperiode
SINCE = datetime(2025, 1, 1)
UNTIL = datetime(2026, 1, 14)
POSTS_PER_ACCOUNT = 200  # Max posts per account


async def collect_instagram_historical(db):
    """Verzamel historische Instagram data."""
    print("\n" + "=" * 60)
    print("INSTAGRAM - HISTORISCHE DATA 2025")
    print("=" * 60)
    print(f"Periode: {SINCE.strftime('%d-%m-%Y')} t/m {UNTIL.strftime('%d-%m-%Y')}")
    print(f"Max posts per account: {POSTS_PER_ACCOUNT}")
    print("-" * 60)

    accounts = AccountQueries.get_all(db)
    ig_accounts = [a for a in accounts if a.platform == "instagram"]

    print(f"Gevonden: {len(ig_accounts)} Instagram accounts\n")

    try:
        collector = InstagramCollector()
    except ImportError as e:
        print(f"FOUT: {e}")
        return 0, 0

    total_posts = 0
    successful = 0
    failed = 0

    for i, account in enumerate(ig_accounts, 1):
        print(f"[{i}/{len(ig_accounts)}] {account.country.upper()} - @{account.handle}")

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

                if count % 20 == 0:
                    print(f"    {count} posts...", end="\r")

            print(f"    {count} posts verzameld")
            total_posts += count

            if count > 0:
                successful += 1
            else:
                failed += 1

            # Rate limiting - langere pauze voor Instagram
            if i < len(ig_accounts):
                wait_time = 10
                print(f"    Wachten {wait_time} seconden (rate limit)...")
                await asyncio.sleep(wait_time)

        except Exception as e:
            print(f"    FOUT: {e}")
            failed += 1
            await asyncio.sleep(30)  # Langere pauze na error
            continue

    return successful, failed, total_posts


async def collect_facebook_historical(db):
    """Verzamel historische Facebook data."""
    print("\n" + "=" * 60)
    print("FACEBOOK - HISTORISCHE DATA 2025")
    print("=" * 60)
    print(f"Periode: {SINCE.strftime('%d-%m-%Y')} t/m {UNTIL.strftime('%d-%m-%Y')}")
    print(f"Max posts per account: {POSTS_PER_ACCOUNT}")
    print("-" * 60)

    accounts = AccountQueries.get_all(db)
    fb_accounts = [a for a in accounts if a.platform == "facebook"]

    print(f"Gevonden: {len(fb_accounts)} Facebook accounts\n")

    try:
        collector = FacebookCollector()
    except ImportError as e:
        print(f"FOUT: {e}")
        return 0, 0, 0

    total_posts = 0
    successful = 0
    failed = 0

    for i, account in enumerate(fb_accounts, 1):
        print(f"[{i}/{len(fb_accounts)}] {account.country.upper()} - @{account.handle}")

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

            print(f"    {count} posts verzameld")
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
    return successful, failed, total_posts


async def main():
    print("=" * 60)
    print("HISTORISCHE DATA COLLECTIE 2025")
    print("=" * 60)
    print(f"Onderzoeksperiode: {SINCE.strftime('%d-%m-%Y')} t/m {UNTIL.strftime('%d-%m-%Y')}")
    print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)

    db = get_connection()

    # Check huidige stand
    current_posts = db.fetchone("SELECT COUNT(*) FROM posts")[0]
    print(f"\nHuidige posts in database: {current_posts}")

    # Instagram collectie
    ig_success, ig_failed, ig_posts = await collect_instagram_historical(db)

    # Facebook collectie
    fb_success, fb_failed, fb_posts = await collect_facebook_historical(db)

    # Eindstatistieken
    new_total = db.fetchone("SELECT COUNT(*) FROM posts")[0]

    print("\n" + "=" * 60)
    print("COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"\nInstagram:")
    print(f"  Accounts succesvol: {ig_success}")
    print(f"  Accounts mislukt: {ig_failed}")
    print(f"  Posts verzameld: {ig_posts}")

    print(f"\nFacebook:")
    print(f"  Accounts succesvol: {fb_success}")
    print(f"  Accounts mislukt: {fb_failed}")
    print(f"  Posts verzameld: {fb_posts}")

    print(f"\nTotaal:")
    print(f"  Posts voor collectie: {current_posts}")
    print(f"  Posts na collectie: {new_total}")
    print(f"  Nieuwe posts: {new_total - current_posts}")

    print(f"\nEinde: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
