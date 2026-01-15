#!/usr/bin/env python3
"""
Facebook data collectie voor alle accounts via Playwright scraping.
"""
import asyncio
import sys
import os
from pathlib import Path

# Fix Windows encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import get_connection
from src.database.queries import AccountQueries, PostQueries
from src.collectors.facebook import FacebookCollector


async def collect_facebook_posts(account, collector, db, limit=30):
    """Verzamel posts voor een Facebook account."""
    print(f"  Verzamelen @{account.handle}...")

    count = 0
    try:
        async for post in collector.collect_posts(account.handle, limit=limit):
            post.account_id = account.id
            PostQueries.upsert(post, db)
            count += 1

            if count % 5 == 0:
                print(f"    {count} posts...", end="\r")

        print(f"    {count} posts verzameld")
        return count

    except Exception as e:
        print(f"    FOUT: {e}")
        return 0


async def main():
    print("=" * 60)
    print("FACEBOOK DATA COLLECTIE")
    print("=" * 60)

    db = get_connection()
    accounts = AccountQueries.get_all(db)

    # Filter alleen Facebook accounts
    fb_accounts = [a for a in accounts if a.platform == "facebook"]

    print(f"\nGevonden: {len(fb_accounts)} Facebook accounts")
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
            count = await collect_facebook_posts(account, collector, db, limit=20)
            total_posts += count
            if count > 0:
                successful += 1
            else:
                failed += 1

            # Rate limiting
            if i < len(fb_accounts):
                print("    Wachten 5 seconden...")
                await asyncio.sleep(5)

        except Exception as e:
            print(f"    KRITIEKE FOUT: {e}")
            failed += 1
            continue

    await collector.close()

    print("\n" + "=" * 60)
    print("FACEBOOK COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Accounts succesvol: {successful}")
    print(f"Accounts mislukt: {failed}")
    print(f"Totaal posts verzameld: {total_posts}")


if __name__ == "__main__":
    asyncio.run(main())
