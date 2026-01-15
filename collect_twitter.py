#!/usr/bin/env python3
"""
Twitter/X data collectie voor alle accounts via Nitter scraping.
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
from src.collectors.twitter import TwitterCollector


async def collect_twitter_posts(account, collector, db, limit=30):
    """Verzamel posts voor een Twitter account."""
    print(f"  Verzamelen @{account.handle}...")

    count = 0
    try:
        async for post in collector.collect_posts(account.handle, limit=limit):
            post.account_id = account.id
            PostQueries.upsert(post, db)
            count += 1

            if count % 10 == 0:
                print(f"    {count} tweets...", end="\r")

        print(f"    {count} tweets verzameld")
        return count

    except Exception as e:
        print(f"    FOUT: {e}")
        return 0


async def main():
    print("=" * 60)
    print("TWITTER/X DATA COLLECTIE")
    print("=" * 60)

    db = get_connection()
    accounts = AccountQueries.get_all(db)

    # Filter alleen Twitter accounts
    tw_accounts = [a for a in accounts if a.platform == "twitter"]

    print(f"\nGevonden: {len(tw_accounts)} Twitter accounts")
    print("-" * 60)

    try:
        collector = TwitterCollector()
    except ImportError as e:
        print(f"FOUT: {e}")
        print("Installeer met: pip install httpx beautifulsoup4 lxml")
        return

    total_posts = 0
    successful = 0
    failed = 0

    for i, account in enumerate(tw_accounts, 1):
        print(f"\n[{i}/{len(tw_accounts)}] {account.country.upper()} - @{account.handle}")

        try:
            count = await collect_twitter_posts(account, collector, db, limit=30)
            total_posts += count
            if count > 0:
                successful += 1
            else:
                failed += 1

            # Rate limiting
            if i < len(tw_accounts):
                print("    Wachten 3 seconden...")
                await asyncio.sleep(3)

        except Exception as e:
            print(f"    KRITIEKE FOUT: {e}")
            failed += 1
            continue

    await collector.close()

    print("\n" + "=" * 60)
    print("TWITTER COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Accounts succesvol: {successful}")
    print(f"Accounts mislukt: {failed}")
    print(f"Totaal tweets verzameld: {total_posts}")


if __name__ == "__main__":
    asyncio.run(main())
