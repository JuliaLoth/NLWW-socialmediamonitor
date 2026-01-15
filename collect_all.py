#!/usr/bin/env python3
"""
Batch data collectie voor alle Instagram accounts.
Verzamelt posts en berekent communicatie profielen.
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
from src.collectors.instagram import InstagramCollector
from src.analysis.communication import (
    classify_posts_batch, calculate_account_comm_profile,
    get_posts_for_classification
)


async def collect_instagram_posts(account, collector, db, limit=30):
    """Verzamel posts voor een Instagram account."""
    print(f"\n  Verzamelen @{account.handle}...")

    count = 0
    try:
        async for post in collector.collect_posts(account.handle, limit=limit):
            post.account_id = account.id
            PostQueries.upsert(post, db)
            count += 1

            # Progress indicator
            if count % 10 == 0:
                print(f"    {count} posts...", end="\r")

        print(f"    {count} posts verzameld")
        return count

    except Exception as e:
        print(f"    FOUT: {e}")
        return 0


async def main():
    print("=" * 60)
    print("BATCH DATA COLLECTIE - MFA Social Media Monitor")
    print("=" * 60)

    db = get_connection()
    accounts = AccountQueries.get_all(db)

    # Filter alleen Instagram accounts
    ig_accounts = [a for a in accounts if a.platform == "instagram"]

    print(f"\nGevonden: {len(ig_accounts)} Instagram accounts")
    print("-" * 60)

    collector = InstagramCollector()

    total_posts = 0
    successful = 0
    failed = 0

    for i, account in enumerate(ig_accounts, 1):
        print(f"\n[{i}/{len(ig_accounts)}] {account.country.upper()}")

        try:
            count = await collect_instagram_posts(account, collector, db, limit=30)
            total_posts += count
            if count > 0:
                successful += 1
            else:
                failed += 1

            # Rate limiting - wacht tussen accounts
            if i < len(ig_accounts):
                print("    Wachten 5 seconden...")
                await asyncio.sleep(5)

        except Exception as e:
            print(f"    KRITIEKE FOUT: {e}")
            failed += 1
            continue

    print("\n" + "=" * 60)
    print("COLLECTIE VOLTOOID")
    print("=" * 60)
    print(f"Accounts succesvol: {successful}")
    print(f"Accounts mislukt: {failed}")
    print(f"Totaal posts verzameld: {total_posts}")

    # Classificeer posts
    print("\n" + "-" * 60)
    print("COMMUNICATIE ANALYSE")
    print("-" * 60)

    for account in ig_accounts:
        posts = get_posts_for_classification(account.id, limit=100, db=db)
        if posts:
            print(f"  Classificeren {account.handle}: {len(posts)} posts...")
            classify_posts_batch(posts, db)
            calculate_account_comm_profile(account.id, db)

    print("\nKlaar!")


if __name__ == "__main__":
    asyncio.run(main())
