#!/usr/bin/env python3
"""Debug dashboard queries."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import get_readonly_connection

db = get_readonly_connection()

print("=== KWANTITATIEF - Facebook ===")
account_stats = db.fetchall("""
    SELECT
        a.country, a.handle, a.platform,
        COUNT(p.id) as posts,
        SUM(p.likes) as total_likes
    FROM accounts a
    LEFT JOIN posts p ON a.id = p.account_id
    WHERE a.platform = 'facebook' AND a.country != 'nederland'
    GROUP BY a.id, a.country, a.handle, a.platform
    HAVING COUNT(p.id) > 0
    ORDER BY total_likes DESC
""")
print(f"Accounts gevonden: {len(account_stats)}")
for row in account_stats:
    print(f"  {row[0]:15} @{row[1]:30} {row[3]} posts, {row[4]} likes")

print()
print("=== KWALITATIEF - Content Types ===")
content_data = db.fetchall("""
    SELECT pc.content_type, COUNT(*) as count
    FROM post_classification pc
    JOIN posts p ON pc.post_id = p.id
    JOIN accounts a ON p.account_id = a.id
    WHERE a.country != 'nederland'
    GROUP BY pc.content_type
    ORDER BY count DESC
""")
print(f"Content types gevonden: {len(content_data)}")
for ct, cnt in content_data:
    print(f"  {ct}: {cnt}")

print()
print("=== KWALITATIEF - Formality ===")
formality_data = db.fetchall("""
    SELECT a.country, a.platform, cp.avg_formality_score, cp.pct_procedural
    FROM account_comm_profile cp
    JOIN accounts a ON cp.account_id = a.id
    WHERE cp.avg_formality_score IS NOT NULL AND a.country != 'nederland'
    ORDER BY cp.avg_formality_score DESC
""")
print(f"Profielen gevonden: {len(formality_data)}")
for row in formality_data:
    print(f"  {row[0]:15} ({row[1]:10}): formality={row[2]:.2f}, procedures={row[3]:.0f}%")
