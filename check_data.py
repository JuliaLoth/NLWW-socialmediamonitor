#!/usr/bin/env python3
"""Check wat data we hebben."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.database.connection import get_readonly_connection

db = get_readonly_connection()

# Check welke maanden we hebben voor VAE
print("=== Metrics per maand voor VAE ===")
rows = db.fetchall("""
    SELECT m.year_month, m.total_posts, m.total_likes, m.avg_engagement_rate, a.handle
    FROM monthly_metrics m
    JOIN accounts a ON m.account_id = a.id
    WHERE a.country = 'vae'
    ORDER BY m.year_month
""")
for row in rows:
    print(f"  {row[0]}: {row[1]} posts, {row[2]} likes, {row[3]:.4f}% - @{row[4]}")

# Check posts per maand
print("\n=== Posts per maand in database ===")
rows2 = db.fetchall("""
    SELECT strftime('%Y-%m', posted_at) as maand, COUNT(*) as posts
    FROM posts p
    JOIN accounts a ON p.account_id = a.id
    WHERE a.country = 'vae'
    GROUP BY maand
    ORDER BY maand
""")
for row in rows2:
    print(f"  {row[0]}: {row[1]} posts")
