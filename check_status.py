#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')
from src.database.connection import get_readonly_connection

db = get_readonly_connection()

print('=== HUIDIGE STATUS ===')
total = db.fetchone('SELECT COUNT(*) FROM posts')[0]
classified = db.fetchone('SELECT COUNT(*) FROM post_classification')[0]

print(f'Totaal posts: {total}')
print(f'Geclassificeerd: {classified} ({classified/total*100:.0f}%)')
print(f'Nog te doen: {total - classified}')

print()
print('Per land:')
data = db.fetchall("""
    SELECT a.country, COUNT(p.id) as total, COUNT(pc.post_id) as done
    FROM accounts a
    LEFT JOIN posts p ON a.id = p.account_id
    LEFT JOIN post_classification pc ON p.id = pc.post_id
    WHERE a.platform = 'instagram' AND a.status = 'active'
    GROUP BY a.country
    ORDER BY (COUNT(p.id) - COUNT(pc.post_id)) DESC
""")
for country, total, done in data:
    todo = total - done
    pct = (done/total*100) if total > 0 else 0
    status = 'OK' if todo == 0 else f'{todo} nog'
    print(f'  {country:15} {done:3}/{total:3} ({pct:3.0f}%) - {status}')

# Check waarom posts niet geclassificeerd zijn
print()
print('Ongeclassificeerde posts (reden):')
unclassified = db.fetchall("""
    SELECT a.country, LENGTH(p.caption_snippet) as len
    FROM posts p
    JOIN accounts a ON p.account_id = a.id
    LEFT JOIN post_classification pc ON p.id = pc.post_id
    WHERE pc.post_id IS NULL
      AND a.platform = 'instagram' AND a.status = 'active'
""")
short = sum(1 for _, l in unclassified if (l or 0) < 10)
none = sum(1 for _, l in unclassified if l is None)
print(f'  Te korte tekst (<10 chars): {short}')
print(f'  Geen tekst: {none}')

# Check accounts die meer posts nodig hebben
print()
print('Accounts met minder dan 100 posts:')
need_more = db.fetchall("""
    SELECT a.country, a.handle, COUNT(p.id) as posts
    FROM accounts a
    LEFT JOIN posts p ON a.id = p.account_id
    WHERE a.platform = 'instagram' AND a.status = 'active'
    GROUP BY a.id, a.country, a.handle
    HAVING COUNT(p.id) < 100
    ORDER BY COUNT(p.id) ASC
""")
for country, handle, posts in need_more:
    needed = 100 - posts
    print(f'  {country:15} @{handle:25} {posts:3} posts (nodig: {needed})')
