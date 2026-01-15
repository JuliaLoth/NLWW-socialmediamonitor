#!/usr/bin/env python3
"""
Vertaal de meest voorkomende woorden uit posts naar Nederlands met Claude Haiku.
Slaat vertalingen op in database voor gebruik in woordwolk.
"""
import sys
import os
from pathlib import Path
from collections import Counter
import re
from dotenv import load_dotenv

# Laad .env file
load_dotenv(Path(__file__).parent / '.env')

os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

import anthropic
from src.database.connection import get_connection


def main():
    print("=" * 60)
    print("WOORDEN VERTALEN NAAR NEDERLANDS")
    print("=" * 60)

    db = get_connection()

    # Maak tabel als die niet bestaat
    db.execute('''
        CREATE TABLE IF NOT EXISTS word_translations (
            original_word VARCHAR PRIMARY KEY,
            dutch_word VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Haal captions op - max 30 per land voor evenwichtige verdeling
    print("Posts ophalen (max 30 per land)...")
    captions = db.fetchall("""
        WITH ranked_posts AS (
            SELECT p.caption_snippet, a.country,
                   ROW_NUMBER() OVER (PARTITION BY a.country ORDER BY p.posted_at DESC) as rn
            FROM posts p
            JOIN accounts a ON p.account_id = a.id
            WHERE a.platform = 'instagram' AND a.status = 'active'
              AND p.caption_snippet IS NOT NULL
        )
        SELECT caption_snippet FROM ranked_posts WHERE rn <= 30
    """)

    print(f"Aantal posts: {len(captions)}")

    # Combineer alle tekst
    all_text = " ".join([c[0] for c in captions if c[0]])

    # Verwijder URLs, mentions, hashtags
    all_text = re.sub(r'http\S+', '', all_text)
    all_text = re.sub(r'@\w+', '', all_text)
    all_text = re.sub(r'#\w+', '', all_text)
    all_text = re.sub(r'[^\w\s]', ' ', all_text)

    # Stopwoorden
    stopwords = set([
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'must', 'shall', 'can', 'this', 'that',
        'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
        'de', 'het', 'een', 'en', 'van', 'in', 'is', 'op', 'te', 'dat',
        'die', 'voor', 'met', 'zijn', 'niet', 'aan', 'er', 'maar', 'om',
        'ook', 'als', 'bij', 'nog', 'wel', 'naar', 'kan', 'tot', 'dan',
        'al', 'was', 'nu', 'meer', 'zo', 'hier', 'our', 'your', 'their',
        'its', 'my', 'his', 'her', 'us', 'them', 'who', 'what', 'which'
    ])

    # Tel woord frequenties
    words = all_text.split()
    words = [w for w in words if len(w) > 2 and w.lower() not in stopwords]
    word_counts = Counter(words)
    top_words = word_counts.most_common(150)

    print(f"Top woorden gevonden: {len(top_words)}")

    # Check welke al vertaald zijn
    words_list = [w for w, _ in top_words]
    existing = db.fetchall("""
        SELECT original_word FROM word_translations
    """)
    existing_words = set(row[0] for row in existing)

    words_to_translate = [w for w in words_list if w not in existing_words]
    print(f"Nog te vertalen: {len(words_to_translate)}")

    if not words_to_translate:
        print("Alle woorden zijn al vertaald!")
        return

    # Vertaal met Haiku
    print("-" * 60)
    print("Vertalen met Claude Haiku...")

    client = anthropic.Anthropic()
    batch_size = 25
    total_translated = 0

    for i in range(0, len(words_to_translate), batch_size):
        batch = words_to_translate[i:i+batch_size]
        words_str = "\n".join(batch)

        print(f"\nBatch {i//batch_size + 1}: {len(batch)} woorden")

        try:
            response = client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=1000,
                messages=[{
                    "role": "user",
                    "content": f"""Vertaal elk woord naar Nederlands. Geef alleen de Nederlandse vertaling, één per regel, exact in dezelfde volgorde als de input.

Regels:
- Als het woord al Nederlands is, herhaal het
- Als het Arabisch/Perzisch/etc is, vertaal naar Nederlands
- Als het Engels is, vertaal naar Nederlands
- Geef ALLEEN het vertaalde woord, geen uitleg

Woorden:
{words_str}"""
                }]
            )

            translated_words = response.content[0].text.strip().split("\n")

            for orig, nl in zip(batch, translated_words):
                nl_word = nl.strip().lower()
                # Verwijder eventuele nummering of extra tekst
                nl_word = re.sub(r'^\d+[\.\)]\s*', '', nl_word)
                nl_word = re.sub(r'\s*\(.*\)$', '', nl_word)

                if nl_word and len(nl_word) > 1:
                    db.execute("""
                        INSERT OR REPLACE INTO word_translations (original_word, dutch_word)
                        VALUES (?, ?)
                    """, [orig, nl_word])
                    print(f"  {orig} -> {nl_word}")
                    total_translated += 1

        except Exception as e:
            print(f"  FOUT: {e}")
            continue

    print("\n" + "=" * 60)
    print("VERTALING VOLTOOID")
    print("=" * 60)
    print(f"Totaal vertaald: {total_translated}")

    # Toon statistieken
    total = db.fetchone("SELECT COUNT(*) FROM word_translations")[0]
    print(f"Totaal in database: {total}")


if __name__ == "__main__":
    main()
