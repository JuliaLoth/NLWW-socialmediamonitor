#!/usr/bin/env python3
"""
Herclassificeer alleen posts die als 'overig' zijn geclassificeerd.
"""
import sys
import os
from pathlib import Path

# Fix Windows encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

# Load .env
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value

from src.database.connection import get_connection
from src.analysis.llm_classifier import ClaudeClassifier
from datetime import datetime


def main():
    print("=" * 60, flush=True)
    print("HERCLASSIFICATIE - Alleen 'overig' posts", flush=True)
    print("=" * 60, flush=True)

    db = get_connection()

    # Check API
    try:
        classifier = ClaudeClassifier()
        print("Claude API: OK", flush=True)
    except Exception as e:
        print(f"FOUT: {e}", flush=True)
        return

    # Haal alle posts op die als 'overig' zijn geclassificeerd
    overig_posts = db.fetchall("""
        SELECT pc.post_id, p.caption_snippet
        FROM post_classification pc
        JOIN posts p ON pc.post_id = p.id
        WHERE pc.content_type = 'overig'
    """)

    print(f"Posts te herclassificeren: {len(overig_posts)}", flush=True)
    print("-" * 60, flush=True)

    reclassified = 0
    still_overig = 0

    for i, (post_id, caption) in enumerate(overig_posts, 1):
        if not caption or len(caption.strip()) < 10:
            continue

        # Classificeer opnieuw
        result = classifier.classify_post(caption)
        new_type = result.get("content_type", "overig")

        if new_type != "overig":
            # Update classificatie
            db.execute("""
                UPDATE post_classification
                SET content_type = ?,
                    tone_formality = ?,
                    classified_at = ?,
                    classification_method = 'llm_sonnet_v2'
                WHERE post_id = ?
            """, [
                new_type,
                result.get("tone_formality", 0.5),
                datetime.now(),
                post_id
            ])
            reclassified += 1
        else:
            still_overig += 1

        # Voortgang
        if i % 10 == 0:
            print(f"  {i}/{len(overig_posts)} verwerkt ({reclassified} gewijzigd)", flush=True)

    print()
    print("=" * 60)
    print("HERCLASSIFICATIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal verwerkt: {len(overig_posts)}")
    print(f"Gewijzigd naar diplomatiek_nieuws of ander: {reclassified}")
    print(f"Blijft overig: {still_overig}")


if __name__ == "__main__":
    main()
