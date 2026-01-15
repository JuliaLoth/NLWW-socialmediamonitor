#!/usr/bin/env python3
"""
Classificeer alleen posts die NOG NIET geclassificeerd zijn.
Gebruikt Haiku voor snelheid en kosten.
"""
import sys
import os
from pathlib import Path

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
from src.analysis.communication import calculate_account_comm_profile, save_post_classification
from src.database.models import PostClassification
from datetime import datetime


class HaikuClassifier(ClaudeClassifier):
    """Haiku voor snelheid en lagere kosten."""
    def __init__(self, api_key=None):
        super().__init__(api_key)
        self.model = "claude-3-5-haiku-20241022"


def main():
    print("=" * 60)
    print("CLASSIFICATIE - Alleen nieuwe posts")
    print("=" * 60)

    db = get_connection()

    # Tel hoeveel posts nog niet geclassificeerd zijn
    unclassified = db.fetchall("""
        SELECT p.id, p.caption_snippet, p.account_id, a.country, a.handle
        FROM posts p
        JOIN accounts a ON p.account_id = a.id
        LEFT JOIN post_classification pc ON p.id = pc.post_id
        WHERE pc.post_id IS NULL
          AND a.platform = 'instagram'
          AND a.status = 'active'
          AND p.caption_snippet IS NOT NULL
          AND LENGTH(p.caption_snippet) >= 10
        ORDER BY a.country, p.posted_at DESC
    """)

    print(f"Posts te classificeren: {len(unclassified)}")

    if len(unclassified) == 0:
        print("Alle posts zijn al geclassificeerd!")
        return

    # Geschatte kosten
    estimated_cost = len(unclassified) * 0.001  # ~$0.001 per post met Haiku
    print(f"Geschatte kosten: ~${estimated_cost:.2f}")
    print("-" * 60)

    # Check API
    try:
        classifier = HaikuClassifier()
        print(f"Claude API: OK (model: {classifier.model})")
    except Exception as e:
        print(f"FOUT: {e}")
        return

    classified = 0
    current_account = None
    account_posts = 0

    for i, (post_id, caption, account_id, country, handle) in enumerate(unclassified, 1):
        # Track account changes for profile updates
        if account_id != current_account:
            if current_account is not None and account_posts > 0:
                calculate_account_comm_profile(current_account, db)
                print(f"  Profiel bijgewerkt ({account_posts} posts)")
            current_account = account_id
            account_posts = 0
            print(f"\n[{country.upper()}] @{handle}")

        # Classificeer
        try:
            result = classifier.classify_post(caption)

            is_service = result.get("communication_orientation", "zender") == "service"
            completeness = result.get("information_completeness", {})
            score = completeness.get("score", 0.0) if isinstance(completeness, dict) else 0.0

            classification = PostClassification(
                post_id=post_id,
                content_type=result.get("content_type", "overig"),
                tone_formality=result.get("tone_formality", 0.5),
                tone_service_oriented=is_service,
                tone_empathetic=None,
                tone_proactive=result.get("has_call_to_action", False),
                days_advance=None,
                timing_class="nvt",
                has_call_to_action=result.get("has_call_to_action", False),
                has_link=None,
                has_contact_info=None,
                has_deadline=bool(result.get("detected_deadline")),
                completeness_score=score,
                language=result.get("language"),
                uses_emoji=None,
                uses_formal_pronouns=None,
                classified_at=datetime.now(),
                classification_method="llm_haiku"
            )
            save_post_classification(classification, db)
            classified += 1
            account_posts += 1

            if classified % 20 == 0:
                print(f"  {classified}/{len(unclassified)} posts verwerkt...")

        except Exception as e:
            print(f"  Fout bij post: {e}")
            continue

    # Update laatste account profiel
    if current_account is not None and account_posts > 0:
        calculate_account_comm_profile(current_account, db)
        print(f"  Profiel bijgewerkt ({account_posts} posts)")

    print("\n" + "=" * 60)
    print("CLASSIFICATIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal geclassificeerd: {classified} posts")


if __name__ == "__main__":
    main()
