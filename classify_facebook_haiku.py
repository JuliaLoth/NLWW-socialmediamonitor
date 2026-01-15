#!/usr/bin/env python3
"""
Classificeer Facebook posts met Claude Haiku (goedkoper).
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
from src.database.queries import AccountQueries, PostQueries
from src.analysis.llm_classifier import ClaudeClassifier
from src.analysis.communication import calculate_account_comm_profile, save_post_classification
from src.database.models import PostClassification
from datetime import datetime


class HaikuClassifier(ClaudeClassifier):
    """Classifier die Haiku gebruikt ipv Sonnet."""

    def __init__(self, api_key=None):
        super().__init__(api_key)
        self.model = "claude-3-5-haiku-20241022"  # Haiku model


def main():
    print("=" * 60, flush=True)
    print("FACEBOOK CLASSIFICATIE - Claude Haiku", flush=True)
    print("=" * 60, flush=True)

    db = get_connection()

    # Check API
    try:
        classifier = HaikuClassifier()
        print(f"Claude API: OK (model: {classifier.model})", flush=True)
    except Exception as e:
        print(f"FOUT: {e}", flush=True)
        return

    # Haal alleen Facebook accounts op
    accounts = AccountQueries.get_all(db)
    fb_accounts = [a for a in accounts if a.platform == "facebook"]

    print(f"Facebook accounts te verwerken: {len(fb_accounts)}", flush=True)
    print("-" * 60, flush=True)

    total_classified = 0

    for i, account in enumerate(fb_accounts, 1):
        print(f"\n[{i}/{len(fb_accounts)}] {account.country.upper()} - @{account.handle}", flush=True)

        # Haal posts op
        posts = PostQueries.get_by_account(account.id, limit=500, db=db)

        if not posts:
            print("  Geen posts gevonden", flush=True)
            continue

        print(f"  Posts: {len(posts)} te classificeren", flush=True)

        try:
            for j, post in enumerate(posts, 1):
                # Check of post al geclassificeerd is
                existing = db.fetchone("SELECT 1 FROM post_classification WHERE post_id = ?", [post.id])
                if existing:
                    continue

                text = post.caption_snippet or ""

                # Skip lege posts
                if len(text.strip()) < 10:
                    continue

                # Classificeer
                result = classifier.classify_post(text)

                # Bepaal service-oriented
                is_service = result.get("communication_orientation", "zender") == "service"

                # Haal completeness_score
                completeness = result.get("information_completeness", {})
                if isinstance(completeness, dict):
                    score = completeness.get("score", result.get("completeness_score", 0.0))
                else:
                    score = result.get("completeness_score", 0.0)

                classification = PostClassification(
                    post_id=post.id,
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
                total_classified += 1

                # Voortgang elke 10 posts
                if j % 10 == 0:
                    print(f"    {j}/{len(posts)} posts verwerkt", flush=True)

            # Herbereken profiel
            calculate_account_comm_profile(account.id, db)
            print(f"  Profiel bijgewerkt", flush=True)

        except Exception as e:
            print(f"  FOUT: {e}", flush=True)
            import traceback
            traceback.print_exc()
            continue

    print("\n" + "=" * 60)
    print("CLASSIFICATIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal geclassificeerd: {total_classified} posts")


if __name__ == "__main__":
    main()
