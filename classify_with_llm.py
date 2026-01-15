#!/usr/bin/env python3
"""
LLM Classificatie voor alle posts.
Gebruikt Claude API voor nauwkeurige classificatie.
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
from src.database.models import Post
from src.analysis.llm_classifier import ClaudeClassifier
from src.analysis.communication import calculate_account_comm_profile, save_post_classification
from src.database.models import PostClassification
from datetime import datetime


def main():
    print("=" * 60, flush=True)
    print("LLM CLASSIFICATIE - Claude Sonnet", flush=True)
    print("=" * 60, flush=True)

    db = get_connection()

    # NIET verwijderen - ga verder waar we gebleven waren
    print("Doorgaan met classificatie...", flush=True)

    # Check API
    try:
        classifier = ClaudeClassifier()
        print("Claude API: OK", flush=True)
    except Exception as e:
        print(f"FOUT: {e}", flush=True)
        return

    # Haal accounts op - ALLEEN Instagram (Sonnet is duur)
    accounts = AccountQueries.get_all(db)
    social_accounts = [a for a in accounts if a.platform == "instagram"]

    print(f"Accounts te verwerken: {len(social_accounts)}", flush=True)
    print("-" * 60, flush=True)

    total_classified = 0

    for i, account in enumerate(social_accounts, 1):
        print(f"\n[{i}/{len(social_accounts)}] {account.country.upper()} - @{account.handle} ({account.platform})", flush=True)

        # Haal ALLE posts op voor dit account (geen limit)
        posts = PostQueries.get_by_account(account.id, limit=500, db=db)

        if not posts:
            print("  Geen posts gevonden", flush=True)
            continue

        print(f"  Posts: {len(posts)} te classificeren", flush=True)

        # Classificeer posts individueel (voorkomt timeouts)
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

                # Classificeer individuele post
                result = classifier.classify_post(text)

                # Bepaal service-oriented uit communication_orientation
                is_service = result.get("communication_orientation", "zender") == "service"

                # Haal completeness_score uit information_completeness indien aanwezig
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
                    classification_method="llm_sonnet"
                )
                save_post_classification(classification, db)
                total_classified += 1

                # Voortgang tonen elke 10 posts
                if j % 10 == 0:
                    print(f"    {j}/{len(posts)} posts verwerkt", flush=True)

            # Herbereken profiel
            calculate_account_comm_profile(account.id, db)
            print(f"  Profiel bijgewerkt")

        except Exception as e:
            print(f"  FOUT: {e}")
            import traceback
            traceback.print_exc()
            continue

    print("\n" + "=" * 60)
    print("CLASSIFICATIE VOLTOOID")
    print("=" * 60)
    print(f"Totaal geclassificeerd: {total_classified} posts")


if __name__ == "__main__":
    main()
