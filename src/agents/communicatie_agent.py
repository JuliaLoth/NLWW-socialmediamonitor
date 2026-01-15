"""
Communicatie Analyse Agent

Analyseert tone of voice, content types, en interactiepatronen
van overheidsaccounts op social media.
"""
import asyncio
import logging
from typing import Optional
from datetime import datetime

from .base import BaseAgent, AgentCapability
from .job_queue import JobQueue, Job, JobResult, JobType, JobStatus
from ..database.connection import Database, get_connection
from ..database.queries import AccountQueries, PostQueries
from ..database.models import Post, PostClassification, AccountCommProfile
from ..analysis.communication import (
    classify_post, classify_posts_batch, save_post_classification,
    calculate_account_comm_profile, get_posts_for_classification,
    get_classification_summary
)

logger = logging.getLogger(__name__)


# Voeg nieuwe job types toe
class CommJobType:
    """Extra job types voor communicatie analyse."""
    CLASSIFY_POSTS = "classify_posts"
    CLASSIFY_ACCOUNT = "classify_account"
    CALCULATE_COMM_PROFILE = "calculate_comm_profile"
    ANALYZE_COMMENTS = "analyze_comments"
    BATCH_CLASSIFY = "batch_classify"


class CommunicatieAgent(BaseAgent):
    """
    Agent voor communicatie analyse van social media posts.

    Verantwoordelijkheden:
    - Classificeren van posts op tone of voice en content type
    - Analyseren van interactiepatronen in comments
    - Berekenen van communicatieprofielen per account
    - Integratie met LLM voor geavanceerde classificatie
    """

    name = "CommunicatieAgent"
    capabilities = [
        AgentCapability.ANALYZE,
    ]

    def __init__(self, job_queue: JobQueue, db: Optional[Database] = None):
        self.job_queue = job_queue
        self.db = db or get_connection()

    def get_job_types(self) -> list:
        """Return job types die deze agent afhandelt."""
        return [
            CommJobType.CLASSIFY_POSTS,
            CommJobType.CLASSIFY_ACCOUNT,
            CommJobType.CALCULATE_COMM_PROFILE,
            CommJobType.ANALYZE_COMMENTS,
            CommJobType.BATCH_CLASSIFY,
        ]

    async def process_job(self, job: Job) -> JobResult:
        """Verwerk een job."""
        try:
            if job.job_type == CommJobType.CLASSIFY_POSTS:
                return await self._classify_posts(job.payload)
            elif job.job_type == CommJobType.CLASSIFY_ACCOUNT:
                return await self._classify_account(job.payload)
            elif job.job_type == CommJobType.CALCULATE_COMM_PROFILE:
                return await self._calculate_profile(job.payload)
            elif job.job_type == CommJobType.ANALYZE_COMMENTS:
                return await self._analyze_comments(job.payload)
            elif job.job_type == CommJobType.BATCH_CLASSIFY:
                return await self._batch_classify(job.payload)
            else:
                return JobResult(
                    success=False,
                    error=f"Onbekend job type: {job.job_type}"
                )
        except Exception as e:
            logger.error(f"Fout bij verwerken job {job.id}: {e}")
            return JobResult(success=False, error=str(e))

    async def _classify_posts(self, payload: dict) -> JobResult:
        """
        Classificeer specifieke posts.

        Payload:
            post_ids: list[str] - IDs van posts om te classificeren
        """
        post_ids = payload.get("post_ids", [])
        if not post_ids:
            return JobResult(success=False, error="Geen post_ids opgegeven")

        classified = 0
        for post_id in post_ids:
            # Haal post op
            row = self.db.fetchone("""
                SELECT id, account_id, platform_post_id, posted_at, content_type,
                       likes, comments, shares, views, url, caption_snippet,
                       hashtags, collected_at, last_updated
                FROM posts WHERE id = ?
            """, [post_id])

            if row:
                post = Post(*row)
                classification = classify_post(post)
                save_post_classification(classification, self.db)
                classified += 1

            # Rate limiting
            await asyncio.sleep(0.01)

        logger.info(f"Geclassificeerd: {classified} posts")
        return JobResult(
            success=True,
            data={"posts_classified": classified}
        )

    async def _classify_account(self, payload: dict) -> JobResult:
        """
        Classificeer alle posts van een account.

        Payload:
            account_id: str - ID van account
            limit: int - Max aantal posts (default 100)
        """
        account_id = payload.get("account_id")
        limit = payload.get("limit", 100)

        if not account_id:
            return JobResult(success=False, error="Geen account_id opgegeven")

        # Haal ongeclassificeerde posts op
        posts = get_posts_for_classification(account_id, limit, self.db)

        if not posts:
            logger.info(f"Geen posts te classificeren voor {account_id}")
            return JobResult(
                success=True,
                data={"posts_classified": 0, "message": "Geen nieuwe posts"}
            )

        # Classificeer posts
        classifications = classify_posts_batch(posts, self.db)

        logger.info(f"Account {account_id}: {len(classifications)} posts geclassificeerd")
        return JobResult(
            success=True,
            data={"posts_classified": len(classifications)}
        )

    async def _calculate_profile(self, payload: dict) -> JobResult:
        """
        Bereken communicatieprofiel voor een account.

        Payload:
            account_id: str - ID van account
        """
        account_id = payload.get("account_id")

        if not account_id:
            return JobResult(success=False, error="Geen account_id opgegeven")

        profile = calculate_account_comm_profile(account_id, self.db)

        logger.info(f"Profiel berekend voor {account_id}: "
                    f"{profile.total_posts_analyzed} posts geanalyseerd")

        return JobResult(
            success=True,
            data={
                "account_id": account_id,
                "posts_analyzed": profile.total_posts_analyzed,
                "dominant_tone": profile.dominant_tone,
                "avg_formality": profile.avg_formality_score,
                "pct_procedural": profile.pct_procedural,
                "avg_completeness": profile.avg_completeness,
            }
        )

    async def _analyze_comments(self, payload: dict) -> JobResult:
        """
        Analyseer comments op posts.

        Payload:
            post_id: str - ID van post (optioneel)
            account_id: str - ID van account (optioneel)
        """
        # TODO: Implementeer comment analyse
        # Dit vereist eerst het uitbreiden van collectors om comments op te halen
        return JobResult(
            success=True,
            data={"message": "Comment analyse nog niet geïmplementeerd"}
        )

    async def _batch_classify(self, payload: dict) -> JobResult:
        """
        Batch classificeer alle ongeclassificeerde posts.

        Payload:
            limit: int - Max aantal posts per batch (default 500)
        """
        limit = payload.get("limit", 500)

        # Haal alle ongeclassificeerde posts op
        posts = get_posts_for_classification(limit=limit, db=self.db)

        if not posts:
            return JobResult(
                success=True,
                data={"posts_classified": 0, "message": "Geen posts te classificeren"}
            )

        # Classificeer in batches
        batch_size = 50
        total_classified = 0

        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            classify_posts_batch(batch, self.db)
            total_classified += len(batch)

            # Progress logging
            logger.info(f"Batch classificatie: {total_classified}/{len(posts)}")

            # Rate limiting
            await asyncio.sleep(0.1)

        # Update profielen voor alle accounts
        account_ids = set(p.account_id for p in posts)
        for account_id in account_ids:
            calculate_account_comm_profile(account_id, self.db)

        return JobResult(
            success=True,
            data={
                "posts_classified": total_classified,
                "accounts_updated": len(account_ids)
            }
        )


async def run_communication_analysis(account_id: Optional[str] = None,
                                      limit: int = 100,
                                      db: Optional[Database] = None) -> dict:
    """
    Convenience functie om communicatie analyse uit te voeren.
    Kan gebruikt worden zonder job queue.
    """
    db = db or get_connection()

    # Haal posts op
    if account_id:
        posts = get_posts_for_classification(account_id, limit, db)
    else:
        posts = get_posts_for_classification(limit=limit, db=db)

    if not posts:
        return {"posts_classified": 0, "message": "Geen posts gevonden"}

    # Classificeer
    classifications = classify_posts_batch(posts, db)

    # Update profielen
    account_ids = set(p.account_id for p in posts)
    profiles = {}
    for acc_id in account_ids:
        profile = calculate_account_comm_profile(acc_id, db)
        profiles[acc_id] = {
            "posts_analyzed": profile.total_posts_analyzed,
            "dominant_tone": profile.dominant_tone,
            "pct_procedural": profile.pct_procedural,
        }

    return {
        "posts_classified": len(classifications),
        "accounts_updated": len(account_ids),
        "profiles": profiles,
        "summary": get_classification_summary(db)
    }
