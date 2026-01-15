"""
Data Agent - Verantwoordelijk voor data verzameling en opslag.
"""
import asyncio
from datetime import datetime, date, timedelta
from typing import Optional
import logging
import uuid
import yaml

from .base import BaseAgent
from .job_queue import JobQueue, Job, JobType, JobResult
from ..database.connection import Database, get_connection
from ..database.models import Account, Post, FollowerSnapshot, create_schema, generate_uuid
from ..database.queries import AccountQueries, PostQueries, FollowerQueries
from ..collectors import InstagramCollector, TwitterCollector, FacebookCollector
from ..collectors.base import CollectorResult
from ..config.settings import settings, ACCOUNTS_CONFIG

logger = logging.getLogger(__name__)


class DataAgent(BaseAgent):
    """
    Agent voor data verzameling van social media platforms.

    Handelt de volgende job types af:
    - COLLECT_ACCOUNT: Verzamel recente posts voor één account
    - COLLECT_HISTORICAL: Verzamel 12 maanden historie voor één account
    - UPDATE_FOLLOWERS: Update follower counts
    - UPDATE_POST_ENGAGEMENT: Update engagement op recente posts
    """

    def __init__(self, job_queue: JobQueue, db: Optional[Database] = None):
        super().__init__(job_queue, db, name="DataAgent")

        # Initialize collectors
        self._collectors = {}

    def get_job_types(self) -> list[JobType]:
        return [
            JobType.COLLECT_ACCOUNT,
            JobType.COLLECT_HISTORICAL,
            JobType.UPDATE_FOLLOWERS,
            JobType.UPDATE_POST_ENGAGEMENT,
        ]

    def _get_collector(self, platform: str):
        """Get of create collector voor platform."""
        if platform not in self._collectors:
            if platform == "instagram":
                self._collectors[platform] = InstagramCollector()
            elif platform == "twitter":
                self._collectors[platform] = TwitterCollector()
            elif platform == "facebook":
                self._collectors[platform] = FacebookCollector()
            else:
                raise ValueError(f"Onbekend platform: {platform}")

        return self._collectors[platform]

    async def process_job(self, job: Job) -> JobResult:
        """Verwerk een data collectie job."""
        try:
            if job.type == JobType.COLLECT_ACCOUNT:
                return await self._collect_account(job.payload)

            elif job.type == JobType.COLLECT_HISTORICAL:
                return await self._collect_historical(job.payload)

            elif job.type == JobType.UPDATE_FOLLOWERS:
                return await self._update_followers(job.payload)

            elif job.type == JobType.UPDATE_POST_ENGAGEMENT:
                return await self._update_post_engagement(job.payload)

            else:
                return JobResult(
                    success=False,
                    error=f"Onbekend job type: {job.type}"
                )

        except Exception as e:
            logger.error(f"DataAgent job fout: {e}", exc_info=True)
            return JobResult(success=False, error=str(e))

    async def _collect_account(self, payload: dict) -> JobResult:
        """
        Verzamel recente posts voor een account.
        Alleen nieuwe posts sinds laatste collectie.
        """
        account_id = payload.get("account_id")
        if not account_id:
            return JobResult(success=False, error="account_id ontbreekt")

        # Haal account op
        account = AccountQueries.get_by_id(account_id, self.db)
        if not account:
            return JobResult(success=False, error=f"Account niet gevonden: {account_id}")

        # Bepaal sinds wanneer te collecten
        latest_post = PostQueries.get_latest_post_date(account_id, self.db)
        since = latest_post if latest_post else datetime.now() - timedelta(days=30)

        logger.info(f"Collectie voor {account.handle} sinds {since.date()}")

        # Verzamel data
        collector = self._get_collector(account.platform)
        result = await collector.collect(account, since=since, limit=50)

        if not result.success:
            return JobResult(
                success=False,
                error=result.error,
                data={"posts_collected": 0}
            )

        # Sla posts op
        for post in result.posts:
            PostQueries.upsert(post, self.db)

        # Update follower snapshot
        if result.followers is not None:
            snapshot = FollowerSnapshot(
                id=generate_uuid(),
                account_id=account_id,
                date=date.today(),
                followers=result.followers,
                following=result.following,
            )
            FollowerQueries.upsert(snapshot, self.db)

        return JobResult(
            success=True,
            message=f"{result.posts_collected} posts verzameld",
            data={
                "posts_collected": result.posts_collected,
                "followers": result.followers,
            }
        )

    async def _collect_historical(self, payload: dict) -> JobResult:
        """
        Verzamel 12 maanden historische data voor een account.
        """
        account_id = payload.get("account_id")
        months = payload.get("months", 12)

        if not account_id:
            return JobResult(success=False, error="account_id ontbreekt")

        account = AccountQueries.get_by_id(account_id, self.db)
        if not account:
            return JobResult(success=False, error=f"Account niet gevonden: {account_id}")

        logger.info(f"Historische collectie voor {account.handle}: {months} maanden")

        # Verzamel historische data
        collector = self._get_collector(account.platform)
        result = await collector.collect_historical(account, months=months)

        if not result.success:
            return JobResult(
                success=False,
                error=result.error,
                data={"posts_collected": 0}
            )

        # Sla posts op
        for post in result.posts:
            PostQueries.upsert(post, self.db)

        # Sla huidige followers op
        if result.followers is not None:
            snapshot = FollowerSnapshot(
                id=generate_uuid(),
                account_id=account_id,
                date=date.today(),
                followers=result.followers,
                following=result.following,
            )
            FollowerQueries.upsert(snapshot, self.db)

        return JobResult(
            success=True,
            message=f"{result.posts_collected} historische posts verzameld",
            data={
                "posts_collected": result.posts_collected,
                "followers": result.followers,
            }
        )

    async def _update_followers(self, payload: dict) -> JobResult:
        """
        Update follower counts voor alle accounts.
        """
        accounts = AccountQueries.get_all(self.db)
        updated = 0
        errors = []

        for account in accounts:
            try:
                collector = self._get_collector(account.platform)
                followers, following = await collector.collect_profile(account.handle)

                if followers is not None:
                    snapshot = FollowerSnapshot(
                        id=generate_uuid(),
                        account_id=account.id,
                        date=date.today(),
                        followers=followers,
                        following=following,
                    )
                    FollowerQueries.upsert(snapshot, self.db)
                    updated += 1

            except Exception as e:
                errors.append(f"{account.handle}: {e}")
                logger.warning(f"Follower update fout voor {account.handle}: {e}")

        return JobResult(
            success=len(errors) == 0,
            message=f"{updated} accounts bijgewerkt",
            data={"updated": updated, "errors": errors}
        )

    async def _update_post_engagement(self, payload: dict) -> JobResult:
        """
        Update engagement stats op recente posts (eerste 7 dagen).
        """
        days = payload.get("days", 7)
        posts = PostQueries.get_posts_for_update(days, self.db)

        updated = 0

        # Group posts by account
        posts_by_account = {}
        for post in posts:
            if post.account_id not in posts_by_account:
                posts_by_account[post.account_id] = []
            posts_by_account[post.account_id].append(post)

        for account_id, account_posts in posts_by_account.items():
            account = AccountQueries.get_by_id(account_id, self.db)
            if not account:
                continue

            try:
                collector = self._get_collector(account.platform)

                # Re-collect posts to get updated engagement
                result = await collector.collect(
                    account,
                    since=datetime.now() - timedelta(days=days),
                    limit=len(account_posts)
                )

                for new_post in result.posts:
                    # Update existing post with new engagement
                    new_post.last_updated = datetime.now()
                    PostQueries.upsert(new_post, self.db)
                    updated += 1

            except Exception as e:
                logger.warning(f"Engagement update fout voor {account.handle}: {e}")

        return JobResult(
            success=True,
            message=f"{updated} posts bijgewerkt",
            data={"updated": updated}
        )

    async def close(self):
        """Sluit alle collectors."""
        for collector in self._collectors.values():
            if hasattr(collector, 'close'):
                await collector.close()


async def load_accounts_from_yaml(db: Optional[Database] = None):
    """
    Laad account configuratie uit YAML en sync naar database.
    """
    db = db or get_connection()

    if not ACCOUNTS_CONFIG.exists():
        logger.warning(f"Account config niet gevonden: {ACCOUNTS_CONFIG}")
        return

    with open(ACCOUNTS_CONFIG, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    accounts_data = config.get('accounts', {})
    count = 0

    for country, country_data in accounts_data.items():
        platforms = country_data.get('platforms', {})

        for platform, handles in platforms.items():
            for handle_data in handles:
                if isinstance(handle_data, dict):
                    handle = handle_data.get('handle', '')
                    if not handle:
                        continue

                    status = handle_data.get('status', 'active')
                    if status in ('inactief', 'inactive', 'gehackt', 'hacked'):
                        status = 'inactive'
                    else:
                        status = 'active'

                    display_name = handle_data.get('display_name')
                    notes = handle_data.get('notes')
                else:
                    handle = handle_data
                    status = 'active'
                    display_name = None
                    notes = None

                account = Account(
                    id=Account.generate_id(country, platform, handle),
                    country=country,
                    platform=platform,
                    handle=handle,
                    display_name=display_name,
                    status=status,
                    notes=notes,
                )

                AccountQueries.upsert(account, db)
                count += 1

    logger.info(f"{count} accounts geladen uit configuratie")
