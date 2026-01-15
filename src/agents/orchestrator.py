"""
Orchestrator - Coordineert alle agents en beheert de workflow.
"""
import asyncio
from datetime import datetime
from typing import Optional
import logging

from .job_queue import JobQueue, JobType, JobResult
from .base import BaseAgent
from .data_agent import DataAgent, load_accounts_from_yaml
from .analyse_agent import AnalyseAgent
from .rapport_agent import RapportAgent
from ..database.connection import Database, get_connection
from ..database.models import create_schema
from ..database.queries import AccountQueries

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Coördineert het multi-agent systeem.

    Verantwoordelijkheden:
    - Initialiseer database en agents
    - Schedule en coördineer workflows
    - Monitor agent status
    - Handle errors en retries
    """

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_connection()
        self.job_queue = JobQueue()

        # Initialize agents
        self.data_agent = DataAgent(self.job_queue, self.db)
        self.analyse_agent = AnalyseAgent(self.job_queue, self.db)
        self.rapport_agent = RapportAgent(self.job_queue, self.db)

        self._agent_tasks: list[asyncio.Task] = []
        self._running = False

    async def initialize(self):
        """
        Initialiseer het systeem.
        - Maak database schema
        - Laad account configuratie
        """
        logger.info("Orchestrator initialisatie gestart")

        # Create database schema
        create_schema()

        # Load accounts from YAML config
        await load_accounts_from_yaml(self.db)

        logger.info("Orchestrator initialisatie voltooid")

    async def start_agents(self):
        """Start alle agents als background tasks."""
        if self._running:
            logger.warning("Agents draaien al")
            return

        self._running = True

        # Start each agent as a task
        self._agent_tasks = [
            asyncio.create_task(self.data_agent.run(), name="DataAgent"),
            asyncio.create_task(self.analyse_agent.run(), name="AnalyseAgent"),
            asyncio.create_task(self.rapport_agent.run(), name="RapportAgent"),
        ]

        logger.info("Alle agents gestart")

    async def stop_agents(self):
        """Stop alle agents."""
        self._running = False

        # Signal agents to stop
        self.data_agent.stop()
        self.analyse_agent.stop()
        self.rapport_agent.stop()

        # Wait for tasks to complete
        if self._agent_tasks:
            await asyncio.gather(*self._agent_tasks, return_exceptions=True)
            self._agent_tasks = []

        logger.info("Alle agents gestopt")

    async def run_daily_collection(self) -> dict:
        """
        Voer dagelijkse data collectie workflow uit.

        1. Verzamel nieuwe posts voor alle accounts
        2. Update engagement op recente posts
        3. Bereken maandelijkse metrics
        4. Genereer benchmark data
        """
        logger.info("Dagelijkse collectie gestart")
        results = {"success": True, "steps": []}

        # Step 1: Collect new posts for all active accounts
        accounts = AccountQueries.get_all(self.db)
        logger.info(f"Collectie voor {len(accounts)} accounts")

        for account in accounts:
            await self.job_queue.enqueue(
                JobType.COLLECT_ACCOUNT,
                {"account_id": account.id},
                priority=5
            )

        # Wait for collection to complete
        await self.job_queue.wait_for_completion([JobType.COLLECT_ACCOUNT])
        results["steps"].append({"step": "collect_accounts", "accounts": len(accounts)})

        # Step 2: Update engagement on recent posts
        await self.job_queue.enqueue(
            JobType.UPDATE_POST_ENGAGEMENT,
            {"days": 7},
            priority=6
        )
        await self.job_queue.wait_for_completion([JobType.UPDATE_POST_ENGAGEMENT])
        results["steps"].append({"step": "update_engagement"})

        # Step 3: Calculate monthly metrics
        now = datetime.now()
        year_month = f"{now.year:04d}-{now.month:02d}"

        await self.job_queue.enqueue(
            JobType.CALCULATE_MONTHLY,
            {"year_month": year_month},
            priority=4
        )
        await self.job_queue.wait_for_completion([JobType.CALCULATE_MONTHLY])
        results["steps"].append({"step": "calculate_metrics", "month": year_month})

        # Step 4: Calculate benchmarks
        await self.job_queue.enqueue(
            JobType.CALCULATE_BENCHMARKS,
            {"year_month": year_month},
            priority=4
        )
        await self.job_queue.wait_for_completion([JobType.CALCULATE_BENCHMARKS])
        results["steps"].append({"step": "calculate_benchmarks"})

        # Step 5: Detect anomalies
        await self.job_queue.enqueue(
            JobType.DETECT_ANOMALIES,
            {"year_month": year_month, "threshold_pct": 30},
            priority=5
        )
        await self.job_queue.wait_for_completion([JobType.DETECT_ANOMALIES])
        results["steps"].append({"step": "detect_anomalies"})

        logger.info("Dagelijkse collectie voltooid")
        return results

    async def run_historical_backfill(self, country: Optional[str] = None) -> dict:
        """
        Voer historische data verzameling uit.

        Args:
            country: Optioneel land om te backfillen, of None voor alle landen
        """
        logger.info(f"Historische backfill gestart voor: {country or 'alle landen'}")

        accounts = AccountQueries.get_all(self.db)
        if country:
            accounts = [a for a in accounts if a.country == country]

        for account in accounts:
            await self.job_queue.enqueue(
                JobType.COLLECT_HISTORICAL,
                {"account_id": account.id, "months": 12},
                priority=8  # Lage prioriteit
            )

        logger.info(f"Historische backfill jobs aangemaakt voor {len(accounts)} accounts")

        return {
            "accounts_queued": len(accounts),
            "message": "Historische backfill jobs aangemaakt"
        }

    async def generate_reports(
        self,
        report_type: str = "monthly",
        year_month: Optional[str] = None,
        year: Optional[int] = None
    ) -> dict:
        """
        Genereer rapporten.

        Args:
            report_type: 'monthly' of 'yearly'
            year_month: Maand voor monthly rapport
            year: Jaar voor yearly rapport
        """
        if report_type == "monthly":
            if not year_month:
                now = datetime.now()
                year_month = f"{now.year:04d}-{now.month:02d}"

            # PDF
            await self.job_queue.enqueue(
                JobType.GENERATE_PDF,
                {"report_type": "monthly", "year_month": year_month},
                priority=3
            )

            # Excel
            await self.job_queue.enqueue(
                JobType.EXPORT_EXCEL,
                {"export_type": "monthly", "year_month": year_month},
                priority=3
            )

        elif report_type == "yearly":
            if not year:
                year = datetime.now().year

            # PDF
            await self.job_queue.enqueue(
                JobType.GENERATE_PDF,
                {"report_type": "yearly", "year": year},
                priority=3
            )

            # Excel
            await self.job_queue.enqueue(
                JobType.EXPORT_EXCEL,
                {"export_type": "yearly", "year": year},
                priority=3
            )

        await self.job_queue.wait_for_completion([JobType.GENERATE_PDF, JobType.EXPORT_EXCEL])

        return {"message": f"{report_type.capitalize()} rapporten gegenereerd"}

    async def get_status(self) -> dict:
        """Haal systeem status op."""
        job_status = await self.job_queue.get_status_summary()
        accounts = AccountQueries.get_all(self.db)

        return {
            "running": self._running,
            "agents": {
                "data_agent": self.data_agent.running,
                "analyse_agent": self.analyse_agent.running,
                "rapport_agent": self.rapport_agent.running,
            },
            "jobs": job_status,
            "accounts": {
                "total": len(accounts),
                "by_platform": AccountQueries.count_by_platform(self.db),
            }
        }

    async def cleanup(self):
        """Cleanup resources."""
        await self.stop_agents()
        await self.job_queue.cleanup_old_jobs(days=30)

        # Close collectors
        await self.data_agent.close()

        logger.info("Orchestrator cleanup voltooid")


async def run_orchestrator_workflow(workflow: str = "daily"):
    """
    Utility functie om een orchestrator workflow uit te voeren.
    """
    orchestrator = Orchestrator()

    try:
        await orchestrator.initialize()
        await orchestrator.start_agents()

        if workflow == "daily":
            result = await orchestrator.run_daily_collection()
        elif workflow == "backfill":
            result = await orchestrator.run_historical_backfill()
        elif workflow == "reports":
            result = await orchestrator.generate_reports()
        else:
            result = {"error": f"Onbekende workflow: {workflow}"}

        return result

    finally:
        await orchestrator.cleanup()
