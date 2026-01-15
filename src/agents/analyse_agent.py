"""
Analyse Agent - Verantwoordelijk voor metrics berekening en trend analyse.
"""
from datetime import datetime
from typing import Optional
import logging

from .base import BaseAgent
from .job_queue import JobQueue, Job, JobType, JobResult
from ..database.connection import Database, get_connection
from ..database.queries import AccountQueries, MetricsQueries
from ..analysis.metrics import calculate_monthly_metrics, calculate_all_monthly_metrics
from ..analysis.trends import analyze_trends, get_trend_summary
from ..analysis.benchmarks import calculate_benchmarks, get_platform_comparison, get_regional_comparison

logger = logging.getLogger(__name__)


class AnalyseAgent(BaseAgent):
    """
    Agent voor data analyse en metrics berekening.

    Handelt de volgende job types af:
    - CALCULATE_MONTHLY: Bereken maandelijkse metrics voor alle accounts
    - CALCULATE_BENCHMARKS: Bereken benchmark vergelijkingen
    - DETECT_ANOMALIES: Detecteer ongewone veranderingen
    """

    def __init__(self, job_queue: JobQueue, db: Optional[Database] = None):
        super().__init__(job_queue, db, name="AnalyseAgent")

    def get_job_types(self) -> list[JobType]:
        return [
            JobType.CALCULATE_MONTHLY,
            JobType.CALCULATE_BENCHMARKS,
            JobType.DETECT_ANOMALIES,
        ]

    async def process_job(self, job: Job) -> JobResult:
        """Verwerk een analyse job."""
        try:
            if job.type == JobType.CALCULATE_MONTHLY:
                return await self._calculate_monthly(job.payload)

            elif job.type == JobType.CALCULATE_BENCHMARKS:
                return await self._calculate_benchmarks(job.payload)

            elif job.type == JobType.DETECT_ANOMALIES:
                return await self._detect_anomalies(job.payload)

            else:
                return JobResult(
                    success=False,
                    error=f"Onbekend job type: {job.type}"
                )

        except Exception as e:
            logger.error(f"AnalyseAgent job fout: {e}", exc_info=True)
            return JobResult(success=False, error=str(e))

    async def _calculate_monthly(self, payload: dict) -> JobResult:
        """
        Bereken maandelijkse metrics.

        Payload:
            year_month: str - Specifieke maand (YYYY-MM) of None voor huidige
            account_id: str - Specifiek account of None voor alle
        """
        year_month = payload.get("year_month")

        if year_month:
            year, month = map(int, year_month.split("-"))
        else:
            now = datetime.now()
            year, month = now.year, now.month
            year_month = f"{year:04d}-{month:02d}"

        account_id = payload.get("account_id")

        if account_id:
            # Bereken voor specifiek account
            metrics = calculate_monthly_metrics(account_id, year, month, self.db)
            if metrics:
                MetricsQueries.upsert(metrics, self.db)
                return JobResult(
                    success=True,
                    message=f"Metrics berekend voor {account_id}",
                    data={"year_month": year_month, "accounts_processed": 1}
                )
            else:
                return JobResult(
                    success=False,
                    error=f"Geen data voor {account_id} in {year_month}"
                )
        else:
            # Bereken voor alle accounts
            results = calculate_all_monthly_metrics(year, month, self.db)

            return JobResult(
                success=True,
                message=f"Metrics berekend voor {len(results)} accounts",
                data={
                    "year_month": year_month,
                    "accounts_processed": len(results)
                }
            )

    async def _calculate_benchmarks(self, payload: dict) -> JobResult:
        """
        Bereken benchmark vergelijkingen.

        Payload:
            year_month: str - Maand om te analyseren
        """
        year_month = payload.get("year_month")

        if not year_month:
            now = datetime.now()
            year_month = f"{now.year:04d}-{now.month:02d}"

        # Bereken engagement rate benchmarks
        engagement_benchmarks = calculate_benchmarks(
            year_month,
            "avg_engagement_rate",
            self.db
        )

        # Bereken follower benchmarks
        follower_benchmarks = calculate_benchmarks(
            year_month,
            "avg_followers",
            self.db
        )

        # Platform vergelijking
        platform_comparison = get_platform_comparison(year_month, self.db)

        # Regionale vergelijking
        regional_comparison = get_regional_comparison(year_month, self.db)

        return JobResult(
            success=True,
            message="Benchmarks berekend",
            data={
                "year_month": year_month,
                "engagement_rankings": len(engagement_benchmarks),
                "follower_rankings": len(follower_benchmarks),
                "platforms_compared": len(platform_comparison),
                "regions_compared": len(regional_comparison),
            }
        )

    async def _detect_anomalies(self, payload: dict) -> JobResult:
        """
        Detecteer ongewone veranderingen in metrics.

        Kijkt naar:
        - Plotselinge grote stijging/daling in engagement
        - Ongewone follower veranderingen
        - Accounts die lang niet gepost hebben

        Payload:
            year_month: str - Maand om te analyseren
            threshold_pct: float - Percentage verandering om als anomalie te markeren
        """
        year_month = payload.get("year_month")
        threshold = payload.get("threshold_pct", 50)  # 50% verandering = anomalie

        if not year_month:
            now = datetime.now()
            year_month = f"{now.year:04d}-{now.month:02d}"

        # Bereken vorige maand
        year, month = map(int, year_month.split("-"))
        if month == 1:
            prev_year, prev_month = year - 1, 12
        else:
            prev_year, prev_month = year, month - 1
        prev_year_month = f"{prev_year:04d}-{prev_month:02d}"

        accounts = AccountQueries.get_all(self.db)
        anomalies = []

        for account in accounts:
            trends = analyze_trends(
                account.id,
                year_month,
                prev_year_month,
                self.db
            )

            for trend in trends:
                if abs(trend.change_pct) >= threshold:
                    anomalies.append({
                        "account_id": account.id,
                        "country": account.country,
                        "platform": account.platform,
                        "metric": trend.metric,
                        "change_pct": trend.change_pct,
                        "direction": trend.direction.value,
                        "current_value": trend.current_value,
                        "previous_value": trend.previous_value,
                    })

        # Check voor inactieve accounts (geen posts in huidige maand)
        current_metrics = MetricsQueries.get_all_for_month(year_month, self.db)
        accounts_with_posts = {m.account_id for m in current_metrics if m.total_posts > 0}

        inactive = []
        for account in accounts:
            if account.id not in accounts_with_posts:
                inactive.append({
                    "account_id": account.id,
                    "country": account.country,
                    "platform": account.platform,
                    "reason": "geen_posts",
                })

        # Sorteer anomalies op change_pct
        anomalies.sort(key=lambda x: abs(x["change_pct"]), reverse=True)

        return JobResult(
            success=True,
            message=f"{len(anomalies)} anomalieen gedetecteerd",
            data={
                "year_month": year_month,
                "threshold_pct": threshold,
                "anomalies": anomalies,
                "inactive_accounts": inactive,
                "summary": {
                    "total_anomalies": len(anomalies),
                    "strong_growth": len([a for a in anomalies if a["change_pct"] > 0]),
                    "strong_decline": len([a for a in anomalies if a["change_pct"] < 0]),
                    "inactive": len(inactive),
                }
            }
        )


def get_analysis_summary(
    year_month: str,
    db: Optional[Database] = None
) -> dict:
    """
    Genereer complete analyse samenvatting voor een maand.
    Handig voor dashboard/rapportage.
    """
    db = db or get_connection()

    # Trend samenvatting
    trend_summary = get_trend_summary(year_month, db)

    # Platform vergelijking
    platform_comparison = get_platform_comparison(year_month, db)

    # Regional vergelijking
    regional_comparison = get_regional_comparison(year_month, db)

    # Top en bottom performers
    from .benchmarks import get_top_performers, get_bottom_performers
    top_performers = get_top_performers(year_month, n=5, db=db)
    bottom_performers = get_bottom_performers(year_month, n=5, db=db)

    return {
        "year_month": year_month,
        "trends": trend_summary,
        "by_platform": platform_comparison,
        "by_region": regional_comparison,
        "top_performers": top_performers,
        "bottom_performers": bottom_performers,
    }
