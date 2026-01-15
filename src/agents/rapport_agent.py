"""
Rapport Agent - Verantwoordelijk voor output generatie.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging

from .base import BaseAgent
from .job_queue import JobQueue, Job, JobType, JobResult
from ..database.connection import Database, get_connection
from ..config.settings import EXPORTS_DIR

logger = logging.getLogger(__name__)


class RapportAgent(BaseAgent):
    """
    Agent voor rapport en export generatie.

    Handelt de volgende job types af:
    - GENERATE_DASHBOARD_DATA: Bereid data voor voor dashboard
    - GENERATE_PDF: Genereer PDF rapport
    - EXPORT_EXCEL: Exporteer naar Excel
    """

    def __init__(self, job_queue: JobQueue, db: Optional[Database] = None):
        super().__init__(job_queue, db, name="RapportAgent")

    def get_job_types(self) -> list[JobType]:
        return [
            JobType.GENERATE_DASHBOARD_DATA,
            JobType.GENERATE_PDF,
            JobType.EXPORT_EXCEL,
        ]

    async def process_job(self, job: Job) -> JobResult:
        """Verwerk een rapport job."""
        try:
            if job.type == JobType.GENERATE_DASHBOARD_DATA:
                return await self._generate_dashboard_data(job.payload)

            elif job.type == JobType.GENERATE_PDF:
                return await self._generate_pdf(job.payload)

            elif job.type == JobType.EXPORT_EXCEL:
                return await self._export_excel(job.payload)

            else:
                return JobResult(
                    success=False,
                    error=f"Onbekend job type: {job.type}"
                )

        except Exception as e:
            logger.error(f"RapportAgent job fout: {e}", exc_info=True)
            return JobResult(success=False, error=str(e))

    async def _generate_dashboard_data(self, payload: dict) -> JobResult:
        """
        Bereid data voor voor het dashboard.
        Dit cached de analyse resultaten voor snelle toegang.
        """
        year_month = payload.get("year_month")

        if not year_month:
            now = datetime.now()
            year_month = f"{now.year:04d}-{now.month:02d}"

        # Import analysis functions
        from ..analysis.benchmarks import (
            get_platform_comparison,
            get_regional_comparison,
            get_top_performers,
            get_bottom_performers
        )
        from ..analysis.trends import get_trend_summary

        # Generate all dashboard data
        platform_data = get_platform_comparison(year_month, self.db)
        regional_data = get_regional_comparison(year_month, self.db)
        top_performers = get_top_performers(year_month, n=10, db=self.db)
        bottom_performers = get_bottom_performers(year_month, n=10, db=self.db)
        trend_summary = get_trend_summary(year_month, self.db)

        return JobResult(
            success=True,
            message="Dashboard data gegenereerd",
            data={
                "year_month": year_month,
                "platforms": len(platform_data),
                "countries": len(regional_data),
                "has_trends": bool(trend_summary.get("growing")),
            }
        )

    async def _generate_pdf(self, payload: dict) -> JobResult:
        """
        Genereer PDF rapport.

        Payload:
            report_type: 'monthly' of 'yearly'
            year_month: str voor monthly (YYYY-MM)
            year: int voor yearly
            output_path: Optional[str]
        """
        report_type = payload.get("report_type", "monthly")

        try:
            from ..outputs.reports.pdf_generator import generate_monthly_pdf, generate_yearly_pdf

            if report_type == "monthly":
                year_month = payload.get("year_month")
                if not year_month:
                    now = datetime.now()
                    year_month = f"{now.year:04d}-{now.month:02d}"

                output_path = payload.get("output_path")
                if output_path:
                    output_path = Path(output_path)

                result_path = generate_monthly_pdf(year_month, output_path, self.db)

                return JobResult(
                    success=True,
                    message=f"PDF rapport gegenereerd",
                    data={"file_path": str(result_path)}
                )

            elif report_type == "yearly":
                year = payload.get("year", datetime.now().year)
                output_path = payload.get("output_path")
                if output_path:
                    output_path = Path(output_path)

                result_path = generate_yearly_pdf(year, output_path, self.db)

                return JobResult(
                    success=True,
                    message=f"Jaarrapport PDF gegenereerd",
                    data={"file_path": str(result_path)}
                )

            else:
                return JobResult(
                    success=False,
                    error=f"Onbekend rapport type: {report_type}"
                )

        except ImportError as e:
            return JobResult(
                success=False,
                error=f"PDF generatie niet beschikbaar: {e}"
            )

    async def _export_excel(self, payload: dict) -> JobResult:
        """
        Exporteer data naar Excel.

        Payload:
            export_type: 'monthly' of 'yearly'
            year_month: str voor monthly
            year: int voor yearly
            output_path: Optional[str]
        """
        export_type = payload.get("export_type", "monthly")

        try:
            from ..outputs.reports.excel_export import export_monthly_report, export_yearly_report

            if export_type == "monthly":
                year_month = payload.get("year_month")
                if not year_month:
                    now = datetime.now()
                    year_month = f"{now.year:04d}-{now.month:02d}"

                output_path = payload.get("output_path")
                if output_path:
                    output_path = Path(output_path)

                result_path = export_monthly_report(year_month, output_path, self.db)

                return JobResult(
                    success=True,
                    message="Excel export gegenereerd",
                    data={"file_path": str(result_path)}
                )

            elif export_type == "yearly":
                year = payload.get("year", datetime.now().year)
                output_path = payload.get("output_path")
                if output_path:
                    output_path = Path(output_path)

                result_path = export_yearly_report(year, output_path, self.db)

                return JobResult(
                    success=True,
                    message="Jaarlijkse Excel export gegenereerd",
                    data={"file_path": str(result_path)}
                )

            else:
                return JobResult(
                    success=False,
                    error=f"Onbekend export type: {export_type}"
                )

        except ImportError as e:
            return JobResult(
                success=False,
                error=f"Excel export niet beschikbaar: {e}"
            )
