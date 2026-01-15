"""
Job Queue systeem voor agent communicatie.
Gebruikt SQLite voor persistente job storage.
"""
import sqlite3
import json
import uuid
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, Any
from pathlib import Path
import asyncio
import logging

from ..config.settings import settings

logger = logging.getLogger(__name__)


class JobType(str, Enum):
    """Beschikbare job types."""
    # Data Agent
    COLLECT_ACCOUNT = "collect_account"
    COLLECT_HISTORICAL = "collect_historical"
    UPDATE_FOLLOWERS = "update_followers"
    UPDATE_POST_ENGAGEMENT = "update_post_engagement"

    # Analyse Agent
    CALCULATE_MONTHLY = "calculate_monthly"
    CALCULATE_BENCHMARKS = "calculate_benchmarks"
    DETECT_ANOMALIES = "detect_anomalies"

    # Rapport Agent
    GENERATE_DASHBOARD_DATA = "generate_dashboard_data"
    GENERATE_PDF = "generate_pdf"
    EXPORT_EXCEL = "export_excel"


class JobStatus(str, Enum):
    """Job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    """Job definitie."""
    id: str
    type: JobType
    priority: int  # 1 = hoogste, 10 = laagste
    status: JobStatus
    payload: dict
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    result: Optional[dict] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "type": self.type.value if isinstance(self.type, JobType) else self.type,
            "priority": self.priority,
            "status": self.status.value if isinstance(self.status, JobStatus) else self.status,
            "payload": json.dumps(self.payload),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "result": json.dumps(self.result) if self.result else None,
        }

    @classmethod
    def from_row(cls, row: tuple) -> "Job":
        """Create Job from database row."""
        return cls(
            id=row[0],
            type=JobType(row[1]),
            priority=row[2],
            status=JobStatus(row[3]),
            payload=json.loads(row[4]) if row[4] else {},
            created_at=datetime.fromisoformat(row[5]) if row[5] else None,
            started_at=datetime.fromisoformat(row[6]) if row[6] else None,
            completed_at=datetime.fromisoformat(row[7]) if row[7] else None,
            error=row[8],
            retries=row[9] or 0,
            max_retries=row[10] or 3,
            result=json.loads(row[11]) if row[11] else None,
        )


@dataclass
class JobResult:
    """Resultaat van een job."""
    success: bool
    message: Optional[str] = None
    data: Optional[dict] = None
    error: Optional[str] = None


class JobQueue:
    """
    Persistente job queue met SQLite backend.
    Thread-safe en ondersteunt priorities.
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        priority INTEGER DEFAULT 5,
        status TEXT DEFAULT 'pending',
        payload TEXT,
        created_at TEXT,
        started_at TEXT,
        completed_at TEXT,
        error TEXT,
        retries INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        result TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority);
    CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or settings.job_queue_path
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.executescript(self.SCHEMA)
        logger.info(f"Job queue database geinitialiseerd: {self.db_path}")

    def _get_conn(self) -> sqlite3.Connection:
        """Get database connection."""
        return sqlite3.connect(str(self.db_path))

    async def enqueue(
        self,
        job_type: JobType,
        payload: dict,
        priority: int = 5,
        max_retries: int = 3
    ) -> Job:
        """Voeg een nieuwe job toe aan de queue."""
        job = Job(
            id=str(uuid.uuid4()),
            type=job_type,
            priority=priority,
            status=JobStatus.PENDING,
            payload=payload,
            created_at=datetime.now(),
            max_retries=max_retries,
        )

        async with self._lock:
            with self._get_conn() as conn:
                data = job.to_dict()
                conn.execute("""
                    INSERT INTO jobs (id, type, priority, status, payload, created_at, max_retries)
                    VALUES (:id, :type, :priority, :status, :payload, :created_at, :max_retries)
                """, data)
                conn.commit()

        logger.debug(f"Job toegevoegd: {job.type.value} (id={job.id[:8]})")
        return job

    async def get_next(self, job_types: Optional[list[JobType]] = None) -> Optional[Job]:
        """
        Haal de volgende beschikbare job op.
        Markeert de job als RUNNING.
        """
        async with self._lock:
            with self._get_conn() as conn:
                # Build query
                query = """
                    SELECT id, type, priority, status, payload, created_at,
                           started_at, completed_at, error, retries, max_retries, result
                    FROM jobs
                    WHERE status = 'pending'
                """
                params = []

                if job_types:
                    placeholders = ",".join(["?" for _ in job_types])
                    query += f" AND type IN ({placeholders})"
                    params.extend([jt.value for jt in job_types])

                query += " ORDER BY priority ASC, created_at ASC LIMIT 1"

                cursor = conn.execute(query, params)
                row = cursor.fetchone()

                if not row:
                    return None

                job = Job.from_row(row)

                # Mark as running
                conn.execute("""
                    UPDATE jobs SET status = 'running', started_at = ?
                    WHERE id = ?
                """, [datetime.now().isoformat(), job.id])
                conn.commit()

                job.status = JobStatus.RUNNING
                job.started_at = datetime.now()

                logger.debug(f"Job gestart: {job.type.value} (id={job.id[:8]})")
                return job

    async def complete(self, job_id: str, result: JobResult):
        """Markeer een job als voltooid."""
        async with self._lock:
            with self._get_conn() as conn:
                status = JobStatus.COMPLETED if result.success else JobStatus.FAILED
                conn.execute("""
                    UPDATE jobs
                    SET status = ?, completed_at = ?, result = ?, error = ?
                    WHERE id = ?
                """, [
                    status.value,
                    datetime.now().isoformat(),
                    json.dumps(result.data) if result.data else None,
                    result.error,
                    job_id
                ])
                conn.commit()

        log_func = logger.debug if result.success else logger.warning
        log_func(f"Job {'voltooid' if result.success else 'gefaald'}: {job_id[:8]}")

    async def retry(self, job: Job) -> bool:
        """
        Retry een gefaalde job.
        Returns True als retry is ingepland, False als max retries bereikt.
        """
        if job.retries >= job.max_retries:
            return False

        async with self._lock:
            with self._get_conn() as conn:
                conn.execute("""
                    UPDATE jobs
                    SET status = 'pending', retries = retries + 1,
                        started_at = NULL, completed_at = NULL, error = NULL
                    WHERE id = ?
                """, [job.id])
                conn.commit()

        logger.info(f"Job retry gepland: {job.id[:8]} (poging {job.retries + 1})")
        return True

    async def cancel(self, job_id: str):
        """Annuleer een job."""
        async with self._lock:
            with self._get_conn() as conn:
                conn.execute("""
                    UPDATE jobs SET status = 'cancelled' WHERE id = ?
                """, [job_id])
                conn.commit()

    async def get_pending_count(self, job_types: Optional[list[JobType]] = None) -> int:
        """Tel aantal pending jobs."""
        with self._get_conn() as conn:
            query = "SELECT COUNT(*) FROM jobs WHERE status = 'pending'"
            params = []

            if job_types:
                placeholders = ",".join(["?" for _ in job_types])
                query += f" AND type IN ({placeholders})"
                params.extend([jt.value for jt in job_types])

            cursor = conn.execute(query, params)
            return cursor.fetchone()[0]

    async def get_running_count(self) -> int:
        """Tel aantal running jobs."""
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'running'")
            return cursor.fetchone()[0]

    async def wait_for_completion(
        self,
        job_types: Optional[list[JobType]] = None,
        timeout: float = 3600
    ) -> bool:
        """
        Wacht tot alle jobs van bepaalde types voltooid zijn.
        Returns True als alles voltooid, False bij timeout.
        """
        start = datetime.now()
        while True:
            pending = await self.get_pending_count(job_types)
            running = await self.get_running_count()

            if pending == 0 and running == 0:
                return True

            elapsed = (datetime.now() - start).total_seconds()
            if elapsed > timeout:
                return False

            await asyncio.sleep(1)

    async def get_status_summary(self) -> dict[str, int]:
        """Haal status samenvatting op."""
        with self._get_conn() as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) FROM jobs GROUP BY status
            """)
            return {row[0]: row[1] for row in cursor.fetchall()}

    async def cleanup_old_jobs(self, days: int = 30):
        """Verwijder oude voltooide jobs."""
        with self._get_conn() as conn:
            conn.execute("""
                DELETE FROM jobs
                WHERE status IN ('completed', 'failed', 'cancelled')
                AND completed_at < datetime('now', ?)
            """, [f"-{days} days"])
            deleted = conn.total_changes
            conn.commit()

        if deleted > 0:
            logger.info(f"{deleted} oude jobs verwijderd")

    async def clear_all(self):
        """Verwijder alle jobs (voor testing)."""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM jobs")
            conn.commit()
        logger.warning("Alle jobs verwijderd")
