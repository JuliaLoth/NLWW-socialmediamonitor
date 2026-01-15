"""
Base Agent class voor het multi-agent systeem.
"""
from abc import ABC, abstractmethod
from typing import Optional
import asyncio
import logging

from ..database.connection import Database, get_connection
from .job_queue import JobQueue, Job, JobType, JobResult, JobStatus

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Abstract base class voor alle agents.

    Elke agent:
    - Pollt de job queue voor taken
    - Verwerkt taken van specifieke types
    - Rapporteert resultaten terug
    """

    def __init__(
        self,
        job_queue: JobQueue,
        db: Optional[Database] = None,
        name: Optional[str] = None
    ):
        self.job_queue = job_queue
        self.db = db or get_connection()
        self.name = name or self.__class__.__name__
        self.running = False
        self._current_job: Optional[Job] = None

    @abstractmethod
    def get_job_types(self) -> list[JobType]:
        """
        Welke job types handelt deze agent af.
        Override in subclass.
        """
        pass

    @abstractmethod
    async def process_job(self, job: Job) -> JobResult:
        """
        Verwerk een job en return het resultaat.
        Override in subclass.
        """
        pass

    async def run(self, poll_interval: float = 1.0):
        """
        Main agent loop.
        Pollt continue de job queue en verwerkt taken.
        """
        self.running = True
        job_types = self.get_job_types()

        logger.info(f"{self.name} gestart, luistert naar: {[jt.value for jt in job_types]}")

        while self.running:
            try:
                # Haal volgende job op
                job = await self.job_queue.get_next(job_types)

                if job:
                    self._current_job = job
                    logger.info(f"{self.name} verwerkt job: {job.type.value} (id={job.id[:8]})")

                    try:
                        # Verwerk de job
                        result = await self.process_job(job)

                        # Rapporteer resultaat
                        await self.job_queue.complete(job.id, result)

                        if not result.success and job.retries < job.max_retries:
                            # Schedule retry
                            await self.job_queue.retry(job)

                    except Exception as e:
                        # Job processing failed
                        logger.error(f"{self.name} job fout: {e}", exc_info=True)
                        await self.job_queue.complete(
                            job.id,
                            JobResult(success=False, error=str(e))
                        )

                        if job.retries < job.max_retries:
                            await self.job_queue.retry(job)

                    finally:
                        self._current_job = None
                else:
                    # Geen jobs beschikbaar, wacht even
                    await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                logger.info(f"{self.name} gestopt door cancellation")
                break
            except Exception as e:
                logger.error(f"{self.name} loop fout: {e}", exc_info=True)
                await asyncio.sleep(poll_interval)

        logger.info(f"{self.name} gestopt")

    def stop(self):
        """Stop de agent loop."""
        self.running = False
        logger.info(f"{self.name} stop aangevraagd")

    async def run_once(self) -> Optional[JobResult]:
        """
        Verwerk precies 1 job en stop.
        Handig voor testing of handmatige runs.
        """
        job_types = self.get_job_types()
        job = await self.job_queue.get_next(job_types)

        if not job:
            return None

        logger.info(f"{self.name} verwerkt single job: {job.type.value}")

        try:
            result = await self.process_job(job)
            await self.job_queue.complete(job.id, result)
            return result
        except Exception as e:
            logger.error(f"{self.name} job fout: {e}")
            result = JobResult(success=False, error=str(e))
            await self.job_queue.complete(job.id, result)
            return result

    @property
    def current_job(self) -> Optional[Job]:
        """Huidige job die verwerkt wordt."""
        return self._current_job

    async def get_pending_jobs(self) -> int:
        """Aantal wachtende jobs voor deze agent."""
        return await self.job_queue.get_pending_count(self.get_job_types())
