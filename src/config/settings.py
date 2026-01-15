"""
Configuratie instellingen voor NL Embassy Social Media Monitor.
"""
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import os


# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
EXPORTS_DIR = DATA_DIR / "exports"
DB_PATH = DATA_DIR / "embassy_monitor.duckdb"
JOB_QUEUE_PATH = DATA_DIR / "job_queue.sqlite"

# Config files
ACCOUNTS_CONFIG = PROJECT_ROOT / "src" / "config" / "accounts.yaml"


@dataclass
class RateLimitConfig:
    """Rate limiting configuratie per platform."""
    requests_per_minute: int
    daily_max: int
    min_delay_seconds: float = 1.0


@dataclass
class Settings:
    """Applicatie-brede instellingen."""

    # Database
    db_path: Path = DB_PATH
    job_queue_path: Path = JOB_QUEUE_PATH

    # Rate limits per platform
    rate_limits: dict = None

    # Collection settings
    historical_months: int = 12  # Hoeveel maanden terug te gaan
    post_update_days: int = 7    # Dagen om engagement te blijven updaten
    max_retries: int = 3

    # Nitter instances voor Twitter scraping
    nitter_instances: list = None

    def __post_init__(self):
        if self.rate_limits is None:
            self.rate_limits = {
                "instagram": RateLimitConfig(
                    requests_per_minute=10,
                    daily_max=200,
                    min_delay_seconds=6.0
                ),
                "facebook": RateLimitConfig(
                    requests_per_minute=5,
                    daily_max=100,
                    min_delay_seconds=12.0
                ),
                "twitter": RateLimitConfig(
                    requests_per_minute=20,
                    daily_max=500,
                    min_delay_seconds=3.0
                ),
            }

        if self.nitter_instances is None:
            # Publieke Nitter instances (kunnen veranderen)
            self.nitter_instances = [
                "https://nitter.poast.org",
                "https://nitter.privacydev.net",
                "https://nitter.woodland.cafe",
            ]

        # Zorg dat directories bestaan
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


# Singleton instance
settings = Settings()


# Platform namen in Nederlands
PLATFORM_NAMES_NL = {
    "instagram": "Instagram",
    "facebook": "Facebook",
    "twitter": "X (Twitter)",
    "x": "X (Twitter)",
}

# Land namen in Nederlands (MFA accounts)
COUNTRY_NAMES_NL = {
    "nederland": "Nederland",
    "turkije": "Turkije",
    "india": "India",
    "china": "China",
    "indonesie": "Indonesie",
    "filipijnen": "Filipijnen",
    "marokko": "Marokko",
    "zuid_afrika": "Zuid-Afrika",
    "vae": "VAE",
    "ksa": "Saoedi-Arabie",
    "vk": "Verenigd Koninkrijk",
    "suriname": "Suriname",
    "thailand": "Thailand",
    "rusland": "Rusland",
    "ghana": "Ghana",
    "iran": "Iran",
    "koeweit": "Koeweit",
    "egypte": "Egypte",
    "pakistan": "Pakistan",
    "jordanie": "Jordanie",
    "oman": "Oman",
}
