"""
Analysis module voor metrics berekening en trend analyse.
"""
from .metrics import calculate_engagement_rate, calculate_monthly_metrics
from .trends import analyze_trends, TrendDirection
from .benchmarks import calculate_benchmarks, BenchmarkResult

__all__ = [
    "calculate_engagement_rate",
    "calculate_monthly_metrics",
    "analyze_trends",
    "TrendDirection",
    "calculate_benchmarks",
    "BenchmarkResult",
]
