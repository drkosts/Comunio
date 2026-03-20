"""Base constants and shared helpers for crud modules."""

from typing import Dict, Tuple

# Configure logging for debugging market value calculations
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Season date ranges configuration
SEASON_DATE_RANGES: Dict[str, Tuple[str, str]] = {
    "2024/2025": ("2024-07-01", "2025-06-30"),
    "2023/2024": ("2023-06-01", "2024-06-30"),
    "2025/2026": ("2025-06-30", "2026-06-30"),
}

DEFAULT_DATE_RANGE = ("2000-01-01", "2030-01-01")


def get_date_range(spielzeit: str) -> Tuple[str, str]:
    """Get date range for a given spielzeit (season)"""
    return SEASON_DATE_RANGES.get(spielzeit, DEFAULT_DATE_RANGE)
