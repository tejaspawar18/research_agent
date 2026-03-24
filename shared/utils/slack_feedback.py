"""Helpers for correlating Slack feedback to stored article messages."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
import uuid


POSITIVE_REACTIONS = {
    "+1",
    "thumbsup",
    "white_check_mark",
    "heavy_check_mark",
    "100",
    "star",
    "tada",
    "clap",
}

NEGATIVE_REACTIONS = {
    "-1",
    "thumbsdown",
    "x",
    "no_entry",
    "confused",
}


def get_week_year(dt: Optional[date] = None) -> str:
    """Return ISO week partition key, for example ``2026-W06``."""
    if dt is None:
        dt = date.today()
    iso_cal = dt.isocalendar()
    return f"{iso_cal[0]}-W{iso_cal[1]:02d}"


def get_week_year_from_message_ts(message_ts: Optional[str]) -> str:
    """Return the week key derived from a Slack message timestamp."""
    if not message_ts:
        return get_week_year()

    try:
        ts_value = float(message_ts)
    except (TypeError, ValueError):
        return get_week_year()

    message_date = datetime.fromtimestamp(ts_value, tz=timezone.utc).date()
    return get_week_year(message_date)


def get_candidate_week_years(message_ts: Optional[str]) -> List[str]:
    """Return likely week partitions for a Slack timestamp.

    We include adjacent UTC dates to be resilient around ISO-week boundaries and
    older rows written using local-date based partitioning.
    """
    if not message_ts:
        return [get_week_year()]

    try:
        base_date = datetime.fromtimestamp(float(message_ts), tz=timezone.utc).date()
    except (TypeError, ValueError):
        return [get_week_year()]

    candidates: List[str] = []
    for offset in (0, -1, 1):
        candidate = get_week_year(base_date + timedelta(days=offset))
        if candidate not in candidates:
            candidates.append(candidate)

    current_week = get_week_year()
    if current_week not in candidates:
        candidates.append(current_week)

    return candidates


def map_reaction_to_feedback_type(reaction_name: Optional[str]) -> str:
    """Map a Slack reaction name to a normalized feedback type."""
    normalized = (reaction_name or "").strip().lower()
    if normalized in POSITIVE_REACTIONS:
        return "positive"
    if normalized in NEGATIVE_REACTIONS:
        return "negative"
    return "reaction"


def build_feedback_id(*parts: Optional[str]) -> uuid.UUID:
    """Build a deterministic feedback id so Slack event retries stay idempotent."""
    seed = "|".join((part or "").strip() for part in parts)
    return uuid.uuid5(uuid.NAMESPACE_URL, seed)
