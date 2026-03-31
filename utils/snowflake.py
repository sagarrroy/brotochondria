"""
Brotochondria — Snowflake ↔ Datetime conversion
Discord Snowflakes encode timestamps. We use this for date filtering and checkpoints.
"""
from datetime import datetime, timezone

DISCORD_EPOCH = 1420070400000  # 2015-01-01T00:00:00+00:00 in ms


def snowflake_to_datetime(snowflake: int) -> datetime:
    """Convert a Discord Snowflake ID to a UTC datetime."""
    timestamp_ms = (snowflake >> 22) + DISCORD_EPOCH
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def datetime_to_snowflake(dt: datetime) -> int:
    """Convert a datetime to a synthetic Discord Snowflake."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    timestamp_ms = int(dt.timestamp() * 1000)
    return (timestamp_ms - DISCORD_EPOCH) << 22


def date_str_to_snowflake(date_str: str) -> int:
    """Convert an ISO date string (e.g. '2023-01-01') to a synthetic Snowflake."""
    dt = datetime.fromisoformat(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return datetime_to_snowflake(dt)
