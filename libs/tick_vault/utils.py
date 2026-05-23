from datetime import UTC, datetime, timedelta


def generate_hourly_datetimes(start: datetime, end: datetime) -> list[datetime]:
    """
    Generate all hourly datetime objects within a given range in UTC.

    Creates a list of UTC datetime objects, one for each hour from start to end,
    rounded down to the hour. The end hour is excluded to avoid including
    incomplete data (e.g., when called with datetime.now()).

    All input datetimes are normalized to UTC before processing. If naive datetimes
    are provided, they are assumed to be UTC. This avoids daylight saving time
    issues and ensures consistency with financial market data conventions.

    Args:
        start: The start datetime of the range (will be normalized to UTC and rounded down)
        end: The end datetime of the range (will be normalized to UTC and rounded down, excluded)

    Returns:
        list[datetime]: A list of timezone-aware UTC datetime objects, one for each
            complete hour in the range [start_hour, end_hour). Returns empty list
            if the rounded start and end are the same hour.

    Raises:
        TypeError: If start or end are not datetime objects
        ValueError: If end is before or equal to start

    Examples:
        >>> start = datetime(2024, 3, 2, 12, 30)
        >>> end = datetime(2024, 3, 2, 15, 45)
        >>> generate_hourly_datetimes(start, end)
        [datetime(2024, 3, 2, 12, 0, tzinfo=timezone.utc),
         datetime(2024, 3, 2, 13, 0, tzinfo=timezone.utc),
         datetime(2024, 3, 2, 14, 0, tzinfo=timezone.utc),
         datetime(2024, 3, 2, 15, 0, tzinfo=timezone.utc)]

        >>> # Same hour after rounding
        >>> start = datetime(2024, 3, 2, 12, 15)
        >>> end = datetime(2024, 3, 2, 12, 45)
        >>> generate_hourly_datetimes(start, end)
        []

        >>> # Handles timezone-aware datetimes
        >>> from datetime import timezone
        >>> start = datetime(2024, 3, 2, 12, 0, tzinfo=timezone.utc)
        >>> end = datetime(2024, 3, 2, 14, 0, tzinfo=timezone.utc)
        >>> result = generate_hourly_datetimes(start, end)
        >>> all(dt.tzinfo == timezone.utc for dt in result)
        True
    """
    if not isinstance(start, datetime):
        raise TypeError(f"Start must be a datetime object, got {type(start).__name__}")

    if not isinstance(end, datetime):
        raise TypeError(f"End must be a datetime object, got {type(end).__name__}")

    if end <= start:
        raise ValueError(
            f"End datetime must be after start datetime. Got start={start}, end={end}"
        )

    # Normalize to UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    else:
        start = start.astimezone(UTC)

    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    else:
        end = end.astimezone(UTC)

    # Round down to the hour
    current = start.replace(minute=0, second=0, microsecond=0)
    end_hour = end.replace(minute=0, second=0, microsecond=0)

    datetimes: list[datetime] = []
    while current < end_hour:
        datetimes.append(current)
        current += timedelta(hours=1)

    return datetimes


def format_relative_tick_path(symbol: str, time: datetime) -> str:
    """
    Format a relative path for tick data following Dukascopy's structure.

    Dukascopy uses 0-indexed months (00-11 instead of 01-12) in their URL
    and filesystem structure. This function ensures consistent path formatting
    across both URL generation and local file storage.

    Args:
        symbol: Trading pair symbol (e.g., 'XAUUSD', 'EURUSD', 'BTCUSD')
        time: The datetime for the tick data hour (should be rounded to the hour)

    Returns:
        str: A relative path string in the format:
            {SYMBOL}/{YEAR}/{MONTH}/{DAY}/{HOUR}h_ticks.bi5
            where MONTH is 0-indexed (00-11)

    Examples:
        >>> format_relative_tick_path('XAUUSD', datetime(2024, 3, 2, 12))
        'XAUUSD/2024/02/02/12h_ticks.bi5'

        >>> # January is month 0
        >>> format_relative_tick_path('EURUSD', datetime(2024, 1, 15, 9))
        'EURUSD/2024/00/15/09h_ticks.bi5'

        >>> # December is month 11
        >>> format_relative_tick_path('BTCUSD', datetime(2024, 12, 31, 23))
        'BTCUSD/2024/11/31/23h_ticks.bi5'

    Note:
        This function does NOT validate that the datetime is rounded to the hour.
        If you need validation, use the TickChunk model which enforces this constraint.

        The 0-indexed month convention matches Dukascopy's datafeed structure:
        - January = 00
        - February = 01
        - ...
        - December = 11
    """
    year = time.year
    month = time.month - 1  # Dukascopy uses 0-indexed months (00-11)
    day = time.day
    hour = time.hour
    return f"{symbol}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
