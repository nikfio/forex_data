"""Reader module for loading and decoding tick data into pandas DataFrames."""

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
from tqdm import tqdm

from .constants import PIPET_SIZE_REGISTRY
from .decoder import decode_chunk
from .logger import logger
from .metadata import MetadataDB


def read_tick_data(
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    pipet_scale: float | None = None,
    strict: bool = True,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Read and decode tick data for a symbol within a time range.

    This function:
    1. Determines the actual time range based on start/end parameters and available data
    2. Verifies data continuity by checking for gaps in the metadata database
    3. Retrieves all available chunks within the specified time range
    4. Decodes each chunk from compressed binary format
    5. Combines all decoded ticks into a single pandas DataFrame

    The returned DataFrame is sorted by time and contains one row per tick with
    ask/bid prices and volumes.

    Args:
        symbol: The trading pair symbol (e.g., 'XAUUSD', 'EURUSD', 'BTCUSD')
        start: Start datetime of the range (inclusive, rounded down to hour).
            If None, uses the first available timestamp in the database.
        end: End datetime of the range (exclusive, rounded down to hour).
            If None, uses the last available timestamp in the database.
        pipet_scale: Optional price scaling factor. If None, uses the value from
            PIPET_SIZE_REGISTRY for the symbol. This scales the raw integer prices
            to actual floating-point prices.
        strict: If True, raises an error if start/end are outside the available
            data range or if gaps exist. If False, clips start/end to the available
            data range and only checks for gaps within that clipped range.
        show_progress: If true, shows the progressbar

    Returns:
        pd.DataFrame: DataFrame with columns:
            - time (datetime): Tick timestamp
            - ask (float): Ask price
            - bid (float): Bid price
            - ask_volume (int): Ask volume
            - bid_volume (int): Bid volume
            Sorted by time in ascending order. Returns empty DataFrame if no data
            is available in the range.

    Raises:
        ValueError: If no data exists for the symbol in the database, or if strict=True
            and start is before the first available data or end is after the last
            available data.
        RuntimeError: If gaps are detected in the data (missing hours between
            first and last downloaded chunks) within the specified range.
        FileNotFoundError: If a chunk file is missing from disk despite being
            marked as available in the metadata database.

    Example:
        >>> from datetime import datetime
        >>>
        >>> # Read all available data
        >>> df = read_tick_data(symbol='XAUUSD')
        >>>
        >>> # Read specific date range
        >>> df = read_tick_data(
        ...     symbol='XAUUSD',
        ...     start=datetime(2024, 3, 1, 0),
        ...     end=datetime(2024, 3, 2, 0)
        ... )
        >>>
        >>> # Non-strict mode: clips to available data if out of range
        >>> df = read_tick_data(
        ...     symbol='XAUUSD',
        ...     start=datetime(2020, 1, 1),  # May be before first available data
        ...     end=datetime(2030, 12, 31),  # May be after last available data
        ...     strict=False
        ... )
        >>>
        >>> # Custom pipet scale for non-standard symbols
        >>> df = read_tick_data(
        ...     symbol='CUSTOM',
        ...     start=datetime(2024, 3, 1),
        ...     end=datetime(2024, 3, 2),
        ...     pipet_scale=0.01
        ... )
    """
    logger.info(f"Reading tick data for {symbol}")

    # Get pipet scale for price conversion
    if pipet_scale is None:
        if symbol not in PIPET_SIZE_REGISTRY:
            raise ValueError(
                f"Pipet scale is not registered for {symbol} symbol. "
                "Manually pass it or add it to PIPET_SIZE_REGISTRY registry."
            )
        pipet_scale = PIPET_SIZE_REGISTRY[symbol]

    # Initialize database and get the first and last available timestamps
    with MetadataDB() as db:
        first_chunk = db.first_chunk(symbol)
        last_chunk = db.last_chunk(symbol)

        # Check if any data exists for this symbol
        if first_chunk is None or last_chunk is None:
            logger.error(f"No data found for symbol {symbol}")
            raise ValueError(f"No data available for symbol {symbol} in the database")

        first_time = first_chunk.time
        last_time = last_chunk.time

        logger.debug(
            f"Available data range for {symbol}: {first_time.isoformat()} to {last_time.isoformat()}"
        )

        # Process start parameter
        if start is None:
            # Use first available timestamp
            start = first_time
            logger.debug(
                f"Start not specified, using first available: {start.isoformat()}"
            )
        else:
            # Ensure UTC
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            else:
                start = start.astimezone(UTC)
            if strict:
                # In strict mode, verify start is within available range
                if start < first_time:
                    raise ValueError(
                        f"Requested start time {start.isoformat()} is before the first available "
                        f"data at {first_time.isoformat()}. Use strict=False to clip to available range."
                    )
                logger.debug(f"Start validated in strict mode: {start.isoformat()}")
            else:
                # In non-strict mode, clip start to available range
                if start < first_time:
                    logger.warning(
                        f"Requested start time {start.isoformat()} is before first available data "
                        f"at {first_time.isoformat()}. Clipping to first available."
                    )
                    start = first_time
                else:
                    logger.debug(f"Using specified start: {start.isoformat()}")

        # Process end parameter
        if end is None:
            # Use last available timestamp (add 1 hour to make it exclusive)
            end = last_time + timedelta(hours=1)
            logger.debug(
                f"End not specified, using last available + 1 hour: {end.isoformat()}"
            )
        else:
            # Ensure UTC
            if end.tzinfo is None:
                end = end.replace(tzinfo=UTC)
            else:
                end = end.astimezone(UTC)

            if strict:
                # In strict mode, verify end is within available range

                last_time_exclusive = last_time + timedelta(hours=1)
                if end > last_time_exclusive:
                    raise ValueError(
                        f"Requested end time {end.isoformat()} is after the last available "
                        f"data at {last_time.isoformat()}. Use strict=False to clip to available range."
                    )
                logger.debug(f"End validated in strict mode: {end.isoformat()}")
            else:
                # In non-strict mode, clip end to available range

                last_time_exclusive = last_time + timedelta(hours=1)
                if end > last_time_exclusive:
                    logger.warning(
                        f"Requested end time {end.isoformat()} is after last available data "
                        f"at {last_time.isoformat()}. Clipping to last available + 1 hour."
                    )
                    end = last_time_exclusive
                else:
                    logger.debug(f"Using specified end: {end.isoformat()}")

        logger.info(f"Final time range for {symbol}: {start.date()} to {end.date()}")

        # Check for gaps in the data - raises RuntimeError if gaps exist
        logger.debug(f"Checking for data gaps in {symbol}")
        db.check_for_gaps(symbol, start, end)

        # Get all available chunks in the time range
        logger.debug(f"Retrieving available chunks for {symbol}")
        chunks = db.get_available_chunks(symbol, start, end)

    # Handle empty result
    if not chunks:
        logger.warning(
            f"No data available for {symbol} in range {start.date()} to {end.date()}"
        )
        return pd.DataFrame(columns=["time", "ask", "bid", "ask_volume", "bid_volume"])

    logger.info(f"Decoding {len(chunks)} chunks for {symbol}")

    # Decode all chunks and collect arrays
    array_results = []

    with tqdm(
        total=len(chunks),
        desc=f"Reading {symbol}",
        unit="chunk",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        colour="green",
        disable=not show_progress,
    ) as pbar:
        for chunk in chunks:
            try:
                array = decode_chunk(chunk, pipet_scale=pipet_scale)
                array_results.append(array)
                pbar.update(1)
            except FileNotFoundError:
                pbar.close()
                logger.error(
                    f"Chunk file missing for {chunk.symbol} at {chunk.time.isoformat()}"
                )
                raise
            except Exception as e:
                pbar.close()
                logger.error(
                    f"Failed to decode chunk {chunk.symbol} at {chunk.time.isoformat()}: {e}"
                )
                raise

    # Concatenate all structured arrays and convert to DataFrame
    logger.info("Concatenating the results and building DataFrame.")
    df = pd.DataFrame(np.concatenate(array_results))

    logger.info(
        f"Successfully loaded {len(df)} ticks for {symbol} "
        f"from {df['time'].min()} to {df['time'].max()}"
    )

    return df
