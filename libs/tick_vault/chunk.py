from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from .config import CONFIG
from .constants import DUKASCOPY_DATA_FEED_BASE
from .logger import logger
from .utils import format_relative_tick_path


class TickChunk(BaseModel, frozen=True):
    """
    Represents a single hour of tick data for a trading symbol.

    This model encapsulates the symbol and time information needed to fetch
    or store tick data from Dukascopy's datafeed. It ensures data integrity
    by validating that times are rounded to the hour.

    Attributes:
        symbol: Trading pair symbol (e.g., 'XAUUSD', 'EURUSD', 'BTCUSD')
        time: The hour for which to fetch data (must be rounded to the hour)
    """

    symbol: str = Field(
        description="Trading pair symbol (e.g., 'XAUUSD', 'EURUSD', 'BTCUSD')",
        min_length=1,
    )

    time: datetime = Field(
        description="The hour for which to fetch data (must be rounded to the hour)",
    )

    @field_validator("time", mode="before")
    @classmethod
    def validate_and_normalize_time(cls, time: datetime) -> datetime:
        """
        Validate that the datetime is rounded to the hour and normalize to UTC.

        The datetime must have zero minutes, seconds, and microseconds.
        If the datetime is naive (no timezone), it's assumed to be UTC and
        converted to a timezone-aware UTC datetime. If it's already timezone-aware,
        it's converted to UTC.

        This ensures all tick data timestamps are in UTC, avoiding daylight saving
        time issues and maintaining consistency with financial market conventions.

        Args:
            time: The datetime to validate and normalize

        Returns:
            datetime: The validated datetime in UTC with tzinfo=timezone.utc

        Raises:
            ValueError: If datetime contains non-zero minutes, seconds, or microseconds
            TypeError: if time is not a datetime

        Examples:
            Valid: datetime(2024, 3, 2, 12, 0, 0, 0) -> normalized to UTC
            Valid: datetime(2024, 3, 2, 12, 0, 0, 0, tzinfo=timezone.utc) -> already UTC
            Invalid: datetime(2024, 3, 2, 12, 30, 0, 0) -> raises ValueError
        """
        if not isinstance(time, datetime):
            raise TypeError(
                "Expected datetime object for time but got"
                f" {time} which is {type(time).__name__}"
            )

        if time.minute != 0 or time.second != 0 or time.microsecond != 0:
            raise ValueError(
                f"Datetime must be rounded to the hour (minute=0, second=0, microsecond=0). "
                f"Got: {time} (minute={time.minute}, second={time.second}, microsecond={time.microsecond})"
            )

        # Normalize to UTC
        if time.tzinfo is None:
            # Naive datetime - assume UTC
            return time.replace(tzinfo=UTC)
        else:
            # Timezone-aware - convert to UTC
            return time.astimezone(UTC)

    @property
    def url(self) -> str:
        """
        Build the Dukascopy datafeed URL for this tick chunk.

        The URL follows Dukascopy's structure:
        https://datafeed.dukascopy.com/datafeed/{SYMBOL}/{YEAR}/{MONTH}/{DAY}/{HOUR}h_ticks.bi5

        Returns:
            str: The complete URL to fetch the data file

        Example:
            >>> chunk = TickChunk(symbol='XAUUSD', time=datetime(2024, 3, 2, 12))
            >>> chunk.url
            'https://datafeed.dukascopy.com/datafeed/XAUUSD/2024/02/02/12h_ticks.bi5'
        """
        return DUKASCOPY_DATA_FEED_BASE + format_relative_tick_path(
            self.symbol, self.time
        )

    def path(self, base: str | Path | None = None) -> Path:
        """
        Build a local filesystem path mirroring Dukascopy's structure.

        The path follows the same hierarchy as the source:
        {base}/{SYMBOL}/{YEAR}/{MONTH}/{DAY}/{HOUR}h_ticks.bi5

        Args:
            base: The base directory where data files are stored. If None,
                uses CONFIG.save_directory. Must be a directory, not a file.

        Returns:
            Path: The complete filesystem path for storing the data file

        Raises:
            ValueError: If base is provided and points to a file instead of a directory

        Example:
            >>> chunk = TickChunk(symbol='XAUUSD', time=datetime(2024, 3, 2, 12))
            >>> chunk.path()  # Uses CONFIG.save_directory
            PosixPath('data/XAUUSD/2024/02/02/12h_ticks.bi5')
            >>> chunk.path('./custom_dir')  # Uses provided directory
            PosixPath('custom_dir/XAUUSD/2024/02/02/12h_ticks.bi5')
        """
        if base:
            base_dir = Path(base)
            if base_dir.is_file():
                raise ValueError(f"Expected a directory for the base path, got: {base}")
        else:
            base_dir = CONFIG.save_directory

        return base_dir / format_relative_tick_path(self.symbol, self.time)

    def save(self, content: bytes, base: str | Path | None = None) -> None:
        """
        Save tick data content to disk.

        Creates parent directories if they don't exist. Overwrites any previous data.

        Args:
            content: The raw tick data bytes to save
            base: The base directory where data files are stored. If None,
                uses CONFIG.save_directory. Must be a directory, not a file.

        Raises:
            TypeError: If content is not bytes
            ValueError: If content is empty

        Example:
            >>> chunk = TickChunk(symbol='XAUUSD', time=datetime(2024, 3, 2, 12))
            >>> chunk.save(b'\\x00\\x01\\x02')
        """
        if not isinstance(content, bytes):
            raise TypeError(f"Content must be bytes, got {type(content).__name__}")

        if not content:
            raise ValueError("Got empty content")

        file_path = self.path(base)
        logger.debug(f"Saving chunk to {file_path}")

        # Create parent directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content to file
        file_path.write_bytes(content)

    def load(self, base: str | Path | None = None) -> bytes:
        """
        Load tick data content from disk.

        Returns:
            bytes: The raw tick data bytes
            base: The base directory where data files are stored. If None,
                uses CONFIG.save_directory. Must be a directory, not a file.

        Raises:
            FileNotFoundError: If the file doesn't exist on disk

        Example:
            >>> chunk = TickChunk(symbol='XAUUSD', time=datetime(2024, 3, 2, 12))
            >>> data = chunk.load()
            >>> isinstance(data, bytes)
            True
        """
        file_path = self.path(base)
        logger.debug(f"Loading chunk from {file_path}")

        if not file_path.exists():
            raise FileNotFoundError(
                f"File not found: {file_path}. "
                f"Chunk for {self.symbol} at {self.time.isoformat()} has not been downloaded."
            )

        return file_path.read_bytes()
