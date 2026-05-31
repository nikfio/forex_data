from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """
    Configuration settings for TickVault.

    Attributes:
        base_directory: Root directory for all TickVault data including downloads,
            metadata database, and logs. All subdirectories are created automatically.
            Default: ./tick_vault_data

        fetch_max_retry_attempts: Maximum number of fast retry attempts for failed
            HTTP requests before entering cooldown. Does not include the
            initial attempt.
            Recommended: 2-3 for typical network conditions.
            Default: 2

        fetch_base_retry_delay: Base delay in seconds for exponential backoff retry
            strategy. Actual delay is calculated as: base_delay * (2 ** attempt).
            For example, with base_delay=1.0:
            - 1st retry: 1.0s
            - 2nd retry: 2.0s
            - 3rd retry: 4.0s
            Default: 1.0

        fetch_cooldown_delay: Delay in seconds before retrying after all fast retry
            attempts are exhausted. Implements a "cool down and retry" strategy for
            temporary server unavailability (e.g., Dukascopy 503 errors).
            Default: 240.0 (4 minutes)

        fetch_cooldown_retries: Number of cooldown retry cycles after fast retries
            are exhausted. Each cycle waits fetch_cooldown_delay seconds, then
            re-attempts the full fast-retry sequence.
            Default: 3

        worker_per_proxy: Number of concurrent download workers to spawn per proxy.
            Higher values increase download speed but may trigger rate limiting.
            Total workers = worker_per_proxy * number_of_proxies.
            Recommended: 5-15 depending on network capacity and rate limits.
            Default: 10

        worker_queue_timeout: Maximum time in seconds that workers wait for queue
            operations before assuming the parent process has crashed. This prevents
            hanging workers when the orchestrator fails unexpectedly.
            Should be significantly larger than batch_timeout and cooldown_delay.
            Default: 300.0

        metadata_update_batch_timeout: Time in seconds to wait for accumulating
            chunks before flushing a partial batch to the database. Smaller values
            provide more frequent updates but increase database overhead.
            Must be less than worker_queue_timeout to prevent false timeouts.
            Default: 1.0

        metadata_update_batch_size: Number of download results to accumulate before
            performing a batch database write. Larger batches are more efficient but
            risk more data loss on crashes. Balance between performance and safety.
            Recommended: 50-200 depending on reliability requirements.
            Default: 100

        base_log_level: Minimum logging level for all log messages. Messages below
            this level are discarded entirely. Controls both console and file output.
            Default: DEBUG (capture everything)

        console_log_level: Minimum logging level for console output. This can be
            higher than base_log_level to reduce console noise while maintaining
            detailed file logs. Must be >= base_log_level.
            Default: INFO (important messages only)
    """

    model_config = SettingsConfigDict(
        env_prefix="TICK_VAULT_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    base_directory: Path = Field(
        default=Path("./tick_vault_data"),
        description="Root directory for all TickVault data",
    )

    fetch_max_retry_attempts: int = Field(
        default=2,
        ge=0,
        le=10,
        description="Maximum retry attempts for failed requests (0-10)",
    )

    fetch_base_retry_delay: float = Field(
        default=1.0,
        gt=0,
        le=60.0,
        description="Base delay in seconds for exponential backoff (0-60)",
    )

    fetch_cooldown_delay: float = Field(
        default=240.0,
        ge=0,
        le=600.0,
        description=(
            "Cooldown delay in seconds before retrying after fast "
            "retries exhausted (0-600)"
        ),
    )

    fetch_cooldown_retries: int = Field(
        default=3,
        ge=0,
        le=5,
        description=(
            "Number of cooldown retry cycles after fast "
            "retries exhausted (0-5)"
        ),
    )

    worker_per_proxy: int = Field(
        default=2,
        ge=1,
        le=100,
        description="Number of download workers per proxy (1-100)",
    )

    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        description="User-Agent header value mimicking a modern web browser",
    )

    request_pacing_min: float = Field(
        default=0.5,
        ge=0.0,
        description="Minimum randomized delay in seconds between HTTP requests",
    )

    request_pacing_max: float = Field(
        default=1.5,
        ge=0.0,
        description="Maximum randomized delay in seconds between HTTP requests",
    )

    request_timeout: float = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for HTTP requests",
    )

    worker_queue_timeout: float = Field(
        default=1800.0,
        gt=0,
        description="Timeout in seconds for worker queue operations",
    )

    metadata_update_batch_timeout: float = Field(
        default=1.0,
        gt=0,
        description="Timeout in seconds for batch accumulation",
    )

    metadata_update_batch_size: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of chunks per batch database write (1-10000)",
    )

    base_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="DEBUG",
        description="Base logging level for all outputs",
    )

    console_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Console logging level",
    )

    @model_validator(mode="after")
    def validate_timeouts(self) -> Self:
        """
        Ensure batch timeout is less than worker queue timeout.

        The metadata worker uses batch_timeout for accumulating chunks and
        worker_queue_timeout for detecting parent process failures. If batch_timeout
        is too large, workers may exit prematurely thinking the parent crashed.
        """
        if self.metadata_update_batch_timeout >= self.worker_queue_timeout:
            raise ValueError(
                "metadata_update_batch_timeout "
                f"({self.metadata_update_batch_timeout}s) "
                "must be less than worker_queue_timeout "
                f"({self.worker_queue_timeout}s) "
                "to prevent false timeout detections"
            )
        return self

    @model_validator(mode="after")
    def validate_log_levels(self) -> Self:
        """
        Ensure console log level is not more verbose than base log level.

        Console logging should be a subset of file logging, so console_log_level
        should be >= base_log_level in severity.
        """
        level_order = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}

        if level_order[self.console_log_level] < level_order[self.base_log_level]:
            raise ValueError(
                f"console_log_level ({self.console_log_level}) cannot be more verbose "
                f"than base_log_level ({self.base_log_level})"
            )
        return self

    @property
    def save_directory(self) -> Path:
        """Directory where downloaded tick data files are stored."""
        return self.base_directory / "downloads"

    @property
    def metadata_db_path(self) -> Path:
        """Path to the SQLite metadata database file."""
        return self.base_directory / "metadata.db"

    @property
    def log_file_path(self) -> Path:
        """Path to the application log file."""
        return self.base_directory / "logs.log"


# Global configuration instance
CONFIG = Config()


def reload_config(**kwargs) -> None:
    """
    Reload the global CONFIG with new settings.

    Configuration is loaded with the following priority (highest to lowest):
    1. Explicit keyword arguments passed to this function
    2. Environment variables (prefixed with TICK_VAULT_)
    3. Variables from .env file in the current directory
    4. Default values defined in the Config model

    Args:
        **kwargs: Explicit configuration values that override all other sources.
            Must match Config field names exactly (case-insensitive).

    Raises:
        ValidationError: If any configuration value fails validation

    Examples:
        >>> # Load from default .env file
        >>> reload_config()

        >>> # Load from specific .env file
        >>> reload_config(env_file='/path/to/.env.production')

        >>> # Skip .env file, use only environment variables and defaults
        >>> reload_config(env_file=None)

        >>> # Override specific values directly
        >>> reload_config(worker_per_proxy=20, fetch_max_retry_attempts=5)

        >>> # Combine custom .env file with overrides
        >>> reload_config(
        ...     env_file='.env.production',
        ...     base_directory='/data/ticks'
        ... )

    Environment variable examples:
        ```bash
        # In .env file or shell
        TICK_VAULT_BASE_DIRECTORY=/data/tick_vault
        TICK_VAULT_WORKER_PER_PROXY=20
        TICK_VAULT_FETCH_MAX_RETRY_ATTEMPTS=5
        TICK_VAULT_BASE_LOG_LEVEL=INFO
        ```
    """
    # Update the existing CONFIG singleton in-place so that other modules
    # that already imported CONFIG see the updated values.
    new_config = Config(**kwargs)
    for key, val in new_config.__dict__.items():
        CONFIG.__dict__[key] = val
    if hasattr(new_config, "__pydantic_fields_set__"):
        CONFIG.__pydantic_fields_set__.clear()
        CONFIG.__pydantic_fields_set__.update(new_config.__pydantic_fields_set__)

    # Update logger file handler with the new log file path
    try:
        from .logger import update_log_file
        update_log_file()
    except Exception:
        pass
