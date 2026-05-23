import asyncio
from datetime import UTC, datetime

from tqdm.asyncio import tqdm

from .chunk import TickChunk
from .config import CONFIG
from .download_worker import download_worker
from .logger import logger
from .metadata import MetadataDB
from .metadata_worker import metadata_worker


async def download_range(
    symbol: str,
    start: datetime,
    end: datetime = datetime.now(tz=UTC),
    proxies: list[str] | None = None,
) -> None:
    """
    Download tick data for a symbol within a date range using concurrent workers.

    This orchestrator manages the entire download pipeline: finding chunks to
    download, distributing work across multiple workers (optionally with different
    proxies), tracking progress, and handling errors gracefully.

    Args:
        symbol: The trading pair symbol (e.g., 'XAUUSD', 'EURUSD')
        start: Start datetime of the range (inclusive, rounded down to hour)
        end: Optional, End datetime of the range (exclusive, rounded down to hour).
            If not provided uses datetime.now
        proxies: Optional list of proxy URLs. If None or empty, workers run
            without proxies. Workers are distributed evenly across proxies.

    Raises:
        ForbiddenError: If access is blocked/forbidden during download
        RuntimeError: If max retries exceeded or unexpected errors occur
        ValueError: If database operations fail

    Example:
        >>> await download_range(
        ...     symbol='XAUUSD',
        ...     start=datetime(2024, 1, 1),
        ...     end=datetime(2024, 2, 1),
        ...     proxies=['http://127.0.0.1:8080', 'http://127.0.0.1:9090', 'http://proxy2:8080'],
        ... )
    """
    logger.info(f"Starting download for {symbol} from {start.date()} to {end.date()}")

    # Handle empty proxies list
    if proxies is None or len(proxies) == 0:
        worker_proxies = [None]
    else:
        worker_proxies = proxies

    # Initialize database and find chunks to download
    with MetadataDB() as db:
        chunks_to_download = db.find_not_attempted_chunks(symbol, start, end)

    # If nothing to download, exit early
    if not chunks_to_download:
        logger.info(
            f"All data for {symbol} from {start.date()} to {end.date()} already downloaded"
        )
        return

    total_chunks = len(chunks_to_download)
    logger.info(f"Found {total_chunks} chunks to download for {symbol}")

    # Create queues
    downloader_input_queue: asyncio.Queue[TickChunk | None] = asyncio.Queue()
    downloader_output_queue: asyncio.Queue[TickChunk] = asyncio.Queue()
    metadata_queue: asyncio.Queue[TickChunk | None] = asyncio.Queue()

    # Calculate total number of workers (capped by available chunks)
    max_workers = len(worker_proxies) * CONFIG.worker_per_proxy
    actual_workers = min(max_workers, total_chunks)
    logger.debug(f"Using {actual_workers} workers across {len(worker_proxies)} proxies")

    # Start metadata worker
    metadata_task = asyncio.create_task(metadata_worker(metadata_queue))

    # Start download workers (distributed across proxies)
    download_tasks = []
    worker_index = 0

    logger.debug("Starting download workers and metadata worker")
    for _ in range(CONFIG.worker_per_proxy):
        for proxy in worker_proxies:
            if worker_index >= actual_workers:
                break

            # Create worker task
            task = asyncio.create_task(
                download_worker(proxy, downloader_input_queue, downloader_output_queue)
            )
            download_tasks.append(task)
            worker_index += 1

        if worker_index >= actual_workers:
            break

    all_tasks = download_tasks + [metadata_task]

    # Populate initial chunks (one per worker to start)
    chunks_remaining = list(chunks_to_download)  # Make a copy
    for _ in range(actual_workers):
        if chunks_remaining:
            await downloader_input_queue.put(chunks_remaining.pop(0))

    # Create progress bar
    pbar = tqdm(
        total=total_chunks,
        desc=f"Downloading {symbol}",
        unit="chunk",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        colour="green",
    )

    completed = 0
    error_occurred = False

    try:
        # Process results as they come in
        while completed < total_chunks:
            # Propagate child errors
            [t.result() for t in all_tasks if t.done()]

            # Get result from download workers
            chunk = await downloader_output_queue.get()

            # Forward to metadata worker
            await metadata_queue.put(chunk)

            # Update progress
            completed += 1
            pbar.update(1)

            # Feed next chunk to workers if available
            if chunks_remaining:
                await downloader_input_queue.put(chunks_remaining.pop(0))

        pbar.close()
        logger.info(f"Successfully downloaded {total_chunks} chunks for {symbol}")

    except Exception as e:
        logger.error(f"Error during download: {e}", exc_info=True)
        error_occurred = True
        pbar.close()
        raise

    finally:
        logger.debug("Sending stop signals to workers")

        # Send stop signals to all workers
        for _ in range(actual_workers):
            await downloader_input_queue.put(None)

        # Stop metadata worker
        await metadata_queue.put(None)

        logger.debug("Waiting for workers to finish")
        # Wait for all workers to finish
        try:
            if error_occurred:
                # Cancel tasks if error occurred
                for task in download_tasks:
                    task.cancel()
                metadata_task.cancel()

                await asyncio.gather(
                    *download_tasks, metadata_task, return_exceptions=True
                )
            else:
                # Normal shutdown
                await asyncio.gather(*download_tasks, metadata_task)
        except asyncio.CancelledError:
            pass

        logger.debug("Workers cancelled")
