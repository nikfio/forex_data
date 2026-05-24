"""Download worker module for fetching tick data chunks."""

import asyncio
import random

from httpx import AsyncClient

from .chunk import TickChunk
from .config import CONFIG
from .fetcher import fetch_with_retry
from .logger import logger


async def download_worker(
    proxy: str | None,
    input_queue: asyncio.Queue[TickChunk | None],
    output_queue: asyncio.Queue[TickChunk],
) -> None:
    """
    Worker coroutine that downloads tick data chunks from a queue.

    This worker processes chunks from input_queue, fetches their data,
    saves successful downloads to disk, and reports results to output_queue
    for metadata tracking. The worker runs until it receives None as a
    sentinel value or times out waiting for work.

    Any errors from fetch_with_retry (ForbiddenError, RuntimeError) are
    raised immediately to stop all workers and alert the parent process.

    Args:
        proxy: Optional proxy URL for the HTTP client (e.g., 'http://proxy:8080')
        input_queue: Queue of TickChunk objects to download. None signals shutdown.
        output_queue: Queue for reporting results

    Raises:
        ForbiddenError: If access is blocked/forbidden (propagated from
            fetch_with_retry)
        RuntimeError: If max retries exceeded or unexpected errors occur
            (propagated from fetch_with_retry)
        FileExistsError: If attempting to save a chunk that already exists on disk

    Workflow:
        1. Fetch chunk from input_queue (with timeout)
        2. If timeout occurs, exit gracefully (assumes parent crashed)
        3. If chunk is None, break and exit
        4. Fetch data using fetch_with_retry (which handles retries internally)
        5. If data exists (not None), save to disk using chunk.save()
        6. Put chunk to output_queue
        7. Apply a randomized pacing delay to be respectful of Dukascopy's servers
        8. Any errors are raised immediately to stop the download process
    """
    logger.debug(f"Download worker started with proxy: {proxy}")
    async with AsyncClient(
        proxy=proxy,
        headers={"User-Agent": CONFIG.user_agent},
        timeout=CONFIG.request_timeout,
    ) as client:
        while True:
            try:
                # Wait for chunk with timeout
                chunk = await asyncio.wait_for(
                    input_queue.get(), timeout=CONFIG.worker_queue_timeout
                )
            except TimeoutError:
                logger.warning("Download worker timeout - assuming parent crashed")
                # Timeout - assume parent process crashed or no more work
                break

            # None is the sentinel value to stop the worker
            if chunk is None:
                logger.debug("Download worker received stop signal")
                break

            # Fetch data with automatic retry logic
            # Any ForbiddenError or RuntimeError will be raised immediately
            content = await fetch_with_retry(client, chunk.url)

            if content is not None:
                # Data exists - save to disk
                chunk.save(content)
                logger.debug(
                    f"Downloaded and saved chunk: {chunk.symbol} "
                    f"{chunk.time.isoformat()}"
                )
            else:
                logger.debug(
                    f"No data exists for chunk: {chunk.symbol} {chunk.time.isoformat()}"
                )

            await output_queue.put(chunk)

            # Respectful pacing between sequential chunk requests
            pacing_delay = random.uniform(
                CONFIG.request_pacing_min,
                CONFIG.request_pacing_max
            )
            await asyncio.sleep(pacing_delay)

    logger.debug("Download worker shutting down")
