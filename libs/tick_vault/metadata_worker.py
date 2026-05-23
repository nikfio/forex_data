"""Metadata worker module for tracking download results."""

import asyncio

from .chunk import TickChunk
from .config import CONFIG
from .logger import logger
from .metadata import MetadataDB


async def metadata_worker(
    result_queue: asyncio.Queue[TickChunk | None], db_path: str | None = None
) -> None:
    """
    Worker coroutine that processes download results and updates metadata in batches.

    This worker consumes results from result_queue and updates the metadata
    database using batch inserts. It accumulates chunks in a batch and commits when:
    1. Batch size reaches CONFIG.metadata_update_batch_size
    2. Timeout occurs waiting for next chunk (partial batch flush)
    3. Shutdown signal (None) is received (final batch flush)

    The worker runs until it receives None as a sentinel value or times out
    waiting for results, assuming the parent process crashed.

    Args:
        result_queue: Queue of TickChunk objects or None for shutdown
        db_path: Optional path to the metadata database. If None, uses
            CONFIG.metadata_db_path

    Raises:
        ValueError: If database path validation fails
        sqlite3.Error: If database operations fail

    Workflow:
        1. Initialize database connection
        2. Accumulate chunks in a batch
        3. When batch is full or timeout occurs, flush to database
        4. On shutdown signal (None), flush any remaining chunks
        5. Close database connection on exit
    """
    logger.debug("Metadata worker started")

    with MetadataDB(db_path) as db:
        batch: list[TickChunk] = []

        while True:
            try:
                # Wait for chunk with timeout
                chunk = await asyncio.wait_for(
                    result_queue.get(), timeout=CONFIG.metadata_update_batch_timeout
                )

                # None is the sentinel value to stop the worker
                if chunk is None:
                    logger.debug("Metadata worker received stop signal")
                    # Process any remaining chunks in batch
                    if batch:
                        logger.debug(f"Final batch flush: {len(batch)} chunks")
                        db.insert_rows(batch)
                    break

                # Add to batch
                logger.debug(f"Flushing batch of {len(batch)} chunks to database")
                batch.append(chunk)

                # Process batch if it reaches target size
                if len(batch) >= CONFIG.metadata_update_batch_size:
                    db.insert_rows(batch)
                    batch.clear()

            except TimeoutError:
                # Timeout - process accumulated batch if any
                if batch:
                    db.insert_rows(batch)
                    batch.clear()

                # Check if we should exit (parent process may have crashed)
                # If the queue has been empty for the main timeout period, exit
                try:
                    chunk = await asyncio.wait_for(
                        result_queue.get(),
                        timeout=CONFIG.worker_queue_timeout
                        - CONFIG.metadata_update_batch_timeout,
                    )

                    if chunk is None:
                        break

                    # Got a chunk, start new batch
                    batch.append(chunk)

                except TimeoutError:
                    logger.warning("Metadata worker timeout - assuming parent crashed")
                    # Long timeout - assume parent crashed, exit
                    raise  # To ensure other processes stop, maybe download workers are stuck

    logger.debug("Metadata worker shutting down")
