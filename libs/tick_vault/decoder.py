"""Decoder module for reading and decoding tick data chunks."""

import lzma

import numpy as np

from .chunk import TickChunk
from .constants import PIPET_SIZE_REGISTRY
from .logger import logger


def decode_chunk(chunk: TickChunk, pipet_scale: float | None = None) -> np.ndarray:
    """
    Decode a single tick data chunk into a structured numpy array.

    Args:
        chunk: The TickChunk to decode
        pipet_scale: Optional price scaling factor. If None, uses the value from
            PIPET_SIZE_REGISTRY for the chunk's symbol.

    Returns:
        np.ndarray: Structured array with fields:
            - time: datetime64[ms] - Tick timestamps
            - ask: float64 - Ask prices (scaled)
            - bid: float64 - Bid prices (scaled)
            - ask_volume: int64 - Ask volumes (scaled)
            - bid_volume: int64 - Bid volumes (scaled)

    Raises:
        FileNotFoundError: If the chunk file doesn't exist on disk
        ValueError: If pipet_scale is None and symbol not in PIPET_SIZE_REGISTRY
        lzma.LZMAError: If decompression fails
    """

    # Get pipet scale for price conversion
    if pipet_scale is None:
        if chunk.symbol not in PIPET_SIZE_REGISTRY:
            raise ValueError(
                f"Pipet scale is not registered for {chunk.symbol} symbol. "
                "Manually pass it or add it to PIPET_SIZE_REGISTRY registry."
            )
        pipet_scale = PIPET_SIZE_REGISTRY[chunk.symbol]

    # Load and decompress the chunk data
    try:
        buffer = lzma.decompress(chunk.load())
    except lzma.LZMAError as e:
        logger.error(f"Failed to decompress chunk {chunk.symbol} at {chunk.time}: {e}")
        raise

    raw_data = np.frombuffer(
        buffer,
        dtype=np.dtype(
            [
                ("time", ">u4"),
                ("ask", ">u4"),
                ("bid", ">u4"),
                ("ask_volume", ">f4"),
                ("bid_volume", ">f4"),
            ]
        ),
    )
    result = np.empty(
        len(raw_data),
        dtype=np.dtype(
            [
                ("time", "datetime64[ms]"),
                ("ask", "float64"),
                ("bid", "float64"),
                ("ask_volume", "int64"),
                ("bid_volume", "int64"),
            ]
        ),
    )

    # Time:
    start = np.datetime64(chunk.time.replace(tzinfo=None), "ms")
    result["time"] = start + raw_data["time"].astype(np.int64).astype("timedelta64[ms]")

    # Prices:
    result["ask"] = raw_data["ask"].astype(np.float64) * pipet_scale
    result["bid"] = raw_data["bid"].astype(np.float64) * pipet_scale

    # Volumes:
    result["ask_volume"] = np.round(
        raw_data["ask_volume"].astype(np.float64) * 1e6
    ).astype(np.int64)
    result["bid_volume"] = np.round(
        raw_data["bid_volume"].astype(np.float64) * 1e6
    ).astype(np.int64)

    return result
