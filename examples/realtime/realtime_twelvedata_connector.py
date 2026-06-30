# -*- coding: utf-8 -*-

import sys
from datetime import timedelta
from pathlib import Path

from loguru import logger

from forex_data import (
    TwelveDataConnector
)


def main():

    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level:<8}</level> | {message}"
        )
    )

    # setup connector instance
    connector = TwelveDataConnector(
        plan="free",
        data_path=Path.home() / ".test_database"
    )

    symbol = "EUR/USD"

    # real-time price
    logger.info(f"Fetching live real-time price for {symbol}...")
    result_lf = connector.get_realtime_price(symbol=symbol)
    logger.info(f"Received DataFrame:\n{result_lf.collect()}")

    # historical data
    logger.info(
        "Fetching live historical data for EUR/USD starting "
        "2026-01-01 10:00:00..."
    )
    result_lf = connector.get_data(
        symbol="EUR/USD",
        timeframe="5m",
        start_date="2026-01-01 10:00:00",
        end_date="2026-01-01 10:05:00"
    )

    logger.info(f"Received DataFrame:\n{result_lf.collect().head(n=5)}")

    # recent data
    logger.info("Fetching live recent data for EUR/USD within 1 day window...")
    result_lf = connector.get_recent_data(
        symbol="EUR/USD",
        timeframe="1h",
        interval_window=timedelta(days=1)
    )

    logger.info(f"Received DataFrame:\n{result_lf.collect().head(n=5)}")


if __name__ == "__main__":
    main()
