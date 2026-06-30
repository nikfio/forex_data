# -*- coding: utf-8 -*-
"""
Typer-based command-line interface for the Forex Data Aggregator.
"""

from typing import List
import typer

from forex_data import HistoricalManagerDB

# Initialize the Typer app
app = typer.Typer(
    name="fx-agg",
    help="CLI tool to manage and aggregate Forex historical market data.",
    no_args_is_help=True
)


@app.command(name="generate-database")
def generate_database(
    tickers: List[str] = typer.Argument(
        ...,
        help="Ticker symbol or list of symbols (e.g., EURUSD, GBPUSD)."
    ),
    start_date: str = typer.Argument(
        ...,
        help="Start date for data retrieval (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)."
    ),
    end_date: str = typer.Argument(
        "now",
        help="End date for data retrieval (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)."
    ),
    timeframe: List[str] = typer.Option(
        ["1D"],
        "--timeframe",
        "-t",
        help=(
            "Timeframe interval(s) (e.g., 1m, 5m, 1h, 1D). "
            "Can be specified multiple times or comma-separated."
        )
    ),
    config: str = typer.Option(
        "",
        "--config",
        "-c",
        help="YAML configuration file path or a YAML formatted string."
    ),
):
    """
    Generate and cache historical forex data in the database.

    Runs for the specified tickers and date range.
    """
    # Normalize the input tickers list (splitting by commas if needed)
    normalized_tickers = []
    for ticker_arg in tickers:
        parts = [t.strip().upper() for t in ticker_arg.split(",") if t.strip()]
        normalized_tickers.extend(parts)

    # Normalize the input timeframes list (splitting by commas if needed)
    normalized_timeframe = []
    for tf_arg in timeframe:
        parts = [t.strip().lower() for t in tf_arg.split(",") if t.strip()]
        normalized_timeframe.extend(parts)

    if not normalized_tickers:
        typer.secho("Error: No valid tickers provided.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    if not normalized_timeframe:
        normalized_timeframe = ["1d"]

    typer.secho(
        f"Initializing database manager with config: "
        f"{config if config else 'Default configuration'}",
        fg=typer.colors.BLUE
    )

    try:
        manager = HistoricalManagerDB(config=config)
        # add requested timeframes
        # with this action we need just one tick download if needed
        manager.add_timeframe(normalized_timeframe)
    except Exception as e:
        typer.secho(
            f"Failed to initialize HistoricalManagerDB: {e}",
            fg=typer.colors.RED,
            err=True
        )
        raise typer.Exit(code=1)

    for ticker in normalized_tickers:
        typer.secho(
            f"Generating database for ticker: {ticker} | "
            f"Timeframes: {', '.join(normalized_timeframe)} | "
            f"Range: {start_date} to {end_date}",
            fg=typer.colors.CYAN
        )

        try:
            # Query / download data. HistoricalManagerDB automatically downloads
            # missing historical periods and caches them locally.
            lazy_frame = manager.get_data(
                ticker=ticker,
                timeframe=normalized_timeframe[0],
                start=start_date,
                end=end_date
            )

            # Collect lazy frame if applicable to get actual row count
            if hasattr(lazy_frame, "collect"):
                df = lazy_frame.collect()
            else:
                df = lazy_frame

            row_count = len(df)
            typer.secho(
                f"Successfully retrieved tick data for {ticker} from start date "
                f"{start_date} to end date {end_date}",
                fg=typer.colors.GREEN
            )

        except Exception as e:
            typer.secho(
                f"Error downloading tick data for ticker {ticker} "
                f"and processing timeframes {', '.join(normalized_timeframe)}: {e}",
                fg=typer.colors.RED,
                err=True
            )
            manager.close()
            raise typer.Exit(code=1)

    manager.close()
    typer.secho(
        f"Database generation complete, tickers processed: "
        f"{', '.join(normalized_tickers)}, timeframes: "
        f"{', '.join(normalized_timeframe)}, "
        f"Date range: {start_date} to {end_date}",
        fg=typer.colors.GREEN,
        bold=True
    )


if __name__ == "__main__":
    app()
