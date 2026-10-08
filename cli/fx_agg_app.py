# -*- coding: utf-8 -*-
"""
Typer-based command-line interface for the Forex Data Aggregator.
"""

import datetime
from pathlib import Path
import time
from typing import List
import typer
import yaml

from forex_data import HistoricalManagerDB
from forex_data.data_management.charts import DataCharts


def _load_default_data_path() -> str:
    """Load default data path from appconfig/data_config.yaml if available."""
    candidates = [
        Path(__file__).resolve().parent.parent / "appconfig" / "data_config.yaml",
        Path("appconfig/data_config.yaml"),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            try:
                with open(candidate, "r") as f:
                    cfg = yaml.safe_load(f)
                    if isinstance(cfg, dict) and "DATA_PATH" in cfg:
                        return str(cfg["DATA_PATH"])
            except Exception:
                pass
    return "~/.database"


DEFAULT_DATA_PATH = _load_default_data_path()

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
    data_path: str = typer.Option(
        DEFAULT_DATA_PATH,
        "--data-path",
        "-d",
        help="Database root directory path."
    ),
    offline: bool = typer.Option(
        False,
        "--offline",
        "--no-download",
        help="Run in offline mode without attempting remote downloads."
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

    cfg_desc = config if config else f"Default (data_path={data_path})"
    typer.secho(
        f"Initializing database manager with config: {cfg_desc}",
        fg=typer.colors.BLUE
    )

    init_kwargs = {"config": config}
    if data_path:
        init_kwargs["data_path"] = data_path
    if offline:
        init_kwargs["offline"] = offline

    try:
        manager = HistoricalManagerDB(**init_kwargs)
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
                f"Successfully retrieved tick data ({row_count} rows) "
                f"for {ticker} from start date {start_date} to end date {end_date}",
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


@app.command(name="plot")
def plot_data(
    ticker: str = typer.Argument(..., help="Ticker symbol (e.g., EURUSD)."),
    start_date: str = typer.Argument(..., help="Start date."),
    end_date: str = typer.Argument(
        "now", help="End date (YYYY-MM-DD) or 'now' or 'live' for real-time plot."
    ),
    timeframe: str = typer.Argument(
        ..., help="Timeframe interval (e.g., 1D, 1h, 15m)."
    ),
    chart_type: str = typer.Option(
        "ohlc", "--type", help="Type of chart (ohlc, line, scatter)."
    ),
    data_path: str = typer.Option(
        DEFAULT_DATA_PATH,
        "--data-path",
        "-d",
        help="Database root directory path."
    ),
    offline: bool = typer.Option(
        False,
        "--offline",
        "--no-download",
        help="Run in offline mode without attempting remote downloads."
    ),
    config: str = typer.Option("", "--config", "-c", help="YAML config file path."),
    port: int = typer.Option(8050, "--port", help="Port for the Dash realtime server."),
    update_interval: int = typer.Option(
        1000, "--interval", help="Realtime refresh interval in ms."
    )
):
    """
    Launch an interactive plot for historical or live forex data.
    """
    typer.secho(f"Initializing DataCharts for {ticker}...", fg=typer.colors.BLUE)

    is_live = end_date.lower() == "live"
    if end_date.lower() == "now":
        end_date = datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

    init_kwargs = {"config": config}
    if data_path:
        init_kwargs["data_path"] = data_path
    if offline:
        init_kwargs["offline"] = offline

    if is_live:
        try:
            manager = DataCharts(**init_kwargs)
            manager.realtime_connector = manager

            typer.secho(
                f"Starting live plot for {ticker} at port {port}",
                fg=typer.colors.GREEN
            )
            manager.start_realtime_plot(
                ticker=ticker,
                timeframe=timeframe,
                start_date=start_date,
                update_interval_ms=update_interval,
                chart_type=chart_type,
                port=port
            )

            typer.secho(
                "Press Ctrl+C to stop the real-time server.",
                fg=typer.colors.YELLOW
            )
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            typer.secho("\nStopping real-time server...", fg=typer.colors.YELLOW)
        except Exception as e:
            typer.secho(f"Error: {e}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        finally:
            manager.close()
    else:
        try:
            manager = DataCharts(**init_kwargs)
            typer.secho(
                f"Plotting {ticker}: {start_date} -> {end_date}...",
                fg=typer.colors.CYAN
            )
            manager.plot_historical(
                ticker=ticker,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                chart_type=chart_type
            )
        except Exception as e:
            typer.secho(f"Error plotting data: {e}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        finally:
            manager.close()


if __name__ == "__main__":
    app()
