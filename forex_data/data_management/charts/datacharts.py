from typing import Any
from forex_data.data_management.historicaldata import HistoricalManagerDB
from forex_data.data_management.charts.common import SupportedChartType
from forex_data.data_management.charts.figures import create_historical_figure
from forex_data.data_management.charts.realtime import RealtimeDashApp
from forex_data.data_management.common import is_empty_dataframe
from loguru import logger
import polars as pl
import datetime


class DataCharts(HistoricalManagerDB):
    def __init__(
        self,
        config: str = "",
        realtime_connector: Any = None,
        data_path: Any = None,
        **kwargs: Any
    ):
        """
        Initializes DataCharts instance.

        Args:
            config: Path to the data configuration file.
            realtime_connector: Connector object to fetch real-time data.
            data_path: Path to database directory.
            **kwargs: Additional parameters passed to HistoricalManagerDB.
        """
        init_kwargs = {"config": config, **kwargs}
        if data_path is not None:
            init_kwargs["data_path"] = data_path
        super().__init__(**init_kwargs)
        self.realtime_connector = realtime_connector
        self._realtime_apps = {}

    def plot_historical(
        self,
        ticker: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        chart_type: str = SupportedChartType.OHLC
    ) -> None:
        """
        Plots historical data for the specified ticker and date range.

        Args:
            ticker: Currency pair symbol.
            timeframe: Candle timeframe.
            start_date: Start date string.
            end_date: End date string.
            chart_type: Type of chart to plot.
        """
        chart_data = self.get_data(
            ticker=ticker,
            timeframe=timeframe,
            start=start_date,
            end=end_date
        )

        if chart_data is None or is_empty_dataframe(chart_data):
            logger.warning(
                f"No data found for {ticker} from {start_date} to {end_date}")
            return

        fig = create_historical_figure(chart_data, ticker, chart_type)
        fig.show()

    def start_realtime_plot(
        self,
        ticker: str,
        timeframe: str,
        start_date: str,
        update_interval_ms: int = 1000,
        chart_type: str = SupportedChartType.OHLC,
        port: int = 8050
    ):
        """
        Starts a realtime charting server for the specified ticker.

        Args:
            ticker: Currency pair symbol.
            timeframe: Timeframe for realtime data.
            start_date: Start date for the realtime data query.
            update_interval_ms: Refresh interval in milliseconds.
            chart_type: Type of chart to plot.
            port: Port for the Dash server.
        """
        if self.realtime_connector is None:
            raise ValueError(
                "realtime_connector was not provided during initialization.")

        def data_fetcher() -> pl.DataFrame:
            # Assuming realtime_connector has a method to fetch data up to current time
            # For demonstration, we'll fetch from start_date to now
            now_str = datetime.datetime.now(
                datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            return self.realtime_connector.get_data(
                ticker=ticker,
                timeframe=timeframe,
                start=start_date,
                end=now_str
            )

        app_id = f"{ticker}_{timeframe}_{port}"
        if app_id in self._realtime_apps:
            logger.warning(f"Realtime chart for {app_id} is already running.")
            return

        app = RealtimeDashApp(
            data_fetcher_callback=data_fetcher,
            ticker=ticker,
            chart_type=chart_type,
            interval_ms=update_interval_ms,
            port=port
        )
        app.start()
        self._realtime_apps[app_id] = app

    def close(self):
        super().close()
        # Realtime apps run in daemon threads, they close on program exit,
        # but you can add explicit cleanup logic here if needed.
        self._realtime_apps.clear()
