import threading
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
from forex_data.data_management.charts.figures import create_historical_figure
from forex_data.data_management.charts.common import SupportedChartType
from typing import Callable
import polars as pl
from loguru import logger
from forex_data.data_management.common import is_empty_dataframe


class RealtimeDashApp:
    def __init__(
        self,
        data_fetcher_callback: Callable[[], pl.DataFrame],
        ticker: str,
        chart_type: str = SupportedChartType.OHLC,
        interval_ms: int = 1000,
        host: str = "127.0.0.1",
        port: int = 8050
    ):
        self.data_fetcher_callback = data_fetcher_callback
        self.ticker = ticker
        self.chart_type = chart_type
        self.interval_ms = interval_ms
        self.host = host
        self.port = port
        self.app = Dash(__name__)
        self._thread = None
        self._setup_layout()

    def _setup_layout(self):
        self.app.layout = html.Div([
            html.H1(f"Realtime Chart: {self.ticker}"),
            dcc.Graph(id='live-update-graph'),
            dcc.Interval(
                id='interval-component',
                interval=self.interval_ms,
                n_intervals=0
            )
        ])

        @self.app.callback(
            Output('live-update-graph', 'figure'),
            Input('interval-component', 'n_intervals')
        )
        def update_graph_live(n):
            try:
                df = self.data_fetcher_callback()
                if df is None or is_empty_dataframe(df):
                    return go.Figure()
                fig = create_historical_figure(df, self.ticker, self.chart_type)
                return fig
            except Exception as e:
                logger.error(f"Error updating realtime chart: {e}")
                return go.Figure()

    def start(self):
        """Starts the Dash app in a daemon thread."""
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._run_server,
                daemon=True
            )
            self._thread.start()
            logger.info(
                f"Realtime chart server started at http://{self.host}:{self.port}")
        else:
            logger.warning("Realtime chart server is already running.")

    def _run_server(self):
        # Disable reloader and debug to avoid main thread requirement
        self.app.run(host=self.host, port=self.port, debug=False, use_reloader=False)

    def stop(self):
        """
        Dash does not provide a graceful shutdown for the development server

        run via app.run(). Because it is started in a daemon thread, it will

        terminate when the main program exits.
        """
        pass
