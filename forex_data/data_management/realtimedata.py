# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

from loguru import logger

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from uuid import uuid4
from shutil import rmtree
from io import StringIO
from requests import Session

from attrs import (
    define,
    field,
    validate,
    validators
)

# PANDAS
from pandas import (
    DataFrame as pandas_dataframe,
    read_csv as pandas_read_csv
)

# PYARROW
from pyarrow import (
    Table as pyarrow_Table,
    table as pyarrow_table,
    compute as pc
)

# POLARS
from polars import (
    col,
    from_epoch,
    from_dict as polars_fromdict,
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe
)

from numpy import array

# python base

# external

# polygon-io source
import polygon
from polygon import (
    RESTClient as polygonio_client,
    BadResponse
)

# alpha-vantage source
from alpha_vantage.foreignexchange import ForeignExchange as av_forex_client

from .common import (
    DEFAULT_PATHS,
    BASE_DATA_COLUMN_NAME,
    SUPPORTED_DATA_FILES,
    SUPPORTED_DATA_ENGINES,
    TEMP_FOLDER,
    DTYPE_DICT,
    PYARROW_DTYPE_DICT,
    POLARS_DTYPE_DICT,
    DATA_COLUMN_NAMES,
    REALTIME_DATA_PROVIDER,
    ALPHA_VANTAGE_API_KEY,
    CANONICAL_INDEX,
    DATE_NO_HOUR_FORMAT,
    POLYGON_IO_API_KEY,
    validator_dir_path,
    get_attrs_names,
    any_date_to_datetime64,
    empty_dataframe,
    shape_dataframe,
    get_dataframe_row,
    astype,
    polars_datetime,
    sort_dataframe,
    reframe_data,
    get_pair_symbols,
    to_source_symbol,
    get_date_interval,
    polygon_agg_to_dict,
    AV_LIST_URL
)
from ..config import (
    _apply_config
)

# constants
READ_RETRY_COUNT = 2
READ_PAUSE = 1
READ_CHUNKSIZE = 96

MINIMAL_RECENT_TIME_WINDOW_DAYS = 3


__all__ = ['RealtimeManager']

# Realtime data manager
#  source data providers to test APIs: polygon-IO, alpha-vantage


@define(kw_only=True, slots=True)
class RealtimeManager:

    # interface parameters
    config: str = field(default='',
                        validator=validators.instance_of(str))
    providers_key: dict = field(default=dict(),
                                validator=validators.instance_of(dict))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))
    data_path: Union[str,
                     Path] = field(default=str(DEFAULT_PATHS.BASE_PATH),
                                   validator=validators.or_(validators.instance_of(str),
                                                            validators.instance_of(Path)))
    ssl_verify: bool = field(default=True,
                             validator=validators.instance_of(bool))

    # internal parameters
    _dataframe_type = field(default=pandas_dataframe)
    _realtimedata_path = field(
        default=Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.REALTIME_DATA_FOLDER,
        validator=validator_dir_path(create_if_missing=True)
    )
    _realtime_data_path = field(
        default=None,
        validator=validators.optional(
            validators.instance_of(Path)))
    _session = field(factory=Session)

    # if a valid config file or string
    # is passed
    # arguments contained are assigned here
    # if instantiation passed values are present
    # they will override the related argument
    # value in the next initialization step

    # if neither by instantation or config file
    # an argument value is set, the argument
    # will be set by asociated defined default
    # or factory

    def __init__(self, **kwargs: Any) -> None:

        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)

        if not _apply_config(
                self,
                kwargs,
                _class_attributes_name,
                _not_assigned_attrs_index_mask):

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        validate(self)

        self.__attrs_post_init__()

    def __attrs_post_init__(self) -> None:

        self._session.verify = self.ssl_verify
        if not self.ssl_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # create data folder if not exists
        self.data_path = Path(self.data_path).expanduser().resolve()
        if (
            not self.data_path.exists() or
            not self.data_path.is_dir()
        ):

            self.data_path.mkdir(parents=True,
                                 exist_ok=True)

        # set up log sink for historical manager
        # Remove existing handlers for this sink to prevent duplicate log entries
        self._realtimedata_path = self.data_path / DEFAULT_PATHS.REALTIME_DATA_FOLDER
        log_path = self._realtimedata_path / 'log' / 'forexrtdata.log'

        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            logger.remove(handler_id)

        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('rtmanager' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))
        # checks on data folder path
        if (
            not self._realtimedata_path.is_dir() or
            not self._realtimedata_path.exists()
        ):

            self._realtimedata_path.mkdir(parents=True,
                                          exist_ok=True)

        if self.engine == 'pandas':

            self._dataframe_type = pandas_dataframe

        elif self.engine == 'pyarrow':

            self._dataframe_type = pyarrow_table

        elif self.engine == 'polars':

            self._dataframe_type = polars_dataframe

        elif self.engine == 'polars_lazy':

            self._dataframe_type = polars_lazyframe

        # Each instance gets its own unique temp subfolder under Temp/
        # so that parallel RealtimeManager instances never share or
        # conflict on the same temporary directory.
        self._realtime_data_path = (
            self._realtimedata_path / TEMP_FOLDER / str(uuid4())
        )
        self._realtime_data_path.mkdir(parents=True, exist_ok=True)

        self._clear_temporary_data_folder()

    def _clear_temporary_data_folder(self) -> None:

        # delete temporary data path
        if (
            self._realtime_data_path.exists() and
            self._realtime_data_path.is_dir()
        ):

            try:

                rmtree(str(self._realtime_data_path))

            except Exception as e:

                # failure is not sign of malfunction
                # not to log
                pass

    def _getClient(self, provider: str) -> Any:

        if provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:

            return av_forex_client(key=self.providers_key[ALPHA_VANTAGE_API_KEY])

        elif provider == REALTIME_DATA_PROVIDER.POLYGON_IO:

            return polygonio_client(api_key=self.providers_key[POLYGON_IO_API_KEY])

    def tickers_list(self,
                     data_source,
                     asset_class: Optional[str] = None) -> List[str]:

        # return list of symbols for tickers actively treated by data providers

        tickers_list = list()

        if data_source == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:

            # compose URL for tickers listing request
            # decode content
            with self._session as s:  # type: ignore[attr-defined]
                listing_downloaded = s.get(AV_LIST_URL.format(
                    api_key=self._av_api_key))  # type: ignore[attr-defined]
                decoded_content = listing_downloaded.content.decode('utf-8')
                tickers_df = pandas_read_csv(
                    StringIO(decoded_content), sep=',', header=0)

            if asset_class:

                if asset_class == ASSET_TYPE.FOREX:

                    logger.error('alpha vantage listing not including forex tickers')
                    raise ValueError

                elif asset_class == ASSET_TYPE.ETF:

                    assetType_req_index = tickers_df[:, 'assetType'] == 'ETF'

                elif asset_class == ASSET_TYPE.STOCK:

                    assetType_req_index = tickers_df[:, 'assetType'] == 'Stock'

                tickers_list = tickers_df.loc[assetType_req_index, 'symbol'].to_list()

            else:

                tickers_list = tickers_df.loc[:, 'symbol'].to_list()

        elif data_source == REALTIME_DATA_PROVIDER.POLYGON_IO:

            if asset_class:

                if asset_class == ASSET_TYPE.FOREX:

                    poly_asset_class = 'fx'

            else:

                poly_asset_class = None

            # call function for forex asset_class
            listing_downloaded = self._poly_reader.get_exchanges(  # type: ignore[attr-defined]
                asset_class=poly_asset_class)

            tickers_list = [item.acronym for item in listing_downloaded]

        return tickers_list

    def get_realtime_quote(self, ticker: str) -> Any:

        with self._getClient(REALTIME_DATA_PROVIDER.POLYGON_IO) as client:

            to_symbol, from_symbol = get_pair_symbols(ticker.upper())

            poly_resp = client.get_last_forex_quote(from_symbol,
                                                    to_symbol)

        return poly_resp

    def get_data(self,
                 ticker,
                 start=None,
                 end=None,
                 timeframe=None,
                 ) -> Union[polars_lazyframe, polars_dataframe]:
        """
        Retrieve real-time OHLC data for the specified ticker and timeframe.

        Fetches intraday forex data from Polygon.io API for the specified date range
        and timeframe. Data is automatically reframed to the requested timeframe.

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD', 'USDJPY').
                Case-insensitive.
            start (str | datetime, optional): Start date for data retrieval. Accepts:
                - ISO format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
                - datetime object
                Default is None.
            end (str | datetime, optional): End date for data retrieval. Same format
                as start. Must be after start date. Default is None.
            timeframe (str, optional): Target timeframe for aggregation. If specified,
                minute data will be reframed to this timeframe (e.g., '5m', '1h', '1D').
                Default is None (returns minute data).

        Returns:
            polars.DataFrame | polars.LazyFrame: DataFrame containing OHLC data with columns:

                - timestamp: datetime column with candle timestamps
                - open: Opening price (float32)
                - high: Highest price (float32)
                - low: Lowest price (float32)
                - close: Closing price (float32)

            Returns empty DataFrame if API call fails.

        Raises:
            BadResponse: If Polygon.io API request fails (handled internally, returns empty DataFrame)

        Example::

            # Get hourly data for 5 days
            manager = RealtimeManager(config='data_config.yaml')
            data = manager.get_data(
                ticker='EURUSD',
                start='2024-01-10',
                end='2024-01-15',
                timeframe='1h'
            )
            print(f"Retrieved {len(data)} hourly candles")
            # Output: Retrieved 120 hourly candles

        Note:
            - Requires valid Polygon.io API key in configuration
            - Free tier has rate limits  and historical data restrictions
            - Data is fetched at 1-minute resolution and aggregated to requested timeframe
            - Failed requests return an empty DataFrame with a warning logged

        """

        start = any_date_to_datetime64(start)
        end = any_date_to_datetime64(end)

        # forward request only to polygon-io
        # set ticker in polygon format

        ticker_polygonio = to_source_symbol(
            ticker.upper(),
            REALTIME_DATA_PROVIDER.POLYGON_IO
        )

        try:

            client = self._getClient(REALTIME_DATA_PROVIDER.POLYGON_IO)

            poly_aggs = []

            # TODO: set up try-except with BadResponse to manage provider
            # subcription limitation

            # using Polygon-io client
            for a in client.list_aggs(ticker=ticker_polygonio,
                                      multiplier=1,
                                      timespan='minute',
                                      from_=start,
                                      to=end,
                                      adjusted=True,
                                      sort='asc'):

                poly_aggs.append(a)

        except BadResponse as e:

            # to log
            logger.warning(e)
            return self._dataframe_type([])

        # clear temporary data folder
        self._clear_temporary_data_folder()

        data_df = self._parse_aggs_data(REALTIME_DATA_PROVIDER.POLYGON_IO,
                                        data=poly_aggs,
                                        engine=self.engine)

        return reframe_data(data_df, timeframe)
