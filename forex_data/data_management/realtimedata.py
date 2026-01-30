# -*- coding: utf-8 -*-
"""
Created on Mon Apr 25 18:07:21 2022

@author: fiora
"""

from loguru import logger

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from shutil import rmtree
from io import StringIO

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
from dotty_dict import dotty
from dotty_dict import Dotty

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
    YEARS,
    MONTHS,
    DATE_FORMAT_SQL,
    DATE_FORMAT_HISTDATA_CSV,
    HISTDATA_URL_TICKDATA_TEMPLATE,
    HISTDATA_BASE_DOWNLOAD_METHOD,
    HISTDATA_BASE_DOWNLOAD_URL,
    DEFAULT_PATHS,
    DATA_TYPE,
    BASE_DATA_COLUMN_NAME,
    DATA_FILE_COLUMN_INDEX,
    SUPPORTED_DATA_FILES,
    SUPPORTED_DATA_ENGINES,
    ASSET_TYPE,
    TEMP_FOLDER,
    TEMP_CSV_FILE,
    DTYPE_DICT,
    PYARROW_DTYPE_DICT,
    POLARS_DTYPE_DICT,
    DATA_COLUMN_NAMES,
    FILENAME_TEMPLATE,
    DATA_KEY,
    TICK_TIMEFRAME,
    FILENAME_STR,
    REALTIME_DATA_PROVIDER,
    ALPHA_VANTAGE_API_KEY,
    CANONICAL_INDEX,
    DATE_NO_HOUR_FORMAT,
    POLYGON_IO_API_KEY,
    validator_file_path,
    validator_dir_path,
    get_attrs_names,
    any_date_to_datetime64,
    empty_dataframe,
    is_empty_dataframe,
    shape_dataframe,
    get_dataframe_column,
    get_dataframe_row,
    get_dataframe_element,
    get_dotty_leafs,
    astype,
    read_csv,
    polars_datetime,
    sort_dataframe,
    concat_data,
    list_remove_duplicates,
    get_dotty_key_field,
    reframe_data,
    write_csv,
    write_parquet,
    read_parquet,
    to_pandas_dataframe,
    get_pair_symbols,
    to_source_symbol,
    get_date_interval,
    polygon_agg_to_dict,
    AV_LIST_URL,
    PAIR_ALPHAVANTAGE_FORMAT,
    PAIR_POLYGON_FORMAT
)
from ..config import (
    read_config_file,
    read_config_string,
    read_config_folder
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

    # internal parameters
    _db_dict = field(factory=dotty,
                     validator=validators.instance_of(Dotty))
    _dataframe_type = field(default=pandas_dataframe)
    _realtimedata_path = field(
        default=Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.REALTIME_DATA_FOLDER,
        validator=validator_dir_path(create_if_missing=True)
    )
    _temporary_data_path = field(
        default=(Path(DEFAULT_PATHS.BASE_PATH) /
                 DEFAULT_PATHS.REALTIME_DATA_FOLDER /
                 TEMP_FOLDER),
        validator=validator_dir_path(create_if_missing=True))

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

        if 'config' in kwargs.keys():

            if kwargs['config']:

                config_path = Path(kwargs['config'])

                if (
                    config_path.exists() and
                    config_path.is_dir()
                ):

                    config_filepath = read_config_folder(
                        config_path, file_pattern='data_config.yaml')

                else:

                    config_filepath = Path()

                config_args = {}
                if config_filepath.exists() \
                        and  \
                        config_filepath.is_file() \
                        and  \
                        config_filepath.suffix == '.yaml':

                    # read parameters from config file
                    # and force keys to lower case
                    config_args = {key.lower(): val for key, val in
                                   read_config_file(str(config_filepath)).items()}

                elif isinstance(kwargs['config'], str):

                    # read parameters from config file
                    # and force keys to lower case
                    config_args = {key.lower(): val for key, val in
                                   read_config_string(kwargs['config']).items()}

                else:

                    logger.critical('invalid config type '
                                    f'{kwargs["config"]}: '
                                    'required str or Path, got '
                                    f'{type(kwargs["config"])}')
                    raise TypeError

                # check consistency of config_args
                if (
                        not isinstance(config_args, dict) or
                    not bool(config_args)
                ):

                    logger.critical(f'config {kwargs["config"]} '
                                    'has no valid yaml formatted data')
                    raise TypeError

                # set args from config file
                attrs_keys_configfile = \
                    set(_class_attributes_name).intersection(config_args.keys())

                for attr_key in attrs_keys_configfile:

                    self.__setattr__(attr_key,
                                     config_args[attr_key])

                    _not_assigned_attrs_index_mask[
                        _class_attributes_name.index(attr_key)
                    ] = False

                # set args from instantiation
                # override if attr already has a value from config
                attrs_keys_input = \
                    set(_class_attributes_name).intersection(kwargs.keys())

                for attr_key in attrs_keys_input:

                    self.__setattr__(attr_key,
                                     kwargs[attr_key])

                    _not_assigned_attrs_index_mask[
                        _class_attributes_name.index(attr_key)
                    ] = False

                # attrs not present in config file or instance inputs
                # --> self.attr leads to KeyError
                # are manually assigned to default value derived
                # from __attrs_attrs__

                for attr_key in array(_class_attributes_name)[
                        _not_assigned_attrs_index_mask
                ]:

                    try:

                        attr = [attr
                                for attr in self.__attrs_attrs__
                                if attr.name == attr_key][0]

                    except KeyError:

                        logger.error('KeyError: initializing object has no '
                                     f'attribute {attr.name}')
                        raise

                    except IndexError:

                        logger.error('IndexError: initializing object has no '
                                     f'attribute {attr.name}')
                        raise

                    else:

                        # assign default value
                        # try default and factory sabsequently
                        # if neither are present
                        # assign None
                        if hasattr(attr, 'default'):

                            if hasattr(attr.default, 'factory'):

                                self.__setattr__(attr.name,
                                                 attr.default.factory())

                            else:

                                self.__setattr__(attr.name,
                                                 attr.default)

                        else:

                            self.__setattr__(attr.name,
                                             None)

        else:

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        validate(self)

        self.__attrs_post_init__()

    def __attrs_post_init__(self) -> None:

        # set up log sink for historical manager
        # Remove existing handlers for this sink to prevent duplicate log entries
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

        self._temporary_data_path = self._realtimedata_path \
            / TEMP_FOLDER

        self._clear_temporary_data_folder()

    def _clear_temporary_data_folder(self) -> None:

        # delete temporary data path
        if (
            self._temporary_data_path.exists() and
            self._temporary_data_path.is_dir()
        ):

            try:

                rmtree(str(self._temporary_data_path))

            except Exception as e:

                logger.warning('Deleting temporary data folder '
                               f'{str(self._temporary_data_path)} not successfull: {e}')

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

    def get_daily_close(self,
                        ticker,
                        last_close=False,
                        recent_days_window=None,
                        day_start=None,
                        day_end=None,
                        ) -> Any:
        """
        Retrieve daily OHLC data for the specified ticker.

        Fetches daily forex data from Alpha Vantage API. Supports three modes of operation:
        last close only, recent N days window, or specific date range.

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD', 'USDJPY').
                Case-insensitive.
            last_close (bool, optional): If True, returns only the most recent daily close.
                Default is False.
            recent_days_window (int, optional): Number of recent days to retrieve.
                Mutually exclusive with day_start/day_end. Default is None.
            day_start (str, optional): Start date for data retrieval in 'YYYY-MM-DD' format.
                Used with day_end to specify exact date range. Default is None.
            day_end (str, optional): End date for data retrieval in 'YYYY-MM-DD' format.
                Used with day_start to specify exact date range. Default is None.

        Returns:
            polars.DataFrame | polars.LazyFrame: DataFrame containing daily OHLC data with columns:

                - timestamp: datetime column with daily timestamps
                - open: Opening price (float32)
                - high: Highest price (float32)
                - low: Lowest price (float32)
                - close: Closing price (float32)

            Returns empty DataFrame if API call fails.

        Raises:
            AssertionError: If recent_days_window is not an integer when provided
            BadResponse: If Alpha Vantage API request fails (handled internally)

        Example::

            # Get last close only
            manager = RealtimeManager(config='data_config.yaml')
            latest = manager.get_daily_close(ticker='EURUSD', last_close=True)

            # Get last 10 days
            recent = manager.get_daily_close(ticker='EURUSD', recent_days_window=10)

            # Get specific date range
            range_data = manager.get_daily_close(
                ticker='EURUSD',
                day_start='2024-01-01',
                day_end='2024-01-31'
            )

        Note:
            - Requires valid Alpha Vantage API key in configuration
            - Free tier has 25 requests per day limit
            - outputsize='compact' returns ~100 most recent data points
            - outputsize='full' can return several years of data
            - Use last_close=True for minimal data transfer

        """

        to_symbol, from_symbol = get_pair_symbols(ticker.upper())

        try:

            client = self._getClient(REALTIME_DATA_PROVIDER.ALPHA_VANTAGE)

            if last_close:

                res = client.get_currency_exchange_daily(
                    to_symbol,
                    from_symbol,
                    outputsize='compact'
                )

                # parse response and return
                return self._parse_data_daily_alphavantage(
                    res,
                    last_close=True
                )

            else:

                if not day_start or not day_end:
                    assert isinstance(recent_days_window, int), \
                        'recent_days_window must be integer'

                # careful that option "outputsize='full'" does not have constant day start
                # so it is not possible to guarantee a consistent meeting of the
                # function input 'day_start' and 'recent_days_window' when
                # they imply a large interval outside of the
                # "outputsize='full'" option
                res = client.get_currency_exchange_daily(
                    from_symbol,
                    to_symbol,
                    outputsize='full'
                )

                # parse response and return
                return self._parse_data_daily_alphavantage(
                    res,
                    last_close=False,
                    recent_days_window=recent_days_window,
                    day_start=day_start,
                    day_end=day_end)

        except BadResponse as e:

            logger.warning(e)
            return self._dataframe_type([])

        except Exception as e:

            logger.warning(f'Raised Exception: {e}')
            return self._dataframe_type([])

    def _parse_aggs_data(self, data_provider: str, **kwargs: Any) -> Any:

        if data_provider == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:

            return self._parse_data_daily_alphavantage(**kwargs)

        elif data_provider == REALTIME_DATA_PROVIDER.POLYGON_IO:

            return self._parse_data_aggs_polygonio(**kwargs)

        else:

            logger.error(f'data provider {data_provider} is invalid '
                         '- supported providers: {REALTIME_DATA_PROVIDER_LIST}')

            return self._dataframe_type()

    def _parse_data_daily_alphavantage(
        self,
        daily_data,
        last_close=False,
        recent_days_window=None,
        day_start=None,
        day_end=None
    ) -> Any:

        if not last_close:

            if isinstance(recent_days_window, int):
                # set window as DateOffset str with num and days
                days_window = '{days_num}D'.format(days_num=recent_days_window)

                day_start, day_end = get_date_interval(interval_end_mode='now',
                                                       interval_timespan=days_window,
                                                       normalize=True,
                                                       bdays=True)

            else:

                day_start = any_date_to_datetime64(day_start)
                day_end = any_date_to_datetime64(day_end)

        # parse alpha vantage response from daily api request
        resp_data_dict = daily_data[CANONICAL_INDEX.AV_DF_DATA_INDEX]

        # raw response data to dictionary
        timestamp = list(resp_data_dict.keys())
        data_values = resp_data_dict.values()
        open_data = [item['1. open'] for item in data_values]
        high_data = [item['2. high'] for item in data_values]
        low_data = [item['3. low'] for item in data_values]
        close_data = [item['4. close'] for item in data_values]

        if self.engine == 'pandas':

            df = pandas_dataframe(
                {
                    BASE_DATA_COLUMN_NAME.TIMESTAMP: timestamp,
                    BASE_DATA_COLUMN_NAME.OPEN: open_data,
                    BASE_DATA_COLUMN_NAME.HIGH: high_data,
                    BASE_DATA_COLUMN_NAME.LOW: low_data,
                    BASE_DATA_COLUMN_NAME.CLOSE: close_data
                }
            )

            # final cast to standard dtypes
            df = astype(df, DTYPE_DICT.TIME_TF_DTYPE)

            # sort by timestamp
            df = sort_dataframe(df, BASE_DATA_COLUMN_NAME.TIMESTAMP)

            # timestamp as column to include it in return data
            df.reset_index(inplace=True)

            if last_close:

                # get most recent line --> lowest num index
                df = get_dataframe_row(df, shape_dataframe(df)[0] - 1)

            else:

                # return data based on filter output
                df = df[
                    (df[BASE_DATA_COLUMN_NAME.TIMESTAMP] >= day_start) &
                    (df[BASE_DATA_COLUMN_NAME.TIMESTAMP] <= day_end)
                ]

        elif self.engine == 'pyarrow':

            df = pyarrow_table(
                {
                    BASE_DATA_COLUMN_NAME.TIMESTAMP: timestamp,
                    BASE_DATA_COLUMN_NAME.OPEN: open_data,
                    BASE_DATA_COLUMN_NAME.HIGH: high_data,
                    BASE_DATA_COLUMN_NAME.LOW: low_data,
                    BASE_DATA_COLUMN_NAME.CLOSE: close_data
                }
            )

            # final cast to standard dtypes
            df = astype(df, PYARROW_DTYPE_DICT.TIME_TF_DTYPE)

            # sort by timestamp
            df = sort_dataframe(df, BASE_DATA_COLUMN_NAME.TIMESTAMP)

            if last_close:

                df = get_dataframe_row(df, shape_dataframe(df)[0] - 1)

            else:

                mask = pc.and_(
                    pc.greater(df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                               day_start),
                    pc.less(df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                            day_end)
                )

                data_df = pyarrow_Table.from_arrays(df.filter(mask).columns,
                                                    schema=df.schema)

        elif self.engine == 'polars':

            df = polars_fromdict(
                {
                    BASE_DATA_COLUMN_NAME.TIMESTAMP: timestamp,
                    BASE_DATA_COLUMN_NAME.OPEN: open_data,
                    BASE_DATA_COLUMN_NAME.HIGH: high_data,
                    BASE_DATA_COLUMN_NAME.LOW: low_data,
                    BASE_DATA_COLUMN_NAME.CLOSE: close_data
                }
            )

            # convert timestamp column to datetime data type
            df = \
                df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        polars_datetime('ms'),
                        format=DATE_NO_HOUR_FORMAT
                    )
                )

            # final cast to standard dtypes
            df = astype(df, POLARS_DTYPE_DICT.TIME_TF_DTYPE)

            # sort by timestamp
            df = sort_dataframe(df, BASE_DATA_COLUMN_NAME.TIMESTAMP)

            if last_close:

                df = get_dataframe_row(df, shape_dataframe(df)[0] - 1)

            else:

                # filter on date
                df = \
                    (
                        df
                        .filter(
                            col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(day_start,
                                                                            day_end
                                                                            )
                        ).clone()
                    )

        elif self.engine == 'polars_lazy':

            df = polars_lazyframe(
                {
                    BASE_DATA_COLUMN_NAME.TIMESTAMP: timestamp,
                    BASE_DATA_COLUMN_NAME.OPEN: open_data,
                    BASE_DATA_COLUMN_NAME.HIGH: high_data,
                    BASE_DATA_COLUMN_NAME.LOW: low_data,
                    BASE_DATA_COLUMN_NAME.CLOSE: close_data
                }
            )

            # convert timestamp column to datetime data type
            df = \
                df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        polars_datetime('ms'),
                        format=DATE_NO_HOUR_FORMAT
                    )
                )

            # final cast to standard dtypes
            df = astype(df, POLARS_DTYPE_DICT.TIME_TF_DTYPE)

            # sort by timestamp
            df = sort_dataframe(df, BASE_DATA_COLUMN_NAME.TIMESTAMP)

            if last_close:

                df = get_dataframe_row(df, shape_dataframe(df)[0] - 1)

            else:

                # filter on date
                df = \
                    (
                        df
                        .filter(
                            col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(day_start,
                                                                            day_end
                                                                            )
                        ).clone()
                    )

        # sort by timestamp
        return df

    def _parse_data_aggs_polygonio(
        self,
        data=None,
        engine='polars'
    ) -> Union[polars_lazyframe, polars_dataframe]:

        if engine == 'pandas':

            # parse data and format data as common defined
            df = pandas_dataframe(data)

            # keep base data columns
            extra_columns = list(set(df.columns).difference(DATA_COLUMN_NAMES.TF_DATA))
            df.drop(extra_columns, axis=1, inplace=True)

            df.index = any_date_to_datetime64(
                df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                unit='ms'
            )

            # convert to conventional dtype
            df = astype(df, DTYPE_DICT.TIME_TF_DTYPE)

        elif engine == 'pyarrow':

            # TODO: convert Agg items into dicts
            #       call Table.from_pylist and set also
            #       schema appropriate

            data_dict_list = [polygon_agg_to_dict(agg)
                              for agg in data]

            df = pyarrow_Table.from_pylist(data_dict_list)

            extra_columns = list(
                set(df.column_names).difference(DATA_COLUMN_NAMES.TF_DATA))

            df = df.drop_columns(extra_columns)

            # convert to conventional dtype
            df = astype(df, PYARROW_DTYPE_DICT.TIME_TF_DTYPE)

        elif engine == 'polars':

            df = polars_dataframe(data)

            # sort by timestamp
            df = sort_dataframe(df, BASE_DATA_COLUMN_NAME.TIMESTAMP)

            extra_columns = list(set(df.columns).difference(DATA_COLUMN_NAMES.TF_DATA))

            df = df.drop(extra_columns)

            # convert timestamp column to datetime data type
            df = df.with_columns(
                from_epoch(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                           time_unit='ms').alias(BASE_DATA_COLUMN_NAME.TIMESTAMP)
            )

        elif engine == 'polars_lazy':

            if data:

                df = polars_lazyframe(data)

                extra_columns = list(set(
                    df.collect_schema().names()).difference(
                    DATA_COLUMN_NAMES.TF_DATA
                )
                )

                df = df.drop(extra_columns)

                # convert timestamp column to datetime data type
                df = df.with_columns(
                    from_epoch(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                               time_unit='ms').alias(BASE_DATA_COLUMN_NAME.TIMESTAMP)
                )

                # convert to conventional dtype
                df = astype(df, POLARS_DTYPE_DICT.TIME_TF_DTYPE)

            else:

                df = empty_dataframe('polars_lazy')

        # sort by timestamp
        return sort_dataframe(df, BASE_DATA_COLUMN_NAME.TIMESTAMP)

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
