
from loguru import logger
from typing import Any, Dict, List, Optional, Union, Literal
from datetime import datetime, date, timedelta, timezone
from uuid import uuid4
from filelock import FileLock
from textwrap import dedent

from attrs import (
    define,
    field,
    validate,
    validators
)

# PANDAS
from pandas import (
    DataFrame as pandas_dataframe,
    to_datetime,
    to_timedelta,
    Timedelta
)

# PYARROW
from pyarrow import (
    int64 as pyarrow_int64,
    string as pyarrow_string,
    BufferReader,
    csv as arrow_csv,
    compute as pc,
    schema,
    Table,
    table as pyarrow_table,
    duration
)

# POLARS
from polars import (
    String as polars_string,
    col,
    DataFrame as PolarsDataFrame,
    LazyFrame as PolarsLazyFrame
)

from zipfile import (
    ZipFile,
    ZipExtFile,
    BadZipFile
)

from re import (
    search,
    match
)

from mplfinance import (
    plot as mpf_plot,
    show as mpf_show
)

from numpy import array

from pathlib import Path
from requests import Session
from io import BytesIO
from shutil import rmtree

# internally defined
from .common import *
from ..config import (
    _apply_config
)

from .database import (
    DatabaseConnector,
    LocalDBConnector,
    LocalDBYearConnector
)

from .remoteconnector import (
    RemoteConnector,
    HistDataConnector,
    DukascopyConnector
)

__all__ = ['HistoricalManagerDB']


# HISTORICAL DATA MANAGER
@define(kw_only=True, slots=True)
class HistoricalManagerDB:

    # interface parameters
    config: str = field(default='',
                        validator=validators.instance_of(str))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))
    data_path: Union[str,
                     Path] = field(default=str(DEFAULT_PATHS.BASE_PATH),
                                   validator=validators.or_(validators.instance_of(str),
                                                            validators.instance_of(Path)))
    db_files_year_partitioning: bool = field(default=True,
                                             validator=validators.instance_of(bool))
    ssl_verify: bool = field(default=True,
                             validator=validators.instance_of(bool))
    polars_gpu_engine: bool = field(default=False,
                                    validator=validators.instance_of(bool))
    connector_id: str = field(default='',
                              validator=validators.optional(validators.instance_of(str)))
    max_discrepancy_with_now: str = field(
        default='1D',
        validator=validators.and_(
            validators.instance_of(str),
            validate_timedelta_str
        )
    )

    # internal
    _db_connector = field(factory=DatabaseConnector)
    _histdata_connector = field(factory=list, validator=validators.instance_of(list))
    _tf_list = field(factory=list, validator=validators.instance_of(list))
    _dataframe_type = field(default=pandas_dataframe)
    _histdata_path = field(
        default=Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.HIST_DATA_FOLDER,
        validator=validators.instance_of(Path))
    _temporary_data_path = field(
        default=Path(DEFAULT_PATHS.BASE_PATH) / TEMP_FOLDER,
        validator=validators.optional(
            validators.instance_of(Path)))
    _histdata_tickers_list = field(factory=list, validator=validators.instance_of(list))
    _tickers_years_dict = field(factory=dict, validator=validators.instance_of(dict))

    # if a valid config file or string
    # is passed
    # arguments contained are assigned here
    # if instantiation passed values are present
    # they will override the related argument
    # value in the next initialization step

    # if neither by instantation or config file
    # an argument value is set, the argument
    # will be set by asociated defined default
    # or factory generator

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

        else:

            self.__attrs_post_init__(**kwargs)

        validate(self)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

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
        if self.connector_id:
            log_file = f'forexhistdata_{self.connector_id}.log'
        else:
            log_file = 'forexhistdata.log'

        self._histdata_path = self.data_path / DEFAULT_PATHS.HIST_DATA_FOLDER
        log_path = self._histdata_path / 'log' / log_file

        # Remove handlers that match this log file path
        # logger.remove() without args would remove ALL handlers including stderr
        # So we iterate and remove only matching handlers
        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            # Check if this handler writes to our log file
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            # check handler id exists in logger before removing
            try:
                if handler_id in [h[1]._id for h in logger._core.handlers.items()]:
                    logger.remove(handler_id)
            except Exception:
                # handler already removed or not found
                # can proceed with no side effects
                pass

        # Now add the handler
        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('histmanager' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        # set up dataframe engine internal var based on config selection
        if self.engine == 'pandas':

            self._dataframe_type = pandas_dataframe

        elif self.engine == 'pyarrow':

            self._dataframe_type = pyarrow_table

        elif self.engine == 'polars':

            self._dataframe_type = PolarsDataFrame

        elif self.engine == 'polars_lazy':

            self._dataframe_type = PolarsLazyFrame

        else:

            logger.bind(target='histmanager').error(
                f'Engine {self.engine} not supported')
            raise ValueError(f'Engine {self.engine} not supported')

        # instance database connector if selected
        if (
                self.data_type == DATA_TYPE.CSV_FILETYPE or
            self.data_type == DATA_TYPE.PARQUET_FILETYPE
        ):

            if self.db_files_year_partitioning:
                connector_type = LocalDBYearConnector
            else:
                connector_type = LocalDBConnector

            # set instance id if provided by config or arguments
            if self.connector_id:

                # connector_id assigned -> prepare connector to use a dedicated
                # independent data folder under LocalDB
                self._db_connector = connector_type(
                    data_path=str(self._histdata_path / f'LocalDB_{self.connector_id}'),
                    data_type=self.data_type,
                    engine=self.engine,
                    polars_gpu_engine=self.polars_gpu_engine
                )

            else:

                # no id provided so set id=0 as default
                self._db_connector = connector_type(
                    data_path=str(self._histdata_path / 'LocalDB'),
                    data_type=self.data_type,
                    engine=self.engine,
                    polars_gpu_engine=self.polars_gpu_engine
                )

        else:

            logger.bind(target='histmanager').error(
                f'Data type {self.data_type} not supported')
            raise ValueError(f'Data type {self.data_type} not supported')

        # initialize histdata connectors
        self._histdata_connector = [
            HistDataConnector(
                ssl_verify=self.ssl_verify,
                data_path=str(self._histdata_path / 'histdata'),
                engine=self.engine,
                data_type=self.data_type,
            ),
            DukascopyConnector(
                ssl_verify=self.ssl_verify,
                data_path=str(self._histdata_path / 'dukascopy'),
                engine=self.engine,
                data_type=self.data_type,
            )
        ]

        # cache histdata tickers list at initialization (merged from all connectors)
        tickers_set = set()
        for conn in self._histdata_connector:
            try:
                for ticker in conn.get_available_tickers():
                    tickers_set.add(ticker.upper())
            except Exception as e:
                logger.bind(target='histmanager').warning(
                    f"Failed to get tickers from connector {conn.__class__.__name__}: {e}"
                )
        self._histdata_tickers_list = sorted(list(tickers_set))

        # initialize tickers years dict info of data available
        # with the current connector
        self._tickers_years_dict = self._db_connector.create_tickers_years_dict()

    def _clear_temporary_data_folder(self) -> None:

        # delete temporary data path
        if (
            self._temporary_data_path.exists() and
            self._temporary_data_path.is_dir()
        ):

            try:

                rmtree(self._temporary_data_path)

            except Exception as e:

                # failure is not sign of malfunction
                # not to log
                pass

    def get_source_connectors(self) -> List[RemoteConnector]:

        return self._histdata_connector

    def _get_ticker_list(self) -> List[str]:

        # return list of tickers elements as str
        # check if tickers years dict has data for ticker
        if self._tickers_years_dict:
            return list(self._tickers_years_dict.keys())
        else:
            return self._db_connector.get_tickers_list()

    def _get_ticker_timeframes_list(
            self,
            ticker: str) -> List[str]:

        # return list of ticker timeframes elements as str
        # remove TICK_TIMEFRAME from list
        return [tf for tf in self._db_connector.get_ticker_timeframes_list(ticker) if tf != TICK_TIMEFRAME]

    def _get_ticker_keys(
            self,
            ticker: str,
            timeframe: Optional[str] = None) -> List[str]:

        # return list of ticker keys elements as str
        return self._db_connector.get_ticker_keys(ticker,
                                                  timeframe=timeframe)

    def _get_ticker_years_list(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:

        # return list of ticker years covered in data elements as str
        # if timeframe is None means years in data in tick or 1m timeframe

        # check if tickers years dict has data for ticker
        try:

            ticker_years_list = self._tickers_years_dict[ticker][timeframe]

        except Exception:

            # in case data info is not present in internal dict
            # try to retrieve it from db
            ticker_years_list = self._db_connector.get_ticker_years_list(
                ticker, timeframe=timeframe)

        return ticker_years_list

    def _update_db(self, ticker: str = None) -> None:

        if not ticker:
            ticker_list = self._get_ticker_list()
        else:
            ticker_list = [ticker]

        for ticker in ticker_list:

            years_tick = self._tickers_years_dict[ticker][TICK_TIMEFRAME]

            # Collect all missing years per timeframe
            missing_years_per_tf = {}
            all_missing_years = set()

            for tf in self._tf_list:
                if tf not in self._tickers_years_dict[ticker]:
                    self._tickers_years_dict[ticker][tf] = []

                ticker_years_list = self._tickers_years_dict[ticker][tf]
                missing_years = sorted(list(set(years_tick).difference(ticker_years_list)))
                if missing_years:
                    missing_years_per_tf[tf] = missing_years
                    all_missing_years.update(missing_years)

            if not all_missing_years:
                continue

            # call read function optimized for years query
            tick_dataframe = self._db_connector.read_data_year(
                market='forex',
                ticker=ticker,
                timeframe=TICK_TIMEFRAME,
                years=all_missing_years
            )

            if isinstance(self._db_connector, LocalDBYearConnector):
                # Map each missing year to the timeframes that need it
                year_to_tfs = {}
                for tf, missing_years in missing_years_per_tf.items():
                    for year in missing_years:
                        if year not in year_to_tfs:
                            year_to_tfs[year] = []
                        year_to_tfs[year].append(tf)

                # Process each missing year once
                for year in sorted(year_to_tfs.keys()):
                    year_start = f'{year}-01-01 00:00:00.000'
                    year_end = f'{year + 1}-01-01 00:00:00.000'

                    start = datetime.strptime(year_start, DATE_FORMAT_SQL)
                    end = datetime.strptime(year_end, DATE_FORMAT_SQL)

                    # Read missing year from tick_dataframe
                    # use filter to get just the year needed
                    dataframe = tick_dataframe.filter(
                        (col(BASE_DATA_COLUMN_NAME.TIMESTAMP) >= start)
                        &
                        (col(BASE_DATA_COLUMN_NAME.TIMESTAMP) < end)
                    )

                    for tf in year_to_tfs[year]:
                        # reframe to timeframe
                        dataframe_tf = reframe_data(dataframe, tf)

                        # get data id key
                        tf_key = self._db_connector._db_key(
                            'forex',
                            ticker,
                            tf,
                            year
                        )

                        # write to database
                        self._db_connector.write_data(tf_key, dataframe_tf)

                        # update metadata
                        self._db_connector.add_tickers_years_info_to_file(ticker, tf, [year])
                        if year not in self._tickers_years_dict[ticker][tf]:
                            self._tickers_years_dict[ticker][tf].append(year)
                            self._tickers_years_dict[ticker][tf].sort()

                # After all years are processed, verify consistency for each TF
                for tf, missing_years in missing_years_per_tf.items():
                    if set(years_tick).difference(self._tickers_years_dict[ticker][tf]):
                        logger.bind(target='histmanager').critical(
                            f'ticker {ticker}: {tf} timeframe completing'
                            ' operation FAILED')
                        raise KeyError
                    else:
                        logger.bind(
                            target='histmanager').trace(
                            f'ticker {ticker}: {tf} timeframe completing operation successful for {missing_years}')
            else:

                for tf, missing_years in missing_years_per_tf.items():
                    # reframe to timeframe
                    dataframe_tf = reframe_data(tick_dataframe, tf)

                    # get data id key
                    tf_key = self._db_connector._db_key('forex', ticker, tf)

                    # write to database
                    self._db_connector.write_data(tf_key, dataframe_tf)

                    # update metadata
                    self._db_connector.add_tickers_years_info_to_file(ticker, tf, missing_years)
                    self._tickers_years_dict[ticker][tf].extend(missing_years)
                    self._tickers_years_dict[ticker][tf] = sorted(list(set(self._tickers_years_dict[ticker][tf])))

                    # REDO THE CHECK FOR CONSISTENCY
                    if set(years_tick).difference(self._tickers_years_dict[ticker][tf]):

                        logger.bind(target='histmanager').critical(
                            f'ticker {ticker}: {tf} timeframe completing'
                            ' operation FAILED')

                        raise KeyError

                    else:
                        logger.bind(
                            target='histmanager').trace(
                            f'ticker {ticker}: {tf} timeframe completing operation successful for {missing_years}')

    def _download_year(self,
                       ticker,
                       year) -> Union[PolarsDataFrame,
                                      PolarsLazyFrame,
                                      pandas_dataframe,
                                      Table,
                                      None]:

        year_tick_df = empty_dataframe(self.engine)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

        for month in MONTHS:

            month_num = MONTHS.index(month) + 1

            if year == now_utc.year and month_num > now_utc.month:
                break

            month_data = None
            last_err = None
            for connector in self._histdata_connector:
                if (
                    isinstance(connector, HistDataConnector) and
                    year == now_utc.year and
                    month_num == now_utc.month
                ):
                    continue

                try:
                    if ticker.upper() not in connector.get_available_tickers():
                        continue
                except Exception:
                    continue

                try:
                    conn_engine = self.engine
                    if isinstance(connector, DukascopyConnector) and conn_engine not in ('polars', 'polars_lazy'):
                        conn_engine = 'polars'

                    month_data = connector.download_month_raw(
                        ticker,
                        year,
                        month_num,
                        engine=conn_engine
                    )

                    if isinstance(connector, DukascopyConnector) and self.engine not in ('polars', 'polars_lazy'):
                        if self.engine == 'pandas':
                            month_data = month_data.to_pandas()
                        elif self.engine == 'pyarrow':
                            month_data = month_data.to_arrow()

                    is_empty_dataframe(month_data)

                    last_err = None
                    break
                except TickerDataBadTypeException as e:
                    last_err = e
                    logger.bind(target='histmanager').warning(
                        f"Connector {connector.__class__.__name__} failed for {ticker}-{year}-{month}: {e}. Trying fallback."
                    )
                except Exception as e:
                    last_err = e
                    logger.bind(target='histmanager').warning(
                        f"Connector {connector.__class__.__name__} failed for {ticker}-{year}-{month}: {e}. Trying fallback."
                    )

            if last_err is not None:
                if (
                    year == now_utc.year and
                    month_num >= now_utc.month
                ):
                    logger.bind(target='histmanager').warning(
                        f"Ticker {ticker}-{year}-{MONTHS[month_num - 1]} query exceeded data availability for Historical Data"
                    )
                    break
                if isinstance(last_err, TickerDataBadTypeException):
                    raise last_err
                else:
                    raise last_err

            if month_data is not None and not is_empty_dataframe(month_data):

                # if first iteration, assign instead of concat
                if is_empty_dataframe(year_tick_df):

                    year_tick_df = month_data

                else:

                    year_tick_df = concat_data([year_tick_df, month_data])

        return sort_dataframe(year_tick_df,
                              BASE_DATA_COLUMN_NAME.TIMESTAMP)

    def _download(self,
                  ticker,
                  years: List[int]) -> None:

        if not (
            isinstance(years, list)
        ):

            logger.bind(target='histmanager').error(
                'years {years} invalid, must be list type')
            raise TypeError

        if not (
            set(years).issubset(YEARS)
        ):

            logger.bind(target='histmanager').error(
                f'requestedyears{years} not available. '
                f'Years must be limited to: {YEARS}')
            raise ValueError

        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        # execute differently based on connector type
        if isinstance(self._db_connector, LocalDBYearConnector):
            # download and write data for each year
            for year in years:

                year_tick_df = self._download_year(
                    ticker,
                    year
                )

                # get data id key for the year
                tick_key = self._db_connector._db_key('forex',
                                                      ticker,
                                                      TICK_TIMEFRAME,
                                                      year)

                # call to upload df to database if not empty
                if not is_empty_dataframe(year_tick_df):
                    self._db_connector.write_data(tick_key,
                                                  year_tick_df)

                    # update years list in local info file
                    self._db_connector.add_tickers_years_info_to_file(ticker,
                                                                      TICK_TIMEFRAME,
                                                                      [year])

                    # update internal ticker years list info
                    if year not in self._tickers_years_dict[ticker][TICK_TIMEFRAME]:
                        self._tickers_years_dict[ticker][TICK_TIMEFRAME].append(year)
                        # sort
                        self._tickers_years_dict[ticker][TICK_TIMEFRAME].sort()

                else:
                    logger.bind(
                        target='histmanager').warning(
                        f'Data dataframe for {tick_key} is empty, skipping database write')
        else:
            # download data for each year and aggregate
            years_data_df = empty_dataframe(self.engine)
            for year in years:

                year_tick_df = self._download_year(
                    ticker,
                    year
                )

                # if first iteration, assign instead of concat
                if is_empty_dataframe(years_data_df):
                    years_data_df = year_tick_df
                else:
                    years_data_df = concat_data([years_data_df, year_tick_df])

            # get data id key
            tick_key = self._db_connector._db_key('forex',
                                                  ticker,
                                                  TICK_TIMEFRAME)

            # call to upload df to database if not empty
            if not is_empty_dataframe(years_data_df):
                self._db_connector.write_data(tick_key,
                                              years_data_df)

                # update years list in local info file
                self._db_connector.add_tickers_years_info_to_file(ticker,
                                                                  TICK_TIMEFRAME,
                                                                  years)

                # update internal ticker years list info
                self._tickers_years_dict[ticker][TICK_TIMEFRAME].extend(years)
                # sort and remove duplicates
                self._tickers_years_dict[ticker][TICK_TIMEFRAME].sort()
                self._tickers_years_dict[ticker][TICK_TIMEFRAME] = \
                    list_remove_duplicates(self._tickers_years_dict[ticker][TICK_TIMEFRAME])

            else:
                logger.bind(
                    target='histmanager').warning(
                    f'Years data dataframe for {tick_key} is empty, skipping database write')

    def clear_database(self, filter: Optional[str] = None) -> None:

        self._db_connector.clear_database(filter=filter)

        if filter:
            self._tickers_years_dict = self._db_connector.load_tickers_years_info()
        else:
            self._tickers_years_dict.clear()

    def add_timeframe(self, timeframe: str | List[str]) -> None:
        """
        Add and cache a new timeframe to the database.

        Creates aggregated data for the specified timeframe from tick data and
        caches it in the database for faster future access. The timeframe is
        added to the internal list of available timeframes.

        Args:
            timeframe (str | List[str]): Timeframe(s) to add. Can be a single string
                or list of strings. Supported values: '1m', '5m', '15m', '30m',
                '1h', '4h', '1D', '1W', '1M'

        Returns:
            None

        Raises:
            TypeError: If timeframe is not a string or list of strings

        Example:
            >>> manager = HistoricalManagerDB(config='data_config.yaml')
            >>> manager.add_timeframe('1W')  # Add weekly timeframe
            >>> manager.add_timeframe(['4h', '1D'])  # Add multiple timeframes

        Note:
            - Only new timeframes (not already in the list) will be processed
            - Aggregation can take time for large datasets
            - Once added, the timeframe is permanently cached in the database
        """

        if not hasattr(self, '_tf_list'):
            self._tf_list = []

        if isinstance(timeframe, str):

            timeframe = [timeframe]

        if not (
            isinstance(timeframe, list) and
            all([isinstance(tf, str) for tf in timeframe])
        ):

            logger.bind(target='histmanager').error(
                'timeframe invalid: str or list required')
            raise TypeError

        tf_list = [check_timeframe_str(tf, engine=self.engine) for tf in timeframe]

        if not set(tf_list).issubset(self._tf_list):

            # concat timeframe accordingly
            # only just new elements not already present
            self._tf_list.extend(set(tf_list).difference(self._tf_list))

    def get_data(
        self,
        ticker,
        timeframe,
        start,
        end,
        comparison_column_name: List[str] | str | None = None,
        check_level: List[int | float] | int | float | None = None,
        comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
        aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None,
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:
        """
        Retrieve OHLC historical data for the specified ticker and timeframe.

        Fetches historical forex data from the database, automatically downloading
        and aggregating data if not already available. Supports multiple timeframes
        and date ranges.

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD', 'NZDUSD').
                Case-insensitive.
            timeframe (str): Candle timeframe for data aggregation. Supported frames:
                1s (1 second)
                1m (1 minute)
                1h (1 hour)
                1d (1 calendar day)
                1w (1 calendar week)
                1mo (1 calendar month)
                1q (1 calendar quarter)
                1y (1 calendar year)
            and any multiple of these values by a positive integer, e.g.: '2m', '3m', '2h', '3h', etc.
            start (str | datetime): Start date for data retrieval. Accepts:
                - ISO format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
                - datetime object
            end (str | datetime): End date for data retrieval. Same format as start.
                Must be after start date.
            comparison_column_name (List[str] | str | None): Column names to retrieve. Default is None.
            check_level (List[int | float] | int | float | None): Check level for conditions. Default is None.
            comparison_operator (List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None): Condition for data retrieval. Default is None.
            aggregation_mode (SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None): Aggregation mode for data retrieval. Default is None.

        Returns:
            PolarsDataFrame | PolarsLazyFrame: DataFrame containing OHLC data with columns:
                - timestamp: datetime column with candle timestamps
                - open: Opening price (float32)
                - high: Highest price (float32)
                - low: Lowest price (float32)
                - close: Closing price (float32)

        Raises:
            TickerNotFoundError: If the ticker is not available in the historical database
            ValueError: If timeframe is invalid or end date is before start date

        Example:
            >>> manager = HistoricalManagerDB(config='data_config.yaml')
            >>> data = manager.get_data(
            ...     ticker='EURUSD',
            ...     timeframe='1h',
            ...     start='2020-01-01',
            ...     end='2020-01-31'
            ... )
            >>> print(f"Retrieved {len(data)} hourly candles")
            Retrieved 744 hourly candles

        Note:
            - Data is automatically downloaded from histdata.com if not cached locally
            - First call for a new timeframe may take longer as it builds the aggregation
            - Downloaded data is cached for faster subsequent access
            - Ticker names are case-insensitive and automatically normalized
        """

        # check ticker exists in available tickers
        # from histdata database
        if (
            ticker.upper() not in self._histdata_tickers_list
            and
            ticker.lower() not in self._get_ticker_list()
        ):
            logger.bind(target='histmanager').error(
                f'ticker {ticker.upper()} not found in database')
            raise TickerNotFoundError(f'ticker {ticker} not found in database')

        # force ticker parameter to lower case
        ticker = ticker.lower()

        # force timeframe parameter to lower case
        timeframe = timeframe.lower()

        if not check_timeframe_str(timeframe, engine=self.engine):

            logger.bind(target='histmanager').error(
                f'timeframe request {timeframe} invalid')
            raise ValueError(f'timeframe request {timeframe} invalid')

        else:
            if start == 'now':
                raise ValueError("start date cannot be 'now'")
            if end == 'now':
                end = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            start = any_date_to_datetime64(start)
            end = any_date_to_datetime64(end)

        # sanity checks on query dates specific to historical data
        if end < start:

            logger.bind(target='histmanager').error(
                'date interval not coherent, '
                'end must be older than start')
            return self._dataframe_type([])

        if start < HISTORICAL_DB_MIN_DATE:

            logger.bind(target='histmanager').error(
                f'start date {start} is older than the minimum '
                f'date in database {HISTORICAL_DB_MIN_DATE}')
            return self._dataframe_type([])

        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        if start > now_utc:

            logger.bind(target='histmanager').error(
                f'start date {start} is newer than the maximum '
                f'date in database {now_utc}')
            raise ValueError(f'start date {start} is newer now date')

        # get years including interval requested
        years_interval_req = list(range(start.year, end.year + 1, 1))

        # Initialize ticker/timeframe in dict if not present
        if ticker not in self._tickers_years_dict:
            self._tickers_years_dict[ticker] = {}
        if timeframe not in self._tickers_years_dict[ticker]:
            self._tickers_years_dict[ticker][timeframe] = []
        if TICK_TIMEFRAME not in self._tickers_years_dict[ticker]:
            self._tickers_years_dict[ticker][TICK_TIMEFRAME] = []

        # determine if current year data new download
        # has to be requested
        current_year = now_utc.year
        is_current_year_requested = False

        ticker_available_last_timestamp = self._db_connector.read_last_timestamp('forex', ticker)

        if current_year in years_interval_req:

            if not ticker_available_last_timestamp:

                # ticker is not in DD
                # request for current year forced to True
                is_current_year_requested = True

            elif end >= ticker_available_last_timestamp:

                # determine if to include and update of current year data
                # if not is on a weekend day (Saturday or Sunday)
                # set now as previous Friday at 17:00
                if now_utc.weekday() in [5, 6]:
                    now_ref = now_utc - timedelta(days=now_utc.weekday() + 1)
                    now_ref = now_ref.replace(hour=17, minute=0, second=0, microsecond=0)
                else:
                    now_ref = now_utc

                is_current_year_requested = (
                    (
                        now_ref
                        -
                        ticker_available_last_timestamp
                    ).total_seconds()
                    >
                    to_timedelta(self.max_discrepancy_with_now).total_seconds()
                )

            else:

                # db already cover request for current year
                # end date is less recent than available timestamp
                is_current_year_requested = False

        # here determine if to ask for new download
        # if requested years are not already in localdb (tracked by tickers years dict)
        # or if current year is requested
        if (
            not set(years_interval_req).issubset(
                self._tickers_years_dict[ticker][timeframe]) or
            is_current_year_requested
        ):

            # If the current year is requested, we force re-aggregation
            # by removing it from the known timeframe list in memory
            if is_current_year_requested:
                for tf in list(self._tickers_years_dict[ticker].keys()):
                    if tf != TICK_TIMEFRAME and current_year in self._tickers_years_dict[ticker][tf]:
                        self._tickers_years_dict[ticker][tf].remove(current_year)

            year_tf_missing = list(
                set(years_interval_req).difference(
                    self._tickers_years_dict[ticker][timeframe]))

            year_tick_missing = list(set(years_interval_req).difference(
                self._tickers_years_dict[ticker][TICK_TIMEFRAME]
            ))
            if is_current_year_requested and current_year not in year_tick_missing:
                year_tick_missing.append(current_year)

            lock_file = str(Path(self.data_path) / "tickers_years_info.json.lock")

            with FileLock(lock_file):
                # Re-read the JSON file to fetch the latest state from disk
                self._tickers_years_dict = self._db_connector.create_tickers_years_dict()

                # Initialize ticker/timeframe in dict if not present after rebuild
                if ticker not in self._tickers_years_dict:
                    self._tickers_years_dict[ticker] = {}
                if timeframe not in self._tickers_years_dict[ticker]:
                    self._tickers_years_dict[ticker][timeframe] = []
                if TICK_TIMEFRAME not in self._tickers_years_dict[ticker]:
                    self._tickers_years_dict[ticker][TICK_TIMEFRAME] = []

                if is_current_year_requested:
                    for tf in list(self._tickers_years_dict[ticker].keys()):
                        if tf != TICK_TIMEFRAME and current_year in self._tickers_years_dict[ticker][tf]:
                            self._tickers_years_dict[ticker][tf].remove(current_year)

                # Re-calculate missing years after re-reading
                year_tick_missing = list(set(years_interval_req).difference(
                    self._tickers_years_dict[ticker][TICK_TIMEFRAME]
                ))
                if is_current_year_requested and current_year not in year_tick_missing:
                    year_tick_missing.append(current_year)

                # ONLY download years not already in the database
                if year_tick_missing:
                    self._download(
                        ticker,
                        year_tick_missing
                    )
                else:
                    logger.bind(
                        target='histmanager').info(
                        f"Skipped downloading {ticker} {years_interval_req} as it was managed by another process.")

                # add timeframe and update db INSIDE the lock so that
                # a concurrent process can't read a parquet that is still
                # being written by this process
                self.add_timeframe(timeframe)
                self._update_db(ticker)

                if not set(years_interval_req).issubset(
                        self._tickers_years_dict[ticker][timeframe]):

                    logger.bind(target='histmanager').critical(
                        f'processing year data completion for '
                        f'{years_interval_req} not ok')
                    raise ValueError

        # execute a read query on database
        return self._db_connector.read_data(
            market='forex',
            ticker=ticker,
            timeframe=timeframe,
            start=start,
            end=end,
            comparison_column_name=comparison_column_name,
            check_level=check_level,
            comparison_operator=comparison_operator,
            comparison_aggregation_mode=aggregation_mode
        )

    def get_data_window(
        self,
        ticker: str,
        date: date,
        timeframe: str,
        periods: int,
        direction: Literal['backward', 'forward'],
        comparison_column_name: List[str] | str | None = None,
        check_level: List[int | float] | int | float | None = None,
        comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
        comparison_aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None,
    ) -> Union[PolarsDataFrame, PolarsLazyFrame]:

        """
        Retrieve OHLC historical window data for the specified ticker.
        The unit resoluton of the window is set equal to the timeframe.
        Unit resolution is the timespan between two candles (rows) in
        normal conditions: during weekends the rule does not apply.
        The window total number of candles (rows) is specified by timeframe * periods.

        Fetches historical forex data from the database, automatically downloading
        and aggregating data if not already available. Supports multiple timeframes

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD', 'NZDUSD').
                Case-insensitive.
            date (str | datetime): date for data retrieval. Accepts:
                - ISO format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
                - datetime object
            timeframe (str): Candle timeframe for data aggregation. Supported frames:
                1s (1 second)
                1m (1 minute)
                1h (1 hour)
                1d (1 calendar day)
                1w (1 calendar week)
                1mo (1 calendar month)
                1q (1 calendar quarter)
                1y (1 calendar year)
                periods (int): Number of timeframe units to look back or forward.
            direction (Literal['backward', 'forward']): Direction to look back
                ('backward' or 'forward').
            comparison_column_name (List[str] | str | None): List of column names
                to compare.
                If None, no comparison is performed.
            check_level (List[int | float] | int | float | None): List of values
                to compare against.
                If None, no comparison is performed.
            comparison_operator (List[SUPPORTED_SQL_COMPARISON_OPERATORS] |
                SUPPORTED_SQL_COMPARISON_OPERATORS | None): List of comparison
                operators to use for comparison.
                If None, no comparison is performed.
            comparison_aggregation_mode (SUPPORTED_SQL_CONDITION_AGGREGATION_MODES
                | None): Aggregation mode to use for comparison.
                If None, no comparison is performed.

        Returns:
            Union[PolarsDataFrame, PolarsLazyFrame]: DataFrame with the historical
                data.

        Raises:
            TickerNotFoundError: If the ticker is not found.
            TickerDataNotFoundError: If the ticker data is not found.
            TickerDataBadTypeException: If the ticker data is not of the expected
                type.
            TickerDataInvalidException: If the ticker data is invalid.

        Examples:
            >>> get_data_window(
            ...     ticker='EURUSD',
            ...     date='2022-01-01',
            ...     timeframe='1m',
            ...     window=10,
            ...     direction='backward'
            ... )
        """

        # check ticker exists in available tickers
        # from histdata database
        if (
            ticker.upper() not in self._histdata_tickers_list and
            ticker.lower() not in self._get_ticker_list()
        ):
            logger.bind(target='histmanager').error(
                f'ticker {ticker.upper()} not found in database')
            raise TickerNotFoundError(f'ticker {ticker} not found in database')

        # force ticker parameter to lower case
        ticker = ticker.lower()

        # force timeframe parameter to lower case
        timeframe = timeframe.lower()

        if not check_timeframe_str(timeframe, engine=self.engine):

            logger.bind(target='histmanager').error(
                f'timeframe request {timeframe} invalid')
            raise ValueError(f'timeframe request {timeframe} invalid')

        if date == 'now':
            if direction == 'forward':
                raise ValueError("start date cannot be 'now'")
            date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        if direction == 'backward':
            end = any_date_to_datetime64(date)
            start = estimate_start_date_to_business_days(end, timeframe, periods, FOREX_HOLIDAYS)
        else:
            start = any_date_to_datetime64(date)
            end = estimate_end_date_to_business_days(start, timeframe, periods, FOREX_HOLIDAYS)

        # sanity checks on query dates specific to historical data
        if end < start:

            logger.bind(target='histmanager').error(
                'date interval not coherent, '
                'end must be older than start')
            return self._dataframe_type([])

        if start < HISTORICAL_DB_MIN_DATE:

            logger.bind(target='histmanager').error(
                f'start date {start} is older than the minimum '
                f'date in database {HISTORICAL_DB_MIN_DATE}')
            return self._dataframe_type([])

        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        if start > now_utc:

            logger.bind(target='histmanager').error(
                f'start date {start} is newer than the maximum '
                f'date in database {now_utc}')
            raise ValueError(f'start date {start} is newer now date')

        # get years including interval requested
        years_interval_req = list(range(start.year, end.year + 1, 1))

        # Initialize ticker/timeframe in dict if not present
        if ticker not in self._tickers_years_dict:
            self._tickers_years_dict[ticker] = {}
        if timeframe not in self._tickers_years_dict[ticker]:
            self._tickers_years_dict[ticker][timeframe] = []
        if TICK_TIMEFRAME not in self._tickers_years_dict[ticker]:
            self._tickers_years_dict[ticker][TICK_TIMEFRAME] = []

        # determine if current year data new download
        # has to be requested
        is_current_year_requested = False
        current_year = now_utc.year

        ticker_available_last_timestamp = self._db_connector.read_last_timestamp('forex', ticker)

        if current_year in years_interval_req:

            if not ticker_available_last_timestamp:

                # ticker is not in DD
                # request for current year forced to True
                is_current_year_requested = True

            elif end >= ticker_available_last_timestamp:

                # determine if to include and update of current year data
                # if not is on a weekend day (Saturday or Sunday)
                # set now as previous Friday at 17:00
                if now_utc.weekday() in [5, 6]:
                    now_ref = now_utc - timedelta(days=now_utc.weekday() + 1)
                    now_ref = now_ref.replace(hour=17, minute=0, second=0, microsecond=0)
                else:
                    now_ref = now_utc

                is_current_year_requested = (
                    (
                        now_ref
                        -
                        ticker_available_last_timestamp
                    ).total_seconds()
                    >
                    to_timedelta(self.max_discrepancy_with_now).total_seconds()
                )

            else:

                # db already cover request for current year
                # end date is less recent than available timestamp
                is_current_year_requested = False

        # here determine if to ask for new download
        # if requested years are not already in localdb (tracked by tickers years dict)
        # or if current year is requested
        if (
            not set(years_interval_req).issubset(
                self._tickers_years_dict[ticker][timeframe]) or
            is_current_year_requested
        ):

            # If the current year is requested, we force re-aggregation by removing it from the known timeframe list in memory
            if is_current_year_requested:
                for tf in list(self._tickers_years_dict[ticker].keys()):
                    if tf != TICK_TIMEFRAME and current_year in self._tickers_years_dict[ticker][tf]:
                        self._tickers_years_dict[ticker][tf].remove(current_year)

            year_tf_missing = list(
                set(years_interval_req).difference(
                    self._tickers_years_dict[ticker][timeframe]))

            year_tick_missing = list(set(years_interval_req).difference(
                self._tickers_years_dict[ticker][TICK_TIMEFRAME]
            ))
            if is_current_year_requested and current_year not in year_tick_missing:
                year_tick_missing.append(current_year)

            lock_file = str(Path(self.data_path) / "tickers_years_info.json.lock")

            with FileLock(lock_file):
                # Re-read the JSON file to fetch the latest state from disk
                self._tickers_years_dict = self._db_connector.create_tickers_years_dict()

                # Initialize ticker/timeframe in dict if not present after rebuild
                if ticker not in self._tickers_years_dict:
                    self._tickers_years_dict[ticker] = {}
                if timeframe not in self._tickers_years_dict[ticker]:
                    self._tickers_years_dict[ticker][timeframe] = []
                if TICK_TIMEFRAME not in self._tickers_years_dict[ticker]:
                    self._tickers_years_dict[ticker][TICK_TIMEFRAME] = []

                if is_current_year_requested:
                    for tf in list(self._tickers_years_dict[ticker].keys()):
                        if tf != TICK_TIMEFRAME and current_year in self._tickers_years_dict[ticker][tf]:
                            self._tickers_years_dict[ticker][tf].remove(current_year)

                # Re-calculate missing years after re-reading
                year_tick_missing = list(set(years_interval_req).difference(
                    self._tickers_years_dict[ticker][TICK_TIMEFRAME]
                ))
                if is_current_year_requested and current_year not in year_tick_missing:
                    year_tick_missing.append(current_year)

                # ONLY download years not already in the database
                if year_tick_missing:
                    self._download(
                        ticker,
                        year_tick_missing
                    )
                else:
                    logger.bind(
                        target='histmanager').info(
                        f"Skipped downloading {ticker} {years_interval_req} as it was managed by another process.")

                # add timeframe and update db INSIDE the lock so that
                # a concurrent process can't read a parquet that is still
                # being written by this process
                self.add_timeframe(timeframe)
                self._update_db(ticker)

                if not set(years_interval_req).issubset(
                        self._tickers_years_dict[ticker][timeframe]):

                    logger.bind(target='histmanager').critical(
                        f'processing year data completion for '
                        f'{years_interval_req} not ok')
                    raise ValueError

        return self._db_connector.read_data_window(
            market='forex',
            date=any_date_to_datetime64(date),
            ticker=ticker,
            timeframe=timeframe,
            periods=periods,
            direction=direction,
            comparison_column_name=comparison_column_name,
            check_level=check_level,
            comparison_operator=comparison_operator,
            comparison_aggregation_mode=comparison_aggregation_mode
        )

    def plot(
        self,
        ticker,
        timeframe,
        start_date,
        end_date
    ) -> None:
        """
        Plot candlestick chart for the specified ticker and date range.

        Generates an interactive candlestick chart using mplfinance, displaying
        OHLC (Open, High, Low, Close) data for the specified time period.

        Args:
            ticker (str): Currency pair symbol (e.g., 'EURUSD', 'GBPUSD')
            timeframe (str): Candle timeframe (e.g., '1m', '5m', '1h', '1D', '1W')
            start_date (str): Start date in ISO format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
            end_date (str): End date in ISO format 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'

        Returns:
            None: Displays the chart using matplotlib

        Example:
            >>> manager = HistoricalManagerDB(config='data_config.yaml')
            >>> manager.plot(
            ...     ticker='EURUSD',
            ...     timeframe='1D',
            ...     start_date='2020-01-01',
            ...     end_date='2020-12-31'
            ... )

        Note:
            The chart will be displayed in a matplotlib window. The data is automatically
            fetched using get_data() and converted to the appropriate format for plotting.
        """

        chart_data = self.get_data(ticker=ticker,
                                   timeframe=timeframe,
                                   start=start_date,
                                   end=end_date)

        chart_data = to_pandas_dataframe(chart_data)

        if chart_data.index.name != BASE_DATA_COLUMN_NAME.TIMESTAMP:

            chart_data.set_index(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                 inplace=True)

            chart_data.index = to_datetime(chart_data.index)

        else:
            logger.bind(target='histmanager').trace(
                f'Chart data already has {BASE_DATA_COLUMN_NAME.TIMESTAMP} as index')

        # candlestick chart type
        # use mplfinance
        chart_kwargs = dict(style='charles',
                            title=ticker,
                            ylabel='Quotation',
                            xlabel='Timestamp',
                            volume=False,
                            figratio=(12, 8),
                            figscale=1
                            )

        mpf_plot(chart_data, type='candle', **chart_kwargs)

        mpf_show()

    def close(self):

        # update tickers years info file with current data status
        self._db_connector.save_tickers_years_info(self._tickers_years_dict)

        # clear temporary files
        self._clear_temporary_data_folder()

        # call connectors clear method
        for conn in self.get_source_connectors():
            conn.clear_temporary_folder()
