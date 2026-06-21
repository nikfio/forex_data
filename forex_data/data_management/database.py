# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 00:02:36 2025

@author: fiora
"""

'''
    Module to connect to a database instance

    Design constraint:

        start with only support for polars, prefer lazyframe when possibile

        read and write using polars dataframe or lazyframe
        exec requests using SQL query language
        OSS versions for windows required
'''

import time
import requests
from requests import Session
from filelock import FileLock
from io import BytesIO
from zipfile import (
    ZipFile,
    ZipExtFile,
    BadZipFile
)
from textwrap import dedent
from bs4 import BeautifulSoup

from polars import (
    DataFrame as PolarsDataFrame,
    LazyFrame as PolarsLazyFrame,
    concat
)

from pandas import to_timedelta
import json
from datetime import datetime, timedelta
from pathlib import Path as PathType
from typing import Any, Dict, List, Optional, Tuple, Union, cast, Literal
from numpy import array
from re import (
    fullmatch,
    search,
    IGNORECASE
)
from attrs import (
    define,
    field,
    validate,
    validators
)
from loguru import logger
from pathlib import Path


from ..config import _apply_config
from .common import *

# BASE CONNECTOR


@define(kw_only=True, slots=True)
class DatabaseConnector:

    data_path: Union[str, Path] = field(default='', validator=validators.or_(
        validators.instance_of(str), validators.instance_of(Path)))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))
    polars_gpu_engine: bool = field(default=False,
                                    validator=validators.instance_of(bool))

    _tickers_years_info_filepath = field(default=Path('.'))
    _local_files_cache: Any = field(default=None, init=False)
    _last_timestamp_cache: Any = field(default=None, init=False)

    def __init__(self, **kwargs: Any) -> None:

        pass

    def __attrs_post_init__(self) -> None:

        self._last_timestamp_cache = {}
        # create data folder if not exists
        self.data_path = Path(self.data_path).expanduser().resolve()
        if (
            not self.data_path.exists() or
            not self.data_path.is_dir()
        ):

            self.data_path.mkdir(parents=True,
                                 exist_ok=True)

    def connect(self) -> Any:
        """Connect to database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement connect")

    def check_connection(self) -> bool:
        """Check database connection - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement check_connection")

    def write_data(self,
                   target_table: str,
                   dataframe: Union[PolarsDataFrame,
                                    PolarsLazyFrame],
                   clean: bool = False) -> None:
        """Write data to database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement write_data")

    def read_data(
            self,
            market: str,
            ticker: str,
            timeframe: str,
            start: datetime,
            end: datetime) -> PolarsLazyFrame:
        """Read data from database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement read_data")

    def read_data_year(self,
                       market: str,
                       ticker: str,
                       timeframe: str,
                       years: int | List[int]) -> PolarsLazyFrame:
        """Read data for specific year(s) - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement read_data_year")

    def read_data_window(self,
                         market: str,
                         ticker: str,
                         timeframe: str,
                         date: datetime | str | Timestamp | PolarsDatetime,
                         periods: int,
                         direction : Literal['backward', 'forward'],
                         comparison_column_name: List[str] | str | None = None,
                         check_level: List[int | float] | int | float | None = None,
                         comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
                         comparison_aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None
                         ) -> PolarsLazyFrame:
        """Read window of data - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement read_data_window")

    def read_last_timestamp(self,
                            market: str,
                            ticker: str,
                            timeframe: str = None) -> datetime:
        """Read last timestamp from database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement read_last_timestamp")

    def _list_local_data(self) -> List[PathType]:
        if self._local_files_cache is not None:
            return self._local_files_cache

        local_files = []
        local_files_name = []

        # list for all data filetypes supported
        local_files = [file for file in list(self.data_path.rglob(f'*'))
                       if search(self.data_type + '$', file.suffix)]

        local_files_name = [file.name for file in local_files]

        self._local_files_cache = (local_files, local_files_name)
        return self._local_files_cache

    def _list_tables(self) -> List[str]:

        local_files, tables_list = self._list_local_data()

        return tables_list

    def _get_items_from_db_key(self, key) -> tuple:
        return tuple(key.split('_'))

    def _get_file_items(self, filename: str) -> tuple:

        if not (
                isinstance(filename, str)
        ):

            logger.bind(target='localdb').error(
                'filename {filename} invalid type: required str')
            raise TypeError(f'filename {filename} invalid type: required str')

        file_items = self._get_items_from_db_key(filename)

        # return each file details
        return file_items

    def _db_key(self, market: str, ticker: str, timeframe: str) -> str:
        """Generate database key - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _db_key")

    def _get_ticker_years_list_from_db(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:
        """Get list of years for ticker and timeframe - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _get_ticker_years_list_from_db")

    def get_tickers_list(self) -> List[str]:

        tickers_list = []

        local_files, local_files_name = self._list_local_data()

        for filename in local_files_name:

            items = self._get_file_items(filename)
            tickers_list.append(items[DATA_KEY.TICKER_INDEX])

        return list_remove_duplicates(tickers_list)

    def get_ticker_keys(
            self,
            ticker: str,
            timeframe: Optional[str] = None) -> List[str]:

        local_files, local_files_name = self._list_local_data()

        keys = [Path(key).stem for key in local_files_name]
        if timeframe:

            return [
                key for key in keys
                if search(f'{ticker}',
                          key) and
                self._get_items_from_db_key(key)[DATA_KEY.TF_INDEX] ==
                timeframe
            ]

        else:

            return [
                key for key in keys
                if search(f'{ticker}',
                          key)
            ]

    def get_ticker_timeframes_list(
            self,
            ticker: str) -> List[str]:

        """Get timeframes list for ticker - must be implemented by subclasses."""
        local_files, local_files_name = self._list_local_data()

        return list_remove_duplicates([
            self._get_items_from_db_key(Path(key).stem)[DATA_KEY.TF_INDEX] for key in local_files_name
            if search(f'{ticker.lower()}',
                      key.lower())
        ])

    def get_ticker_years_list(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME
    ) -> List[int]:

        # check ticker is present in cache
        if ticker not in self.get_tickers_list():
            logger.bind(target='localdb').error(
                f'Ticker {ticker} not found in cache')
            raise ValueError(f'Ticker {ticker} not found in cache')
        # check if tickers years dict has data for ticker
        # and optionally timeframe
        return self._get_ticker_years_list_from_db(ticker, timeframe)

    def create_tickers_years_dict(self) -> Dict[str, Dict[str, List[int]]]:
        """
        Create a dictionary containing ticker years data, structured as:
        {ticker: {timeframe: [year1, year2, ...]}}

        If no data files exist yet, returns an empty dictionary.
        """

        tickers_years_dict = {}

        tickers_list = self.get_tickers_list()

        # If no tickers exist yet, return empty dict
        if not tickers_list:
            return tickers_years_dict

        for ticker in tickers_list:
            tickers_years_dict[ticker] = {}
            # Get all keys for this ticker and extract timeframes
            ticker_keys = self.get_ticker_keys(ticker)
            timeframes = list(set([
                self._get_items_from_db_key(key)[DATA_KEY.TF_INDEX]
                for key in ticker_keys
            ]))

            for timeframe in timeframes:
                years_list = self._get_ticker_years_list_from_db(ticker, timeframe)
                if years_list:  # Only add if there are years
                    tickers_years_dict[ticker][timeframe] = years_list

        # save tickers years info to file
        self.save_tickers_years_info(tickers_years_dict)
        return tickers_years_dict

    def save_tickers_years_info(
        self,
        ticker_years_dict: Dict[str, Dict[str, List[int]]],
    ) -> None:
        """
        Save ticker years list to a JSON file.

        Parameters
        ----------
        ticker_years_dict : Dict[str, Dict[str, List[int]]]
            Dictionary containing ticker years data, structured as:
            {ticker: {timeframe: [year1, year2, ...]}}
        filename : str, optional
            Name of the JSON file to save the data, by default 'tickers_years.json'

        Raises
        ------
        TypeError
            If ticker_years_dict is not a dictionary
        IOError
            If there's an error writing the file
        """
        if not isinstance(ticker_years_dict, dict):
            logger.bind(target='localdb').error(
                f'ticker_years_dict must be a dictionary, got {type(ticker_years_dict)}'
            )
            raise TypeError(
                f'ticker_years_dict must be a dictionary, got {
                    type(ticker_years_dict)}')

        try:
            with open(self._tickers_years_info_filepath, 'w') as f:
                json.dump(ticker_years_dict, f, indent=2)
        except Exception as e:
            logger.bind(
                target='localdb').error(
                f'Error writing ticker years data to {
                    self._tickers_years_info_filepath}: {e}')
            raise IOError(
                f'Error writing ticker years data to {
                    self._tickers_years_info_filepath}: {e}')

    def add_tickers_years_info_to_file(
            self,
            ticker: str,
            timeframe: str,
            year: Union[int, List[int]]
    ) -> None:
        """
        In local info filepath, update just the years list of the given ticker and timeframe
        by adding the year(s) specified if not already present

        Parameters
        ----------
        ticker : str
            The ticker symbol to update
        timeframe : str
            The timeframe for the ticker data
        year : Union[int, List[int]]
            The year or list of years to add to the years list

        Raises
        ------
        TypeError
            If year is not an integer or list of integers
        """
        # Normalize year to a list
        if isinstance(year, int):
            years_to_add = [year]
        elif isinstance(year, list):
            # Validate all items in list are integers
            if not all(isinstance(y, int) for y in year):
                logger.bind(target='localdb').error(
                    f'All items in year list must be integers'
                )
                raise TypeError(f'All items in year list must be integers')
            years_to_add = year
        else:
            logger.bind(target='localdb').error(
                f'year must be an integer or list of integers, got {type(year)}'
            )
            raise TypeError(
                f'year must be an integer or list of integers, got {type(year)}')

        # Load existing data or create new dict if file doesn't exist
        if self._tickers_years_info_filepath.exists():
            ticker_years_dict = self.load_tickers_years_info()
        else:
            ticker_years_dict = {}
            logger.bind(
                target='localdb').info(
                f'File {
                    self._tickers_years_info_filepath} does not exist. Creating new dict.')

        # Update the dictionary with the new years
        _, changes_made = update_ticker_years_dict(
            ticker_years_dict,
            ticker,
            timeframe,
            years_to_add
        )

        # Only save if changes were made
        if changes_made:
            self.save_tickers_years_info(ticker_years_dict)

    def clear_tickers_years_info(self, filter: Optional[str] = None) -> None:
        """
        Clear the tickers years info file.
        If filter is specified, it has to be a ticker value and so
        only the tickers years info related to the filter are cleared.
        If filter is not specified, the entire file is cleared.
        Parameters
        ----------
        filter : Optional[str], optional
            Filter to apply to the tickers years info file, by default None
            Filter has to be a ticker value
        """
        if filter:
            ticker_years_dict = self.load_tickers_years_info()
            ticker_years_dict = {
                k: v for k,
                v in ticker_years_dict.items() if filter.lower() != k.lower()}
            self.save_tickers_years_info(ticker_years_dict)
        else:
            self._tickers_years_info_filepath.unlink(missing_ok=True)

    def clear_database(self, filter: Optional[str] = None) -> None:
        """
        Clear database files
        If filter is provided and is a ticker present in database (files present)
        delete only files related to that ticker
        """

        # create a list of data files
        # with extension matching either one of the supported data types
        data_files = [
            file for file in self.data_path.rglob('*')
            if file.is_file() and any(
                search(suffix, file.suffix, IGNORECASE)
                for suffix in SUPPORTED_DATA_FILES
            )
        ]

        if filter:

            # in local path search for files having filter in path stem
            # and delete them
            # list all files in local path ending with data_type
            # and use re.search to catch matches
            if isinstance(filter, str):

                if data_files:
                    for file in data_files:
                        if search(filter, file.stem, IGNORECASE):
                            file.unlink(missing_ok=True)

                    # clear just ticker years info in all tickers years info json file
                    self.clear_tickers_years_info(filter=filter)
                else:
                    logger.bind(target='localdb').info(
                        f'No data files found in {self.data_path} with filter {filter}')

            else:
                logger.bind(target='localdb').error(
                    f'Filter {filter} invalid type: required str')

        else:

            # clear all data files in data path
            for file in data_files:
                file.unlink(missing_ok=True)

            # clear the json file containing tickers years info
            self.clear_tickers_years_info()

    def load_tickers_years_info(self) -> Dict[str, Dict[str, List[int]]]:
        """
        Load ticker years list from a JSON file.

        Returns
        -------
        Dict[str, Dict[str, List[int]]]
            Dictionary containing ticker years data, structured as:
            {ticker: {timeframe: [year1, year2, ...]}}

        Raises
        ------
        FileNotFoundError
            If the JSON file doesn't exist
        IOError
            If there's an error reading the file
        """
        if not self._tickers_years_info_filepath.exists():
            logger.bind(target='localdb').error(
                f'File {self._tickers_years_info_filepath} not found'
            )
            raise FileNotFoundError(
                f'File {self._tickers_years_info_filepath} not found')

        try:
            with open(self._tickers_years_info_filepath, 'r') as f:
                ticker_years_dict = json.load(f)
            return ticker_years_dict
        except json.JSONDecodeError as e:
            logger.bind(target='localdb').error(
                f'Error decoding JSON from {self._tickers_years_info_filepath}: {e}'
            )
            raise IOError(
                f'Error decoding JSON from {self._tickers_years_info_filepath}: {e}')
        except Exception as e:
            logger.bind(
                target='localdb').error(
                f'Error reading ticker years data from {
                    self._tickers_years_info_filepath}: {e}')
            raise IOError(
                f'Error reading ticker years data from {
                    self._tickers_years_info_filepath}: {e}')


# LOCAL DATABASE CONNECTOR CLASS


@define(kw_only=True, slots=True)
class LocalDBConnector(DatabaseConnector):

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

        super().__attrs_post_init__()

        # set up log sink for LocalDB
        # Remove existing handlers for this sink to prevent duplicate log entries
        log_path = Path(self.data_path) / 'log' / 'localdb.log'

        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            try:
                logger.remove(handler_id)
            except ValueError:
                # Handler already removed by another thread
                pass

        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('localdb' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        self.data_path = Path(self.data_path)
        self._tickers_years_info_filepath = self.data_path / 'tickers_years_info.json'

    def _db_key(self,
                market: str,
                ticker: str,
                timeframe: str
                ) -> str:
        """

        get a str key of dotted divided elements

        key template = ticker.timeframe.data_type

        Parameters
        ----------
        ticker : TYPE
            DESCRIPTION.
        year : TYPE
            DESCRIPTION.
        data_type : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        return '_'.join([market.lower(),
                         ticker.lower(),
                         timeframe.lower()])

    def _get_filename(self, market: str, ticker: str, tf: str) -> str:

        # based on standard filename template
        return FILENAME_STR.format(market=market.lower(),
                                   ticker=ticker.lower(),
                                   tf=tf.lower(),
                                   file_ext=self.data_type.lower())

    def _get_ticker_years_list_from_db(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:

        ticker_years_list = []
        table = ''

        local_files, local_files_name = self._list_local_data()

        files = [
            key for key in local_files
            if search(f'{ticker.lower()}',
                      str(key.stem)) and
            self._get_items_from_db_key(str(key.stem))[DATA_KEY.TF_INDEX] ==
            timeframe.lower()
        ]

        dataframe = None

        if len(files) == 1:

            if self.data_type == DATA_TYPE.CSV_FILETYPE:

                dataframe = read_csv(self.engine, files[0])

            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                dataframe = read_parquet(self.engine, files[0])

            try:

                query = f'''SELECT DISTINCT STRFTIME({
                    BASE_DATA_COLUMN_NAME.TIMESTAMP}, '%Y')
                            AS YEAR
                            FROM self'''
                read = dataframe.sql(query)

            except Exception as e:

                logger.bind(target='localdb').error(
                    f'Error querying table {table}: {e}')
                raise

            else:

                ticker_years_list = [int(row[0]) for row in collect_lazyframe(read, self.polars_gpu_engine).iter_rows()]

        return ticker_years_list

    def write_data(
        self,
        target_table: str,
        dataframe: Union[PolarsDataFrame, PolarsLazyFrame],
        clean: bool = False
    ) -> None:

        self._local_files_cache = None
        items = self._get_items_from_db_key(target_table)

        filename = self._get_filename(items[DATA_KEY.MARKET],
                                      items[DATA_KEY.TICKER_INDEX],
                                      items[DATA_KEY.TF_INDEX])

        filepath = (self.data_path /
                    items[DATA_KEY.MARKET] /
                    items[DATA_KEY.TICKER_INDEX] /
                    filename)

        # Per-file lock prevents concurrent processes from corrupting the
        # parquet file by interleaving a read-existing + write sequence.
        file_lock_path = str(filepath) + '.lock'
        with FileLock(file_lock_path):
            if (
                    not filepath.exists() or
                not filepath.is_file()
            ):

                filepath.parent.mkdir(parents=True,
                                      exist_ok=True)

            else:

                if self.data_type == DATA_TYPE.CSV_FILETYPE:

                    dataframe_ex = read_csv(self.engine, filepath)

                elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                    dataframe_ex = read_parquet(self.engine, filepath)

                dataframe = concat_data([dataframe, dataframe_ex])
                # clean duplicated timestamps rows, keep first by default
                dataframe = dataframe.unique(
                    subset=[
                        BASE_DATA_COLUMN_NAME.TIMESTAMP],
                    keep='first').sort(
                    BASE_DATA_COLUMN_NAME.TIMESTAMP)

            if self.data_type == DATA_TYPE.CSV_FILETYPE:

                write_csv(dataframe, filepath)

            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                write_parquet(dataframe, filepath)

    def read_data(self,
                  market: str,
                  ticker: str,
                  timeframe: str,
                  start: datetime,
                  end: datetime,
                  comparison_column_name: List[str] | str | None = None,
                  check_level: List[int | float] | int | float | None = None,
                  comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
                  comparison_aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None
                  ) -> PolarsLazyFrame:

        comparisons_len = 0

        # Validate and normalize condition parameters if provided
        if comparison_column_name is not None or check_level is not None or comparison_operator is not None:
            comparisons_len = len(comparison_column_name)

            if isinstance(comparison_column_name, str):
                comparison_column_name = [comparison_column_name]

            if isinstance(check_level, (int, float)):
                check_level = [check_level]

            if isinstance(comparison_operator, str):
                comparison_operator = [comparison_operator]

            if any([col not in list(SUPPORTED_BASE_DATA_COLUMN_NAME.__args__)
                   for col in comparison_column_name]):
                logger.bind(
                    target='localdb').error(
                    f'comparison_column_name must be a supported column name: {
                        list(
                            SUPPORTED_BASE_DATA_COLUMN_NAME.__args__)}')
                raise ValueError(
                    'comparison_column_name must be a supported column name')

            if any([cond not in list(SUPPORTED_SQL_COMPARISON_OPERATORS.__args__)
                   for cond in comparison_operator]):
                logger.bind(
                    target='localdb').error(
                    f'comparison_operator must be a supported SQL comparison operator: {
                        list(
                            SUPPORTED_SQL_COMPARISON_OPERATORS.__args__)}')
                raise ValueError(
                    'comparison_operator must be a supported SQL comparison operator')

            if (
                (
                    comparison_aggregation_mode is not None and
                    comparisons_len > 1
                ) and
                comparison_aggregation_mode not in list(
                    SUPPORTED_SQL_CONDITION_AGGREGATION_MODES.__args__)
            ):
                logger.bind(
                    target='localdb').error(
                    f'comparison_aggregation_mode must be a supported SQL condition aggregation mode: {
                        list(
                            SUPPORTED_SQL_CONDITION_AGGREGATION_MODES.__args__)}')
                raise ValueError(
                    'comparison_aggregation_mode must be a supported SQL condition aggregation mode')

            if len(comparison_column_name) != len(check_level) or len(
                    comparison_column_name) != len(comparison_operator):
                logger.bind(target='localdb').error(
                    'comparison_column_name, check_level and comparison_operator must have the same length')
                raise ValueError(
                    'comparison_column_name, check_level and comparison_operator must have the same length')

            comparisons_len = len(comparison_column_name)

        dataframe = PolarsLazyFrame()

        filename = self._get_filename(market,
                                      ticker,
                                      timeframe)

        filepath = (self.data_path /
                    market /
                    ticker /
                    filename)

        if self.engine == 'polars':

            dataframe = PolarsDataFrame()

        elif self.engine == 'polars_lazy':

            dataframe = PolarsLazyFrame()

        else:

            logger.bind(target='localdb').error(
                f'Engine {self.engine} or data type {self.data_type} not supported')
            raise ValueError(
                f'Engine {self.engine} or data type {self.data_type} not supported')

        if (
            filepath.exists() and
            filepath.is_file()
        ):
            # Per-file lock ensures we don't read while write_data is writing.
            # IMPORTANT: for polars lazy engine, read_parquet() only creates a
            # *scan plan* — the actual file I/O happens at .collect() time,
            # which would be AFTER the lock is released. To close this race
            # window we eagerly .collect() inside the lock and re-wrap as
            # .lazy() so all downstream SQL / cast / collect calls work on
            # an in-memory frame rather than a deferred file scan.
            file_lock_path = str(filepath) + '.lock'
            with FileLock(file_lock_path):
                if self.data_type == DATA_TYPE.CSV_FILETYPE:

                    dataframe = read_csv(self.engine, filepath)
                    # Eagerly materialise inside the lock (see comment above)
                    if hasattr(dataframe, 'collect'):
                        dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()

                elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                    dataframe = read_parquet(self.engine, filepath)
                    # Eagerly materialise inside the lock (see comment above)
                    if hasattr(dataframe, 'collect'):
                        dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()

            try:

                start_str = start.isoformat()
                end_str = end.isoformat()

                # Build base query with timestamp filter
                query = f'''SELECT * FROM self
                        WHERE
                        {BASE_DATA_COLUMN_NAME.TIMESTAMP} >= '{start_str}'
                        AND
                        {BASE_DATA_COLUMN_NAME.TIMESTAMP} <= '{end_str}'
                        '''
                # Aggregate conditional filters if provided
                # with the aggregation mode specified
                if comparisons_len > 0:

                    if comparisons_len == 1:
                        # only one condition
                        query += f'''AND
                                 {comparison_column_name[0]} {comparison_operator[0]} {check_level[0]}
                                 '''
                    else:
                        # multiple conditions
                        # wrap all conditions in parentheses with aggregation mode
                        # between them
                        query += f'''AND
                                 ({comparison_column_name[0]} {comparison_operator[0]} {check_level[0]}
                                 '''
                        for col, level, cond, index in zip(
                                comparison_column_name[1:], check_level[1:], comparison_operator[1:], range(1, comparisons_len)):

                            if index == comparisons_len - 1:
                                # closing conditions needs closing bracket
                                query += f'''{comparison_aggregation_mode}
                                    {col} {cond} {level})
                                    '''
                            else:
                                # intermediate conditions
                                query += f'''{comparison_aggregation_mode}
                                    {col} {cond} {level}
                                    '''
                # Close query with timestamp ordering
                query += f'ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}'
                dataframe = dataframe.sql(query)

            except Exception as e:

                logger.bind(target='localdb').error(
                    f'executing query {query} failed: {e}')

            else:

                if timeframe == TICK_TIMEFRAME:

                    # final cast to standard dtypes
                    dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)

                else:

                    # final cast to standard dtypes
                    dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TF_DTYPE)

        else:

            logger.bind(target='localdb').critical(f'file {filepath} not found')
            raise FileNotFoundError(f"file {filepath} not found")

        return dataframe

    def read_data_year(self,
                       market: str,
                       ticker: str,
                       timeframe: str,
                       years: int | List[int]) -> PolarsLazyFrame:
        """
        Read data for specific year(s) using SQL YEAR() filter.
        """
        if isinstance(years, int):
            years_list = [years]
        else:
            years_list = sorted(years)

        filename = self._get_filename(market, ticker, timeframe)
        filepath = (self.data_path / market / ticker / filename)

        if not (filepath.exists() and filepath.is_file()):
            logger.bind(target='localdb').critical(f'File not found: {filepath}')
            raise FileNotFoundError(f"File not found: {filepath}")

        # Per-file lock ensures we don't read while write_data is writing.
        file_lock_path = str(filepath) + '.lock'
        with FileLock(file_lock_path):
            if self.data_type == DATA_TYPE.CSV_FILETYPE:
                dataframe = read_csv(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()
            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                dataframe = read_parquet(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()

        # Apply year filter via SQL
        year_filter_str = ", ".join([str(y) for y in years_list])
        query = f'''SELECT * FROM self
                WHERE
                EXTRACT(YEAR FROM {BASE_DATA_COLUMN_NAME.TIMESTAMP}) IN ({year_filter_str})
                ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}
                '''

        try:
            dataframe = dataframe.sql(query)
        except Exception as e:
            logger.bind(target='localdb').error(
                f'executing query {query} failed: {e}')
            raise

        # Cast types
        if timeframe == TICK_TIMEFRAME:
            dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
        else:
            dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TF_DTYPE)

        return dataframe

    def read_data_window(self,
                         market: str,
                         ticker: str,
                         timeframe: str,
                         date: datetime | str | Timestamp | PolarsDatetime,
                         periods: int,
                         direction : Literal['backward', 'forward'],
                         comparison_column_name: List[str] | str | None = None,
                         check_level: List[int | float] | int | float | None = None,
                         comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
                         comparison_aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None
                         ) -> PolarsLazyFrame:
        """
        Read window of data specified by input requirements:
        the data window has timespan in order to return a dataframe
        with rows size equal to periods.
        Query the local db to calculate the start date of the window
        if direction is forward, or end date if backward.
        """
        if direction == 'backward':
            end_date = date
            start_date = estimate_start_date_to_business_days(
                end_date,
                timeframe,
                periods,
                holidays=FOREX_HOLIDAYS
            )
        elif direction == 'forward':
            start_date = date
            end_date = estimate_end_date_to_business_days(
                start_date,
                timeframe,
                periods,
                holidays=FOREX_HOLIDAYS
            )

        filename = self._get_filename(market, ticker, timeframe)
        filepath = (self.data_path / market / ticker / filename)

        if not (filepath.exists() and filepath.is_file()):
            logger.bind(target='localdb').critical(f'file {filepath} not found')
            raise FileNotFoundError(f"file {filepath} not found")

        # Per-file lock ensures we don't read while write_data is writing.
        file_lock_path = str(filepath) + '.lock'
        with FileLock(file_lock_path):
            if self.data_type == DATA_TYPE.CSV_FILETYPE:
                dataframe = read_csv(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()
            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                dataframe = read_parquet(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()

        if direction == 'backward':
            query = f'''SELECT MIN({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS start_time,
                        MAX({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS end_time
                        FROM (
                            SELECT {BASE_DATA_COLUMN_NAME.TIMESTAMP} FROM self
                            WHERE {BASE_DATA_COLUMN_NAME.TIMESTAMP} <= '{end_date}'
                            ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP} DESC
                            LIMIT {periods}
                        ) AS sub
                        '''
        elif direction == 'forward':
            query = f'''SELECT MIN({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS start_time,
                        MAX({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS end_time
                        FROM (
                            SELECT {BASE_DATA_COLUMN_NAME.TIMESTAMP} FROM self
                            WHERE {BASE_DATA_COLUMN_NAME.TIMESTAMP} >= '{start_date}'
                            ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP} ASC
                            LIMIT {periods}
                        ) AS sub
                        '''

        try:
            window_dates = dataframe.sql(query)
            if isinstance(window_dates, PolarsLazyFrame):
                window_dates = collect_lazyframe(window_dates, self.polars_gpu_engine)
        except Exception as e:
            logger.bind(target='localdb').error(f'executing query {query} failed: {e}')
            return PolarsLazyFrame([])
        else:
            start_date_from_db = window_dates.item(0, 0)
            end_date_from_db = window_dates.item(0, 1)

        return self.read_data(
            market,
            ticker,
            timeframe,
            start_date_from_db,
            end_date_from_db,
            comparison_column_name,
            check_level,
            comparison_operator,
            comparison_aggregation_mode,
        )

    def read_last_timestamp(
        self,
        market: str,
        ticker: str,
        timeframe: str = None
    ) -> datetime:
        """
        Read last timestamp from database.
        If timeframe is not set (None), retrieve the smallest timeframe
        available in local database

        Args:
            market (str): Market name
            ticker (str): Ticker symbol
            timeframe (str, optional): Timeframe of the data

        Returns:
            datetime: Last timestamp in the local database for the
                specified market, ticker and timeframe (or smallest available
                timeframe if timeframe is not set)
        """
        if timeframe is None:
            # get timeframes available
            timeframes_available = self.get_ticker_timeframes_list(ticker)
            if not timeframes_available:
                logger.bind(target='localdb').critical(
                    f'No timeframes available for {market} {ticker}')
                raise ValueError(f"No timeframes available for {market} {ticker}")

            # get smallest timeframe
            timeframe = timeframes_available[0]

        cache_key = (market.lower(), ticker.lower(), timeframe.lower())
        if self._last_timestamp_cache is not None and cache_key in self._last_timestamp_cache:
            return self._last_timestamp_cache[cache_key]

        filename = self._get_filename(market, ticker, timeframe)
        filepath = (self.data_path / market / ticker / filename)

        if not (filepath.exists() and filepath.is_file()):
            logger.bind(target='localdb').critical(f'file {filepath} not found')
            raise FileNotFoundError(f"file {filepath} not found")

        # Per-file lock ensures we don't read while write_data is writing.
        file_lock_path = str(filepath) + '.lock'
        with FileLock(file_lock_path):
            if self.data_type == DATA_TYPE.CSV_FILETYPE:
                dataframe = read_csv(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()
            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                dataframe = read_parquet(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()

        query = f'''SELECT MAX({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS end_time
                    FROM self
                    '''

        try:
            window_dates = dataframe.sql(query)
            if isinstance(window_dates, PolarsLazyFrame):
                window_dates = collect_lazyframe(window_dates, self.polars_gpu_engine)
        except Exception as e:
            logger.bind(target='localdb').error(f'executing query {query} failed: {e}')
            raise
        else:
            end_date_from_db = window_dates.item(0, 0)

        if self._last_timestamp_cache is not None:
            self._last_timestamp_cache[cache_key] = end_date_from_db
        return end_date_from_db


@define(kw_only=True, slots=True)
class LocalDBYearConnector(DatabaseConnector):

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

        super().__attrs_post_init__()

        # set up log sink for LocalDB
        # Remove existing handlers for this sink to prevent duplicate log entries
        log_path = Path(self.data_path) / 'log' / 'localdb.log'

        handlers_to_remove = []
        for handler_id, handler in logger._core.handlers.items():
            if hasattr(handler, '_sink') and hasattr(handler._sink, '_path'):
                if str(handler._sink._path) == str(log_path):
                    handlers_to_remove.append(handler_id)

        for handler_id in handlers_to_remove:
            try:
                logger.remove(handler_id)
            except ValueError:
                # Handler already removed by another thread
                pass

        logger.add(log_path,
                   level="TRACE",
                   rotation="5 MB",
                   filter=lambda record: ('localdb' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        self.data_path = Path(self.data_path)
        self._tickers_years_info_filepath = self.data_path / 'tickers_years_info.json'

    def _db_key(self,
                market: str,
                ticker: str,
                timeframe: str,
                year: int
                ) -> str:
        """

        get a str key of dotted divided elements

        key template = ticker.timeframe.data_type

        Parameters
        ----------
        ticker : TYPE
            DESCRIPTION.
        year : TYPE
            DESCRIPTION.
        data_type : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        return '_'.join([market.lower(),
                         ticker.lower(),
                         timeframe.lower(),
                         str(year)])

    def _get_filename(self, market: str, ticker: str, tf: str, year: int) -> str:

        # based on standard filename template
        return FILENAME_YEAR_STR.format(
            market=market.lower(),
            ticker=ticker.lower(),
            year=year,
            tf=tf.lower(),
            file_ext=self.data_type.lower())

    def _get_ticker_years_list_from_db(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:

        years_list = []
        local_files, local_files_name = self._list_local_data()

        files = [
            key for key in local_files
            if search(f'{ticker.lower()}',
                      str(key.stem)) and
            len(self._get_items_from_db_key(str(key.stem))) > DATA_KEY.YEAR_INDEX and
            self._get_items_from_db_key(str(key.stem))[DATA_KEY.TF_INDEX] ==
            timeframe.lower()
        ]

        # read year info from file name
        years_list = [int(self._get_items_from_db_key(str(key.stem))[DATA_KEY.YEAR_INDEX]) for key in files]
        years_list = sorted(list(set(years_list)))

        return years_list

    def write_data(
        self,
        target_table: str,
        dataframe: Union[PolarsDataFrame, PolarsLazyFrame],
        clean: bool = False
    ) -> None:
        '''
        Write data to database.
        Since LocalDBYearConnector deals with a single year, it's only possible to write a single year dataframe.
        Thus the target_table must be composed by 4 items (market, ticker, timeframe and year).
        In this object performance is prioritized, the method writes a single year data but sanity check that it is a year
        must be done by the caller. This to avoid other calls especially to polars collect() calls.

        Parameters
        ----------
        target_table : str
            Target table name
        dataframe : Union[PolarsDataFrame, PolarsLazyFrame]
            Data to write
        clean : bool, optional
            Clean data before writing, by default False
        '''

        self._local_files_cache = None
        items = self._get_items_from_db_key(target_table)

        filename = self._get_filename(items[DATA_KEY.MARKET],
                                      items[DATA_KEY.TICKER_INDEX],
                                      items[DATA_KEY.TF_INDEX],
                                      items[DATA_KEY.YEAR_INDEX])

        filepath = (self.data_path /
                    items[DATA_KEY.MARKET] /
                    items[DATA_KEY.TICKER_INDEX] /
                    items[DATA_KEY.TF_INDEX] /
                    filename)

        # Per-file lock prevents concurrent processes from corrupting the
        # parquet file by interleaving a read-existing + write sequence.
        file_lock_path = str(filepath) + '.lock'
        with FileLock(file_lock_path):
            if (
                not filepath.exists()
                or
                not filepath.is_file()
            ):

                filepath.parent.mkdir(parents=True,
                                      exist_ok=True)

            if self.data_type == DATA_TYPE.CSV_FILETYPE:

                write_csv(dataframe, filepath)

            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                write_parquet(dataframe, filepath)

    def read_data(self,
                  market: str,
                  ticker: str,
                  timeframe: str,
                  start: datetime,
                  end: datetime,
                  comparison_column_name: List[str] | str | None = None,
                  check_level: List[int | float] | int | float | None = None,
                  comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
                  comparison_aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None
                  ) -> PolarsLazyFrame:

        comparisons_len = 0

        # Validate and normalize condition parameters if provided
        if comparison_column_name is not None or check_level is not None or comparison_operator is not None:
            comparisons_len = len(comparison_column_name)

            if isinstance(comparison_column_name, str):
                comparison_column_name = [comparison_column_name]

            if isinstance(check_level, (int, float)):
                check_level = [check_level]

            if isinstance(comparison_operator, str):
                comparison_operator = [comparison_operator]

            if any([col not in list(SUPPORTED_BASE_DATA_COLUMN_NAME.__args__)
                   for col in comparison_column_name]):
                logger.bind(
                    target='localdb').error(
                    f'comparison_column_name must be a supported column name: {list(SUPPORTED_BASE_DATA_COLUMN_NAME.__args__)}')
                raise ValueError(
                    'comparison_column_name must be a supported column name')

            if any([cond not in list(SUPPORTED_SQL_COMPARISON_OPERATORS.__args__)
                   for cond in comparison_operator]):
                logger.bind(
                    target='localdb').error(
                    f'comparison_operator must be a supported SQL comparison operator: {list(SUPPORTED_SQL_COMPARISON_OPERATORS.__args__)}')
                raise ValueError(
                    'comparison_operator must be a supported SQL comparison operator')

            if (
                (
                    comparison_aggregation_mode is not None and
                    comparisons_len > 1
                ) and
                comparison_aggregation_mode not in list(
                    SUPPORTED_SQL_CONDITION_AGGREGATION_MODES.__args__)
            ):
                logger.bind(
                    target='localdb').error(
                    f'comparison_aggregation_mode must be a supported SQL condition aggregation mode: {list(SUPPORTED_SQL_CONDITION_AGGREGATION_MODES.__args__)}')
                raise ValueError(
                    'comparison_aggregation_mode must be a supported SQL condition aggregation mode')

            if len(comparison_column_name) != len(check_level) or len(
                    comparison_column_name) != len(comparison_operator):
                logger.bind(target='localdb').error(
                    'comparison_column_name, check_level and comparison_operator must have the same length')
                raise ValueError(
                    'comparison_column_name, check_level and comparison_operator must have the same length')

            comparisons_len = len(comparison_column_name)

        if self.engine == 'polars':
            dataframe = PolarsDataFrame()
        elif self.engine == 'polars_lazy':
            dataframe = PolarsLazyFrame()
        else:
            logger.bind(target='localdb').error(
                f'Engine {self.engine} or data type {self.data_type} not supported')
            raise ValueError(
                f'Engine {self.engine} or data type {self.data_type} not supported')

        # set the years to read
        # in order to know which files to look for
        years = list(range(start.year, end.year + 1))

        dataframes_list = []

        # iterate over the years and read the data
        for year in years:
            filename = self._get_filename(market, ticker, timeframe, year)
            filepath = (self.data_path / market / ticker / timeframe / filename)

            if filepath.exists() and filepath.is_file():
                file_lock_path = str(filepath) + '.lock'
                with FileLock(file_lock_path):
                    if self.data_type == DATA_TYPE.CSV_FILETYPE:
                        df_year = read_csv(self.engine, filepath)
                    elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                        df_year = read_parquet(self.engine, filepath)
                    dataframes_list.append(df_year)

        if not dataframes_list:
            logger.bind(target='localdb').critical(f'No files found for {market} {ticker} {timeframe} between {start} and {end}')
            raise FileNotFoundError(f"No files found for {market} {ticker} {timeframe} between {start} and {end}")

        dataframe = concat(dataframes_list)

        try:
            start_str = start.isoformat()
            end_str = end.isoformat()

            query = f'''SELECT * FROM self
                    WHERE
                    {BASE_DATA_COLUMN_NAME.TIMESTAMP} >= '{start_str}'
                    AND
                    {BASE_DATA_COLUMN_NAME.TIMESTAMP} <= '{end_str}'
                    '''
            if comparisons_len > 0:
                if comparisons_len == 1:
                    query += f'''AND
                             {comparison_column_name[0]} {comparison_operator[0]} {check_level[0]}
                             '''
                else:
                    query += f'''AND
                             ({comparison_column_name[0]} {comparison_operator[0]} {check_level[0]}
                             '''
                    for col, level, cond, index in zip(
                            comparison_column_name[1:], check_level[1:], comparison_operator[1:], range(1, comparisons_len)):
                        if index == comparisons_len - 1:
                            query += f'''{comparison_aggregation_mode}
                                {col} {cond} {level})
                                '''
                        else:
                            query += f'''{comparison_aggregation_mode}
                                {col} {cond} {level}
                                '''
            query += f'ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}'
            dataframe = dataframe.sql(query)

        except Exception as e:
            logger.bind(target='localdb').error(
                f'executing query {query} failed: {e}')

        else:
            if timeframe == TICK_TIMEFRAME:
                dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            else:
                dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TF_DTYPE)

        return dataframe

    def read_data_year(self,
                       market: str,
                       ticker: str,
                       timeframe: str,
                       years: int | List[int]) -> PolarsLazyFrame:
        """
        Read data for specific year(s) using SQL YEAR() filter.
        """
        if isinstance(years, int):
            years_list = [years]
        else:
            years_list = sorted(years)

        dataframes_list = []
        for y in years_list:
            filename = self._get_filename(market, ticker, timeframe, y)
            filepath = (self.data_path / market / ticker / timeframe / filename)

            if filepath.exists():
                file_lock_path = str(filepath) + '.lock'
                with FileLock(file_lock_path):
                    if self.data_type == DATA_TYPE.CSV_FILETYPE:
                        df_year = read_csv(self.engine, filepath)
                    elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                        df_year = read_parquet(self.engine, filepath)
                    dataframes_list.append(df_year)

        if not dataframes_list:
            logger.bind(target='localdb').critical(
                f'No files found for {market} {ticker} {timeframe} for years {years_list}')
            raise FileNotFoundError(
                f"No files found for {market} {ticker} {timeframe} for years {years_list}")

        dataframe = concat(dataframes_list)

        # Apply year filter via SQL
        year_filter_str = ", ".join([str(y) for y in years_list])
        query = f'''SELECT * FROM self
                WHERE
                EXTRACT(YEAR FROM {BASE_DATA_COLUMN_NAME.TIMESTAMP}) IN ({year_filter_str})
                ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}
                '''

        try:
            dataframe = dataframe.sql(query)
        except Exception as e:
            logger.bind(target='localdb').error(
                f'executing query {query} failed: {e}')
            raise

        # Cast types
        if timeframe == TICK_TIMEFRAME:
            dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
        else:
            dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TF_DTYPE)

        return dataframe

    def read_data_window(self,
                         market: str,
                         ticker: str,
                         timeframe: str,
                         date: datetime | str | Timestamp | PolarsDatetime,
                         periods: int,
                         direction : Literal['backward', 'forward'],
                         comparison_column_name: List[str] | str | None = None,
                         check_level: List[int | float] | int | float | None = None,
                         comparison_operator: List[SUPPORTED_SQL_COMPARISON_OPERATORS] | SUPPORTED_SQL_COMPARISON_OPERATORS | None = None,
                         comparison_aggregation_mode: SUPPORTED_SQL_CONDITION_AGGREGATION_MODES | None = None
                         ) -> PolarsLazyFrame:
        """
        Read window of data specified by input requirements:
        the data window has timespan in order to return a dataframe
        with rows size equal to periods.
        Query the local db to calculate the start date of the window
        if direction is forward, or end date if backward.


        Args:
            market (str): Market name (e.g., 'forex').
            ticker (str): Trading pair (e.g., 'XAUUSD').
            timeframe (str): Timeframe (e.g., 'TICK').
            date (datetime | str | Timestamp | PolarsDatetime): Start or end date for the window.
            periods (int): Number of timeframe units to look back or forward.
            direction (str): Direction to look back ('backward' or 'forward').
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
            PolarsDataFrame: DataFrame with data for the window.
        """

        # based on timeframe and window size (units of timeframe)
        # estimate how many years the query should cover
        if direction == 'backward':
            end_date = date
            start_date = estimate_start_date_to_business_days(
                end_date,
                timeframe,
                periods,
                holidays=FOREX_HOLIDAYS
            )

            # consider (start_date.year - 1) with -1 as margin
            # e.g. if start_date is 2000-01-01, we need to include 1999 files
            years_list = range(start_date.year - 1, end_date.year + 1)

        elif direction == 'forward':
            start_date = date
            end_date = estimate_end_date_to_business_days(
                start_date,
                timeframe,
                periods,
                holidays=FOREX_HOLIDAYS
            )

            # consider end_date.year + 2 with +1 as margin
            # e.g. if end_date is 2022-12-31, we need to include 2023 files
            years_list = range(start_date.year, end_date.year + 2)

        # get a hook (lazyframe) to the local db files needed
        dataframes_list = []
        for y in years_list:
            filename = self._get_filename(market, ticker, timeframe, y)
            filepath = (self.data_path / market / ticker / timeframe / filename)

            if filepath.exists():
                file_lock_path = str(filepath) + '.lock'
                with FileLock(file_lock_path):
                    if self.data_type == DATA_TYPE.CSV_FILETYPE:
                        df_year = read_csv(self.engine, filepath)
                    elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                        df_year = read_parquet(self.engine, filepath)
                    dataframes_list.append(df_year)

        if not dataframes_list:
            logger.bind(target='localdb').critical(
                f'No files found for {market} {ticker} {timeframe} for years {years_list}')
            raise FileNotFoundError(
                f"No files found for {market} {ticker} {timeframe} for years {years_list}")

        if not dataframes_list:
            logger.bind(target='localdb').critical(f'No files found for {market} {ticker} {timeframe} between {start} and {end}')
            raise FileNotFoundError(f"No files found for {market} {ticker} {timeframe} between {start} and {end}")

        # aggregated lazyframe to query from
        dataframe = concat(dataframes_list)

        # produce a SQL query to local database to count the number of entries (rows)
        # from start date or end date
        # the count stops when it reaches 'window' number of rows
        # the query shall return the first and last datetime entries
        # in ascending order of timestamp
        if direction == 'backward':
            query = f'''SELECT MIN({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS start_time,
                        MAX({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS end_time
                        FROM (
                            SELECT {BASE_DATA_COLUMN_NAME.TIMESTAMP} FROM self
                            WHERE {BASE_DATA_COLUMN_NAME.TIMESTAMP} <= '{end_date}'
                            ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP} DESC
                            LIMIT {periods}
                        ) AS sub
                        '''
        elif direction == 'forward':
            query = f'''SELECT MIN({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS start_time,
                        MAX({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS end_time
                        FROM (
                            SELECT {BASE_DATA_COLUMN_NAME.TIMESTAMP} FROM self
                            WHERE {BASE_DATA_COLUMN_NAME.TIMESTAMP} >= '{start_date}'
                            ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP} ASC
                            LIMIT {periods}
                        ) AS sub
                        '''

        try:

            window_dates = dataframe.sql(query)
            if isinstance(window_dates, PolarsLazyFrame):
                window_dates = collect_lazyframe(window_dates, self.polars_gpu_engine)

        except Exception as e:
            logger.bind(target='localdb').error(
                f'executing query {query} failed: {e}')

            return PolarsLazyFrame([])

        else:

            # get the corrected dates
            start_date_from_db = window_dates.item(0, 0)
            end_date_from_db = window_dates.item(0, 1)

        return self.read_data(
            market,
            ticker,
            timeframe,
            start_date_from_db,
            end_date_from_db,
            comparison_column_name,
            check_level,
            comparison_operator,
            comparison_aggregation_mode,
        )

    def read_last_timestamp(
        self,
        market: str,
        ticker: str,
        timeframe: str = None
    ) -> datetime:
        """
        Read last timestamp from database.
        If timeframe is not set (None), retrieve the smallest timeframe
        available in local database

        Args:
            market (str): Market name
            ticker (str): Ticker symbol
            timeframe (str, optional): Timeframe of the data

        Returns:
            datetime: Last timestamp in the local database for the
                specified market, ticker and timeframe (or smallest available
                timeframe if timeframe is not set)
        """

        # if tickers is not in database return None
        if ticker not in self.get_tickers_list():
            return None

        if timeframe is None:
            # get timeframes available
            timeframes_available = self.get_ticker_timeframes_list(ticker)
            if not timeframes_available:
                logger.bind(target='localdb').critical(
                    f'No timeframes available for {market} {ticker}')
                raise ValueError(f"No timeframes available for {market} {ticker}")

            # if 'tick' is available, get 'tick' as minimum timeframe
            if 'tick' in timeframes_available:
                timeframe = 'tick'
            else:
                # sort timeframes from smallest to largest
                timeframes_available = sorted(
                    timeframes_available,
                    key=lambda s: to_timedelta(s).total_seconds(),
                    reverse=False
                )
                # get smallest timeframe
                timeframe = timeframes_available[0]

        cache_key = (market.lower(), ticker.lower(), timeframe.lower())
        if self._last_timestamp_cache is not None and cache_key in self._last_timestamp_cache:
            return self._last_timestamp_cache[cache_key]

        # Get list of years available for this ticker and timeframe
        years_list = self._get_ticker_years_list_from_db(ticker, timeframe)
        if not years_list:
            logger.bind(target='localdb').critical(
                f'No data found for {market} {ticker} {timeframe}')
            raise FileNotFoundError(f"No data found for {market} {ticker} {timeframe}")
        latest_year = max(years_list)

        filename = self._get_filename(market, ticker, timeframe, latest_year)
        filepath = (self.data_path / market / ticker / timeframe / filename)

        if not (filepath.exists() and filepath.is_file()):
            logger.bind(target='localdb').critical(f'file {filepath} not found')
            raise FileNotFoundError(f"file {filepath} not found")

        # Per-file lock ensures we don't read while write_data is writing.
        file_lock_path = str(filepath) + '.lock'
        with FileLock(file_lock_path):
            if self.data_type == DATA_TYPE.CSV_FILETYPE:
                dataframe = read_csv(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()
            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                dataframe = read_parquet(self.engine, filepath)
                if hasattr(dataframe, 'collect'):
                    dataframe = collect_lazyframe(dataframe, self.polars_gpu_engine).lazy()

        query = f'''SELECT MAX({BASE_DATA_COLUMN_NAME.TIMESTAMP}) AS end_time
                    FROM self
                    '''

        try:
            window_dates = dataframe.sql(query)
            if isinstance(window_dates, PolarsLazyFrame):
                window_dates = collect_lazyframe(window_dates, self.polars_gpu_engine)
        except Exception as e:
            logger.bind(target='localdb').error(f'executing query {query} failed: {e}')
            raise
        else:
            end_date_from_db = window_dates.item(0, 0)

        if self._last_timestamp_cache is not None:
            self._last_timestamp_cache[cache_key] = end_date_from_db
        return end_date_from_db
