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


from adbc_driver_sqlite import dbapi as sqlite_dbapi
from filelock import FileLock
from .common import *
from polars import (
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe,
    read_database
)
import json
from datetime import datetime
from pathlib import Path as PathType
from typing import Any, Dict, List, Optional, Tuple, Union, cast
from numpy import array
from collections import OrderedDict
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

# Import polars types directly

# Import from adbc_driver_sqlite

# Import from sqlalchemy for text queries
try:
    from sqlalchemy import text
except ImportError:
    # Fallback if sqlalchemy not available
    def text(s: str) -> str:
        return s

# Import from common module - explicit imports for items not in __all__

# Import remaining items via star import
# Import from config module
from ..config import _apply_config


'''
BASE CONNECTOR
'''


@define(kw_only=True, slots=True)
class DatabaseConnector:

    data_path: Union[str, Path] = field(default='', validator=validators.or_(
        validators.instance_of(str), validators.instance_of(Path)))

    def __init__(self, **kwargs: Any) -> None:

        pass

    def __attrs_post_init__(self) -> None:

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
                   dataframe: Union[polars_dataframe,
                                    polars_lazyframe],
                   clean: bool = False) -> None:
        """Write data to database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement write_data")

    def read_data(
            self,
            market: str,
            ticker: str,
            timeframe: str,
            start: datetime,
            end: datetime) -> polars_lazyframe:
        """Read data from database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement read_data")

    def exec_sql(self) -> None:
        """Execute SQL - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement exec_sql")

    def _db_key(self, market: str, ticker: str, timeframe: str) -> str:
        """Generate database key - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _db_key")

    def get_tickers_list(self) -> List[str]:
        """Get list of tickers - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_tickers_list")

    def get_ticker_keys(
            self,
            ticker: str,
            timeframe: Optional[str] = None) -> List[str]:
        """Get ticker keys - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_ticker_keys")

    def get_ticker_years_list(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:
        """Get years list for ticker - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_ticker_years_list")

    def clear_database(self, filter: Optional[str] = None) -> None:
        """Clear database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement clear_database")


'''
DUCKDB CONNECTOR:

    TABLE TEMPLATE:
        <trading field (e.g. Forex, Stocks)>.ticker.timeframe

'''


@define(kw_only=True, slots=True)
class DuckDBConnector(DatabaseConnector):

    _duckdb_filepath = field(default='',
                             validator=validators.instance_of(str))

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

        self.__attrs_post_init__(**kwargs)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        super().__attrs_post_init__(**kwargs)

        # set up log sink for DuckDB
        # Remove existing handlers for this sink to prevent duplicate log entries
        log_path = self.data_path / 'log' / 'duckdb.log'

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
                   filter=lambda record: ('duckdb' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        # create duck file path if not exists
        if (
                not Path(self.duckdb_filepath).exists() or
            not Path(self.duckdb_filepath).is_file()
        ):

            Path(self.duckdb_filepath).parent.mkdir(parents=True,
                                                    exist_ok=True)
        else:

            logger.bind(target='duckdb').trace(
                f'DuckDB file {self.duckdb_filepath} already exists')

        # set autovacuum
        conn = self.connect()

        # check auto vacuum property
        cur = conn.cursor()
        cur.execute('PRAGMA main.auto_vacuum')
        cur.execute('PRAGMA main.auto_vacuum = 2')
        cur.close()
        conn.close()

    def connect(self) -> Any:

        try:

            con = sqlite_dbapi.connect(uri=self.duckdb_filepath)

        except Exception as e:

            logger.bind(target='duckdb').error(f'ADBC-SQLITE: connection error: {e}')
            raise

        else:

            return con

    def check_connection(self) -> bool:

        out_check_connection = False

        conn = self.connect()

        out_check_connection = False

        try:

            info = read_database(text('SHOW DATABASES'), conn)

        except Exception as e:

            logger.bind(target='duckdb').error(
                f'Error during connection to {self.duckdb_filepath}')

        else:

            logger.bind(target='duckdb').trace(f'{info}')

            out_check_connection = not is_empty_dataframe(info)

        return out_check_connection

    def _to_duckdb_column_types(self, columns_dict: Dict[str, Any]) -> Dict[str, str]:

        duckdb_columns_dict = {}

        for key, value in columns_dict.items():

            match key:

                case BASE_DATA_COLUMN_NAME.TIMESTAMP:

                    duckdb_columns_dict[BASE_DATA_COLUMN_NAME.TIMESTAMP] = 'TIMESTAMP_MS'

                case BASE_DATA_COLUMN_NAME.ASK \
                    | BASE_DATA_COLUMN_NAME.BID \
                    | BASE_DATA_COLUMN_NAME.OPEN \
                    | BASE_DATA_COLUMN_NAME.HIGH \
                    | BASE_DATA_COLUMN_NAME.LOW \
                    | BASE_DATA_COLUMN_NAME.CLOSE \
                    | BASE_DATA_COLUMN_NAME.VOL \
                        | BASE_DATA_COLUMN_NAME.P_VALUE:

                    duckdb_columns_dict[key] = 'FLOAT'

                case BASE_DATA_COLUMN_NAME.TRANSACTIONS:

                    duckdb_columns_dict[key] = 'UBIGINT'

                case BASE_DATA_COLUMN_NAME.OTC:

                    duckdb_columns_dict[key] = 'FLOAT'

        # force timestamp as first key
        if not list(duckdb_columns_dict.keys())[0] == BASE_DATA_COLUMN_NAME.TIMESTAMP:

            o_dict = OrderedDict(duckdb_columns_dict.items())
            o_dict.move_to_end(BASE_DATA_COLUMN_NAME.TIMESTAMP, last=False)

            duckdb_columns_dict = dict(o_dict)

        else:
            logger.bind(
                target='duckdb').trace(
                f'Timestamp is already the first column in {
                    duckdb_columns_dict.keys()}')

        return duckdb_columns_dict

    def _list_tables(self) -> List[str]:

        tables_list: List[str] = []

        conn = self.connect()

        try:

            tables = read_database(query='SELECT * FROM sqlite_master',
                                   connection=conn)

        except Exception as e:

            logger.bind(target='duckdb').error(
                f'Error list tables for {self.duckdb_filepath}: {e}')

        else:

            tables_list = list(tables['tbl_name'])

        conn.close()

        return tables_list

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

    def _get_items_from_db_key(self,
                               key
                               ) -> tuple:

        return tuple(key.split('_'))

    def get_tickers_list(self) -> List[str]:

        tickers_list = []

        for table_name in self._list_tables():

            items = self._get_items_from_db_key(table_name)

            tickers_list.append(items[DATA_KEY.TICKER_INDEX])

        return list_remove_duplicates(tickers_list)

    def get_ticker_keys(
            self,
            ticker: str,
            timeframe: Optional[str] = None) -> List[str]:

        ticker_keys_list = []

        for table_name in self._list_tables():

            items = self._get_items_from_db_key(table_name)

            if items[DATA_KEY.TICKER_INDEX] == ticker.lower():

                if timeframe:

                    if items[DATA_KEY.TF_INDEX] == timeframe.lower():

                        ticker_keys_list.append(table_name)

                else:

                    ticker_keys_list.append(table_name)

        return ticker_keys_list

    def get_ticker_years_list(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:

        ticker_years_list = []
        table = ''
        key_found = False

        for table_name in self._list_tables():

            items = self._get_items_from_db_key(table_name)

            if (
                    items[DATA_KEY.TICKER_INDEX] == ticker.lower() and
                items[DATA_KEY.TF_INDEX] == timeframe.lower()
            ):

                table = table_name
                key_found = True

                break

        if key_found:

            conn = self.connect()

            try:

                query = f'''SELECT DISTINCT STRFTIME('%Y', CAST({
                    BASE_DATA_COLUMN_NAME.TIMESTAMP} AS TEXT))
                        AS YEAR
                        FROM {table}'''
                read = read_database(query, conn)

            except Exception as e:

                logger.bind(target='duckdb').error(f'Error querying table {table}: {e}')
                raise

            else:

                ticker_years_list = [int(row[0]) for row in read.iter_rows()]

            conn.commit()
            conn.close()

        return ticker_years_list

    def write_data(self,
                   target_table: str,
                   dataframe: Union[polars_dataframe,
                                    polars_lazyframe],
                   clean: bool = False) -> None:

        duckdb_cols_dict = {}
        if isinstance(dataframe, polars_lazyframe):

            duckdb_cols_dict = self._to_duckdb_column_types(
                dict(dataframe.collect_schema()))
            dataframe = dataframe.collect()

        else:

            duckdb_cols_dict = self._to_duckdb_column_types(dict(dataframe.schema))

        duckdb_cols_str = ', '.join([f"{key} {duckdb_cols_dict[key]}"
                                     for key in duckdb_cols_dict])

        # open a connection
        conn = self.connect()

        # exec stable creation
        table_list = self._list_tables()

        if_table_exists = 'replace'
        if target_table in table_list:

            # stable_describe = read_database(f'DESCRIBE {target_table}')

            # get existing stable column structure
            # if they match, append data
            # if no match, replace stable

            if_table_exists = 'append'

        target_length = len(dataframe)

        table_write = dataframe.write_database(
            table_name=target_table,
            connection=conn,
            if_table_exists=if_table_exists,
            engine='adbc'
        )

        conn.commit()
        conn.close()

        # clean stage
        if clean:

            conn = self.connect()

            # delete duplicates
            query_clean = f'''DELETE FROM {target_table}
                            WHERE ROWID NOT IN (
                            SELECT MIN(ROWID)
                            FROM {target_table}
                            GROUP BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}
                            );'''

            cur = conn.cursor()
            res = cur.execute(query_clean)

            # Close
            cur.close()
            conn.commit()
            conn.close()

            conn = self.connect()
            cur = conn.cursor()
            vacuum = cur.execute('PRAGMA main.incremental_vacuum')

            # Close
            cur.close()
            conn.commit()
            conn.close()

    def read_data(self,
                  market: str,
                  ticker: str,
                  timeframe: str,
                  start: datetime,
                  end: datetime
                  ) -> polars_lazyframe:

        dataframe = polars_lazyframe()

        table = self._db_key(market, ticker, timeframe)
        # check if database is available
        if table in self._list_tables():

            # open a connection
            conn = self.connect()

            try:

                start_str = start.isoformat()
                end_str = end.isoformat()
                '''
                Here you could use also
                WHERE CAST({BASE_DATA_COLUMN_NAME.TIMESTAMP} AS TEXT)
                '''
                query = f'''SELECT * FROM {table}
                            WHERE {BASE_DATA_COLUMN_NAME.TIMESTAMP}
                            BETWEEN '{start_str}' AND '{end_str}'
                            ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}'''
                dataframe = read_database(query, conn).lazy()

            except Exception as e:

                logger.bind(target='duckdb').error(
                    f'executing query {query} failed: {e}')

            else:

                if timeframe == TICK_TIMEFRAME:

                    # final cast to standard dtypes
                    dataframe = dataframe.cast(
                        cast(Any, POLARS_DTYPE_DICT.TIME_TICK_DTYPE))

                else:

                    # final cast to standard dtypes
                    dataframe = dataframe.cast(
                        cast(Any, POLARS_DTYPE_DICT.TIME_TF_DTYPE))

        # close
        conn.commit()
        conn.close()

        return dataframe

    def clear_database(self, filter: Optional[str] = None) -> None:
        """
        Clear database tables
        If filter is provided, delete only tables related to that ticker
        """
        tables = self._list_tables()
        conn = self.connect()
        cur = conn.cursor()

        for table in tables:
            if filter:
                if search(filter, table, IGNORECASE):
                    cur.execute(f"DROP TABLE {table}")
            else:
                cur.execute(f"DROP TABLE {table}")

        cur.execute("VACUUM")
        cur.close()
        conn.commit()
        conn.close()


'''
LOCAL DATA FILES MANAGER

'''


@define(kw_only=True, slots=True)
class LocalDBConnector(DatabaseConnector):

    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))

    _tickers_years_info_filepath = field(default=Path('.'))

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

        self.__attrs_post_init__(**kwargs)

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

    def _get_info_from_db_key(self,
                              key
                              ) -> tuple:

        return tuple(key.split('_'))

    def _get_items_from_db_key(self,
                               key
                               ) -> tuple:

        return tuple(key.split('_'))

    def _get_file_details(self, filename: str) -> Tuple[str, str, str]:

        if not (
                isinstance(filename, str)
        ):

            logger.bind(target='localdb').error(
                'filename {filename} invalid type: required str')
            raise TypeError(f'filename {filename} invalid type: required str')

        file_items = self._get_items_from_db_key(filename)

        # return each file details
        return file_items

    def _get_filename(self, market: str, ticker: str, tf: str) -> str:

        # based on standard filename template
        return FILENAME_STR.format(market=market.lower(),
                                   ticker=ticker.lower(),
                                   tf=tf.lower(),
                                   file_ext=self.data_type.lower())

    def _list_local_data(self) -> List[PathType]:

        local_files = []
        local_files_name = []

        # list for all data filetypes supported
        local_files = [file for file in list(self.data_path.rglob(f'*'))
                       if search(self.data_type + '$', file.suffix)]

        local_files_name = [file.name for file in local_files]

        return local_files, local_files_name

    def _list_tables(self) -> List[str]:

        local_files, tables_list = self._list_local_data()

        return tables_list

    def get_tickers_list(self) -> List[str]:

        tickers_list = []

        local_files, local_files_name = self._list_local_data()

        for filename in local_files_name:

            items = self._get_file_details(filename)
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

        local_files, local_files_name = self._list_local_data()

        return list_remove_duplicates([
            self._get_items_from_db_key(Path(key).stem)[DATA_KEY.TF_INDEX] for key in local_files_name
            if search(f'{ticker.lower()}',
                      key.lower())
        ])

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

                ticker_years_list = [int(row[0]) for row in read.collect().iter_rows()]

        return ticker_years_list

    def get_ticker_years_list(
            self,
            ticker: str,
            timeframe: str = TICK_TIMEFRAME) -> List[int]:

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
            self, ticker: str, timeframe: str, year: Union[int, List[int]]) -> None:
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

    def write_data(
        self,
        target_table: str,
        dataframe: Union[polars_dataframe, polars_lazyframe],
        clean: bool = False
    ) -> None:

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
                  ) -> polars_lazyframe:

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

        dataframe = polars_lazyframe()

        filename = self._get_filename(market,
                                      ticker,
                                      timeframe)

        filepath = (self.data_path /
                    market /
                    ticker /
                    filename)

        if self.engine == 'polars':

            dataframe = polars_dataframe()

        elif self.engine == 'polars_lazy':

            dataframe = polars_lazyframe()

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
                        dataframe = dataframe.collect().lazy()

                elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                    dataframe = read_parquet(self.engine, filepath)
                    # Eagerly materialise inside the lock (see comment above)
                    if hasattr(dataframe, 'collect'):
                        dataframe = dataframe.collect().lazy()

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


@define(kw_only=True, slots=True)
class LocalDBYearConnector(DatabaseConnector):

    # interface parameters
    config          : str = field(default='',
                                  validator=validators.instance_of(str))
    data_type       : str = field(default='parquet',
                                  validator=validators.in_(SUPPORTED_DATA_FILES))
    engine          : str = field(default='polars_lazy',
                                  validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    # internal parameters
    _db_dict = field(factory=dotty, validator=validators.instance_of(Dotty))
    _tf_list = field(factory=list, validator=validators.instance_of(list))
    _dataframe_type = field(default = pandas_dataframe)
    _data_path = field(default = Path(DEFAULT_PATHS.BASE_PATH), 
                           validator=validator_dir_path(create_if_missing=True))
    _histdata_path = field(
                        default = Path(DEFAULT_PATHS.BASE_PATH) / DEFAULT_PATHS.HIST_DATA_FOLDER, 
                        validator=validator_dir_path(create_if_missing=True))
    _temporary_data_path = field(
                        default = (Path(DEFAULT_PATHS.BASE_PATH) 
                                    / DEFAULT_PATHS.HIST_DATA_FOLDER
                                    / TEMP_FOLDER), 
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
    # or factory generator
        
    def __init__(self, **kwargs):
            
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)
        
        if kwargs['config']:
            
            config_path = Path(kwargs['config'])
            
            if (
                config_path.exists() 
                and  
                config_path.is_dir() 
                ):
                
                config_filepath = read_config_folder(config_path,
                                                     file_pattern='_config.yaml')
            
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
            if  (
                    not isinstance(config_args, dict)
                    or
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
                    
                    logger.warning('KeyError: initializing object has no '
                                    f'attribute {attr.name}')
                    
                except IndexError:
                    
                    logger.warning('IndexError: initializing object has no '
                                    f'attribute {attr.name}')
                
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
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        self.__attrs_post_init__(**kwargs)
        
            
    def __attrs_post_init__(self, **kwargs):
        
        # reset logging handlers
        logger.remove()
        
        # checks on data folder path
        if ( 
            not self._histdata_path.is_dir() 
            or
            not self._histdata_path.exists()
            ):
                    
            self._histdata_path.mkdir(parents=True,
                                      exist_ok=True)
            
        # add logging file handle
        logger.add(self._data_path / 'forexdata.log', 
                   level="TRACE",
                   rotation="5 MB"
        )
        
        # set up dataframe engine internal var based on config selection
        if self.engine == 'pandas':
            
            self._dataframe_type = pandas_dataframe 

        elif self.engine == 'pyarrow':
            
            self._dataframe_type = pyarrow_table 
            
        elif self.engine == 'polars':
            
            self._dataframe_type = polars_dataframe 
        
        elif self.engine == 'polars_lazy':

            self._dataframe_type = polars_lazyframe  
            
        self._temporary_data_path = self._histdata_path \
                                        / TEMP_FOLDER
                                        
        self._clear_temporary_data_folder()
        
      
    def _clear_temporary_data_folder(self):
        
        # delete temporary data path
        if (
            self._temporary_data_path.exists()
            and
            self._temporary_data_path.is_dir()
            ):
            
            try:
                
                rmtree(str(self._temporary_data_path))
                
            except Exception as e:
                
                logger.warning('Deleting temporary data folder '
                               f'{str(self._temporary_data_path)} not successfull: {e}')
        
    # TODO: EVALUATE TO REMOVE DATA_TYPE FROM KEY
    def _db_key(self, 
                ticker    : str, 
                year      : str,
                timeframe : str
        ) -> str:
        """

        get a str key of dotted divided elements

        key template = ticker.year.timeframe.data_type

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

        tf = check_timeframe_str(timeframe)

        return '.'.join([ticker, 'Y'+str(year), tf])


    def _get_ticker_list(self):
        
        return self._db_dict.keys()
        
    
    def _get_ticker_keys(self, ticker):
        
        return [
            key for key in get_dotty_leafs(self._db_dict)
            if search(f'^{ticker}',
                      key)
        ]
        
    
    def _get_years_list(self, ticker, vartype):

        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dict.copy()

        try:
            # pop at year level in data copy
            year_db = db_copy.pop(ticker.upper())
            years_keys = year_db.keys()
        except KeyError:
            
            logger.info(f'ticker {ticker} no years data in instance '
                        'in memory database')    
            return []
        
        else:

            # get year value from data keys
            years_list = [key[FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:]
                          for key in years_keys]
            
            # remove duplicates
            years_list = list_remove_duplicates(years_list)

        # sort to have oldest year first
        years_list.sort(key=int)

        # return list of elements as manipulation is easier
        # return type based on input specification
        if vartype == 'str':

            return [str(year) for year in years_list]

        elif vartype == 'int':

            return [int(year) for year in years_list]

        else:

            return [int(year) for year in years_list]


    def _get_year_timeframe_list(self, ticker, year):

        # work on copy as pop operation is 'inplace'
        # so the original db is not modified
        db_copy = self._db_dict.get(ticker).copy()

        # get key at timeframe level
        tf_key = 'Y{year}'.format(year=year)

        # pop at timeframe level in data copy
        tf_db = db_copy.pop(tf_key)

        if tf_db:

            try:
                tf_keys = tf_db.keys()
            except KeyError:
                # no active timeframe found --> return empty list
                return []
            else:

                return [key for key in tf_keys]

        else:

            # empty db --> return empty list
            return []


    def _get_tf_complete_years(self, 
                               ticker):

        # check input years list if each year is complete
        # across tick and timeframes requested

        # instantiate empty list
        years_complete = list()

        for year in self._get_years_list(ticker, 'int'):

            year_complete = all([
                # create key for dataframe type
                isinstance(self._db_dict.get(self._db_key(ticker,
                                                          year,
                                                          tf)),
                           type(self._dataframe_type([])))
                for tf in self._tf_list
            ])

            if year_complete:

                # append year in list of data found in local folder
                years_complete.append(year)

        return years_complete


    def _download_month_raw(self,
                            ticker,
                            url, 
                            year, 
                            month_num
        ):
        """

        Download a month data


        Parameters
        ----------
        year : TYPE
            DESCRIPTION.
        month_num : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        session = Session()
        r = session.get(url)
        
        with logger.catch(exception=AttributeError,
                          level='CRITICAL',
                          message=f'token value was not found scraping '
                                  f'url {url}: probably {ticker} '
                                  f'is not supported by histdata.com'):
            
            token = search('id="tk" value="(.*?)"', r.text).groups()[0]

        ''' Alternative: using BeautifulSoup parser
        r = session.get(url, allow_redirects=True)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        with logger.catch(exception=AttributeError,
                          level='CRITICAL',
                          message=f'token value was not found scraping url {url}'):
            
            token = soup.find('input', {'id': 'tk'}).attrs['value']
        
        '''
        
        headers = {'Referer': url}
        data = {
            'tk': token, 
            'date': year,
            'datemonth': "%d%02d" % (year, month_num), 
            'platform': 'ASCII',
            'timeframe': 'T', 
            'fxpair': ticker
        }
            
        r = session.request(
            HISTDATA_BASE_DOWNLOAD_METHOD,
            HISTDATA_BASE_DOWNLOAD_URL,
            data=data,
            headers=headers,
            stream=True
        )
        
        bio = BytesIO()
          
        # write content to stream
        bio.write(r.content)
        
        try:
            
            zf = ZipFile(bio)
            
        except BadZipFile:

            # here will be a warning log
            logger.error(f'{ticker} - {year} - {MONTHS[month_num-1]}: '
                         f'not found or invalid download')
                            
            return None

        else:

            # return opened zip file
            return zf.open(zf.namelist()[0])


    def _add_tf_data_key(self,
                         ticker,
                         year,
                         tf):
        
        year_tf_key = self._db_key(ticker,
                                  year,
                                  tf)

        if self._db_dict.get(year_tf_key) is None \
            or not isinstance(self._db_dict.get(year_tf_key),
                              type(self._dataframe_type([]))):

            # get tick key
            year_tick_key = self._db_key(ticker,
                                         year,
                                         'TICK')

            try:

                aux_base_df = self._db_dict.get(year_tick_key)

            except KeyError:

                # to logging
                logger.error(f'Requested to reframe {ticker} '
                             f'{year} in timeframe {tf} '
                             'but tick data was not found')

            else:

                # produce reframed data at the timeframe requested
                self._db_dict[year_tf_key] \
                    = reframe_data(aux_base_df, tf)
        

    def _complete_years_timeframe(self) -> None:

        for ticker in self._get_ticker_list():
            
            # get all years available from db keys
            years_list = self._get_years_list(ticker, 'int')
    
            # get years that has not all timeframes
            years_complete = self._get_tf_complete_years(ticker)
    
            # get years not having all timeframes data
            years_incomplete = set(years_list).difference(years_complete)
    
            ticker_keys = self._get_ticker_keys(ticker)
                
            # get years missing timeframes data but with tick data available
            # in current data instance (no further search offline)
            incomplete_with_tick = [
                                    int(get_dotty_key_field(key, 
                                                        DATA_KEY.YEAR_INDEX)[1:])
                                    for key in ticker_keys
                                    if get_dotty_key_field(key, DATA_KEY.TF_INDEX)
                                    == TICK_TIMEFRAME
                                    and
                                    int(get_dotty_key_field(key, 
                                                        DATA_KEY.YEAR_INDEX)[1:])
                                    in years_incomplete
                                    ]
    
            # complete years reframing from tick/minimal timeframe data
            for year in incomplete_with_tick:
    
                for tf in self._tf_list:
    
                    self._add_tf_data_key(
                                ticker,
                                year, 
                                tf
                    )
    
            # check consistency, exit if fail
            if not (
                self._get_years_list(ticker, 'int') 
                == self._get_tf_complete_years(ticker)
                ):
                
                logger.critical('timeframe completing operation FAILED')
                
                raise KeyError
                
    
    def _raw_zipfile_to_df(self, 
                           raw_file,
                           temp_filepath,
                           engine='polars'):
        """


        Parameters
        ----------
        raw_files_list : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """

        if engine == 'pandas':
        
            # pandas with python engine can read a runtime opened
            # zip file
        
            # funtions is specific for format of files downloaded
            # parse file passed as input
            
            df = read_csv(  
                    'pandas',
                    raw_file,
                    sep=',',
                    names=DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                    dtype=DTYPE_DICT.TICK_DTYPE,
                    parse_dates=[DATA_FILE_COLUMN_INDEX.TIMESTAMP],
                    date_format=DATE_FORMAT_HISTDATA_CSV,
                    engine = 'c'
            )
            
            # calculate 'p'
            df['p'] = (df['ask'] + df['bid']) / 2
            
        elif engine == 'pyarrow':
        
            # no way found to directly open a runtime zip file
            # with pyarrow
            # strategy rolls back to temporary file download 
            # open and read all
            # delete temporary file
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            
            if (
                Path(temp_filepath).exists()
                and
                Path(temp_filepath).is_file()
                ):
                
                Path(temp_filepath).unlink(missing_ok=True)    
                
            else:
                
                # create temporary files directory if not present
                tempdir_path = Path(temp_filepath).parent
                tempdir_path.mkdir(exist_ok=True)
                
            # download buffer to file
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            # raw_file_dtypes = DTYPE_DICT.TICK_DTYPE.copy()
            # raw_file_dtypes.pop('p')
                        
            # read temporary csv file
            
            # use panda read_csv an its options with 
            # engine = 'pyarrow'
            # dtype_backend = 'pyarrow'
            # df = read_csv(  
            #             'pyarrow',
            #             temp_filepath,
            #             sep=',',
            #             index_col=0,
            #             names=DATA_COLUMN_NAMES.TICK_DATA,
            #             dtype=raw_file_dtypes,
            #             parse_dates=[0],
            #             date_format=DATE_FORMAT_HISTDATA_CSV,
            #             engine = 'pyarrow',
            #             dtype_backend = 'pyarrow'
            # )
            # perform step to convert index
            # into a datetime64 dtype
            # df.index = any_date_to_datetime64(df.index,
            #                         date_format=DATE_FORMAT_HISTDATA_CSV,
            #                         unit='ms')
            
            # use pyarrow native options
            read_opts = arrow_csv.ReadOptions(
                        use_threads  = True,
                        column_names = DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                        
                )
            
            parse_opts = arrow_csv.ParseOptions(
                        delimiter = ','
                )
            
            modtypes = PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            modtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = pyarrow_string()
            modtypes.pop(BASE_DATA_COLUMN_NAME.P_VALUE)
            
            convert_opts = arrow_csv.ConvertOptions(
                        column_types = modtypes
            )
            
            # at first read file with timestmap as a string
            df = read_csv(  
                'pyarrow',
                temp_filepath,
                read_options    = read_opts,
                parse_options   = parse_opts,
                convert_options = convert_opts
            )
            
            # convert timestamp  string array to pyarrow timestamp('ms')
            
            # pandas/numpy solution
            # std_datetime = to_datetime(df[BASE_DATA_COLUMN_NAME.TIMESTAMP].to_numpy(), 
            #                            format=DATE_FORMAT_HISTDATA_CSV)
            
            # timecol = pyarrow_array(std_datetime, 
            #                         type=pyarrow_timestamp('ms'))
            
            # all pyarrow ops solution
            # suggested here
            # https://github.com/apache/arrow/issues/41132#issuecomment-2052555361
            
            mod_format = DATE_FORMAT_HISTDATA_CSV.removesuffix('%f')
            ts2 = pc.strptime(pc.utf8_slice_codeunits(df[BASE_DATA_COLUMN_NAME.TIMESTAMP], 
                                                      0,
                                                      15),
                              format=mod_format, 
                              unit="ms")
            d = pc.utf8_slice_codeunits(df[BASE_DATA_COLUMN_NAME.TIMESTAMP], 
                                        15,
                                        99).cast(pyarrow_int64()).cast(duration("ms"))
            timecol = pc.add(ts2, d)
            
            # calculate 'p'
            p_value = pc.divide(
                            pc.add_checked(df['ask'], df['bid']),
                            2 
            )
       
            # aggregate in a new table
            df = Table.from_arrays(
                [
                    timecol,
                    df[BASE_DATA_COLUMN_NAME.ASK],
                    df[BASE_DATA_COLUMN_NAME.BID],
                    df[BASE_DATA_COLUMN_NAME.VOL],
                    p_value
                ],
                schema = schema(PYARROW_DTYPE_DICT.TIME_TICK_DTYPE.copy().items())
            )
            
        elif engine == 'polars':
            
            # download to temporary csv file 
            # for best performance with polars
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            
            if (
                Path(temp_filepath).exists()
                and
                Path(temp_filepath).is_file()
                ):
                
                Path(temp_filepath).unlink(missing_ok=True)    
                
            else:
                
                # create temporary files directory if not present
                tempdir_path = Path(temp_filepath).parent
                tempdir_path.mkdir(exist_ok=True)
                
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            raw_file_dtypes.pop('p')
            raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string
            
            # read file
            # set schema for columns but avoid timestamp columns
            df = read_csv(
                        'polars',
                        temp_filepath,
                        separator   = ',',
                        has_header  = False,
                        new_columns = DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                        schema      = raw_file_dtypes,
                        use_pyarrow = True
            )
            
            # convert timestamp column to datetime data type
            df = df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        polars_datetime('ms'), 
                        format=DATE_FORMAT_HISTDATA_CSV
                    )
                )
    
            # calculate 'p'
            df = df.with_columns(
                    ( (col('ask') + col('bid')) / 2).alias('p') 
                 ) 
            
            # final cast to standard dtypes
            df = df.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
                
        elif engine == 'polars_lazy':
            
            # download to temporary csv file 
            # for best performance with polars
            
            # alternative using pyarrow
            buf = BufferReader(raw_file.read())
            
            if (
                Path(temp_filepath).exists()
                and
                Path(temp_filepath).is_file()
                ):
                
                Path(temp_filepath).unlink(missing_ok=True)    
                
            else:
                
                # create temporary files directory if not present
                tempdir_path = Path(temp_filepath).parent
                tempdir_path.mkdir(exist_ok=True)
                
            # download buffer to file
            buf.download(temp_filepath)
            
            # from histdata raw files column 'p' is not present
            raw_file_dtypes = POLARS_DTYPE_DICT.TIME_TICK_DTYPE.copy()
            raw_file_dtypes.pop('p')
            raw_file_dtypes[BASE_DATA_COLUMN_NAME.TIMESTAMP] = polars_string
            
            # read file
            # set schema for columns but avoid timestamp columns
            df = read_csv(
                        'polars_lazy',
                        temp_filepath,
                        separator   = ',',
                        has_header  = False,
                        new_columns = DATA_COLUMN_NAMES.TICK_DATA_NO_PVALUE,
                        schema      = raw_file_dtypes
            )
            
            # convert timestamp column to datetime data type
            df = df.with_columns(
                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).str.strptime(
                        polars_datetime('ms'), 
                        format=DATE_FORMAT_HISTDATA_CSV
                    )
                )
    
            # calculate 'p'
            df = df.with_columns(
                    ( (col('ask') + col('bid')) / 2).alias('p') 
                 ) 
            
            # final cast to standard dtypes
            df = df.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            
        else:
            
            logger.error(f'Engine {engine} is not supported')
            raise TypeError
            
        # return dataframe
        return df


    def _year_data_to_file(self,
                           ticker,
                           year,
                           tf=None,
                           engine='polars'
        ):
        """


        Parameters
        ----------
        year : TYPE
            DESCRIPTION.
        tf : TYPE, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        None.

        """

        ticker_path = self._histdata_path / ticker
        if not ticker_path.is_dir() or not ticker_path.exists():
            ticker_path.mkdir(parents=True, exist_ok=False)

        year_path = ticker_path / str(year).upper()
        if not year_path.is_dir() or not year_path.exists():
            year_path.mkdir(parents=True, exist_ok=False)

        # alternative: get year by referenced key
        year_tf_key = self._db_key(ticker, year, tf)
        
        if not (
            isinstance(self._db_dict.get(year_tf_key), 
                       type(self._dataframe_type([])))
            ):
            
            logger.error(f'dat with key {year_tf_key} is no valid DataFrame')
            raise KeyError
            
        year_data = self._db_dict.get(year_tf_key)

        filepath = year_path / self._get_filename(ticker, year, tf)

        if filepath.exists() and filepath.is_file():
            
            logger.info('File {filepath} already exists, skip dump to file')
            return

        # dump data to file
        if self.data_type == DATA_TYPE.CSV_FILETYPE:
            
            # csv data file
            write_csv(year_data, filepath)
            
            
        elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
            
            # parquet data file
            write_parquet(year_data, str(filepath.absolute()))
            

    def _get_file_details(self, filename):

        if not (
                isinstance(filename, str)
        ):
            
            logger.error('filename {filename} invalid type: required str')

        # get years available in offline data (local disk)
        filename_details = filename.replace('_', '.').split(sep='.')

        # store each file details in local variables
        file_ticker = filename_details[FILENAME_TEMPLATE.TICKER_INDEX]
        file_year = int(
            filename_details[FILENAME_TEMPLATE.YEAR_INDEX][FILENAME_TEMPLATE.YEAR_NUMERICAL_CHAR:])
        file_tf = filename_details[FILENAME_TEMPLATE.TF_INDEX]

        # return each file details
        return file_ticker, file_year, file_tf


    def _get_filename(self, ticker, year, tf):

        # based on standard filename template
        return FILENAME_STR.format(ticker   = ticker,
                                   year     = year,
                                   tf       = tf,
                                   file_ext = self.data_type)


    def _update_local_data_folder(self):

        for ticker in self._get_ticker_list():
            
            # get active years loaded on db manager
            years_list = self._get_years_list(ticker, 'int')
    
            # get file names in local folder
            _, local_files_name = self._list_local_data(ticker)
    
            # loop through years
            for year in years_list:
    
                year_tf_list = self._get_year_timeframe_list(ticker, year)
    
                # loop through timeframes loaded
                for tf in year_tf_list:
    
                    tf_filename = self._get_filename(ticker,
                                                     year,
                                                     tf)
    
                    tf_key = self._db_key(ticker, year, tf)
    
                    # check if file is present in local data folder
                    # and if valid dataframe is currently loaded in database
                    if tf_filename not in local_files_name \
                       and isinstance(self._db_dict.get(tf_key),
                                      type(self._dataframe_type([]))):
    
                        self._year_data_to_file(ticker,
                                                year,
                                                tf=tf,
                                                engine=self.engine
                        )


    def _download_year(self,
                       ticker,
                       year
        ):

        year_tick_df = self._dataframe_type([])        

        for month in MONTHS:

            month_num = MONTHS.index(month) + 1
            url = HISTDATA_URL_TICKDATA_TEMPLATE.format(
                                        ticker=ticker.lower(),
                                        year=year,
                                        month_num=month_num)
            
            logger.info(f'To download: {ticker} - {year} - '
                        f'{MONTHS[month_num-1]}') 
            
            file = self._download_month_raw(
                            ticker, 
                            url,
                            year,
                            month_num
                    )
            
            if file:
                
                month_data = self._raw_zipfile_to_df(file,
                                                     str(self._temporary_data_path
                                                         / ( f'{ticker}_' +
                                                         f'{year}_' +
                                                         f'{month}_' +
                                                         TEMP_CSV_FILE )
                                                         ),
                                                     engine = self.engine
                )
                 
                # if first iteration, assign instead of concat
                if is_empty_dataframe(year_tick_df):
                    
                    year_tick_df = month_data
                    
                else:
                    
                    year_tick_df = concat_data([year_tick_df, month_data])
                    
        return sort_dataframe(year_tick_df,
                              BASE_DATA_COLUMN_NAME.TIMESTAMP)


    def _download(self,
                  ticker,
                  years,
                  search_local=False):

        if not(
                isinstance(years, list)
            ):
            
            logger.error('years {years} invalid, must be list type')
            raise TypeError

        if not(
                set(years).issubset(YEARS)
            ):
            
            logger.error('requestedyears{years} not available. '
                        'Years must be limited to: {YEARS}')
            raise ValueError
            
        # convert to list of int
        if not all(isinstance(year, int) for year in years):
            years = [int(year) for year in years]

        # search if years data are already available offline
        if search_local:
            years_tickdata_offline = self._local_load_data(
                                            ticker,
                                            years, 
                                            engine=self.engine
            )
            
        else:

            years_tickdata_offline = list()

        # years not found on local offline path
        # must be downloaded from the net
        tick_years_to_download = set(years).difference(years_tickdata_offline)

        if tick_years_to_download:

            for year in tick_years_to_download:

                year_tick_df = self._download_year(
                                        ticker,
                                        year
                                )

                # get key for dotty dict: TICK
                year_tick_key = self._db_key(ticker, year, 'TICK')
                self._db_dict[year_tick_key] = year_tick_df

        # update manager database
        self._update_db()
        

    def _list_local_data(self,
                         ticker):

        local_files = []
        local_files_name = []
        
        # prepare predefined path and check if exists
        tickerfolder_path = Path(self._histdata_path) / ticker
        if not (
            tickerfolder_path.exists() \
            and 
            tickerfolder_path.is_dir()
        ):
            
            tickerfolder_path.mkdir(parents=True, 
                                    exist_ok=False)
            
        else:
            
            # list all specifed ticker data files in folder path and subdirs
            # list for all data filetypes supported
            
            local_files = [file for file in list(tickerfolder_path.glob(
                                            f'**/{ticker}_*.*'))
                            if match(r'.(\w+)$', file.suffix).groups()[0]
                            in SUPPORTED_DATA_FILES]
            
            local_files_name = [file.name for file in local_files]

            # check compliance of files to convention (see notes)
            # TODO: warning if no compliant and filter out from files found

        return local_files, local_files_name


    def _local_load_data(self, 
                         ticker,
                         years_list,
                         engine='polars'
        ):
        """


        Parameters
        ----------
        folderpath : TYPE
            DESCRIPTION.
        years_to_load : TYPE
            DESCRIPTION.
        timeframe : TYPE, optional
            DESCRIPTION. The default is None.

        Raises
        ------
        FileNotFoundError
            DESCRIPTION.
        NotADirectoryError
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        # list data available in local folder
        local_files, local_files_name = self._list_local_data(ticker)

        years_tick_files_found = list()
        
        # avoid to load data if there are file duplicates by name
        # identify duplicates and remove csv type file
        # in order increase speed if parquet file is available
        local_files_stem = [file.stem for file in local_files]
        
        duplicates_unique = list(unique_everseen(duplicates(local_files_stem)))
        
        local_files_filtered = local_files.copy()
        local_files_name_filtered = local_files_name.copy()
        
        if duplicates_unique:
        
            for item in duplicates_unique:
                
                parquet_file = item \
                                + '.' \
                                + DATA_TYPE.PARQUET_FILETYPE
                                
                csv_file = item \
                            + '.' \
                            + DATA_TYPE.CSV_FILETYPE
                
                if (
                    parquet_file in local_files_name_filtered
                    and
                    csv_file in local_files_name_filtered
                    ):
                    
                    index_to_remove = local_files_name_filtered.index(csv_file)
                    local_files_name_filtered.pop(index_to_remove)
                    local_files_filtered.pop(index_to_remove)
                
        # parse files and fill details list
        for file in local_files_filtered:

            # get years available in offline data (local disk)
            local_filepath_key = file.name.replace('_', '.')

            # get file details
            file_ticker, file_year, file_tf = self._get_file_details(file.name)

            # check at timeframe index file has a valid timeframe
            if check_timeframe_str(file_tf) == file_tf:

                # create key for dataframe type
                year_tf_key = self._db_key(file_ticker,
                                          file_year,
                                          file_tf)

                if self._db_dict.get(year_tf_key) is None:
                    
                    # check if year is needed to be loaded
                    if file_ticker == ticker \
                            and (int(file_year) in years_list):

                        if file_tf == TICK_TIMEFRAME:
                            years_tick_files_found.append(file_year)
    
                        if match(f'.({DATA_TYPE.CSV_FILETYPE})$',
                                file.suffix):
                            
                            if engine == 'pandas':
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
            
                                    self._db_dict[year_tf_key] = \
                                        read_csv(  
                                            'pandas',
                                            file,
                                            sep=',',
                                            header=0,
                                            names=DATA_COLUMN_NAMES.TICK_DATA,
                                            dtype=DTYPE_DICT.TICK_DTYPE,
                                            parse_dates=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                            date_format=DATE_FORMAT_ISO8601,
                                            engine = 'c'
                                    )
                                    
                                # year standard data file
                                elif file_tf in self._tf_list:
            
                                    self._db_dict[year_tf_key] = \
                                        read_csv(
                                            'pandas',
                                            file,
                                            header=0,
                                            names=DATA_COLUMN_NAMES.TF_DATA,
                                            dtype=DTYPE_DICT.TF_DTYPE,
                                            parse_dates=[
                                                BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                            date_format=DATE_FORMAT_ISO8601,
                                            engine='c'
                                    )
                                    
                            elif engine == 'pyarrow':
                                
                                
                                # use panda read_csv an its options with 
                                # engine = 'pyarrow'
                                # dtype_backend = 'pyarrow'
                                # self._db_dict[year_tf_key] = \
                                #     read_csv(  
                                #        engine = 'pyarrow',
                                #        dtype_backend = 'pyarrow'
                                # )
                                
                                # use pyarrow native options
                                read_opts = arrow_csv.ReadOptions(
                                            use_threads  = True,
                                            autogenerate_column_names = False
                                            
                                    )
                                
                                parse_opts = arrow_csv.ParseOptions(
                                            delimiter = ','
                                    )
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
                                    
                                    convert_opts = arrow_csv.ConvertOptions(
                                                column_types = PYARROW_DTYPE_DICT.TIME_TICK_DTYPE,
                                                timestamp_parsers = [arrow_csv.ISO8601]
                                    )
                                 
                                    
                                else:
                                    
                                    convert_opts = arrow_csv.ConvertOptions(
                                                column_types = PYARROW_DTYPE_DICT.TIME_TF_DTYPE,
                                                timestamp_parsers = [arrow_csv.ISO8601]
                                    )
                                    
                                
                                self._db_dict[year_tf_key] = \
                                    read_csv(  
                                            'pyarrow',
                                            file,
                                            read_options    = read_opts,
                                            parse_options   = parse_opts,
                                            convert_options = convert_opts
                                    )
                            
                            # year standard data file    
                            elif engine == 'polars':
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TICK_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE,
                                                 try_parse_dates=True
                                        )
                                                     
                                elif file_tf in self._tf_list:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TF_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TF_DTYPE,
                                                 try_parse_dates=True
                                        )
                                        
                            elif  engine == 'polars_lazy':
                                
                                # tick data file 
                                if file_tf == TICK_TIMEFRAME:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars_lazy', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TICK_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TICK_DTYPE,
                                                 try_parse_dates=True
                                        )
                                                     
                                elif file_tf in self._tf_list:
                                    
                                    self._db_dict[year_tf_key] = \
                                        read_csv('polars_lazy', 
                                                 file,
                                                 new_columns=DATA_COLUMN_NAMES.TF_DATA,
                                                 schema=POLARS_DTYPE_DICT.TIME_TF_DTYPE,
                                                 try_parse_dates=True
                                        )
                        
                            else:
                                
                                logger.error(f'Engine {engine} not supported '
                                             f'available: {SUPPORTED_DATA_ENGINES}')
                                raise TypeError
                    
                        elif  match(f'.({DATA_TYPE.PARQUET_FILETYPE})$',
                                    file.suffix):
                        
                            self._db_dict[year_tf_key] = read_parquet(engine,
                                                                      file)
                            
                        else:
                            
                            logger.error(f'file type {self.data_type} not supported '
                                         f'available: {SUPPORTED_DATA_FILES}')
                            raise TypeError
                            
                            
                        
                else:
                    
                    # continue as data is already present
                    continue
                            
                        
        # return list of years which tick file 
        # has been found and loaded
        return years_tick_files_found
    

    def _update_db(self):

        # complete year keys along timeframes required
        self._complete_years_timeframe()

        # dump new downloaded data not already present in local data folder
        self._update_local_data_folder()
        
        # all data is dumped in database folder
        # okay to clear temporary data folder
        #self._clear_temporary_data_folder()


    def add_timeframe(self, timeframe, update_data=False):

        if not hasattr(self, '_tf_list'):
            self._tf_list = []

        if isinstance(timeframe, str):

            timeframe = [timeframe]

        if not(
            isinstance(timeframe, list) \
            and 
            all([isinstance(tf, str) for tf in timeframe])
            ):
            
            logger.error('timeframe invalid: str or list required')
            raise TypeError

        tf_list = [check_timeframe_str(tf) for tf in timeframe]

        if not set(tf_list).issubset(self._tf_list):

            # concat timeframe accordingly
            # only just new elements not already present
            self._tf_list.extend(set(tf_list).difference(self._tf_list))

            if update_data:

                self._update_db()


    def get_data(self,
                ticker,
                timeframe,
                start,
                end,
                add_timeframe = True,
                add_in_memory = True):
        
        """
        
        Main function to get data, considering data currently managed
        in instance, then searchin in local folders and ultimately
        recurring to web download.

        Parameters
        ----------
        timeframe : TYPE
            DESCRIPTION.
        start : TYPE
            DESCRIPTION.
        end : TYPE
            DESCRIPTION.
        add_timeframe : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        data_df : TYPE
            DESCRIPTION.

        """
        
        # force ticker parameter to upper case
        ticker = ticker.upper()

        if not check_timeframe_str(timeframe):
            
            logger.error(f'timeframe request {timeframe} invalid')
            raise ValueError
            
        else:
            
            start = any_date_to_datetime64(start)
            end = any_date_to_datetime64(end)

        if end < start:
            
            logger.error('date interval not coherent, '
                         'start must be older than end')
            return self._dataframe_type([])

        if not timeframe in self._tf_list \
            and add_timeframe:

            # timeframe list
            self.add_timeframe([timeframe])

        # get years including interval requested
        years_interval_req = list(range(start.year, end.year+1, 1))
        
        # get all keys referring to specific ticker
        ticker_keys = self._get_ticker_keys(ticker)
        
        # get data keys referred to interval years 
        # at the given timeframe
        interval_keys = [key for key in ticker_keys
                          if int(get_dotty_key_field(key, 
                                                     DATA_KEY.YEAR_INDEX)[1:])
                              in years_interval_req \
                              and get_dotty_key_field(key, 
                                                      DATA_KEY.TF_INDEX) 
                                  == timeframe]
        
        # get years covered by interval keys
        interval_keys_years = [int(get_dotty_key_field(key, 
                                                       DATA_KEY.YEAR_INDEX)[1:])
                               for key in interval_keys]
        
        # aggregate data to current instance if necessary
        if years_interval_req != interval_keys_years:
            
            year_tf_missing = list(set(years_interval_req).difference(interval_keys_years))
            
            year_tick_keys = [int(get_dotty_key_field(key, 
                                                      DATA_KEY.YEAR_INDEX)[1:])
                              for key in ticker_keys
                                if get_dotty_key_field(key, 
                                                       DATA_KEY.TF_INDEX) \
                                    == TICK_TIMEFRAME ]
            
            year_tick_missing = list(set(years_interval_req).difference(year_tick_keys))
            
            # if tick is missing --> download missing years
            if year_tick_missing:
                
                self._download(
                    ticker,
                    year_tick_missing, 
                    search_local=True
                )
                
            # if timeframe req is in tf_list 
            # data requested should at this point be available
            # call add data for specific timeframe requested 
            if not timeframe in self._tf_list:
                
                for year in year_tf_missing:
                    
                    # call add single tf data
                    self._add_tf_data_key(
                            ticker, 
                            year,
                            timeframe
                    )
                    
            # get all keys referring to specific ticker
            ticker_keys = self._get_ticker_keys(ticker)
             
            # get years covered by interval keys
            interval_keys_years = [int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:]) 
                                   for key in ticker_keys
                                   if int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:])
                                       in years_interval_req and \
                                        get_dotty_key_field(key, 
                                                            DATA_KEY.TF_INDEX) \
                                        == timeframe]
            
            if not( years_interval_req == interval_keys_years ):
            
                logger.critical(f'processing year data completion for '
                                f'{years_interval_req} not ok')
                raise ValueError
                    
        # at this point data keys necessary are completed
        
        # get data keys referred to interval years 
        # at the given timeframe
        interval_keys = [key for key in ticker_keys
                          if int(get_dotty_key_field(key, DATA_KEY.YEAR_INDEX)[1:])
                              in years_interval_req \
                              and get_dotty_key_field(key, DATA_KEY.TF_INDEX) \
                                  == timeframe]
            
        data_df = self._dataframe_type([])
        
        # TODO: do I have to return explicit copies of date sliced results?
        
        if len(interval_keys) == 1: 

            data_df = self._db_dict.get(interval_keys[0])

            # return data slice
            if self.engine == 'pandas':
            
                data_df = data_df[(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP]
                                   >= start)
                                  & 
                                  (data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP]
                                   <= end)].copy()
                
                # reset index
                data_df.reset_index(drop=True, inplace=True)
            
            elif self.engine == 'pyarrow':
                
                mask = pc.and_(
                            pc.greater(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       start),
                            pc.less(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       end)
                            )
                
                data_df = Table.from_arrays(data_df.filter(mask).columns,
                                            schema=data_df.schema)
                
            elif self.engine == 'polars':
                
                data_df = \
                    \
                    (
                        data_df
                        .filter(
                            col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(start,
                                                                            end   
                            )
                        ).clone()
                    )
            
            elif self.engine == 'polars_lazy':
                
                data_df = \
                    \
                    (
                        data_df
                        .filter(
                            col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(start,
                                                                            end   
                            )
                        ).clone()
                    )
                    
            else:
                
                logger.error(f'engine {self.engine} not supported'
                             ' for get_data function')
                raise TypeError
            
        else:

            # order keys by ascending year value
            interval_keys.sort(key=lambda x: 
                               int(get_dotty_key_field(x, 
                                                       DATA_KEY.YEAR_INDEX)[1:])
                               )

                
            # TODO: need to differentiate dataframe slicing, filtering and
            #       selection depending on engine used
                
            # get data interval
            
            if self.engine == 'pandas':
                
                data_df = concat_data([self._db_dict.get(key)[
                    (ticker_db_dict.get(key)[BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                         >= start)
                    & (ticker_db_dict.get(key)[BASE_DATA_COLUMN_NAME.TIMESTAMP] 
                         <= end)].copy()
                    for key in interval_keys]
                )
                
                data_df.reset_index(drop=True, inplace=True)
                
            elif self.engine == 'pyarrow':
                
                mask = pc.and_(
                            pc.greater(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       start),
                            pc.less(data_df[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                       end)
                            )
                
                data_df = concat_data(
                    [
                        Table.from_arrays(
                            self._db_dict.get(key).filter(mask).columns,
                            schema=data_df.schema
                        )
                        for key in interval_keys
                    ]
                )
                 
            elif self.engine == 'polars':
            
                # slice data using polars sintax:
                    
                data_df = \
                    \
                    concat_data(
                        [
                            (
                                self._db_dict.get(key)
                                .filter(
                                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(start,
                                                                                    end   
                                    )
                                ).clone()
                            )
                            for key in interval_keys
                        ]
                    )
                    
            elif self.engine == 'polars_lazy':
            
                # slice data using polars sintax:
                    
                data_df = \
                    \
                    concat_data(
                        [
                            (
                                self._db_dict.get(key)
                                .filter(
                                    col(BASE_DATA_COLUMN_NAME.TIMESTAMP).is_between(start,
                                                                                    end   
                                    )
                                ).clone()
                            )
                            for key in interval_keys
                        ]
                    )
                    
            else:
                
                logger.error(f'engine {self.engine} not supported'
                             ' for get_data function')
                raise TypeError
                
        # if not requested to save data in memory
        # wipe out db keys dictionary
        if not add_in_memory:
            
            self._db_dict.clear()
            
        return data_df
    

    def plot(self, 
             ticker,
             timeframe,
             start_date,
             end_date
        ):
        """
        Plot data in selected time frame and start and end date bound
        :param date_bounds: start and end of plot
        :param timeframe: timeframe to visualize
        :return: void
        """

        logger.info(f'''Chart request:
                    ticker {ticker}
                    timeframe {timeframe}
                    from {start_date}
                    to {end_date}''')
        
        chart_data = self.get_data(ticker        = ticker,
                                   timeframe     = timeframe,
                                   start         = start_date,
                                   end           = end_date,
                                   add_timeframe = True)

        chart_data = to_pandas_dataframe(chart_data)
        
        if chart_data.index.name != BASE_DATA_COLUMN_NAME.TIMESTAMP:
            
            chart_data.set_index(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                 inplace=True)
            
            chart_data.index = to_datetime(chart_data.index)
        
        # candlestick chart type
        # use mplfinance
        chart_kwargs = dict(style    = 'charles',
                            title    = ticker,
                            ylabel   = 'Quotation',
                            xlabel   = 'Timestamp',
                            volume   = False,
                            figratio = (12,8),
                            figscale = 1
        )
        
        mpf_plot(chart_data,type='candle',**chart_kwargs)

        mpf_show()
        

    
