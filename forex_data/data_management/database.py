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


from loguru import logger
from pathlib import Path
from attrs import (
    define,
    field,
    validate,
    validators
)
from re import (
    fullmatch,
    search,
    IGNORECASE
)
from collections import OrderedDict
from numpy import array
from typing import Any, Dict, List, Optional, Tuple, Union, cast
from pathlib import Path as PathType
from datetime import datetime
import json

# Import polars types directly
from polars import (
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe,
    read_database
)

# Import from adbc_driver_sqlite
from adbc_driver_sqlite import dbapi as sqlite_dbapi

# Import from sqlalchemy for text queries
try:
    from sqlalchemy import text
except ImportError:
    # Fallback if sqlalchemy not available
    def text(s: str) -> str:
        return s

# Import from common module - explicit imports for items not in __all__
from .common import (
    TICK_TIMEFRAME,
    BASE_DATA_COLUMN_NAME,
    DATA_KEY,
    DATA_TYPE,
    SUPPORTED_DATA_FILES,
    SUPPORTED_DATA_ENGINES,
    SUPPORTED_BASE_DATA_COLUMN_NAME,
    SUPPORTED_SQL_COMPARISON_OPERATORS,
    get_attrs_names,
    validator_dir_path,
    is_empty_dataframe,
    list_remove_duplicates,
    POLARS_DTYPE_DICT,
    update_ticker_years_dict
)

# Import remaining items via star import
from .common import *

# Import from config module
from ..config import (
    read_config_file,
    read_config_string,
    read_config_folder
)


'''
BASE CONNECTOR
'''


@define(kw_only=True, slots=True)
class DatabaseConnector:

    data_folder: str = field(default='',
                             validator=validators.instance_of(str))

    def __init__(self, **kwargs: Any) -> None:

        pass

    def __attrs_post_init__(self) -> None:

        # create data folder if not exists
        if (
            not Path(self.data_folder).exists()
            or
            not Path(self.data_folder).is_dir()
        ):

            Path(self.data_folder).mkdir(parents=True,
                                         exist_ok=True)

    def connect(self) -> Any:
        """Connect to database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement connect")

    def check_connection(self) -> bool:
        """Check database connection - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement check_connection")

    def write_data(self, target_table: str, dataframe: Union[polars_dataframe, polars_lazyframe], clean: bool = False) -> None:
        """Write data to database - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement write_data")

    def read_data(self, market: str, ticker: str, timeframe: str, start: datetime, end: datetime) -> polars_lazyframe:
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

    def get_ticker_keys(self, ticker: str, timeframe: Optional[str] = None) -> List[str]:
        """Get ticker keys - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement get_ticker_keys")

    def get_ticker_years_list(self, ticker: str, timeframe: str = TICK_TIMEFRAME) -> List[int]:
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

        if 'config' in kwargs.keys():

            if kwargs['config']:

                config_path = Path(kwargs['config'])

                if (
                    config_path.exists() and
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

                        logger.warning('KeyError: initializing object has no '
                                       f'attribute {attr.name}')
                        raise

                    except IndexError:

                        logger.warning('IndexError: initializing object has no '
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
                logger.trace(f'config {kwargs["config"]} is empty, using default configuration')

        else:

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        validate(self)

        self.__attrs_post_init__(**kwargs)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        super().__attrs_post_init__(**kwargs)

        # set up log sink for DuckDB
        # Remove existing handlers for this sink to prevent duplicate log entries
        log_path = Path(self.data_folder) / 'log' / 'duckdb.log'

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

            logger.bind(target='duckdb').trace(f'DuckDB file {self.duckdb_filepath} already exists')

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

            logger.bind(target='duckdb').error(f'Error during connection to {self.duckdb_filepath}')

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
            logger.bind(target='duckdb').trace(f'Timestamp is already the first column in {duckdb_columns_dict.keys()}')

        return duckdb_columns_dict

    def _list_tables(self) -> List[str]:

        tables_list: List[str] = []

        conn = self.connect()

        try:

            tables = read_database(query='SELECT * FROM sqlite_master',
                                   connection=conn)

        except Exception as e:

            logger.bind(target='duckdb').error(f'Error list tables for {self.duckdb_filepath}: {e}')

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

    def get_ticker_keys(self, ticker: str, timeframe: Optional[str] = None) -> List[str]:

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

    def get_ticker_years_list(self, ticker: str, timeframe: str = TICK_TIMEFRAME) -> List[int]:

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

    def write_data(self, target_table: str, dataframe: Union[polars_dataframe, polars_lazyframe], clean: bool = False) -> None:

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

                logger.bind(target='duckdb').error(f'executing query {query} failed: {e}')

            else:

                if timeframe == TICK_TIMEFRAME:

                    # final cast to standard dtypes
                    dataframe = dataframe.cast(cast(Any, POLARS_DTYPE_DICT.TIME_TICK_DTYPE))

                else:

                    # final cast to standard dtypes
                    dataframe = dataframe.cast(cast(Any, POLARS_DTYPE_DICT.TIME_TF_DTYPE))

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

    data_folder: str = field(default='',
                             validator=validators.instance_of(str))
    data_type: str = field(default='parquet',
                           validator=validators.in_(SUPPORTED_DATA_FILES))
    engine: str = field(default='polars_lazy',
                        validator=validators.in_(SUPPORTED_DATA_ENGINES))

    _local_path = field(
        default=Path('.'),
        validator=validator_dir_path(create_if_missing=False))
    _tickers_years_info_filepath = field(default=Path('.'))

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

                    logger.bind(target='localdb').critical(
                        'invalid config type '
                        f'{kwargs["config"]}: '
                        'required str or Path, got '
                        f'{type(kwargs["config"])}')
                    raise TypeError

                # check consistency of config_args
                if (
                        not isinstance(config_args, dict) or
                    not bool(config_args)
                ):

                    logger.bind(target='localdb').critical(
                        f'config {kwargs["config"]} '
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

                logger.trace(f'config {kwargs["config"]} is empty, using default configuration')

        else:

            # no config file is defined
            # call generated init
            self.__attrs_init__(**kwargs)  # type: ignore[attr-defined]

        validate(self)

        self.__attrs_post_init__(**kwargs)

    def __attrs_post_init__(self, **kwargs: Any) -> None:

        super().__attrs_post_init__()

        # set up log sink for LocalDB
        # Remove existing handlers for this sink to prevent duplicate log entries
        log_path = Path(self.data_folder) / 'log' / 'localdb.log'

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
                   filter=lambda record: ('localdb' == record['extra'].get('target') and
                                          bool(record["extra"].get('target'))))

        self._local_path = Path(self.data_folder)
        self._tickers_years_info_filepath = self._local_path / 'tickers_years_info.json'

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

            logger.bind(target='localdb').error('filename {filename} invalid type: required str')
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
        local_files = [file for file in list(self._local_path.rglob(f'*'))
                       if search(self.data_type + '$', file.suffix)]

        local_files_name = [file.name for file in local_files]

        # check compliance of files to convention (see notes)
        # TODO: warning if no compliant and filter out from files found

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

    def clear_database(self, filter: Optional[str] = None) -> None:

        """
        Clear database files
        If filter is provided and is a ticker present in database (files present)
        delete only files related to that ticker
        """

        if filter:

            # in local path search for files having filter in path stem
            # and delete them
            # list all files in local path ending with data_type
            # and use re.search to catch matches
            if isinstance(filter, str):

                data_files = self._local_path.rglob(f'*.{self.data_type}')
                if data_files:
                    for file in data_files:
                        if search(filter, file.stem, IGNORECASE):
                            file.unlink(missing_ok=True)
                else:
                    logger.bind(target='localdb').info(f'No data files found in {self._local_path} with filter {filter}')

            else:
                logger.bind(target='localdb').error(f'Filter {filter} invalid type: required str')

        else:

            # clear all files in local path at
            # folder level using shutil
            shutil.rmtree(self._local_path)

    def get_ticker_keys(self, ticker: str, timeframe: Optional[str] = None) -> List[str]:

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

    def get_ticker_years_list(self, ticker: str, timeframe: str = TICK_TIMEFRAME) -> List[int]:

        ticker_years_list = []
        table = ''
        key_found = False

        local_files, local_files_name = self._list_local_data()
        ticker_keys = []

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

                logger.bind(target='localdb').error(f'Error querying table {table}: {e}')
                raise

            else:

                ticker_years_list = [int(row[0]) for row in read.collect().iter_rows()]

        return ticker_years_list

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
                years_list = self.get_ticker_years_list(ticker, timeframe)
                if years_list:  # Only add if there are years
                    tickers_years_dict[ticker][timeframe] = years_list

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
            raise TypeError(f'ticker_years_dict must be a dictionary, got {type(ticker_years_dict)}')

        try:
            with open(self._tickers_years_info_filepath, 'w') as f:
                json.dump(ticker_years_dict, f, indent=2)
        except Exception as e:
            logger.bind(target='localdb').error(
                f'Error writing ticker years data to {self._tickers_years_info_filepath}: {e}'
            )
            raise IOError(f'Error writing ticker years data to {self._tickers_years_info_filepath}: {e}')

    def add_tickers_years_info_to_file(self, ticker: str, timeframe: str, year: Union[int, List[int]]) -> None:
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
            raise TypeError(f'year must be an integer or list of integers, got {type(year)}')

        # Load existing data or create new dict if file doesn't exist
        if self._tickers_years_info_filepath.exists():
            ticker_years_dict = self.load_tickers_years_info()
        else:
            ticker_years_dict = {}
            logger.bind(target='localdb').info(
                f'File {self._tickers_years_info_filepath} does not exist. Creating new dict.'
            )

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
            raise FileNotFoundError(f'File {self._tickers_years_info_filepath} not found')

        try:
            with open(self._tickers_years_info_filepath, 'r') as f:
                ticker_years_dict = json.load(f)
            return ticker_years_dict
        except json.JSONDecodeError as e:
            logger.bind(target='localdb').error(
                f'Error decoding JSON from {self._tickers_years_info_filepath}: {e}'
            )
            raise IOError(f'Error decoding JSON from {self._tickers_years_info_filepath}: {e}')
        except Exception as e:
            logger.bind(target='localdb').error(
                f'Error reading ticker years data from {self._tickers_years_info_filepath}: {e}'
            )
            raise IOError(f'Error reading ticker years data from {self._tickers_years_info_filepath}: {e}')

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

        filepath = (self._local_path /
                    items[DATA_KEY.MARKET] /
                    items[DATA_KEY.TICKER_INDEX] /
                    filename)

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

            if any([col not in list(SUPPORTED_BASE_DATA_COLUMN_NAME.__args__) for col in comparison_column_name]):
                logger.bind(target='localdb').error(f'comparison_column_name must be a supported column name: {list(SUPPORTED_BASE_DATA_COLUMN_NAME.__args__)}')
                raise ValueError('comparison_column_name must be a supported column name')

            if any([cond not in list(SUPPORTED_SQL_COMPARISON_OPERATORS.__args__) for cond in comparison_operator]):
                logger.bind(target='localdb').error(f'comparison_operator must be a supported SQL comparison operator: {list(SUPPORTED_SQL_COMPARISON_OPERATORS.__args__)}')
                raise ValueError('comparison_operator must be a supported SQL comparison operator')

            if (
                (
                    comparison_aggregation_mode is not None
                    and
                    comparisons_len > 1
                )
                and
                comparison_aggregation_mode not in list(SUPPORTED_SQL_CONDITION_AGGREGATION_MODES.__args__)
            ):
                logger.bind(target='localdb').error(f'comparison_aggregation_mode must be a supported SQL condition aggregation mode: {list(SUPPORTED_SQL_CONDITION_AGGREGATION_MODES.__args__)}')
                raise ValueError('comparison_aggregation_mode must be a supported SQL condition aggregation mode')

            if len(comparison_column_name) != len(check_level) or len(comparison_column_name) != len(comparison_operator):
                logger.bind(target='localdb').error('comparison_column_name, check_level and comparison_operator must have the same length')
                raise ValueError('comparison_column_name, check_level and comparison_operator must have the same length')

            comparisons_len = len(comparison_column_name)

        dataframe = polars_lazyframe()

        filename = self._get_filename(market,
                                      ticker,
                                      timeframe)

        filepath = (self._local_path /
                    market /
                    ticker /
                    filename)

        if self.engine == 'polars':

            dataframe = polars_dataframe()

        elif self.engine == 'polars_lazy':

            dataframe = polars_lazyframe()

        else:

            logger.bind(target='localdb').error(f'Engine {self.engine} or data type {self.data_type} not supported')
            raise ValueError(f'Engine {self.engine} or data type {self.data_type} not supported')

        if (
            filepath.exists()
            and
            filepath.is_file()
        ):

            if self.data_type == DATA_TYPE.CSV_FILETYPE:

                dataframe = read_csv(self.engine, filepath)

            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:

                dataframe = read_parquet(self.engine, filepath)

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
                        # wrap all conditions in parentheses with aggregation mode between them
                        query += f'''AND
                                 ({comparison_column_name[0]} {comparison_operator[0]} {check_level[0]}
                                 '''
                        for col, level, cond, index in zip(comparison_column_name[1:], check_level[1:], comparison_operator[1:], range(1, comparisons_len)):

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

                logger.bind(target='localdb').error(f'executing query {query} failed: {e}')

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
