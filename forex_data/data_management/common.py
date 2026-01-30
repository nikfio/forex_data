# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:23:19 2022

@author: fiora
"""


from loguru import logger

from re import (
    fullmatch,
    findall,
    search,
    IGNORECASE
)

from typing import (
    cast,
    Any,
    List,
    Literal,
    Dict
)

import requests
from bs4 import BeautifulSoup

from datetime import (
    timedelta,
    datetime
)

from pathlib import Path

from attrs import (
    field,
    validators
)

from holidays import (
    country_holidays
)

# PANDAS
from pandas import (
    DataFrame as pandas_dataframe,
    concat as pandas_concat,
    Timestamp,
    isnull,
    bdate_range,
    to_datetime,
    Timedelta,
    read_parquet as pandas_read_parquet,
    read_csv as pandas_read_csv
)

from pandas.api.types import is_datetime64_any_dtype
from pandas.tseries.frequencies import to_offset
from pandas.tseries.offsets import DateOffset

# PYARROW
from pyarrow import (
    float32 as pyarrow_float32,
    timestamp as pyarrow_timestamp,
    schema as pyarrow_schema,
    Table,
    table as pyarrow_table,
    concat_tables,
    csv as arrow_csv
)

from pyarrow.parquet import (
    write_table,
    read_table
)

# POLARS
from polars import (
    Float32 as polars_float32,
    Datetime as polars_datetime,
    read_csv as polars_read_csv,
    concat as polars_concat,
    col,
    len as polars_len,
    read_parquet as polars_read_parquet,
    from_arrow,
    DataFrame as polars_dataframe,
    LazyFrame as polars_lazyframe,
    scan_csv as polars_scan_csv,
    scan_parquet as polars_scan_parquet
)

# POLYGON real time provider
from polygon.rest.models.aggs import (
    Agg as polygon_agg
)

from dateutil.rrule import (
    rrule,
    DAILY,
    MO,
    TU,
    WE,
    TH,
    FR
)

__all__ = [
    'YEARS',
    'MONTHS',
    'DATE_FORMAT_SQL',
    'DATE_FORMAT_HISTDATA_CSV',
    'HISTDATA_URL_TICKDATA_TEMPLATE',
    'HISTDATA_BASE_DOWNLOAD_METHOD',
    'HISTDATA_BASE_DOWNLOAD_URL',
    'DEFAULT_PATHS',
    'DATA_TYPE',
    'BASE_DATA_COLUMN_NAME',
    'DATA_FILE_COLUMN_INDEX',
    'SUPPORTED_DATA_FILES',
    'SUPPORTED_DATA_ENGINES',
    'ASSET_TYPE',
    'TEMP_FOLDER',
    'TEMP_CSV_FILE',
    'DTYPE_DICT',
    'PYARROW_DTYPE_DICT',
    'POLARS_DTYPE_DICT',
    'DATA_COLUMN_NAMES',
    'FILENAME_TEMPLATE',
    'DATA_KEY',
    'TICK_TIMEFRAME',
    'FILENAME_STR',
    'REALTIME_DATA_PROVIDER',
    'ALPHA_VANTAGE_API_KEY',
    'CANONICAL_INDEX',
    'DATE_NO_HOUR_FORMAT',
    'POLYGON_IO_API_KEY',
    'AV_LIST_URL',
    'PAIR_ALPHAVANTAGE_FORMAT',
    'PAIR_POLYGON_FORMAT',
    'SQL_COMPARISON_OPERATORS',
    'SUPPORTED_SQL_COMPARISON_OPERATORS',
    'SUPPORTED_BASE_DATA_COLUMN_NAME',
    'SQL_CONDITION_AGGREGATION_MODES',
    'SUPPORTED_SQL_CONDITION_AGGREGATION_MODES',

    'validator_file_path',
    'validator_dir_path',
    'get_attrs_names',
    'check_timeframe_str',
    'any_date_to_datetime64',
    'empty_dataframe',
    'is_empty_dataframe',
    'shape_dataframe',
    'get_dataframe_column',
    'get_dataframe_row',
    'get_dataframe_element',
    'get_dotty_leafs',
    'astype',
    'read_csv',
    'polars_datetime',
    'sort_dataframe',
    'concat_data',
    'list_remove_duplicates',
    'get_dotty_key_field',
    'reframe_data',
    'write_csv',
    'write_parquet',
    'read_parquet',
    'to_pandas_dataframe',
    'get_pair_symbols',
    'to_source_symbol',
    'get_date_interval',
    'polygon_agg_to_dict',
    'get_histdata_tickers',
    'TickerNotFoundError',
    'TickerDataNotFoundError',
    'TickerDataBadTypeException',
    'TickerDataInvalidException',
    'business_days_data',
    'update_ticker_years_dict',
]

# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================

# TickerNotFoundError:
# This exception is raised when the ticker requested is misspelled
# or does not exist in the database.


class TickerNotFoundError(Exception):
    pass


# TickerDataNotFoundError:
# This exception is raised when the ticker is found
# but data is not available or data retrieval failed.
class TickerDataNotFoundError(Exception):
    pass


# TickerDataBadTypeException:
# This exception is raised when the ticker data
# is found but data type is not compliant with the expected type.
class TickerDataBadTypeException(Exception):
    pass


# TickerDataInvalidException:
# This exception is raised when the ticker data
# is not found or invalid for generic reasons.
class TickerDataInvalidException(Exception):
    pass


# common functions, constants and templates
TEMP_FOLDER = "Temp"
TEMP_CSV_FILE = "Temp.csv"

HISTDATA_URL_TICKDATA_TEMPLATE = (
    'https://www.histdata.com/download-free-forex-historical-data/?/'
    'ascii/tick-data-quotes/{ticker}/{year}/{month_num}'
)

HISTDATA_URL_ONEMINDATA_TEMPLATE = (
    'http://www.histdata.com/download-free-forex-data/?/'
    'ascii/1-minute-bar-quotes/{pair}/{year}/{month_num}'
)

HISTDATA_BASE_DOWNLOAD_URL = "http://www.histdata.com/get.php"
HISTDATA_BASE_DOWNLOAD_METHOD = 'POST'

MONTHS = ['January', 'February', 'March', 'April', 'May', 'June',
          'July', 'August', 'September', 'October', 'November', 'December']
YEARS = list(range(2001, datetime.now().year, 1))


DATE_NO_HOUR_FORMAT = '%Y-%m-%d'
DATE_FORMAT_ISO8601 = 'ISO8601'
DATE_FORMAT_SQL = '%Y-%m-%d %H:%M:%S.%f'
DATE_FORMAT_HISTDATA_CSV = '%Y%m%d %H%M%S%f'

# DATA_KEY_TEMPLATE_STR           = '{ticker}.Y{year}.{tf}'
# DATA_KEY_TEMPLATE_PATTERN       =  '^[A-Za-z]+.Y[0-9]+.[A-Za-z0-9]+'
# FILENAME_STR                    = '{ticker}_Y{year}_{tf}.{file_ext}'
DATA_KEY_TEMPLATE_STR = '{market}.{ticker}.{tf}'
DATA_KEY_TEMPLATE_PATTERN = '^[A-Za-z0-9]_[A-Za-z]+.[A-Za-z0-9]+'
FILENAME_STR = '{market}_{ticker}_{tf}.{file_ext}'
DEFAULT_TIMEZONE = 'utc'
TICK_TIMEFRAME = 'tick'

# ticker PAIR of forex market
SINGLE_CURRENCY_PATTERN_STR = '[A-Za-z]{3}'
TICKER_PATTERN = '^' + SINGLE_CURRENCY_PATTERN_STR  \
    + SINGLE_CURRENCY_PATTERN_STR + '$'
PAIR_GENERIC_FORMAT = '{TO}/{FROM}'

# ALPHAVANTAGE
PAIR_ALPHAVANTAGE_FORMAT = '{TO}/{FROM}'
PAIR_ALPHAVANTAGE_PATTERN = '^' + SINGLE_CURRENCY_PATTERN_STR + '/' \
    + SINGLE_CURRENCY_PATTERN_STR + '$'
ALPHA_VANTAGE_API_KEY = 'ALPHA_VANTAGE_API_KEY'
AV_LIST_URL = (
    'https://www.alphavantage.co/query?'
    'function=LISTING_STATUS&apikey={api_key}'
)

# PAIR POLYGON IO
PAIR_POLYGON_FORMAT = 'C:{TO}{FROM}'
PAIR_POLYGON_PATTERN = '^C:' + SINGLE_CURRENCY_PATTERN_STR + \
    SINGLE_CURRENCY_PATTERN_STR + '$'
POLYGON_IO_API_KEY = 'POLYGON_IO_API_KEY'

# TIME PATTERN
TIME_WINDOW_PATTERN_STR = '^[-+]?[0-9]+[A-Za-z]{1,}$'
TIME_WINDOW_COMPONENTS_PATTERN_STR = '^[-+]?[0-9]+|[A-Za-z]{1,}$'
TIME_WINDOW_UNIT_PATTERN_STR = '[A-Za-z]{1,}$'
GET_YEAR_FROM_TICK_KEY_PATTERN_STR = '^[A-Za-z].Y[0-9].TICK'
YEAR_FIELD_PATTERN_STR = '^Y([0-9]{4,})$'
POLARS_DURATION_PATTERN_STR = '^[0-9]+(ns|us|ms|s|m|h|d|w|mo|q|y|i)$'
PYARROW_DURATION_PATTERN_STR = '^[0-9]+(ns|us|ms|s|m|h|d|w|mo|q|y|i)$'

# auxiliary CONSTANT DEFINITIONS

# dotty key template: <ticker>.Y<year>.<timeframe>.<data-type>


class DATA_KEY:

    MARKET = 0
    TICKER_INDEX = 1
    TF_INDEX = 2


# filename template : <ticker>_Y<year>_<timeframe>.<filetype>
class FILENAME_TEMPLATE:

    TICKER_INDEX = 0
    YEAR_INDEX = 1
    YEAR_NUMERICAL_CHAR = 1
    TF_INDEX = 2
    FILETYPE_INDEX = 3


class DEFAULT_PATHS:

    BASE_PATH = str(Path.home() / '.database')
    HIST_DATA_FOLDER = 'HistoricalData'
    REALTIME_DATA_FOLDER = 'RealtimeData'


class DATA_TYPE:

    CSV_FILETYPE = 'csv'
    PARQUET_FILETYPE = 'parquet'
    DUCKDB = 'duckdb'


class DATA_FILE_COLUMN_INDEX:

    TIMESTAMP = 0


SUPPORTED_DATA_FILES = [
    DATA_TYPE.CSV_FILETYPE,
    DATA_TYPE.PARQUET_FILETYPE,
    DATA_TYPE.DUCKDB
]

# supported dataframe engines
# pyarrow is inserted but reframe operation all in pyarrow
# is not yet available, now it is masked
# to a refame call with polars
# reframe_data() on pyarrow Table
SUPPORTED_DATA_ENGINES = [
    'pandas',
    'pyarrow',
    'polars',
    'polars_lazy'
]

# SINGLE BASE DATA COMPOSIION TEMPLATE: ['open','close','high','low']
# with datetime/timestamp as index
# column names for dataframes TICK and timeframe filtered
# OHLC and related column names


class DATA_COLUMN_NAMES:

    TICK_DATA_NO_PVALUE = ['timestamp', 'ask', 'bid', 'vol']
    TICK_DATA = ['timestamp', 'ask', 'bid', 'vol', 'p']
    TF_DATA = ['timestamp', 'open', 'high', 'low', 'close']
    TICK_DATA_TIME_INDEX = ['ask', 'bid', 'vol', 'p']
    TF_DATA_TIME_INDEX = ['open', 'high', 'low', 'close']
    POLYGON_IO_AGGS = ['open', 'high', 'low', 'close', 'volume', 'vwap',
                               'timestamp', 'transactions']


# SELECTED AS SINGLE BASE DATA COMPOSION TEMPLATE
BASE_DATA = DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
BASE_DATA_WITH_TIME = DATA_COLUMN_NAMES.TF_DATA


class REALTIME_DATA_PROVIDER:

    ALPHA_VANTAGE = 'ALPHA_VANTAGE'
    POLYGON_IO = 'POLYGON-IO'


REALTIME_DATA_PROVIDER_LIST = [REALTIME_DATA_PROVIDER.ALPHA_VANTAGE,
                               REALTIME_DATA_PROVIDER.POLYGON_IO]


class DB_MODE:

    FULL_MODE = 'FULL_MODE'
    HISTORICAL_MODE = 'HISTORICAL_MODE'
    REALTIME_MODE = 'REALTIME_MODE'


class ASSET_TYPE:

    STOCK = 'STOCK'
    ETF = 'ETF'
    FOREX = 'FOREX'


class BASE_DATA_COLUMN_NAME:

    TIMESTAMP = 'timestamp'
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    ASK = 'ask'
    BID = 'bid'
    VOL = 'vol'
    P_VALUE = 'p'
    TRANSACTIONS = 'transactions'
    VWAP = 'vwap'
    OTC = 'otc'


SUPPORTED_BASE_DATA_COLUMN_NAME = Literal[
    BASE_DATA_COLUMN_NAME.TIMESTAMP,
    BASE_DATA_COLUMN_NAME.OPEN,
    BASE_DATA_COLUMN_NAME.HIGH,
    BASE_DATA_COLUMN_NAME.LOW,
    BASE_DATA_COLUMN_NAME.CLOSE,
    BASE_DATA_COLUMN_NAME.ASK,
    BASE_DATA_COLUMN_NAME.BID,
    BASE_DATA_COLUMN_NAME.VOL,
    BASE_DATA_COLUMN_NAME.P_VALUE,
    BASE_DATA_COLUMN_NAME.TRANSACTIONS,
    BASE_DATA_COLUMN_NAME.VWAP,
    BASE_DATA_COLUMN_NAME.OTC
]


class CANONICAL_INDEX:

    AV_LATEST_DATA_INDEX = 0
    AV_DF_DATA_INDEX = 0
    AV_DICT_INFO_INDEX = 1


class SQL_COMPARISON_OPERATORS:

    GREATER_THAN = '>'
    LESS_THAN = '<'
    GREATER_THAN_OR_EQUAL = '>='
    LESS_THAN_OR_EQUAL = '<='
    EQUAL = '=='
    NOT_EQUAL = '!='


SUPPORTED_SQL_COMPARISON_OPERATORS = Literal[
    SQL_COMPARISON_OPERATORS.GREATER_THAN,
    SQL_COMPARISON_OPERATORS.LESS_THAN,
    SQL_COMPARISON_OPERATORS.GREATER_THAN_OR_EQUAL,
    SQL_COMPARISON_OPERATORS.LESS_THAN_OR_EQUAL,
    SQL_COMPARISON_OPERATORS.EQUAL,
    SQL_COMPARISON_OPERATORS.NOT_EQUAL
]


class SQL_CONDITION_AGGREGATION_MODES:

    AND = 'AND'
    OR = 'OR'


SUPPORTED_SQL_CONDITION_AGGREGATION_MODES = Literal[
    SQL_CONDITION_AGGREGATION_MODES.AND,
    SQL_CONDITION_AGGREGATION_MODES.OR
]


# auxiliary functions

# get elements from db key
def get_db_key_elements(key):

    res = fullmatch(DATA_KEY_TEMPLATE_STR, key)

    if res:

        return res.groups()

    else:

        logger.error(
            f'key {key} does not respect regex template {DATA_KEY_TEMPLATE_STR}')
        raise ValueError


# parse argument to get datetime object with date format as input
def infer_date_from_format_dt(s, date_format='ISO8601', unit=None, utc=False):

    if unit:

        return to_datetime(s,
                           unit=unit,
                           utc=utc)

    else:

        return to_datetime(s,
                           format=date_format,
                           utc=utc)


# parse timeframe as string and validate if it is valid
# following pandas DateOffset freqstr rules and 'TICK' (=lowest timeframe available)
# link to official pandas doc
# https://pandas.pydata.org/docs/user_guide/timeseries.html#dateoffset-objects
# add compatibility to polars frequency string

def check_timeframe_str(tf: str | Timedelta | DateOffset, engine: Literal['pandas', 'polars', 'polars_lazy', 'pyarrow'] = 'pandas'):

    if tf == 'TICK':

        return tf

    elif isinstance(tf, Timedelta):

        '''
        Timedelta type is acceptd by all engines supported
        '''
        return tf

    elif engine == 'pandas':

        '''
        timeframe value check for pandas engine
        '''
        try:

            if isinstance(to_offset(tf), DateOffset):

                return tf

            else:

                logger.critical(f"Type check: Invalid timeframe for pandas: {tf}")
                raise ValueError

        except ValueError:

            logger.critical(f"Type check: Invalid timeframe for pandas: {tf}")
            raise

    elif (
            engine == 'polars'
            or
            engine == 'polars_lazy'
            or
            engine == 'pyarrow'
    ):

        '''
        timeframe value check for polars engine
        '''

        if isinstance(tf, str):

            if fullmatch(POLARS_DURATION_PATTERN_STR, tf, flags=IGNORECASE):

                return tf

            else:

                logger.critical(f"Type check: Invalid timeframe for polars: {tf}")
                raise ValueError

        elif isinstance(tf, timedelta):

            return tf

        else:

            logger.critical(f"Type check: Invalid timeframe for polars: {tf}")
            raise ValueError

    else:

        logger.critical(f"Type check: Invalid engine: {engine}")
        raise ValueError

# PAIR symbol functions


def get_pair_symbols(ticker):

    components = findall(SINGLE_CURRENCY_PATTERN_STR, ticker)

    if len(components) == 2:

        return components[0], components[1]

    else:

        return None


def check_symbol(symbol, source):

    if source == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:

        if fullmatch(PAIR_ALPHAVANTAGE_PATTERN, symbol):

            return True

        else:

            return False

    elif source == REALTIME_DATA_PROVIDER.POLYGON_IO:

        if fullmatch(PAIR_POLYGON_FORMAT, symbol):

            return True

        else:

            return False

    else:

        if fullmatch(PAIR_POLYGON_FORMAT, symbol):

            return True

        else:

            return False


def to_source_symbol(ticker, source):

    to_symbol, from_symbol = get_pair_symbols(ticker)

    if source == REALTIME_DATA_PROVIDER.ALPHA_VANTAGE:

        return PAIR_ALPHAVANTAGE_FORMAT.format(TO=to_symbol,
                                               FROM=from_symbol)

    elif source == REALTIME_DATA_PROVIDER.POLYGON_IO:

        return PAIR_POLYGON_FORMAT.format(TO=to_symbol,
                                          FROM=from_symbol)

    else:

        return PAIR_GENERIC_FORMAT.format(TO=to_symbol,
                                          FROM=from_symbol)


# TIMESTAMP RELATED FUNCTIONS

def timewindow_str_to_timedelta(time_window_str):

    if fullmatch(TIME_WINDOW_PATTERN_STR, time_window_str):

        return Timedelta(time_window_str)

    else:

        logger.error('time window pattern not match: '
                     '"<integer_multiplier><unit>" str')
        raise ValueError


def any_date_to_datetime64(any_date,
                           date_format='ISO8601',
                           unit=None,
                           to_pydatetime=False):

    try:

        any_date = infer_date_from_format_dt(any_date,
                                             date_format,
                                             unit=unit)

        if to_pydatetime:

            any_date = any_date.to_pydatetime()

    except Exception as e:

        logger.error(f'date {any_date} conversion failed, '
                     f'failed conversion to {date_format} '
                     'date format')
        raise

    return any_date


def get_date_interval(start=None,
                      end=None,
                      interval_start_mode=None,
                      interval_end_mode='now',
                      interval_timespan=None,
                      freq=None,
                      normalize=False,
                      bdays=False):

    # create start and end date as timestamp instances
    start_date = Timestamp(start)
    end_date = Timestamp(end)

    if interval_timespan:

        # a variety of interval mode could be implemented

        # 'now' - end of date interval is timestamp now
        if interval_end_mode == 'now':

            end_date = Timestamp.now()
            start_date = end_date - timewindow_str_to_timedelta(interval_timespan)

        if bdays:

            components = findall(TIME_WINDOW_COMPONENTS_PATTERN_STR,
                                 interval_timespan)

            # fixed days redundancy check available only with 'd' type requested
            # timespan
            if components[1] == 'd':

                days_list = list(
                    rrule(freq=DAILY,
                          dtstart=start_date,
                          until=end_date,
                          byweekday=(MO, TU, WE, TH, FR))
                )

                while len(days_list) < int(components[0]):

                    start_date = start_date - Timedelta(days=1)

                    days_list = list(
                        rrule(freq=DAILY,
                              dtstart=start_date,
                              until=end_date,
                              byweekday=(MO, TU, WE, TH, FR))
                    )

    # Timestamp() constructor ensures these are Timestamp objects
    if normalize:

        if not isnull(start_date):
            start_date = Timestamp.normalize(start_date)

        if not isnull(end_date):
            end_date = Timestamp.normalize(end_date)

    start_date = any_date_to_datetime64(start_date)
    end_date = any_date_to_datetime64(end_date)

    # generate DateTimeIndex if freq is set
    # otherwise return just start and end of interval
    if freq:

        bdate_dtindex = bdate_range(start=start_date,
                                    end=end_date,
                                    freq=freq,
                                    tz=None,
                                    normalize=normalize,
                                    name='timestamp'
                                    )

        return start_date, end_date, bdate_dtindex

    else:

        return start_date, end_date


# BASE OPERATIONS WITH DATAFRAME
# depending on dataframe engine support
# for supported engines see var SUPPORTED_DATA_ENGINES

# DATA ENGINES TYPES DICTIONARY
class DTYPE_DICT:

    TICK_DTYPE = {'ask': 'float32',
                  'bid': 'float32',
                  'vol': 'float32',
                  'p': 'float32'}
    TF_DTYPE = {'open': 'float32',
                'high': 'float32',
                'low': 'float32',
                'close': 'float32'}
    TIME_TICK_DTYPE = {'timestamp': 'datetime64[ms]',
                       'ask': 'float32',
                       'bid': 'float32',
                       'vol': 'float32',
                       'p': 'float32'}
    TIME_TF_DTYPE = {'timestamp': 'datetime64[ms]',
                     'open': 'float32',
                     'high': 'float32',
                     'low': 'float32',
                     'close': 'float32'}


class PYARROW_DTYPE_DICT:

    TICK_DTYPE = {'ask': pyarrow_float32(),
                  'bid': pyarrow_float32(),
                  'vol': pyarrow_float32(),
                  'p': pyarrow_float32()}
    TF_DTYPE = {'open': pyarrow_float32(),
                'high': pyarrow_float32(),
                'low': pyarrow_float32(),
                'close': pyarrow_float32()}
    TIME_TICK_DTYPE = {'timestamp': pyarrow_timestamp('ms'),
                       'ask': pyarrow_float32(),
                       'bid': pyarrow_float32(),
                       'vol': pyarrow_float32(),
                       'p': pyarrow_float32()}
    TIME_TF_DTYPE = {'timestamp': pyarrow_timestamp('ms'),
                     'open': pyarrow_float32(),
                     'high': pyarrow_float32(),
                     'low': pyarrow_float32(),
                     'close': pyarrow_float32()}


class POLARS_DTYPE_DICT:

    TICK_DTYPE = {'ask': polars_float32,
                  'bid': polars_float32,
                  'vol': polars_float32,
                  'p': polars_float32}
    TF_DTYPE = {'open': polars_float32,
                'high': polars_float32,
                'low': polars_float32,
                'close': polars_float32}
    TIME_TICK_DTYPE = {'timestamp': polars_datetime('ms'),
                       'ask': polars_float32,
                       'bid': polars_float32,
                       'vol': polars_float32,
                       'p': polars_float32}
    TIME_TF_DTYPE = {'timestamp': polars_datetime('ms'),
                     'open': polars_float32,
                     'high': polars_float32,
                     'low': polars_float32,
                     'close': polars_float32}

# DATA ENGINES FUNCTIONS


def empty_dataframe(engine):

    if engine == 'pandas':

        return pandas_dataframe()

    elif engine == 'pyarrow':

        return pyarrow_table([])

    elif engine == 'polars':

        return polars_dataframe()

    elif engine == 'polars_lazy':

        return polars_lazyframe()

    else:

        logger.error('function empty_dataframe not available'
                     f' for engine {engine}')
        raise ValueError


def is_empty_dataframe(dataframe):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe.empty

    elif isinstance(dataframe, Table):

        return (not bool(dataframe))

    elif isinstance(dataframe, polars_dataframe):

        return dataframe.is_empty()

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.collect().is_empty()

    else:

        logger.error('function is_empty_dataframe not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def shape_dataframe(dataframe):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe.shape

    elif isinstance(dataframe, Table):

        return dataframe.shape

    elif isinstance(dataframe, polars_dataframe):

        return dataframe.shape

    elif isinstance(dataframe, polars_lazyframe):

        return (
            dataframe.select(polars_len()).collect().item(0, 0),
            dataframe.collect_schema().len()
        )

    else:

        logger.error('function shape_dataframe not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def sort_dataframe(dataframe, column):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe.sort_values(by=[column])

    elif isinstance(dataframe, Table):

        return dataframe.sort_by(column)

    elif isinstance(dataframe, polars_dataframe):

        return dataframe.sort(column, nulls_last=True)

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.sort(column, nulls_last=True)

    else:

        logger.error('function sort_dataframe not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def get_dataframe_column(dataframe, column):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe[column]

    elif isinstance(dataframe, Table):

        return dataframe[column]

    elif isinstance(dataframe, polars_dataframe):

        return dataframe[column]

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.select(column).collect()

    else:

        logger.error('function get_dataframe_column not available'
                     ' for instance of type'
                     f' {type(dataframe)}')


def get_dataframe_row(dataframe, row):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe.loc[row]

    elif isinstance(dataframe, Table):

        return dataframe.slice(row, 1)

    elif isinstance(dataframe, polars_dataframe):

        return dataframe.slice(row, 1)

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.slice(row, 1)

    else:

        logger.error('function get_dataframe_row not available'
                     ' for instance of type'
                     f' {type(dataframe)}')


def get_dataframe_element(dataframe, column, row):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe[column][row]

    elif isinstance(dataframe, Table):

        return dataframe[column][row]

    elif isinstance(dataframe, polars_dataframe):

        return dataframe[column][row]

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.select(column).collect().item(row, 0)

    else:

        logger.error('function get_dataframe_element not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def dtype_dict_to_pyarrow_schema(dtype_dict):

    return pyarrow_schema(dtype_dict.items())


def astype(dataframe, dtype_dict):

    if isinstance(dataframe, pandas_dataframe):

        return dataframe.astype(dtype_dict)

    elif isinstance(dataframe, Table):

        return dataframe.cast(dtype_dict_to_pyarrow_schema(dtype_dict))

    elif isinstance(dataframe, polars_dataframe):

        return dataframe.cast(dtype_dict)

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.cast(dtype_dict)

    else:

        logger.error('function astype not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def read_parquet(engine, filepath):

    if engine == 'pandas':

        return pandas_read_parquet(filepath)

    elif engine == 'pyarrow':

        return read_table(filepath)

    elif engine == 'polars':

        return polars_read_parquet(filepath)

    elif engine == 'polars_lazy':

        return polars_scan_parquet(filepath)

    else:

        logger.error('function read_parquet not available'
                     f' for engine {engine}')
        raise ValueError


def write_parquet(dataframe, filepath):

    if isinstance(dataframe, pandas_dataframe):

        try:

            dataframe.to_parquet(filepath, index=True)

        except Exception as e:

            logger.exception(f'pandas write parquet failed: {e}')
            raise

    elif isinstance(dataframe, Table):

        try:

            write_table(dataframe, filepath)

        except Exception as e:

            logger.exception(f'pyarrow write parquet failed: {e}')
            raise

    elif isinstance(dataframe, polars_dataframe):

        try:

            dataframe.write_parquet(filepath)

        except Exception as e:

            logger.exception(f'polars write parquet failed: {e}')
            raise

    elif isinstance(dataframe, polars_lazyframe):

        try:

            dataframe.sink_parquet(filepath)

            # alternative to sink_parquet()
            # dataframe.collect(streaming=False).write_parquet(filepath)

        except Exception as e:

            logger.exception(f'polars lazyframe sink '
                             'parquet failed: {e}')
            raise

    else:

        logger.error('function write_parquet not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def read_csv(engine, file, **kwargs):

    if engine == 'pandas':

        return pandas_read_csv(file, **kwargs)

    elif engine == 'pyarrow':

        return arrow_csv.read_csv(file, **kwargs)

    elif engine == 'polars':

        return polars_read_csv(file, **kwargs)

    elif engine == 'polars_lazy':

        return polars_scan_csv(file, **kwargs)

    else:

        logger.error('function read_csv not available'
                     f' for engine {engine}')
        raise ValueError


def write_csv(dataframe, file, **kwargs):

    if isinstance(dataframe, pandas_dataframe):

        try:

            # IMPORTANT
            # pandas dataframe case
            # avoid date_format parameter since it is reported that
            # it makes to_csv to be excessively long with column data
            # being datetime data type
            # see: https://github.com/pandas-dev/pandas/issues/37484
            #      https://stackoverflow.com/questions/65903287/pandas-1-2-1-to-csv-performance-with-datetime-as-the-index-and-setting-date-form

            dataframe.to_csv(file,
                             header=True,
                             **kwargs)

        except Exception as e:

            logger.exception(f'Error writing csv file {file}'
                             f' with data type {type(dataframe)}: {e}')
            raise IOError

    elif isinstance(dataframe, Table):

        try:

            arrow_csv.write_csv(dataframe, file, **kwargs)

        except Exception as e:

            logger.exception(f'Error writing csv file {file}'
                             f' with data type {type(dataframe)}: {e}')
            raise IOError

    elif isinstance(dataframe, polars_dataframe):

        try:

            dataframe.write_csv(file, **kwargs)

        except Exception as e:

            logger.exception(f'Error writing csv file {file}'
                             f' with data type {type(dataframe)}: {e}')
            raise IOError

    elif isinstance(dataframe, polars_lazyframe):

        try:

            dataframe.sink_csv(file, **kwargs)

        except Exception as e:

            logger.exception(f'Error writing csv file {file}'
                             f' with data type {type(dataframe)}: {e}')
            raise IOError

    else:

        logger.error('function write_csv not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def concat_data(data_list=field(validator=validators.instance_of(list))):

    if not isinstance(data_list, list):

        logger.error('required input as list')
        raise TypeError

    # assume data type is unique by input
    # get type from first element

    if isinstance(data_list[0], pandas_dataframe):

        return pandas_concat(data_list,
                             ignore_index=False,
                             copy=False)

    elif isinstance(data_list[0], Table):

        return concat_tables(data_list)

    elif isinstance(data_list[0], polars_dataframe):

        return polars_concat(data_list, how='vertical')

    elif isinstance(data_list[0], polars_lazyframe):

        return polars_concat(data_list, how='vertical')

    else:

        logger.error('function concat not available'
                     ' for instance of type'
                     f' {type(data_list[0])}')
        raise ValueError


def to_pandas_dataframe(dataframe):

    # convert to pandas dataframe
    # useful for those calls
    # requiring pandas as input
    # or pandas functions not covered
    # by other dataframe instance

    if isinstance(dataframe, pandas_dataframe):

        return dataframe

    elif isinstance(dataframe, Table):

        return dataframe.to_pandas()

    elif isinstance(dataframe, polars_dataframe):

        return dataframe.to_pandas(use_pyarrow_extension_array=True)

    elif isinstance(dataframe, polars_lazyframe):

        return dataframe.collect().to_pandas(use_pyarrow_extension_array=True)

    else:

        logger.error('function to_pandas() not available'
                     ' for instance of type'
                     f' {type(dataframe)}')
        raise ValueError


def reframe_data(dataframe, tf):
    '''


    Parameters
    ----------
    data : TYPE
        DESCRIPTION.
    tf : TYPE
        DESCRIPTION.

    Raises
    ------
    ValueError
        DESCRIPTION.

    Returns
    -------
    Dataframe
        DESCRIPTION.

    '''

    if is_empty_dataframe(dataframe):

        return dataframe

    if isinstance(dataframe, pandas_dataframe):

        # assert timeframe input value
        tf = check_timeframe_str(tf, engine='pandas')

        if tf == TICK_TIMEFRAME:

            logger.warning(f'reframe not possible with target {TICK_TIMEFRAME}')

            return dataframe

        if not is_datetime64_any_dtype(dataframe.index):

            if BASE_DATA_COLUMN_NAME.TIMESTAMP in dataframe.columns:

                if not is_datetime64_any_dtype(
                        dataframe[BASE_DATA_COLUMN_NAME.TIMESTAMP]):

                    try:

                        dataframe[BASE_DATA_COLUMN_NAME.TIMESTAMP] = any_date_to_datetime64(
                            dataframe[BASE_DATA_COLUMN_NAME.TIMESTAMP])

                    except Exception as e:

                        logger.exception('Pandas engine: '
                                         'Failed conversion of timestamp columns '
                                         'to DatetimeIndex')
                        raise

            else:

                logger.error('Pandas engine: required column with '
                             f'name {BASE_DATA_COLUMN_NAME.TIMESTAMP}')
                raise ValueError

        # use pandas functions to reframe data on pandas Dataframe

        dataframe = sort_dataframe(dataframe, BASE_DATA_COLUMN_NAME.TIMESTAMP)

        dataframe = dataframe.set_index(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                        inplace=False,
                                        drop=True
                                        )

        # resample based on p value
        if all([col in DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX
                for col in dataframe.columns]):

            # resample along 'p' column, data in ask, bid, p format
            dataframe = dataframe.p.resample(tf).ohlc().interpolate(method='nearest')

        elif all([col in DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
                  for col in dataframe.columns]):

            # resample along given data already in ohlc format
            dataframe = dataframe.resample(tf).interpolate(method='nearest')

        else:

            logger.error(f'data columns {dataframe.columns} invalid, '
                         f'required {DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX} '
                         f'or {DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX}')
            raise ValueError

        return dataframe.reset_index(drop=False)

    elif isinstance(dataframe, Table):

        # assert timeframe input value
        tf = check_timeframe_str(tf, engine='pyarrow')

        '''
            use pyarrow functions to reframe data on pyarrow Table
            could not find easy way to filter an arrow table
            based on time interval

            opened an enhancement issue on github

            https://github.com/apache/arrow/issues/41049

            As a temporary alternative, convert arrow Table to polars
            and perform reframe with polars engine

        '''

        if all([col in DATA_COLUMN_NAMES.TICK_DATA
                for col in dataframe.column_names]):

            # convert to polars dataframe
            dataframe = from_arrow(dataframe,
                                   schema=cast(Any, POLARS_DTYPE_DICT.TIME_TICK_DTYPE))

        elif all([col in DATA_COLUMN_NAMES.TF_DATA
                  for col in dataframe.column_names]):

            # convert to polars dataframe
            dataframe = from_arrow(dataframe,
                                   schema=cast(Any, POLARS_DTYPE_DICT.TIME_TF_DTYPE))

        # perform operation
        # convert to arrow Table and return
        return reframe_data(dataframe, tf).to_arrow()

    elif isinstance(dataframe, polars_dataframe):

        # assert timeframe input value
        tf = check_timeframe_str(tf, engine='polars')

        tf = tf.lower()

        dataframe = sort_dataframe(dataframe, BASE_DATA_COLUMN_NAME.TIMESTAMP)

        if all([col in DATA_COLUMN_NAMES.TICK_DATA
                for col in dataframe.columns]):

            return dataframe.group_by_dynamic(
                BASE_DATA_COLUMN_NAME.TIMESTAMP,
                every=tf).agg(col('p').first().alias(BASE_DATA_COLUMN_NAME.OPEN),
                              col('p').max().alias(BASE_DATA_COLUMN_NAME.HIGH),
                              col('p').min().alias(BASE_DATA_COLUMN_NAME.LOW),
                              col('p').last().alias(BASE_DATA_COLUMN_NAME.CLOSE)
                              )

        elif all([col in DATA_COLUMN_NAMES.TF_DATA
                  for col in dataframe.columns]):

            return dataframe.group_by_dynamic(
                BASE_DATA_COLUMN_NAME.TIMESTAMP,
                every=tf).agg(col(BASE_DATA_COLUMN_NAME.OPEN).first(),
                              col(BASE_DATA_COLUMN_NAME.HIGH).max(),
                              col(BASE_DATA_COLUMN_NAME.LOW).min(),
                              col(BASE_DATA_COLUMN_NAME.CLOSE).last()
                              )

        else:

            logger.error(f'data columns {dataframe.columns} invalid, '
                         f'required {DATA_COLUMN_NAMES.TICK_DATA} '
                         f'or {DATA_COLUMN_NAMES.TF_DATA}')
            raise ValueError

    elif isinstance(dataframe, polars_lazyframe):

        tf = tf.lower()

        dataframe = dataframe.sort('timestamp', nulls_last=True)

        if all([col in DATA_COLUMN_NAMES.TICK_DATA
                for col in dataframe.collect_schema().names()]):

            return dataframe.group_by_dynamic(
                BASE_DATA_COLUMN_NAME.TIMESTAMP,
                every=tf).agg(col('p').first().alias(BASE_DATA_COLUMN_NAME.OPEN),
                              col('p').max().alias(BASE_DATA_COLUMN_NAME.HIGH),
                              col('p').min().alias(BASE_DATA_COLUMN_NAME.LOW),
                              col('p').last().alias(BASE_DATA_COLUMN_NAME.CLOSE)
                              )

        elif all([col in DATA_COLUMN_NAMES.TF_DATA
                  for col in dataframe.collect_schema().names()]):

            return dataframe.group_by_dynamic(
                BASE_DATA_COLUMN_NAME.TIMESTAMP,
                every=tf).agg(col(BASE_DATA_COLUMN_NAME.OPEN).first(),
                              col(BASE_DATA_COLUMN_NAME.HIGH).max(),
                              col(BASE_DATA_COLUMN_NAME.LOW).min(),
                              col(BASE_DATA_COLUMN_NAME.CLOSE).last()
                              )

        else:

            logger.error(f'data columns {dataframe.columns} invalid, '
                         f'required {DATA_COLUMN_NAMES.TICK_DATA} '
                         f'or {DATA_COLUMN_NAMES.TF_DATA}')
            raise ValueError

# UTILS FOR DOTTY DICTIONARY


def get_dotty_key_field(key, index):

    if not isinstance(key, str):

        logger.error(f'dotty key {key} invalid type, str required')
        raise TypeError

    try:

        field = key.split('.')[index]

    except IndexError:

        logger.exception(f'index {index} invalid for key {key}')
        raise

    return field


def get_dotty_keys(dotty_dict,
                   root=False,
                   level=None,
                   parent_key=None):

    dotty_copy = dotty_dict.copy()

    if root:

        return dotty_copy.keys()

    elif level:

        if not (
                isinstance(level, int) and
                level >= 0):

            logger.error('level must be zero or positive integer')
            raise ValueError

        # default start at root key
        level_counter = 0

        pass

    elif parent_key:

        if not isinstance(parent_key, str):

            logger.error('parent key must be str')

        parent_dict = dotty_copy.pop(parent_key)

        if parent_dict:

            try:
                keys = parent_dict.keys()
            except KeyError as err:

                logger.exception(f'{err} : keys not found under {parent_key}')
                return []

            else:

                return [str(k) for k in keys]

        else:

            logger.error('{parent_key} key not exist')
            raise KeyError


def get_dotty_leafs(dotty_dict):

    leaf_keys = list()

    def get_leaf(dotty_dict, parent_key):

        try:

            if dotty_dict.keys():

                for key in dotty_dict.keys():

                    key_w_parent = '{parent}.{key}'.format(parent=parent_key,
                                                           key=key)

                    get_leaf(dotty_dict.get(key), key_w_parent)

        except AttributeError:

            leaf_keys.append(parent_key)

        except ValueError:

            leaf_keys.append(parent_key)

    # root field is temporary to have common start in any case in all leafs
    get_leaf(dotty_dict, 'root')

    # leave out root field from all paths to leafs
    original_leaf_keys = leaf_keys
    leaf_keys = []
    for leaf in original_leaf_keys:
        match_result = search(r'(?<=root.)\\S+', leaf)
        if match_result:
            leaf_keys.append(match_result.group(0))

    return leaf_keys


def get_dotty_key_parent(key):

    if isinstance(key, str):

        logger.error('dotty key must be str type')
        raise TypeError

    # prune last field and rejoin with '.' separator
    # to recreate a dotty key
    parent_key = '.'.join(key.split('.')[:-2])

    return parent_key


# TODO: function that returns all leafs at a given
#       given level


# ATTRS

# ADDED VALIDATORS

def validator_file_path(file_ext=None):

    def validate_file_path(instance, attribute, value):

        try:

            filepath = Path(value)

        except Exception as e:

            logger.error(f'File {value} Path creation error: {e}')
            raise

        else:

            if not (

                    filepath.exists() or
                filepath.is_file()
            ):

                logger.error(f'file {value} not exists')
                raise FileExistsError

    return validate_file_path


def validator_dir_path(create_if_missing=False):

    def validate_or_create_dir(instance, attribute, value):

        if create_if_missing:

            Path(value).mkdir(parents=True, exist_ok=True)

        else:

            if not (
                Path(value).exists() or
                Path(value).is_dir()
            ):

                logger.error(f'Directory {value} not valid')
                raise TypeError()

    return validate_or_create_dir


def validator_list_ge(min_value):

    def validator_list_values(instance, attribute, value):

        if not (
                isinstance(value, list) and
            all([isinstance(val, int)
                 for val in value])
        ):

            logger.error('Required list of int type for argument '
                         f'{attribute}')
            raise TypeError

        if any([
            val < min_value
            for val in value
        ]):

            fails = [
                val for val in value
                if val < min_value
            ]

            logger.error(f'Values in {attribute}: {fails} '
                         f'are not greater than {min_value}')
            raise ValueError

# ATTRIBUTES


def get_attrs_names(instance_object, **kwargs):

    if hasattr(instance_object, '__attrs_attrs__'):

        return [attr.name
                for attr in instance_object.__attrs_attrs__]

    else:

        logger.error('attribute "__attrs__attrs__" not found in '
                     f'object {instance_object}')
        raise KeyError

# GENERIC UTILITIES


def list_remove_duplicates(list_in):

    return list(dict.fromkeys(list_in))

# HISTDATA data provider utilities


# Analyze the Histdata Forex download base page
# https://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes
# and get a list of all avilable tickers in the form as the example "EURUSD"
def get_histdata_tickers() -> List[str]:
    """
    Get all available tickers from HistData.com.

    Returns
    -------
    List[str]
        List of all available tickers (e.g., ['EURUSD', 'GBPUSD', ...]).
    """
    url = "https://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes"

    try:
        requests.head(url, timeout=5)
    except requests.RequestException:
        logger.error(f'Failed to connect to {url}')
        return []

    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        tickers = []
        # Tickers are typically in links that lead to the pair's specific page
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Pattern check based on the observed links
            if "/ascii/1-minute-bar-quotes/" in href:
                parts = href.split('/')
                ticker = parts[-1]
                # Validate it's a valid ticker (usually 6 chars like EURUSD)
                if ticker and len(ticker) >= 6:
                    tickers.append(ticker.upper())

        return sorted(list(set(tickers)))

    except Exception as e:
        logger.error(f"Failed to retrieve tickers from HistData: {e}")
        return []

# REAL TIME PROVIDERS UTILITIES


def polygon_agg_to_dict(agg):

    if not isinstance(agg, polygon_agg):

        logger.error('argument invalid type, required '
                     'polygon.rest.models.aggs.Agg')

    return {
        BASE_DATA_COLUMN_NAME.TIMESTAMP: agg.timestamp,
        BASE_DATA_COLUMN_NAME.OPEN: agg.open,
        BASE_DATA_COLUMN_NAME.HIGH: agg.high,
        BASE_DATA_COLUMN_NAME.LOW: agg.low,
        BASE_DATA_COLUMN_NAME.CLOSE: agg.close,
        BASE_DATA_COLUMN_NAME.VOL: agg.volume,
        BASE_DATA_COLUMN_NAME.TRANSACTIONS: agg.transactions,
        BASE_DATA_COLUMN_NAME.VWAP: agg.vwap,
        BASE_DATA_COLUMN_NAME.OTC: agg.otc
    }


# MARKETS FUNCTIONS

US_HOLIDAYS = country_holidays('US', years=YEARS)
# Convert US_HOLIDAYS to a list of date objects for efficient filtering
US_holiday_dates = [holiday_date for holiday_date in US_HOLIDAYS.keys()]


def business_days_data(dataframe: polars_lazyframe | polars_dataframe) -> polars_dataframe | polars_lazyframe:

    '''
    Remove non-business days data from the input dataframe.
    Filter out weekends data: saturday and sunday.
    Use holidays to get list of country holidays.
    Consider dataframe always have a column named 'timestamp' of type datetime.
    '''

    # Filter out weekends (Saturday=6, Sunday=7) and holidays
    # Use Polars' dt.weekday() where Monday=1, Tuesday=2, ..., Saturday=6, Sunday=7
    return dataframe.filter(
        (col('timestamp').dt.weekday() < 6) &  # Keep Monday(1) through Friday(5)
        (~col('timestamp').dt.date().is_in(US_holiday_dates))  # Exclude holidays
    )


def update_ticker_years_dict(
    ticker_years_dict: Dict[str, Dict[str, List[int]]],
    ticker: str,
    timeframe: str,
    years_to_add: List[int]
) -> bool:
    """
    Update a ticker years dictionary with new years for a specific ticker and timeframe.

    This function modifies the dictionary in place and returns whether any changes were made.

    Parameters
    ----------
    ticker_years_dict : Dict[str, Dict[str, List[int]]]
        Dictionary containing ticker years data, structured as:
        {ticker: {timeframe: [year1, year2, ...]}}
    ticker : str
        The ticker symbol to update
    timeframe : str
        The timeframe for the ticker data
    years_to_add : List[int]
        List of years to add to the years list

    Returns
    -------
    bool
        True if any changes were made, False otherwise
    """
    # Initialize ticker if not present
    if ticker not in ticker_years_dict:
        ticker_years_dict[ticker] = {}

    # Initialize timeframe if not present
    if timeframe not in ticker_years_dict[ticker]:
        ticker_years_dict[ticker][timeframe] = []

    # Track if any changes were made
    changes_made = False

    # Add years if not already present
    for y in years_to_add:
        if y not in ticker_years_dict[ticker][timeframe]:
            ticker_years_dict[ticker][timeframe].append(y)
            changes_made = True

    # Keep years sorted if changes were made
    if changes_made:
        ticker_years_dict[ticker][timeframe].sort()

    return ticker_years_dict, changes_made
