## -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:23:19 2022

@author: fiora
"""

# __all__ = [
#             'BASE_DATA_COLUMN_NAME'
#     ]


from platform import platform

from re import ( 
                fullmatch,
                findall,
                search,
                IGNORECASE
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
                Schema as pyarrow_schema,
                Table,
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
                read_parquet as polars_read_parquet,
                from_arrow
    )

from polars.dataframe import ( 
                DataFrame as polars_dataframe
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

from pathlib import Path

from attrs import (
                field,
                validators
    )

# common functions, constants and templates
TEMP_FOLDER                     = "Temp"
TEMP_CSV_FILE                   = "Temp.csv"

HISTDATA_URL_TICKDATA_TEMPLATE  = 'http://www.histdata.com/download-free-forex-historical-data/?/' \
                                  'ascii/tick-data-quotes/{ticker}/{year}/{month_num}'

HISTDATA_URL_ONEMINDATA_TEMPLATE= 'http://www.histdata.com/download-free-forex-data/?/' \
                                  'ascii/1-minute-bar-quotes/{pair}/{year}/{month_num}'

HISTDATA_BASE_DOWNLOAD_URL      = "http://www.histdata.com/get.php"
HISTDATA_BASE_DOWNLOAD_METHOD   = 'POST'

MONTHS                          = ['January', 'February', 'March', 'April', 'May', 'June',
                                   'July', 'August', 'September', 'October', 'November', 'December']
YEARS                           = list(range(2000, 2022, 1))

DATE_NO_HOUR_FORMAT             = '%Y-%m-%d'
DATE_FORMAT_ISO8601             = 'ISO8601'
DATE_FORMAT_HISTDATA_CSV        = '%Y%m%d %H%M%S%f'

FILENAME_STR                    = '{ticker}_Y{year}_{tf}.{file_ext}'
DEFAULT_TIMEZONE                = 'utc'
TICK_TIMEFRAME                  = 'TICK'

## ticker PAIR of forex market
PAIR_GENERIC_FORMAT             = '{TO}/{FROM}'
SINGLE_CURRENCY_PATTERN_STR     = '[A-Z]{3}'

## PAIR ALPHAVANTAGE
PAIR_ALPHAVANTAGE_FORMAT        = '{TO}/{FROM}'
PAIR_ALPHAVANTAGE_PATTERN       = '^' + SINGLE_CURRENCY_PATTERN_STR + '/' \
                                      + SINGLE_CURRENCY_PATTERN_STR + '$'
ALPHA_VANTAGE_KEY_ENV           = 'ALPHA_VANTAGE_KEY'
AV_LIST_URL                     = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={api_key}'

## PAIR POLYGON IO
PAIR_POLYGON_FORMAT             = 'C:{TO}{FROM}'
PAIR_POLYGON_PATTERN            = '^C:' + SINGLE_CURRENCY_PATTERN_STR + \
                                        SINGLE_CURRENCY_PATTERN_STR + '$'
POLY_IO_KEY_ENV                 = 'POLYGON_IO_KEY'

## TIME PATTERN
TIME_WINDOW_PATTERN_STR         = '^[-+]?[0-9]+[A-Za-z]{1,}$'
TIME_WINDOW_COMPONENTS_PATTERN_STR         = '^[-+]?[0-9]+|[A-Za-z]{1,}$'
TIME_WINDOW_UNIT_PATTERN_STR    = '[A-Za-z]{1,}$'
GET_YEAR_FROM_TICK_KEY_PATTERN_STR = '^[A-Za-z].Y[0-9].TICK'
YEAR_FIELD_PATTERN_STR          = '^Y([0-9]{4,})$'              


### auxiliary CONSTANT DEFINITIONS
     
# dotty key template: <PAIR>.Y<year>.<timeframe>.<data-type>
class DATA_KEY:
    
    PAIR_INDEX              = 0
    YEAR_INDEX              = 1 
    TF_INDEX                = 2 
    DATATYPE_INDEX          = 3 

# filename template : <PAIR>_Y<year>_<timeframe>.<filetype>
class FILENAME_TEMPLATE:
    
    PAIR_INDEX              = 0
    YEAR_INDEX              = 1
    YEAR_NUMERICAL_CHAR     = 1
    TF_INDEX                = 2
    FILETYPE_INDEX          = 3

    
class DEFAULT_PATHS:
    
    HIST_DATA_PATH     = str(Path.home() / '.database'/ 'Historical')
    REALTIME_DATA_PATH = str(Path.home() / '.database'/ 'Realtime')
    
class DATA_FILE_TYPE:
    
    CSV_FILETYPE            = 'csv'
    PARQUET_FILETYPE        = 'parquet'
    
class DATA_FILE_COLUMN_INDEX:
    
    TIMESTAMP               = 0
    
SUPPORTED_DATA_FILES = [
                        DATA_FILE_TYPE.CSV_FILETYPE,
                        DATA_FILE_TYPE.PARQUET_FILETYPE
                    ]

# supported dataframe engines
# pyarrow is inserted but reframe operation all in pyarrow 
# is not yet available, now it is masked 
# to a refame call with polars
# reframe_data() on pyarrow Table
SUPPORTED_DATA_ENGINES = [
                            'pandas',
                            'pyarrow',
                            'polars'
    ]

### SINGLE BASE DATA COMPOSIION TEMPLATE: ['open','close','high','low']
###                                       with datetime/timestamp as index     
# column names for dataframes TICK and timeframe filtered
# OHLC and related column names
class DATA_COLUMN_NAMES:
    
    TICK_DATA_NO_PVALUE     = ['timestamp','ask','bid','vol']
    TICK_DATA               = ['timestamp','ask','bid','vol','p']
    TF_DATA                 = ['timestamp','open','high','low', 'close']
    TICK_DATA_TIME_INDEX    = ['ask','bid','vol','p']
    TF_DATA_TIME_INDEX      = ['open','high','low', 'close']             ## SELECTED AS SINGLE BASE DATA COMPOSION TEMPLATE          
    POLYGON_IO_AGGS         = ['open','high','low', 'close', 'volume', 'vwap', \
                               'timestamp', 'transactions' ]
    
BASE_DATA = DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX ## SELECTED AS SINGLE BASE DATA COMPOSION TEMPLATE  
BASE_DATA_WITH_TIME = DATA_COLUMN_NAMES.TF_DATA

class REALTIME_DATA_PROVIDER:
    
    ALPHA_VANTAGE           = 'ALPHA_VANTAGE'
    POLYGON_IO              = 'POLYGON-IO'

class DB_MODE:
    
    FULL_MODE           = 'FULL_MODE'
    HISTORICAL_MODE     = 'HISTORICAL_MODE'
    REALTIME_MODE       = 'REALTIME_MODE'
    
class ASSET_TYPE:
    
    STOCK               = 'STOCK'
    ETF                 = 'ETF'
    FOREX               = 'FOREX'
    
class BASE_DATA_COLUMN_NAME:
    
    TIMESTAMP = 'timestamp'    
    OPEN      = 'open'
    HIGH      = 'high'
    LOW       = 'low'
    CLOSE     = 'close'
    ASK       = 'ask'
    BID       = 'bid'
    VOL       = 'vol'
    P_VALUE   = 'p'

class CANONICAL_INDEX:

    AV_LATEST_DATA_INDEX    = 0
    AV_DF_DATA_INDEX        = 0
    AV_DICT_INFO_INDEX      = 1    


    
### auxiliary functions

# parse argument to get datetime object with date format as input
def infer_date_from_format_dt(s, date_format='ISO8601', unit=None, utc=False):
    
    if unit:
        
        return to_datetime(s, 
                           unit     = unit,
                           utc      = utc)
    
    else:
        
        return to_datetime(s, 
                           format   = date_format,
                           utc      = utc)


# parse timeframe as string and validate if it is valid
# following pandas DateOffset freqstr rules and 'TICK' (=lowest timeframe available)
# link to official pandas doc
# https://pandas.pydata.org/docs/user_guide/timeseries.html#dateoffset-objects
def check_timeframe_str(tf):
    
    if tf == 'TICK':
        
        return tf
    
    else:
    
        try:
            to_offset(tf) 
        except ValueError:
            raise ValueError("Invalid timeframe: %s" % (tf))
        else: 
            return tf


## PAIR symbol functions        
def get_pair_symbols(ticker):
        
    components  = findall(SINGLE_CURRENCY_PATTERN_STR, ticker)
    
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

def check_time_offset_str(timeoffset_str):

    return isinstance(to_offset(timeoffset_str), DateOffset)


def timewindow_str_to_timedelta(time_window_str):
    
    
    if fullmatch(TIME_WINDOW_PATTERN_STR, time_window_str):
        
        return Timedelta(time_window_str)
    
    else:
        
        raise ValueError('time window pattern not match: "<integer_multiplier><unit>" str')
        
        
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
        
        #TODO: to log
        raise ValueError(f'date {any_date} conversion failed, '
                         f'faile conversion to {date_format} '
                         'date format')
    
# =============================================================================
#   TODO: is it necessary utc timezone when source is naive? 
#   if not any_date.tzinfo:
#         
#         any_date = any_date.tz_localize('utc')
# =============================================================================
    
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
            
            end_date = Timestamp.utcnow()
            start_date = end_date - timewindow_str_to_timedelta(interval_timespan)
            
        
        if bdays:
            
            components  = findall(TIME_WINDOW_COMPONENTS_PATTERN_STR,
                                  interval_timespan)    
            
            # fixed days redundancy check available only with 'd' type requested timespan
            if components[1] == 'd':
                
                days_list = list(
                                    rrule(freq=DAILY, 
                                          dtstart=start_date,
                                          until  =end_date,
                                          byweekday=(MO,TU,WE,TH,FR))
                                )
                
                while len(days_list) < int(components[0]):
                    
                    start_date = start_date - Timedelta(days=1)
                    
                    days_list = list(
                                        rrule(freq=DAILY, 
                                        dtstart=start_date,
                                        until  =end_date,
                                        byweekday=(MO,TU,WE,TH,FR))
                                    )
                
    assert isinstance(start_date, Timestamp) \
           and isinstance(end_date, Timestamp), \
           'start or end is not a valid Timestamp instance'
    
    if normalize:
        
        if not isnull(start_date):
            start_date = Timestamp.normalize(start_date)
            
        if not isnull(end_date):
            end_date   = Timestamp.normalize(end_date)
            
    start_date = any_date_to_datetime64(start_date)
    end_date   = any_date_to_datetime64(end_date)
            
    # generate DateTimeIndex if freq is set
    # otherwise return just start and end of interval
    if freq:
        
        bdate_dtindex = bdate_range(start = start_date,
                                       end   = end_date,
                                       freq  = freq,
                                       tz    = 'UTC',
                                       normalize = normalize,
                                       name  = 'timestamp')
        
        return start_date, end_date, bdate_dtindex
        
    else:
        
        return start_date, end_date
    
    
### BASE OPERATIONS WITH DATAFRAME 
### depending on dataframe engine support
### for supported engines see var SUPPORTED_DATA_ENGINES

# DATA ENGINES TYPES DICTIONARY   
class DTYPE_DICT:
    
    TICK_DTYPE = {'ask': 'float32', 
                  'bid': 'float32',
                  'vol': 'float32',
                  'p'  : 'float32'}
    TF_DTYPE   = {'open': 'float32',
                  'high': 'float32', 
                  'low': 'float32',
                  'close': 'float32'}
    TIME_TICK_DTYPE = {'timestamp' : 'datetime64[ms]',
                       'ask': 'float32', 
                       'bid': 'float32',
                       'vol': 'float32',
                       'p'  : 'float32'}
    TIME_TF_DTYPE   = {'timestamp' : 'datetime64[ms]',
                       'open': 'float32', 
                       'high': 'float32', 
                       'low': 'float32',
                       'close': 'float32'}
    
class PYARROW_DTYPE_DICT:
    
    TICK_DTYPE = {'ask': pyarrow_float32(), 
                  'bid': pyarrow_float32(),
                  'vol': pyarrow_float32(),
                  'p'  : pyarrow_float32()}
    TF_DTYPE   = {'open' : pyarrow_float32(),
                  'high' : pyarrow_float32(), 
                  'low'  : pyarrow_float32(),
                  'close': pyarrow_float32()}
    TIME_TICK_DTYPE = {'timestamp'  : pyarrow_timestamp('ms'),
                       'ask'        : pyarrow_float32(), 
                       'bid'        : pyarrow_float32(),
                       'vol'        : pyarrow_float32(),
                       'p'          : pyarrow_float32()}
    TIME_TF_DTYPE   = {'timestamp'  : pyarrow_timestamp('ms'),
                       'open'       : pyarrow_float32(), 
                       'high'       : pyarrow_float32(), 
                       'low'        : pyarrow_float32(),
                       'close'      : pyarrow_float32()}
    
class POLARS_DTYPE_DICT:
    
    TICK_DTYPE = {'ask': polars_float32, 
                  'bid': polars_float32,
                  'vol': polars_float32,
                  'p'  : polars_float32}
    TF_DTYPE   = {'open' : polars_float32,
                  'high' : polars_float32, 
                  'low'  : polars_float32,
                  'close': polars_float32}
    TIME_TICK_DTYPE = {'timestamp'  : polars_datetime('ms'),
                       'ask'        : polars_float32, 
                       'bid'        : polars_float32,
                       'vol'        : polars_float32,
                       'p'          : polars_float32}
    TIME_TF_DTYPE   = {'timestamp'  : polars_datetime('ms'),
                       'open'       : polars_float32, 
                       'high'       : polars_float32, 
                       'low'        : polars_float32,
                       'close'      : polars_float32}

# DATA ENGINES FUNCTIONS

def empty_dataframe(dataframe_type):
    
    # TODO: based on input return an aempty instance of dataframe
    pass


def is_empty_dataframe(dataframe):
    
    if isinstance(dataframe, pandas_dataframe):
        
        return dataframe.empty
    
    elif isinstance(dataframe, Table):
        
        return (not bool(dataframe)) 
    
    elif isinstance(dataframe, polars_dataframe):
    
        return dataframe.is_empty()
    
    else:
        
        raise ValueError('function is_empty_dataframe not available'
                         ' for instance of type'
                         f' {type(dataframe)}') 
    
    
def dtype_dict_to_pyarrow_schema(dtype_dict):
    
    return pyarrow_schema(dtype_dict.items())

    
def astype(dataframe, dtype_dict):
    
    if isinstance(dataframe, pandas_dataframe):
        
        return dataframe.astype(dtype_dict)
    
    elif isinstance(dataframe, Table):
        
        return dataframe.cast(dtype_dict_to_pyarrow_schema(dtype_dict)) 
    
    elif isinstance(dataframe, polars_dataframe):
    
        return dataframe.cast(dtype_dict)
    
    else:
        
        raise ValueError('function astype not available'
                         ' for instance of type'
                         f' {type(dataframe)}')


def read_parquet(engine, filepath):
    
    if engine == 'pandas':
        
        return pandas_read_parquet(filepath)
    
    elif engine == 'pyarrow':
        
        return read_table(filepath)
    
    elif engine == 'polars':
    
        return polars_read_parquet(filepath)
    
    else:
        
        raise ValueError('function read_parquet not available'
                         f' for engine {engine}')
                         
    
def write_parquet(dataframe, filepath):
    
    if isinstance(dataframe, pandas_dataframe):
        
        try:
            
            dataframe.to_parquet(filepath, index=True)
        
        except Exception as e:
            
            raise Exception(f'pandas write parquet failed: {e}')
    
    elif isinstance(dataframe, Table):
        
        try:
            
            write_table(dataframe, filepath)
            
        except Exception as e:
            
            raise Exception(f'pyarrow write parquet failed: {e}')
            
    elif isinstance(dataframe, polars_dataframe):
    
        try:
            
            dataframe.write_parquet(filepath)
        
        except Exception as e:
            
            raise Exception(f'polars write parquet failed: {e}')
    
    else:
        
        raise ValueError('function write_parquet not available'
                         ' for instance of type'
                         f' {type(dataframe)}')


def read_csv(data_engine, file, **kwargs):
    
    if data_engine == 'pandas':
        
        return pandas_read_csv(file, **kwargs)
    
    elif data_engine == 'pyarrow':
        
        return arrow_csv.read_csv(file, **kwargs)
    
    elif data_engine == 'polars':
    
        return polars_read_csv(file, **kwargs)
    
    else:
        
        raise ValueError('function read_csv not available'
                         f' for engine {data_engine}')
        

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
            
            raise IOError(f'Error writing csv file {file}'
                          f' with data type {type(dataframe)}: {e}')
    
    elif isinstance(dataframe, Table):
        
        try:
            
            arrow_csv.write_csv(dataframe, file, **kwargs)
            
        except Exception as e:
            
            raise IOError(f'Error writing csv file {file}'
                          f' with data type {type(dataframe)}: {e}')
    
    elif isinstance(dataframe, polars_dataframe):
        
        try:
            
           dataframe.write_csv(file, **kwargs)
            
        except Exception as e:
            
            raise IOError(f'Error writing csv file {file}'
                          f' with data type {type(dataframe)}: {e}')
    
    else:
        
        raise ValueError('function write_csv not available'
                         ' for instance of type'
                         f' {type(dataframe)}')


def concat_data(data_list = field(validator=
                                  validators.instance_of(list))):
    
    if not isinstance(data_list, list):
        raise TypeError('required input as list')
        
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
    
    else:
        
        raise ValueError('function concat not available'
                         ' for instance of type'
                         f' {type(data_list[0])}')
    

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
    
    else:
        
        raise ValueError('function to_pandas() not available'
                         ' for instance of type'
                         f' {type(dataframe)}')
    
    
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
    
    
    # assert timeframe input value
    tf = check_timeframe_str(tf)

    assert tf != TICK_TIMEFRAME, \
        f'reframe not possible wih target {TICK_TIMEFRAME}'
    
    if isinstance(dataframe, pandas_dataframe):
        
        if dataframe.empty:
            
            return dataframe 
        
        if not is_datetime64_any_dtype(dataframe.index):
            
            if BASE_DATA_COLUMN_NAME.TIMESTAMP in dataframe.columns:
                
                if not is_datetime64_any_dtype(dataframe[BASE_DATA_COLUMN_NAME.TIMESTAMP]):
                    
                    try:
                        
                        dataframe[BASE_DATA_COLUMN_NAME.TIMESTAMP] = \
                            any_date_to_datetime64(dataframe[BASE_DATA_COLUMN_NAME.TIMESTAMP])
                            
                    except Exception as e:
                        
                        raise TypeError('Pandas engine: '
                                        'Failed conversion of timestamp columns '
                                        'to DatetimeIndex')
                        
            else:
                
                raise ValueError(
                    'Pandas engine: required column with '
                    f'name {BASE_DATA_COLUMN_NAME.TIMESTAMP}'
                )
        
        ## use pandas functions to reframe data on pandas Dataframe
        
        df = dataframe.set_index(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                 inplace=False,
                                 drop=True
        )
        
        # resample based on p value
        if all([col in DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX
                for col in df.columns]):
            
            # resample along 'p' column, data in ask, bid, p format
            df =  df.p.resample(tf).ohlc().interpolate(method=
'nearest')
            
        elif all([col in DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX
                  for col in df.columns]): 
            
            # resample along given data already in ohlc format
            df = df.resample(tf).interpolate(method=
                                             'nearest')
            
        else:
            
            raise ValueError(f'data columns {dataframe.columns} invalid, '
                             f'required {DATA_COLUMN_NAMES.TICK_DATA_TIME_INDEX} '
                             f'or {DATA_COLUMN_NAMES.TF_DATA_TIME_INDEX}')
            
        return df.reset_index(drop=False)
            
    elif isinstance(dataframe, Table):
        
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
            df = from_arrow(dataframe, 
                            schema = POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
            
            
        
        elif all([col in DATA_COLUMN_NAMES.TF_DATA
                for col in dataframe.column_names]):
            
            # convert to polars dataframe
            df = from_arrow(dataframe, 
                            schema = POLARS_DTYPE_DICT.TIME_TF_DTYPE)
            
        
        # perform operation
        # convert to arrow Table and return
        return reframe_data(df, tf).to_arrow()
       
    
    elif isinstance(dataframe, polars_dataframe):
        
        if dataframe.is_empty():
            
            return dataframe
        
        tf = tf.lower()
        
        dataframe = dataframe.sort('timestamp', nulls_last=True)
        
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
            
            raise ValueError(f'data columns {dataframe.columns} invalid, '
                             f'required {DATA_COLUMN_NAMES.TICK_DATA} '
                             f'or {DATA_COLUMN_NAMES.TF_DATA}')


### UTILS FOR DOTTY DICTIONARY

def get_dotty_key_field(key, index):
    
    assert isinstance(key, str), \
            f'dotty key {key} invalid type, str required'
    
    try:
    
        field = key.split('.')[index]
        
    except IndexError:
        
        raise IndexError(f'index {index} invalid for key {key}')
        
    
    return field


def get_dotty_keys(dotty_dict,
                   root=False,
                   level=None,
                   parent_key=None):
    
    dotty_copy = dotty_dict.copy()
    
    if root:
        
        return dotty_copy.keys()
    
    elif level:
        
        assert isinstance(level, int) \
               and level >= 0, 'level must be zero or positive integer'
               
        # default start at root key
        level_counter = 0
        
        pass
        
    elif parent_key:
        
        assert isinstance(parent_key, str), 'parent key must be str'
        
        parent_dict = dotty_copy.pop(parent_key)
        
        if parent_dict:
            
            try:
                keys = parent_dict.keys()
            except KeyError as err:
                
                # TODO: error to be logged, now print
                print( '{error} : keys not found under {parent}'.format(error=str(err),
                                                                        parent=parent_key) )
                return []
                
            else:
                
               return [str(k) for k in keys]
                
        
        else:
            
            raise KeyError( '{parent} key not exist'.format(parent=parent_key) )
                
                
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
            
            leaf_keys.append( parent_key )
            
        except ValueError:
            
            leaf_keys.append( parent_key )
        
    # root field is temporary to have common start in any case in all leafs
    get_leaf(dotty_dict, 'root')
    
    # pull out root field from all paths to leafs
    leaf_keys = [ search('(?<=root.)\S+', leaf).group(0) for leaf in leaf_keys]
    
    return leaf_keys
        

def get_dotty_key_parent(key):
    
    assert isinstance(key, str), 'dotty key must be str type'
    
    # prune last field and rejoin with '.' separator
    # to recreate a dotty key
    parent_key = '.'.join( key.split('.')[:-2] )
    
    return parent_key



# TODO: function that returns all leafs at a given
#       given level
        
        
### ATTRS 

## ADDED VALIDATORS

def validator_dir_path(create_if_missing=False):
    
    def validate_or_create_dir(instance, attribute, value):
    
        if create_if_missing:
            
            Path(value).mkdir(parents=True, exist_ok=True)
            
        else:
            
            if not (
                Path(value).exists()
                and 
                Path(value).is_dir()
            ):
                
                raise ValueError('Required a valid directory path')
                
    return validate_or_create_dir
        
              
def validator_list_timeframe(instance, attribute, value):
    
    if not isinstance(value, list):
        
        raise TypeError(f'Required type list for argument {attribute}')
        
    if not all([
                check_time_offset_str(val) 
                for val in value
                ]):
        
        fails = value[
                 [
                    check_time_offset_str(val) 
                    for val in value
                ]           
        ]
        
        return ValueError('Values are not timeframe compatible: '
                          f'{fails}')
        
    
def validator_list_ge(min_value):
    
    def validator_list_values(instance, attribute, value):
        
        if not(
                isinstance(value, list)
                and 
                all([isinstance(val, int)
                     for val in value])
        ):
            
            raise TypeError('Required list of int type for argument '
                            f'{attribute}')
            
        if any([ 
                val < min_value
                for val in value
            ]):
            
            fails = value[ 
                [ 
                    val < min_value
                    for val in value
                ]
            ]
            
            raise ValueError(f'Values in {attribute}: {fails} '
                             f'are not greater than {min_value}')
    
## ATTRIBUTES

def get_attrs_names(instance_object, **kwargs):
    
    if hasattr(instance_object, '__attrs_attrs__'):
        
        return [attr.name 
                for attr in instance_object.__attrs_attrs__]
        
    else:
        
        raise KeyError('attribute "__attrs__attrs__" not found in '
                       f'object {instance_object}')
        
# GENERIC UTILITIES

def list_remove_duplicates(list_in):
    
    return list(dict.fromkeys(list_in))

    