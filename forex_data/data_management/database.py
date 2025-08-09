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
        search
    )
from numpy import array

from sqlalchemy import (
        create_engine,
        text
    )

from sqlalchemy.engine.cursor import (
        CursorResult as sqlalchemy_CursorResult
    )

from adbc_driver_sqlite import(
        dbapi as sqlite_dbapi
    )

from polars import (
        DataFrame as polars_dataframe,
        LazyFrame as polars_lazyframe,
        read_database
    )

from collections import OrderedDict

from .common import (
        TICKER_PATTERN,
        DATE_FORMAT_SQL,
        TICK_TIMEFRAME,
        BASE_DATA_COLUMN_NAME,
        DATA_KEY_TEMPLATE_PATTERN,
        DATA_KEY,
        DATA_TYPE,
        SUPPORTED_DATA_FILES,
        FILENAME_STR,
        POLARS_DTYPE_DICT,
        validator_dir_path,
        DEFAULT_PATHS,
        SUPPORTED_DATA_ENGINES,
        is_empty_dataframe,
        get_db_key_elements,
        get_attrs_names,
        check_timeframe_str,
        read_csv,
        write_csv,
        read_parquet,
        write_parquet,
        concat_data,
        list_remove_duplicates
    )

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
    
    
    def connect(self):
        
        pass
    
    
    def check_connection(self):
        
        pass
    
     
    def write_data(self):
        
        pass
    
    
    def read_data(self):
        
        pass
    
    
    def exec_sql(self):
        
        pass
    
'''
DUCKDB CONNECTOR:
    
    TABLE TEMPLATE: 
        <trading field (e.g. Forex, Stocks)>.ticker.timeframe 
        
'''
@define(kw_only=True, slots=True)
class DuckDBConnector(DatabaseConnector):
    
    duckdb_filepath  : str = field(default='',
                                  validator=validators.instance_of(str))
    
    
    def __init__(self, **kwargs):
        
        #super().__init__(**kwargs)
        
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)
        
        if 'config' in kwargs.keys():
        
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
                            
            self.__attrs_post_init__(**kwargs)
            
        else:
            
            # no config file is defined
            # call generated init 
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        
    def __attrs_post_init__(self, **kwargs):
        
        # create duck file path if not exists
        if (
                not Path(self.duckdb_filepath).exists()
                or
                not Path(self.duckdb_filepath).is_file()
            ):
            
            Path(self.duckdb_filepath).parent.mkdir(parents=True,
                                                    exist_ok=True)
        
        # set autovacuum
        conn = self.connect()
        
        # check auto vacuum property 
        cur = conn.cursor()
        cur.execute('PRAGMA main.auto_vacuum')
        cur.execute('PRAGMA main.auto_vacuum = 2')
        cur.close()
        conn.close()
    
    def connect(self):
        
        try:
            
            con = sqlite_dbapi.connect(uri=self.duckdb_filepath)
            
        except Exception as e:
            
            logger.error(f'ADBC-SQLITE: connection error: {e}')
            raise

        else:
            
            return con
        
    def check_connection(self):
        
        out_check_connection = False
        
        conn = self.connect()
        
        out_check_connection = False
        
        try:
            
            info = read_database(text('SHOW DATABASES'), conn)
            
        except Exception as e:
            
            logger.error(f'Error during connection to {self._db_uri}')
    
    
        else:
            
            logger.trace(f'{info}')
            
            out_check_connection = not is_empty_dataframe(info)
            
            
        return out_check_connection
        
    
    def _to_duckdb_column_types(self, columns_dict):
        
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
            o_dict = o_dict.move_to_end(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                        last=False)
            
            duckdb_columns_dict = dict(o_dict)
        
        return duckdb_columns_dict
    
    
    def _list_tables(self):
        
        tables_list = {}
    
        conn = self.connect()
        
        try:
                
            tables = read_database(query='SELECT * FROM sqlite_master',
                                   connection=conn)
            
        except Exception as e:
            
            logger.error(f'Error list tables for {self.duckdb_filepath}: {e}')
            
            
        else:
    
            tables_list = list(tables['tbl_name'])
               
        conn.close()
               
        return tables_list
    
    
    def _db_key(self, 
                market      : str,
                ticker      : str,
                timeframe   : str
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

        # skip checks cuse they 
        # are not meant for polars syntax of timeframes/frequencies
        #tf = check_timeframe_str(timeframe)

        return '_'.join([market.lower(), 
                         ticker.lower(),
                         timeframe.lower()])
    
    
    def _get_items_from_db_key(self,
                               key
        ) -> tuple:
        
        return tuple(key.split('_'))
    
    
    def get_tickers_list(self):
        
        tickers_list = []
        
        for table_name in self._list_tables():
            
            items = self._get_items_from_db_key(table_name)
                
            tickers_list.append(items[DATA_KEY.TICKER_INDEX])
        
        return list_remove_duplicates(tickers_list)
    
    
    def get_ticker_keys(self, ticker, timeframe=None):
        
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
    
    
    def get_ticker_years_list(self, ticker, timeframe=TICK_TIMEFRAME):
        
        ticker_years_list = []
        table   = ''
        key_found = False
        
        for table_name in self._list_tables():
                    
            items = self._get_items_from_db_key(table_name)
            
            if (
                    items[DATA_KEY.TICKER_INDEX] == ticker.lower()
                    and
                    items[DATA_KEY.TF_INDEX] == timeframe.lower()
                ):
                
                table = table_name
                key_found = True
                
                break
                        
                        
        if key_found:
            
            conn = self.connect()
            
            try:
                
                query = f'''SELECT DISTINCT STRFTIME('%Y', CAST({BASE_DATA_COLUMN_NAME.TIMESTAMP} AS TEXT))
                        AS YEAR
                        FROM {table}'''
                read = read_database(query, conn)
                
            except Exception as e:
                
                logger.error(f'Error querying table {table}: {e}')
                raise
                
            else:
                
                ticker_years_list = [int(row[0]) for row in read.iter_rows()]
                
            
            conn.commit()
            conn.close()
                            
        return ticker_years_list
    
    
    def write_data(self, target_table, dataframe, clean=False):
        
        
        # have column structure for tdengine
        duckdb_cols_dict = {}
        if isinstance(dataframe, polars_lazyframe):
            
            duckdb_cols_dict = self._to_duckdb_column_types(dict(dataframe.collect_schema()))
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
            
            #stable_describe = read_database(f'DESCRIBE {target_table}')
            
            # get existing stable column structure
            # if they match, append data
            # if no match, replace stable
            
            if_table_exists = 'append'
            
        target_length = len(dataframe)
        
        table_write = dataframe.write_database(
                           table_name = target_table,
                           connection = conn,
                           if_table_exists = if_table_exists,
                           engine = 'adbc'
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
                  market,
                  ticker,
                  timeframe,
                  start,
                  end
        ):
        
        dataframe  = polars_lazyframe()
        
        table = self._db_key(market, ticker, timeframe)
        # check if database is available
        if table in self._list_tables():
    
            # open a connection
            conn = self.connect()
            
            try:
                
                start_str = start.isoformat()
                end_str   = end.isoformat()
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
                
                logger.error(f'executing query {query} failed: {e}')
                
            else:
                
                if timeframe == TICK_TIMEFRAME:
                    
                    # final cast to standard dtypes
                    dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
                    
                else:
                    
                    # final cast to standard dtypes
                    dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TF_DTYPE)
                
        # close
        conn.commit()
        conn.close()
                
        return dataframe
    
    
    def read_data_by_years(self,
                            market,
                            ticker,
                            timeframe,
                            years,
                            mode='between'
        ):
  
        pass
    
    
'''
LOCAL DATA FILES MANAGER

'''

@define(kw_only=True, slots=True)
class LocalDBConnector(DatabaseConnector):
    
    local_data_folder   : str = field(default='',
                                     validator=validators.instance_of(str))
    data_type           : str = field(default='parquet',
                                      validator=validators.in_(SUPPORTED_DATA_FILES))
    engine              : str = field(default='polars_lazy',
                                      validator=validators.in_(SUPPORTED_DATA_ENGINES))
    
    _local_path = field(
                        default = Path('.'), 
                        validator=validator_dir_path(create_if_missing=False))
    
    def __init__(self, **kwargs):
        
        #super().__init__(**kwargs)
        
        _class_attributes_name = get_attrs_names(self, **kwargs)
        _not_assigned_attrs_index_mask = [True] * len(_class_attributes_name)
        
        if 'config' in kwargs.keys():
        
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
                            
            self.__attrs_post_init__(**kwargs)
            
        else:
            
            # no config file is defined
            # call generated init 
            self.__attrs_init__(**kwargs)
            
        validate(self)
        
        
    def __attrs_post_init__(self, **kwargs):
        
        # create duck file path if not exists
        if (
                not Path(self.local_data_folder).exists()
                or
                not Path(self.local_data_folder).is_dir()
            ):
            
            Path(self.local_data_folder).mkdir(parents=True,
                                               exist_ok=True)
            
        self._local_path = Path(self.local_data_folder)
            
            
    def _db_key(self, 
                market      : str,
                ticker      : str,
                timeframe   : str
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

         # skip checks cuse they 
         # are not meant for polars syntax of timeframes/frequencies
         #tf = check_timeframe_str(timeframe)

         return '_'.join([market.lower(), 
                          ticker.lower(),
                          timeframe.lower()])
     
     
    def _get_items_from_db_key(self,
                               key
        ) -> tuple:
        
        return tuple(key.split('_'))
    
    
    def _get_file_details(self, filename):

        if not (
                isinstance(filename, str)
        ):
            
            logger.error('filename {filename} invalid type: required str')

        file_items = self._get_items_from_db_key(filename)

        # return each file details
        return file_items


    def _get_filename(self, market, ticker, tf, file_ext):

        # based on standard filename template
        return FILENAME_STR.format( market   = market,
                                    ticker   = ticker,
                                    tf       = tf,
                                    file_ext = self.data_type)
     
        
    def _list_local_data(self):

        local_files = []
        local_files_name = []
        
        # list for all data filetypes supported
        local_files = [file for file in list(self._local_path.rglob(f'*'))
                        if  search(self.data_type + '$', file.suffix)]
        
        local_files_name = [file.name for file in local_files]

        # check compliance of files to convention (see notes)
        # TODO: warning if no compliant and filter out from files found

        return local_files, local_files_name
    
    
    def _list_tables(self):
         
        local_files, tables_list = self._list_local_data()
        
        return tables_list
    
        
    def get_tickers_list(self):
        
        tickers_list = []
        
        local_files, local_files_name = self._list_local_data()
        
        for filename in local_files_name:
            
            items = self._get_file_details(filename)
            tickers_list.append(items[DATA_KEY.TICKER_INDEX])
                
                
        return list_remove_duplicates(tickers_list)
        
        
    def get_ticker_keys(self, ticker, timeframe=None):
        
        local_files, local_files_name = self._list_local_data()
        
        if timeframe:
            
            return [
                key for key in local_files_name
                if search(f'{ticker}',
                          key)
                    and
                    self._get_items_from_db_key(key)[DATA_KEY.TF_INDEX]
                    == timeframe
            ]
            
        else:
            
            return [
                key for key in local_files_name
                if search(f'{ticker}',
                          key)
            ]
        
        
    def get_ticker_years_list(self, ticker, timeframe=TICK_TIMEFRAME):
        
        ticker_years_list = []
        table   = ''
        key_found = False
        
        local_files, local_files_name = self._list_local_data()
        ticker_keys = []
        
        files = [
            key for key in local_files
            if search(f'{ticker.lower()}',
                      str(key.stem))
                and
                self._get_items_from_db_key(str(key.stem))[DATA_KEY.TF_INDEX]
                == timeframe.lower()
        ]
        
        dataframe = None
        
        if len(files) == 1:
            
            if self.data_type == DATA_TYPE.CSV_FILETYPE:
            
                dataframe = read_csv(self.engine, files[0])
                
            elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
                
                dataframe = read_parquet(self.engine, files[0])
                
            try:
                
                query = f'''SELECT DISTINCT STRFTIME({BASE_DATA_COLUMN_NAME.TIMESTAMP}, '%Y')
                            AS YEAR
                            FROM self'''
                read = dataframe.sql(query)
                
            except Exception as e:
                
                logger.error(f'Error querying table {table}: {e}')
                raise
                
            else:
                
                ticker_years_list = [int(row[0]) for row in read.collect().iter_rows()]
            
            
        return ticker_years_list
    
    
    def write_data(self, target_table, dataframe, clean=False):
        
        items = self._get_items_from_db_key(target_table)
        
        filename = self._get_filename(items[DATA_KEY.MARKET],
                                      items[DATA_KEY.TICKER_INDEX],
                                      items[DATA_KEY.TF_INDEX],
                                      self.data_type)
    
        filepath = (self._local_path 
                        / items[DATA_KEY.MARKET] 
                        / items[DATA_KEY.TICKER_INDEX]
                        / filename)
                        
        if (
                not filepath.exists()
                or
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
            dataframe = dataframe.unique(subset=[BASE_DATA_COLUMN_NAME.TIMESTAMP],
                                         keep='first').sort(BASE_DATA_COLUMN_NAME.TIMESTAMP)
            
            
        if self.data_type == DATA_TYPE.CSV_FILETYPE:
            
            write_csv(dataframe, filepath)
            
        elif self.data_type == DATA_TYPE.PARQUET_FILETYPE:
             
             write_parquet(dataframe, filepath)
              
                
    def read_data(self,
                  market,
                  ticker,
                  timeframe,
                  start,
                  end
        ):
        
        dataframe  = polars_lazyframe()
        
        filename = self._get_filename(market,
                                      ticker,
                                      timeframe,
                                      self.data_type)
    
        filepath = (self._local_path 
                        / market
                        / ticker
                        / filename)
       
        if self.engine == 'polars':
        
            dataframe = polars_dataframe()
            
        elif self.data_type == 'polars_lazy':
            
            dataframe = polars_lazyframe()
            
            
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
                end_str   = end.isoformat()
                 
                query = f'''SELECT * FROM self 
                             WHERE 
                             {BASE_DATA_COLUMN_NAME.TIMESTAMP} >= '{start_str}' 
                             AND
                             {BASE_DATA_COLUMN_NAME.TIMESTAMP} <= '{end_str}'
                             ORDER BY {BASE_DATA_COLUMN_NAME.TIMESTAMP}'''
                dataframe = dataframe.sql(query)
                            
            except Exception as e:
                
                logger.error(f'executing query {query} failed: {e}')
                
            else:

                if timeframe == TICK_TIMEFRAME:
                    
                    # final cast to standard dtypes
                    dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TICK_DTYPE)
                    
                else:
                    
                    # final cast to standard dtypes
                    dataframe = dataframe.cast(POLARS_DTYPE_DICT.TIME_TF_DTYPE)
                    
                    
        return dataframe
    
