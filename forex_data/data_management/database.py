# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 00:02:36 2025

@author: fiora
"""

'''
    Module to connect to a database instance
    
    Design constraint:
        
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

from numpy import array

from sqlalchemy import (
        create_engine,
        text
    )

from polars import (
        DataFrame as polars_dataframe,
        LazyFrame as polars_lazyframe,
        read_database
    )

from collections import OrderedDict

from .common import (
        BASE_DATA_COLUMN_NAME,
        validator_dir_path,
        DEFAULT_PATHS,
        is_empty_dataframe,
        get_db_key_elements,
        get_attrs_names
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
    
    config          : str = field(default='',
                                  validator=validators.instance_of(str))
    name            : str = field(default='',
                                  validator=validators.instance_of(str))
    target_url      : str = field(default='',
                                  validator=validators.instance_of(str))
    
    
    def connect(self):
        
        pass
    
    
    def check_connection(self):
        
        pass
    
    
    def create_bucket(self):
        
        pass
    
    
    def list_buckets(self):
        
        pass
        
    
    def write_data(self):
        
        pass
    
    
    def read_data(self):
        
        pass
    
    
    def exec_sql(self):
        
        pass
    

'''
TDENGINE CONNECTOR:
    
    database    : trading field (e.g. Forex, Stocks)
    supertable  : the single ticker
    table       : Y{year}_{timeframe} 
        
'''
@define(kw_only=True, slots=True)
class TDengineConnector(DatabaseConnector):
    
    user             : str = field(default='',
                                  validator=validators.instance_of(str))
    password         : str = field(default='',
                                  validator=validators.instance_of(str))
    protocol         : str = field(default='',
                                  validator=validators.instance_of(str))
    port             : str = field(default='',
                                  validator=validators.instance_of(str))
    
    # internal parameters
    _db_uri     = field(default='',
                        validator=validators.instance_of(str))
    
    
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
        
        # evaluate also the url:
        # "{protocol}://{self.user}:{self.password}@{self.target_url}:{self.port}/power
        self._db_uri = f"taos://{self.user}:{self.password}@{self.target_url}:{self.port}"
    
    
    def connect(self):
        
        try:
            
            engine = create_engine(self._db_uri)
            
        except Exception as e:
            
            logger.error(f'taos engine creation error: {e}')
            raise
            
        else:
        
            try:
                
                conn = engine.connect()
            
            except Exception as e:
                
                logger.error(f'taos connect error: {e}')
    
            else:
                
                return conn
    
            
    def check_connection(self):
        
        conn = self.connect()
        
        out_check_connection = False
        
        try:
            
            info = read_database(text('SHOW DATABASES'), conn)
            
            info_alive = read_database(text('SHOW db_name.ALIVE'))
            
        except Exception as e:
            
            logger.error(f'Error during connection to {self._db_uri}')
    
    
        else:
            
            logger.trace(f'{info}')
            
            out_check_connection = not is_empty_dataframe(info)
            
            
        return out_check_connection

    def to_tdengine_column_types(columns_dict):
        
        tdengine_columns_dict = {}
        
        for item in columns_dict.items():
            
            match item[0]:
                
                case BASE_DATA_COLUMN_NAME.TIMESTAMP:
                
                    tdengine_columns_dict[item[0]] = 'TIMESTAMP'
                    
                case BASE_DATA_COLUMN_NAME.ASK \
                    | BASE_DATA_COLUMN_NAME.BID \
                    | BASE_DATA_COLUMN_NAME.OPEN \
                    | BASE_DATA_COLUMN_NAME.HIGH \
                    | BASE_DATA_COLUMN_NAME.LOW \
                    | BASE_DATA_COLUMN_NAME.CLOSE \
                    | BASE_DATA_COLUMN_NAME.VOL \
                    | BASE_DATA_COLUMN_NAME.P_VALUE:
                
                    tdengine_columns_dict[item[0]] = 'FLOAT'
                    
                case BASE_DATA_COLUMN_NAME.TRANSACTIONS:
                    
                    tdengine_columns_dict[item[0]] = 'BIGINT UNSIGNED'
                    
                case BASE_DATA_COLUMN_NAME.OTC:
                     
                    tdengine_columns_dict[item[0]] = 'FLOAT'
                
        # force timestamp as first key 
        if not tdengine_columns_dict.keys()[0] == BASE_DATA_COLUMN_NAME.TIMESTAMP:
            
            o_dict = OrderedDict(tdengine_columns_dict.items())
            o_dict = o_dict.move_to_end(BASE_DATA_COLUMN_NAME.TIMESTAMP,
                                        last=False)
            
            tdengine_columns_dict = dict(o_dict)
        
        return tdengine_columns_dict
        
    
    def list_buckets(self):
        
        # SHOW TABLES
        pass
    
    
    def write_data(self, database, stable, columns_dict, dataframe):
        
        # https://docs.tdengine.com/tdengine-reference/sql-manual/manage-databases/
    
        # to be used on higher level
        #table_elements = get_db_key_elements(key)
        
        # open a connection
        conn = self.connect()
        
        
        # exec db creation
        db_create = read_database(text(f'create database if not exists {database}'
                                       f'vgroups 5'
                                       f'precision ms'
                                       f'keep 730'), 
                                  conn)
        
        # evaluate db creation
        
        
        # exec stable creation
        
        # get current stables available
        stable_list = conn.execute(text('SHOW STABLES'))
        
        # have column structure for tdengine
        tdengine_cols_dict = self.to_tdengine_column_types(columns_dict)
        
        tdengine_cols_str = ', '.join([f"{key} {tdengine_cols_dict}" 
                                       for key in tdengine_cols_dict])
        
        if_table_exists = 'fail'
        if stable in stable_list:
            
            stable_describe = conn.execute(text(f'DESCRIBE {database}.{stable}'))
            
            # get existing stable column structure
            # if they match, append data
            # if no match, replace stable
            
            if_table_exists = 'append'
            
        # polars write_database case
        
        conn.execute(text(f'USING {database}'))
        
        if isinstance(dataframe, polars_lazyframe):
            
            dataframe = dataframe.collect()
        
        try:
            
            table_write = dataframe.write_database(
                                table_name = stable,
                                connection = conn,
                                if_table_exists = if_table_exists,
                                engine = 'sqlalchemy'
            )
        
        except Exception as e:
            
            logger.error('write_data error, polars write_database case')
            raise
        
        # taos conenctor case
        
        try:
            
            cursor = conn.cursor()
            cursor.execute(text(f'USING {database}'))
    
            columns_for_insert = list(tdengine_cols_dict.keys())
            placeholders = ", ".join(["%s"] * len(columns_for_insert))
            insert_sql = f"INSERT INTO {stable} ({','.join(columns_for_insert)}) VALUES ({placeholders})"
    
            cursor.executemany(insert_sql, dataframe.rows())
            conn.commit()
            
        except Exception as e:
            
            logger.error('write_data error, polars write_database case')
            raise
        
        # evaluate stable ingestion
        
        
        
    
        # show create statement
        conn.close()
    
    
    def read_data(self):
        
        # https://docs.pola.rs/api/python/dev/reference/api/polars.read_database.html
    
        pass
    
    def exec_sql():
        
        # https://docs.pola.rs/api/python/dev/reference/api/polars.DataFrame.write_database.html
        
        pass
    



'''
POSTGRESQL

'''


'''
LOCAL DATA FILES MANAGER

'''

@define(kw_only=True, slots=True)
class LocalDBFiles(DatabaseConnector):
    
    pass


