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

from attrs import (
        define,
        field
    )



# BASIC

@define(kw_only=True, slots=True)
class DatabaseConnector:
    
    name            : str = field(default=None,
                                  validator=validators.instance_of(str))
    working_folder  : str = field(default = Path(DEFAULT_PATHS.BASE_PATH), 
                                  validator=validator_dir_path(create_if_missing=True))
    target_url      : str = field(default=None,
                                  validator=validators.instance_of(str))
    
        

    def connect():
        
        pass
    
    
    def check_connection():
        
        pass
    
    
    def create_bucket():
        
        pass
    
    
    def list_buckets():
        
        pass
        
    
    def write_data():
        
        pass
    
    
    def read_data():
        
        pass
    
    
    def exec_sql():
        
        pass
    

# TDENGINE

@define(kw_only=True, slots=True)
class TDengineConnector(DatabaseConnector):
    
    def connect():
        
        pass
    
    
    def check_connection():
        
        pass
    
    
    def create_bucket():
        
        pass
    
    
    def list_buckets():
        
        pass
        
    
    def write_data():
        
        pass
    
    
    def read_data():
        
        pass
    
    
    def exec_sql():
        
        pass
        
        
    



# POSTGRESQL