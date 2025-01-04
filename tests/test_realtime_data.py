# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 00:26:52 2024

@author: fiora
"""

# build configuration file
config_file_yaml = \
"""
---
# yaml document for data management configuration

DATA_FILETYPE: 'parquet'

ENGINE: 'polars'

"""

# TODO: find a way to insert api secret key in pipeline environment
#       may be the particular secrets manager or environment 
#       variables if pipeline runs on cloud services