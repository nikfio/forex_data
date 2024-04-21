# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 18:27:09 2023

@author: fiora
"""

import yaml

from pathlib import Path


appconfig_folder = Path(__file__).parent.parent.parent / 'appconfig'

# search for config file

appconfig_filepath = list(appconfig_folder.glob('*appconfig.yaml'))

if ( 
    bool(appconfig_filepath)
    and
    len(appconfig_filepath) == 1
    ):
    
    APPCONFIG_YAML = str(appconfig_filepath[0])
    
else:
    
    APPCONFIG_YAML = ''


def read_config_file(config_file):
    
    # assert compliant filepath 
    filepath = Path(config_file)
    
    assert filepath.exists()  \
           and filepath.is_file() \
           and filepath.suffix == '.yaml', \
           'invalid setting .yaml file: {}'.format(filepath.resolve())
    
    # open and read .yaml configuration file
    with open(filepath) as settings_file:
        
        data = yaml.load(settings_file, Loader=yaml.FullLoader)
    
    
    return data


def read_config_string(config_string):
    
    # open and read .yaml configuration file
    data = yaml.load(config_string, Loader=yaml.FullLoader)
        
    return data

    
def dump_config_file(filepath, data):
    
    pass
