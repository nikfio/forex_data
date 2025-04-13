# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 18:27:09 2023

@author: fiora
"""

import yaml

from pathlib import Path

from loguru import logger 


appconfig_folder = Path(__file__).parent.parent.parent / 'appconfig'

# search for config file

appconfig_filepath = list(appconfig_folder.glob('*appconfig.yaml'))

if ( 
    bool(appconfig_filepath)
    and
    len(appconfig_filepath) == 1
    ):
    
    APPCONFIG_FILE_YAML = str(appconfig_filepath[0])
    
else:
    
     APPCONFIG_FILE_YAML = ''
     
     logger.warning('no config file present')


def read_config_file(config_file):
    
    # assert compliant filepath 
    filepath = Path(config_file)
    
    if not(
           filepath.exists()
           and 
           filepath.is_file() 
           and 
           filepath.suffix == '.yaml'
    ):
            
        logger.error(f'invalid config .yaml file: {config_file}')
        exit(0)
    
    with open(filepath) as stream:
        
        try:
            
            data = yaml.safe_load(stream)
            
        except yaml.YAMLError as e:
            
            logger.error('error loading yaml config data from '
                         f'{str(filepath)}: {e}')
            raise
            
    return data


def read_config_string(config_str):
    
    if not(
           isinstance(config_str, str)
    ):
            
        logger.error('invalid config .yaml string: required type str')
        exit(0)
        
    # open and read .yaml configuration file as a string
    try:
        
        data = yaml.safe_load(config_str)
      
    except yaml.YAMLError as e:
        
        logger.error('error loading yaml config data from '
                     f'{config_str}: {e}')
        raise
    
        
    return data


def read_config_folder(folder_path=None, file_pattern='appconfig.yaml'):

    if (
        Path(folder_path).exists()
        and
        Path(folder_path).is_dir()
        ):
            
        appconfig_filepath = list(Path(folder_path).glob('*'+file_pattern))
        
        if appconfig_filepath:
            
            return appconfig_filepath[0]
        
        else:
            
            return Path()
        
    else:
        
        return Path()