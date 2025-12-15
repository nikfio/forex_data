# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 18:27:09 2023

@author: fiora
"""

import yaml

from pathlib import Path
from typing import Any, Dict, Optional, Union

from loguru import logger


appconfig_folder = Path(__file__).parent.parent.parent / 'appconfig'

# search for config file

appconfig_filepath = list(appconfig_folder.glob('*appconfig.yaml'))


def read_config_file(config_file: str) -> Dict[str, Any]:

    # assert compliant filepath
    filepath = Path(config_file)

    if not (
        filepath.exists()
        and filepath.is_file()
        and filepath.suffix == '.yaml'
    ):

        logger.error(f'invalid config .yaml file: {config_file}')
        exit(0)

    with open(filepath) as stream:

        try:

            data: Dict[str, Any] = yaml.safe_load(stream)

        except yaml.YAMLError as e:

            logger.error('error loading yaml config data from '
                         f'{str(filepath)}: {e}')
            raise

    return data


def read_config_string(config_str: str) -> Dict[str, Any]:

    # open and read .yaml configuration file as a string
    try:

        data: Dict[str, Any] = yaml.safe_load(config_str)

    except yaml.YAMLError as e:

        logger.error('error loading yaml config data from '
                     f'{config_str}: {e}')
        raise

    return data


def read_config_folder(
        folder_path: Optional[Union[str, Path]] = None,
        file_pattern: str = 'appconfig.yaml') -> Path:

    if folder_path and (
        Path(folder_path).exists()
        and Path(folder_path).is_dir()
    ):

        config_filepath = list(Path(folder_path).glob('*' + file_pattern))

        if config_filepath:

            return config_filepath[0]

        else:

            return Path()

    else:

        return Path()
