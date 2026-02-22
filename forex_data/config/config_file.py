# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 18:27:09 2023

@author: fiora
"""

import yaml

from pathlib import Path
from typing import Any, Dict, Optional, Union

from loguru import logger


def read_config_file(config_file: str) -> Dict[str, Any]:

    # assert compliant filepath
    filepath = Path(config_file)

    if not (
        filepath.exists()
        and filepath.is_file()
        and filepath.suffix == '.yaml'
    ):

        raise TypeError(
            f'Config file not exists or invalid extension type: {config_file}')

    if not filepath.suffix == '.yaml':

        raise TypeError(f'required .yaml file, got {filepath.suffix}')

    with open(filepath) as stream:

        try:

            data: Dict[str, Any] = yaml.safe_load(stream)

        except yaml.YAMLError as e:

            raise ValueError('error loading yaml config data from '
                             f'{str(filepath)}: {e}')

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


def _get_config_filepath(
        config_path: Path, file_pattern: str = 'data_config.yaml') -> Path:
    if (
        config_path.exists()
        and config_path.is_dir()
    ):

        return read_config_folder(config_path,
                                  file_pattern=file_pattern)

    elif (
        config_path.exists()
        and config_path.is_file()
        and config_path.suffix == '.yaml'
    ):
        return config_path

    else:
        return Path()


def _apply_config(
        obj, kwargs, _class_attributes_name, _not_assigned_attrs_index_mask,
        file_pattern='data_config.yaml'):
    if 'config' in kwargs.keys():

        if kwargs['config']:
            config_path = Path(kwargs['config'])

        try:
            # Try to check if it's a valid directory path
            # This will fail if config is a YAML string (not a path)
            config_filepath = _get_config_filepath(config_path, file_pattern)

        except (OSError, ValueError):
            # If path operations fail (e.g., string is too long to be a filename),
            # it's likely a YAML string, not a path
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
            # raise since logger still not initialized
            raise TypeError('invalid config type '
                            f'{kwargs["config"]}: '
                            'required str or path to yaml file, got '
                            f'{type(kwargs["config"])}')

        if (
            not isinstance(config_args, dict)
            or not bool(config_args)
        ):
            raise TypeError('invalid config type '
                            f'{kwargs["config"]}: '
                            'required str or path to yaml file, got '
                            f'{type(kwargs["config"])}')

        # set args from config file
        attrs_keys_configfile = \
            set(_class_attributes_name).intersection(config_args.keys())

        for attr_key in attrs_keys_configfile:
            obj.__setattr__(attr_key,
                            config_args[attr_key])
            _not_assigned_attrs_index_mask[
                _class_attributes_name.index(attr_key)
            ] = False

        # set args from instantiation
        # override if attr already has a value from config
        attrs_keys_input = \
            set(_class_attributes_name).intersection(kwargs.keys())

        for attr_key in attrs_keys_input:
            obj.__setattr__(attr_key,
                            kwargs[attr_key])
            _not_assigned_attrs_index_mask[
                _class_attributes_name.index(attr_key)
            ] = False

        # attrs not present in config file or instance inputs
        # --> obj.attr leads to KeyError
        # are manually assigned to default value derived
        # from __attrs_attrs__

        from numpy import array
        for attr_key in array(_class_attributes_name)[
                _not_assigned_attrs_index_mask
        ]:
            try:
                attr = [attr
                        for attr in obj.__attrs_attrs__
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
                        obj.__setattr__(attr.name,
                                        attr.default.factory())
                    else:
                        obj.__setattr__(attr.name,
                                        attr.default)
                else:
                    obj.__setattr__(attr.name,
                                    None)

        return True
    return False
