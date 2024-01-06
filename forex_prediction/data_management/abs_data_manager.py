# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 21:03:45 2023

@author: fiora
"""

from abc import ABC, abstractmethod

from attrs import ( 
                    define,
                    field,
                    validators
                )

from .common import *

class datamanager(ABC):
    
    ticker      : str  = field(validator=validators.instance_of(str))
    timeframe   : list = field(default='1T',
                               validator=validator_list_timeframe)
    
    @abstractmethod
    def add_timeframe(self):
        
        pass
    
    @abstractmethod
    def get_data(self):
        
        pass
