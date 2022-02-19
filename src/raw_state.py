# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 16:17:25 2022

@author: fiora
"""

"""
Description


"""

import numpy as np

class raw_state:
    
    def __init__(self, o, c, h, l, q=None, dtype=np.float32):
        """
        

        Parameters
        ----------
        o : open value.
        c : closing value.
        h : highest value.
        l : lowest value.
        q : current realtime quote.
            The default is None.
            If None it means no realtime quote is available,
            for example when testing on historical data
        dtype : TYPE, optional
            DESCRIPTION. The default is np.float32.

        Returns
        -------
        None.

        """
        
        if not q:
            q = c
            
        # store state in numpy array format
        self.state_nparr = np.array( [o, c, h, l, q], dtype=dtype)
        
        
        
    def as_nparr(self):
        
        # return state in numpy array format
        return self.state_nparr
        
        



