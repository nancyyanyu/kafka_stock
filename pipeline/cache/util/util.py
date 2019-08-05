#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 18:18:09 2019

@author: yanyanyu
"""

import pandas as pd
from datetime import date, timedelta

def string_to_float(string):
    if type(string)==str:
        return float(string)
    else:
        return string

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def splitTextToTriplet(string,n):
    words = string.split()
    grouped_words = [' '.join(words[i: i + n]) for i in range(0, len(words), n)]
    return grouped_words

symbol_list=['AAPL','MSFT','FB','GOOG','^GSPC','AMZN']

def prev_weekday(adate):
    while adate.weekday() >=5: # Mon-Fri are 0-4
        adate -= timedelta(days=1)

    return adate







