#!/bin/env python3
#coding=utf-8
# @Time    : 2018/10/24 上午10:22
# @Author  : kelly
# @Site    : 
# @File    : e_excel_formula.py
# @Software: PyCharm

import math
from scipy.stats import norm
from scipy.stats import t
from scipy.stats import chi2
import numpy as np

class excel_formula():
    def ceil(self,x, s):
        # excel ceilling
        return s * math.ceil(float(x) / s)

    def normsinv(self,x):
        # NORMSINV函数
        return norm.ppf(x)

    def tinv(self,x,s):
        # TINV函数
        return t.ppf(1 - x / 2, s)

    def chiinv(self,x,s):
        # CHIINV函数
        return chi2.isf(x, s)

    def var(self,x):
        # var
        if type(x)!=list:
            raise ImportError
        return np.var(x, ddof=1)

    def varp(self,x):
        # varp
        if type(x) != list:
            raise ImportError
        return np.var(x, ddof=0)