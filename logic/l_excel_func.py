#!/bin/env python3
#coding=utf-8
# @Time    : 2018/10/24 上午10:28
# @Author  : kelly
# @Site    : 
# @File    : l_excel_func.py
# @Software: PyCharm

import entity.e_excel_formula as excel_formula

class excel_func():
    def run(self,func_name,params):
        ret_val = ['ceil','normsinv','tinv','chiinv','var','varp']
        ret_val = 'only ' + str(ret_val) + ' is valid.'
        if func_name.lower() == 'ceil':
            ret_val = excel_formula.excel_formula().ceil(params[0],params[1])
        elif func_name.lower() == 'normsinv':
            ret_val = excel_formula.excel_formula().normsinv(params[0])
        elif func_name.lower() == 'tinv':
            ret_val = excel_formula.excel_formula().tinv(params[0],params[1])
        elif func_name.lower() == 'chiinv':
            ret_val = excel_formula.excel_formula().chiinv(params[0],params[1])
        elif func_name.lower() == 'var':
            ret_val = excel_formula.excel_formula().var(params)
        elif func_name.lower() == 'varp':
            ret_val = excel_formula.excel_formula().varp(params)
        return str(ret_val)