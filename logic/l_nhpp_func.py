#!/bin/env python3
#coding=utf-8
# @Time    : 2018/9/12 下午12:51
# @Author  : kelly
# @Site    : 
# @File    : l_nhpp_func.py
# @Software: PyCharm

import entity.e_nhpp_formula

class nhpp_func():
    def param_sets(self,params):
        fault_mile_list = []
        max_mile_list = []
        for train in params.get('train'):
            fault_mile_list.append(train['fault_mileage'] if train['fault_mileage'] is not None else [])
            max_mile_list.append([float(train['max_mileage'])] if train['max_mileage'] is not None else [0])
        return fault_mile_list,max_mile_list

    def main(self,params):
        fault_mile_list, max_mile_list = self.param_sets(params)
        nhpp_cal = entity.e_nhpp_formula.nhpp_cal()
        nhpp_cal.set_params(fault_mile_list,max_mile_list)
        result={}
        result['beta']=nhpp_cal.get_beta_value()
        result['lambda']=nhpp_cal.get_lambda_value()
        return result