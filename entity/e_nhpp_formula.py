#!/bin/env python3
#coding=utf-8
# @Time    : 2018/9/10 下午5:41
# @Author  : kelly
# @Site    :
# @File    : e_nhpp_formula.py
# @Software: PyCharm

from scipy.optimize import fsolve, newton, brent, excitingmixing, minimize_scalar , fmin, minimize, root
import numpy as np
import sys
import decimal
# np.set_printoptions(suppress=True)   # 不使用科学计数法

decimal.getcontext().prec = 100

sys.setrecursionlimit(1000000) #例如这里设置为一百万

class nhpp_cal():
    def __init__(self):
        # 列车故障里程列表,
        self.fault_mile_list = None
        # 列车最大里程列表
        self.max_mile_list = None
        # 列车故障数量
        self.fault_count_list = None

    def set_params(self, fault_mile_list=None, max_mile_list=None, ):
        # 列车故障里程列表，最大里程列表
        """

        :param fault_mile_list:
        :type fault_mile_list:
        :param max_mile_list:
        :type max_mile_list:
        :return:
        :rtype:
        """
        # self.fault_mile_list = fault_mile_list if fault_mile_list is not None else [
        #     [1102303, 1245423, 1051526, 1189590],
        #     [698030, 656884, 748638, 724081, 802477, 892892, 805924, 892892, 698030, 746482]]
        # self.max_mile_list = max_mile_list if max_mile_list is not None else [[1279498], [1028350]]

        self.fault_mile_list = fault_mile_list if fault_mile_list is not None else [
            [],[],[98],[326, 653, 653],[],[84],[87],[646],[92],[],[258, 328, 377, 621],[61, 539],[254, 276, 298, 640],[76, 538],[635],[349, 404, 561],[],[],[120, 479],[323, 449],[139, 139],[],[573],[165, 408, 604],[249],[344, 497],[265, 586],[166, 206, 348],[],[410, 581],[],[],[],[367],[202, 563, 570],[],[],[],[],[],[]]
        self.max_mile_list = max_mile_list if max_mile_list is not None else [[761],	[759],	[667],	[667],	[665],	[667],	[663],	[653],	[653],	[651],	[650],	[648],	[644],	[642],	[641],	[649],	[631],	[596],	[614],	[582],	[589],	[593],	[589],	[606],	[594],	[613],	[595],	[389],	[601],	[601],	[611],	[608],	[587],	[603],	[585],	[587],	[578],	[578],	[586],	[585],	[582]]

        self.fault_count_list=  []
        for fault_mile in self.fault_mile_list:
            self.fault_count_list.append([len(fault_mile)])

    def set_expr(self):
        # expr_b = self.__sigma_n()+'/(y**(-x) * %s - %s) - x' % (self.__sigma_t_b_ln(),self.__sigma_sigma_ln_t())
        # expr_a = '(%s/%s)**(1/x) - y' %(self.__sigma_t_b(),self.__sigma_n())
        # print('beta : %s' % expr_b)
        # print('theda : %s' % expr_a)
        a = np.log(1) # just highlight the import expression
        self.expr = self.__sigma_n()+'/(%s**(-1) * %s - %s) - x' % ('(%s/%s)'%(self.__sigma_t_b(),self.__sigma_n()),self.__sigma_t_b_ln(),self.__sigma_sigma_ln_t())
        return self.expr

    def get_beta_value(self) -> str:
        if 'expr' not in self.__dict__.keys():
            self.set_expr()

        # res1 = minimize_scalar(self.__x_in_disguise, bounds=(-10**(-6), 10**(-6)), method='bounded')
        # res1 = minimize_scalar(self.__x_in_disguise, (-2,2),method='Golden')
        # res = fmin(self.__x_in_disguise,eval(self.__sigma_n())/(eval(self.__sigma_ln_t())-eval(self.__sigma_sigma_ln_t())))
        # res = minimize(self.__x_in_disguise,np.array([-5,5]))
        # return res.x
        # res2 = root(self.__x_in_disguise,0,tol=10**(-6))
        # res21 = root(self.__x_in_disguise, (0,1,3),tol=10**(-3))
        beta = fsolve(self.__x_in_disguise, 0,xtol=10**(-6))
        # beta1 = fsolve(self.__x_in_disguise,(0,1,3),xtol=10**(-3))
        self.__x = decimal.Decimal(beta[0])
        # self.__x = res2.x[0]
        return str(self.__x)

    def get_lambda_value(self) -> str:
        try:
            self.__x
        except AttributeError as e:
            self.get_beta_value()
        # if '__x' not in self.__dict__.keys():
        #     self.get_beta_value()
        self.expr = '%s/%s' % (self.__sigma_n(),self.__sigma_t_b())
        lambda_1 = fsolve(self.__get_x_result, 0)
        return str('{:.100f}'.format(decimal.Decimal(lambda_1[0])))   # 不使用科学计数法

    def check_x_value(self,x):
        if 'expr' not in self.__dict__.keys():
            self.set_expr()
        self.__x = x
        return fsolve(self.__get_x_result, 0)


    def __sigma_n(self):
        expr = ''
        for fault_count_1 in self.fault_count_list:
            expr = str(fault_count_1[0]) if expr == '' else expr + '+'+ str(fault_count_1[0])
        expr = '('+expr+')'
        # expr = str(eval(expr))
        # print('singma_n: %s' % expr)
        return expr

    def __sigma_t_b(self):
        expr = ''
        for max_mile_1 in self.max_mile_list:
            expr = str(max_mile_1[0])+'**x' if expr == '' else expr + '+' + str(max_mile_1[0])+'**x'
        expr = '(' + expr + ')'
        # print('singma_t_b: %s' % expr)
        return expr

    def __sigma_t_b_ln(self):
        expr = ''
        for max_mile_1 in self.max_mile_list:
            expr = str(max_mile_1[0]) + '**x*np.log(%s)' % str(max_mile_1[0]) if expr == '' else expr + '+' + str(max_mile_1[0]) + '**x*np.log(%s)' % str(max_mile_1[0])
        expr = '(' + expr + ')'
        # print('singma_t_b_ln: %s' % expr)
        return expr

    def __sigma_ln_t(self):
        expr = ''
        for max_mile_1 in self.max_mile_list:
            expr = 'np.log(%s)' % str(max_mile_1[0]) if expr == '' else expr + '+' + 'np.log(%s)' % str(max_mile_1[0])
        expr = '(' + expr + ')'
        # expr = str(eval(expr))
        # print('singma_singma_t: %s' % expr)
        return expr

    def __sigma_sigma_ln_t(self):
        expr = ''
        for fault_mile_1 in self.fault_mile_list:
            if len(fault_mile_1)==0:
                continue
            for i in range(0,len(fault_mile_1)):
                expr = 'np.log(%s)' % str(fault_mile_1[i]) if expr == '' else expr + '+' + 'np.log(%s)' % str(fault_mile_1[i])
        expr = '(' + expr + ')'
        # expr = str(eval(expr))
        # print('singma_singma_t: %s' % expr)
        return expr

    def __x_in_disguise(self,x):
        return eval(self.expr) - x + x

    def __get_x_result(self,x):
        # global formula_1
        return eval(self.expr.replace('x',str(self.__x))) - x
