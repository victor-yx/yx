#!/bin/env python3
#coding=utf-8
# @Time    : 2018/12/1 下午1:54
# @Author  : kelly
# @Site    : 
# @File    : e_normal_calc.py
# @Software: PyCharm

from scipy import interpolate

class interpolate_standard():
    def interpolation(self,x1,x2,y1,y2,newx):
        """
        retrun None if error happened, otherwise would return the interpolated value.
        :param x1:
        :type x1:
        :param x2:
        :type x2:
        :param y1:
        :type y1:
        :param y2:
        :type y2:
        :param newx:
        :type newx:
        :return:
        :rtype:
        """
        # x = np.linspace(0, 10, 11)
        x = [x1,x2]
        y = [y1,y2]
        # x=[  0.   1.   2.   3.   4.   5.   6.   7.   8.   9.  10.]
        # y = np.sin(x)
        xnew = [newx]
        # xnew = np.linspace(0, 10, 10)
        f = interpolate.interp1d(x, y, kind='linear',fill_value="extrapolate")
        try:
            ynew = f(xnew)
        except Exception as e:
            ynew=[None]
        return ynew[0]