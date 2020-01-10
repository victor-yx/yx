#!/bin/env python3
#coding=utf-8
__author__ = 'kelly'

class MyError(ValueError):
    # F/E/I  F for front side, E for end side, I for interface side
    # x  split from code
    # 0  important level , 9 is highest
    # 00 reverse position
    # 00 increase number , alpha is allowed
    # 0  used unique or not, 0 for unique, 1 for non-unique

    errcode = ['0x000000',]
    errmsg = ['let''s start。',]

    # 1
    errcode.append('Ix900011')
    errmsg.append('数据库连接超时。')

    # 2
    errcode.append('Ex300011')
    errmsg.append('SQL错误。')

    # 3
    errcode.append('Ex300021')
    errmsg.append('字符串变量赋值错误。')

    # 4
    errcode.append('Ex300031')
    errmsg.append('输入参数错误。')

    # 5
    errcode.append('Ex300041')
    errmsg.append('返回数据数量超预期。')

    # 6
    errcode.append('Ex300051')
    errmsg.append('预期数据不存在。')

    # 7
    errcode.append('Ex300061')
    errmsg.append('该用户id:%s,不存在车牌:%s的数据。')

    # 8
    errcode.append('Ex300071')
    errmsg.append('该用户id:%s,车牌:%s,存在当前的有效订单数据,订单号:%s。')

    # 9
    errcode.append('Ex900021')
    errmsg.append('订单号:%s,存在未知错误。')

class err_catch():
    def catch(self,object='sys.exc_info()'):
        return_val = []
        tb_curr = object[2]
        while tb_curr.tb_next:
            return_val.append('traceback:%s,function:%s,line:%s' % (
                tb_curr.tb_next.tb_frame.f_code, tb_curr.tb_next.tb_frame.f_code.co_name,
                tb_curr.tb_next.tb_frame.f_lineno))
            tb_curr = tb_curr.tb_next
        return_val.append('valueError:%s' % (str(object[1].args)))
        return return_val