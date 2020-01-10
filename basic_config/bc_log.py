#!/bin/env python3
#coding=utf-8
# @Time    : 2018/6/6 下午12:14
# @Author  : kelly
# @Site    : 
# @File    : bc_log.py
# @Software: PyCharm

import sys
import pathlib
import datetime
import traceback
import basic_config.bc_info as config

# def function():
#     print(sys._getframe().f_code.co_filename)  #当前位置所在的文件名
#     print(sys._getframe().f_code.co_name)      #当前位置所在的函数名
#     print(sys._getframe().f_lineno)            #当前位置所在的行号

class debug():
    def print_debug(self,*kwargs):
        if config.env['debug_flag'] == 'OPEN':
            print(*kwargs)

    def position(self):
        try:
            raise Exception
        except:
            f = sys.exc_info()[2].tb_frame.f_back
        return '%s file:%s funcname:%s line:%s ' % (
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f.f_code.co_filename, f.f_code.co_name, str(f.f_lineno))

    def function_name(self):
        try:
            raise Exception
        except:
            f = sys.exc_info()[2].tb_frame.f_back
        return f.f_code.co_name

    def test(self,exc_info=True):
        if isinstance(exc_info, BaseException):
            exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
        elif not isinstance(exc_info, tuple):
            exc_info = sys.exc_info()[2].tb_frame.f_back
        print(exc_info)

    def trace_stack(self):
        return traceback.extract_stack()

class debugfile():
    __author__ = 'kelly'
    def __init__(self):
        self.filename = sys.argv[0]
        # print('%s start at %s' % (filename,datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        self.logdir = pathlib.Path(config.env['path'] + '/logs')
        self.logfile = pathlib.Path(config.env['path'] + '/logs/debug_' + datetime.date.today().strftime('%Y-%m-%d') + '.txt')

    def printlog(self,author='sys.argv[0]', line='sys._getframe().f_lineno', *args):

        if not (self.logdir.exists()):
            self.logdir.mkdir()

        if not (self.logfile.exists()):
            file = self.logfile.open(mode='a+', encoding='utf-8')
        else:
            file = self.logfile.open(mode='a+', encoding='utf-8')

        # len(args)

        file.write(config.err_keyword() + '\tfilename:%s\terrorline=%s' % (author, line))
        for arg in args:
            try:
                file.write('\t' + str(arg))
            except:
                file.write('\t' + arg.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            file.write('\r\n')

        file.close()

    def printlog2(self,position=config.position(), *args):
        logfile = pathlib.Path(
            config.env['path'] + '/logs/debug_' + datetime.date.today().strftime('%Y-%m-%d') + '.txt')
        if not (self.logdir.exists()):
            self.logdir.mkdir()

        if not (logfile.exists()):
            file = logfile.open(mode='a+', encoding='utf-8')
        else:
            file = logfile.open(mode='a+', encoding='utf-8')

        # len(args)

        # file.write(dbconfig.err_keyword()+'\t%s' % (position))
        file.write('%s' % (position))
        for arg in args:
            try:
                file.write('\t' + str(arg))
            except:
                file.write('\t' + arg.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            file.write('\r\n')

        file.close()

    def cleardebugfile(self):
        file = self.logfile.open(mode='w', encoding='utf-8')
        file.write('')
        file.close()

class log():
    def __init__(self,loglv=config.loglevel_val['info'],*args):
        try:
            if config.env['loglevel']>=loglv:
                debug().print_debug(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),config.env['env'],config.env['hostname'],'loglevel:%s|' % config.loglevel_desc[loglv],*args)
                debugfile().printlog2(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),config.env['hostname'],'loglevel:%s,' % config.loglevel_desc[loglv],*args)
        except Exception as e:
            log(set_loglevel().error(),debug().position(),'log system error',e)

class set_loglevel:
    def __init__(self):
        self.loglevel_desc = 'all'

    def top(self):
        self.loglevel_desc = 'top'
        return config.loglevel_val['top']

    def error(self):
        self.loglevel_desc = 'error'
        return config.loglevel_val['error']

    def warning(self):
        self.loglevel_desc = 'warning'
        return config.loglevel_val['warning']

    def info(self):
        self.loglevel_desc = 'info'
        return config.loglevel_val['info']

    def debug(self):
        self.loglevel_desc = 'debug'
        return config.loglevel_val['debug']

    def funny(self):
        self.loglevel_desc = 'funny'
        return config.loglevel_val['funny']

    def all(self):
        self.loglevel_desc = 'all'
        return config.loglevel_val['all']

class debug1():
    def __init__(self,*args):
        log(set_loglevel().debug(),*args)

class info():
    def __init__(self,*args):
        log(set_loglevel().info(),*args)

class warning():
    def __init__(self,*args):
        log(set_loglevel().warning(),*args)

class error():
    def __init__(self,*args):
        log(set_loglevel().error(),traceback.extract_stack(),*args)

class print_anyway():
    def __init__(self, *args):
        log(set_loglevel().top(), *args)