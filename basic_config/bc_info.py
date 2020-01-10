#!/bin/env python3
#coding=utf-8
# @Time    : 2018/6/6 下午12:14
# @Author  : kelly
# @Site    : 
# @File    : bc_info.py
# @Software: PyCharm


import os
import time
import sys

# dev_pc=['KellymatoMacBook-Air.local']

dev_pc=['LAPTOP-HTT5C61E']

pro_pc=['SG0105WRAMSAPP']  ##yx_1023

loglevel_val = {'top':1,
                'error': 10,
                'warning': 20,
                'info': 30,
                'debug': 40,
                'funny': 50,
                'sexy': 60,
                'all': 99}

loglevel_desc = {v:k for k,v in loglevel_val.items()}

def hostname():
    sys = os.name

    if sys == 'nt':
        hostname = os.getenv('computername')
        return hostname

    elif sys == 'posix':
        # host = os.popen('echo $HOSTNAME')
        host = os.popen('hostname')
        try:
            hostname = host.read().replace('\n','')
            return hostname
        finally:
            host.close()
    else:
        return 'Unkwon hostname'

def position():
    try:
        raise Exception
    except:
        f = sys.exc_info()[2].tb_frame.f_back
    return '%s file:%s funcname:%s line:%s ' % (time.strftime('%Y-%m-%d %H:%M:%S'),f.f_code.co_filename,f.f_code.co_name,str(f.f_lineno))

def err_keyword():
    try:
        raise Exception
    except:
        f = sys.exc_info()[2].tb_frame.f_back
    return 'Search Key Word: Error at %s file:%s funcname:%s line:%s ' % (
        time.strftime('%Y-%m-%d %H:%M:%S'), f.f_code.co_filename, f.f_code.co_name, str(f.f_lineno))

class security:
    def get_white_list(self):
        return '' #white_list

class set_loglevel:
    def __init__(self):
        self.loglevel_desc = 'all'

    def top(self):
        self.loglevel_desc = 'top'
        return loglevel_val['top']

    def error(self):
        self.loglevel_desc = 'error'
        return loglevel_val['error']

    def warning(self):
        self.loglevel_desc = 'warning'
        return loglevel_val['warning']

    def info(self):
        self.loglevel_desc = 'info'
        return loglevel_val['info']

    def debug(self):
        self.loglevel_desc = 'debug'
        return loglevel_val['debug']

    def funny(self):
        self.loglevel_desc = 'funny'
        return loglevel_val['funny']

    def all(self):
        self.loglevel_desc = 'all'
        return loglevel_val['all']

class datetime_style:
    # datetime.datetime.now().strftime(config.datetime_style().long_style())
    def long_style(self):
        return '%Y-%m-%d %H:%M:%S'
    def date_style(self):
        return '%Y-%m-%d'
    def time_style(self):
        return '%H:%M:%S'

class settings():
    def db(self):
        # tconfig = {'host': '192.168.1.12',  # 默认127.0.0.1
        #            'user': 'root',
        #            'password': 'QJF9dDTU',
        #            'port': 3306,  # 默认即为3306
        #            'database': 'darams',
        #            'charset': 'utf8'  # 默认即为utf8
        #            }
        tconfig = {'host': '192.168.221.22',  # 默认127.0.0.1
                   'user': 'root',
                   'password': '123456', #'QJF9dDTU',
                   'port': 3306,  # 默认即为3306
                   'database': 'darams',
                   'charset': 'utf8'  # 默认即为utf8
                   }
        pconfig = {'host': 'localhost',  # 默认127.0.0.1
                   'user': 'root',
                   'password': 'SiFang123456789',
                   'port': 3306,  # 默认即为3306
                   'database': 'darams',
                   'charset': 'utf8'  # 默认即为utf8
                   }
        return pconfig if pro_pc.count(hostname())>0 else tconfig

    def path(self):

        if hostname()=='KellymatoMacBook-Air.local':
            tpath = '/Users/kelly/Desktop/现代工业信息化公司/DARAMS/系统开发/data_manage'
        else:
            tpath = '.'
        ppath = 'D:/python3/project'

        return ppath if pro_pc.count(hostname())>0 else tpath

    def port(self):
        tport = 10009
        pport = 10009
        return pport if pro_pc.count(hostname())>0 else tport

    def debug_flag(self):
        tdebugflag  = 'OPEN'
        pdebugflag = 'OPEN'
        return pdebugflag if pro_pc.count(hostname())>0 else tdebugflag

    def loglevel(self):
        tloglevel=set_loglevel().info()
        ploglevel=set_loglevel().warning()
        return ploglevel if pro_pc.count(hostname())>0 else tloglevel

    def whiteip(self):
        twhiteip=[]
        pwhiteip=['127.0.0.1','localhost']
        return pwhiteip if pro_pc.count(hostname()) > 0 else twhiteip

    def db_es(self):
        tdb_es={'host': '192.168.1.22',
                'port': '9200',
                'http_auth': ('admin', '123456')}
        pdb_es= {'host': '127.0.0.1',
                 'port': '9200'}
        return pdb_es if pro_pc.count(hostname()) > 0 else tdb_es

setting = settings()

env={'env':'prd' if pro_pc.count(hostname())>0 else 'dev',
     'hostname':hostname(),
     'debug_flag':setting.debug_flag(),
     'db_connection':setting.db(),
     'path':setting.path(),
     'loglevel':setting.loglevel(),
     'loglevel_desc':set_loglevel().loglevel_desc,
     'outside':'n' if pro_pc.count(hostname())>0 else 'n', # ssh server request or not  ##yx_1023
     'port':setting.port(),
     'whiteip':setting.whiteip(),
     'db_es':setting.db_es()
     }