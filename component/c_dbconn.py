#!/bin/env python3
#coding=utf-8
# @Time    : 2018/6/6 下午1:36
# @Author  : kelly
# @Site    : 
# @File    : c_dbconn.py
# @Software: PyCharm

import basic_config.bc_info as bc_info
import basic_config.bc_log
import component.c_error as err
import mysql.connector
import elasticsearch

__author__ = 'kelly'
sql_record_num = 1000

class sshserver():

    def __init__(self):
        from sshtunnel import SSHTunnelForwarder
        self.sshserver = SSHTunnelForwarder(
            ('193.112.209.138', 12342),  # B机器的配置
            # ssh_password="sshpasswd",
            ssh_username="root",
            ssh_pkey='/Users/kelly/Desktop/车挣/id_rsa' if bc_info.env['env'] == 'dev' else '/root/.ssh/id_rsa',
            remote_bind_address=(bc_info.env['db_connection']['host'], bc_info.env['db_connection']['port']))  # A机器的配置

    def start(self):
        self.sshserver.start()
    def stop(self):
        self.sshserver.stop()

class db():
    def create_conn(self):
        if self.conn:
            return self.conn

        if self.sshserver is not None:
            # mock prd environment
            sshserver = self.sshserver.sshserver
        try:
            if bc_info.env['outside']=='y' and bc_info.env['env'] == 'dev':
                # from sshtunnel import SSHTunnelForwarder
                # with SSHTunnelForwarder(
                #         ('139.224.68.177', 22),  # B机器的配置
                #         # ssh_password="sshpasswd",
                #         ssh_username="root",
                #         ssh_pkey="/Users/kelly/Desktop/车挣/id_rsa",
                #         remote_bind_address=('rm-uf6dq61h877912xf8.mysql.rds.aliyuncs.com', 3306)) as server:  # A机器的配置
                #     config = {'host': '127.0.0.1',  # 默认127.0.0.1
                #               'user': 'vipinsdba',
                #               'passwd': '3ZYUmN3a',
                #               'port': server.local_bind_port,  # 默认即为3306
                #               'db': 'insdb',
                #               # 'charset': 'utf8'  # 默认即为utf8
                #               }
                #     self.conn = mysql.connector.connect(**config)
                #     cursor = self.conn.cursor()
                #     cursor.execute("select now()")
                #     print(cursor.fetchone())
                #     return self.conn

                config = {'host': '127.0.0.1',  # 默认127.0.0.1
                          'user': bc_info.env['db_connection']['user'], #'root',
                          'passwd': bc_info.env['db_connection']['password'], #'123456',#'QJF9dDTU',
                          'port': sshserver.local_bind_port,  # 默认即为3306
                          'db': bc_info.env['db_connection']['database'], #'darams',
                          'charset': bc_info.env['db_connection']['charset'], #'utf8'  # 默认即为utf8
                          }
                self.conn = mysql.connector.connect(**config)
                # cursor = self.conn.cursor()
                # cursor.execute("select now()")
                # print(cursor.fetchone())
                # return self.conn

            else:
                self.conn = mysql.connector.connect(**bc_info.env['db_connection'])
        except mysql.connector.Error as e:
            basic_config.bc_log.debug().print_debug(bc_info.position(), e,)
            basic_config.bc_log.debugfile().printlog2(basic_config.bc_info.position(), e,)
            raise err.MyError(basic_config.bc_info.position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        return self.conn

    def close_conn(self):
        try:
            if self.conn:
                self.conn.rollback()
                self.conn.close()
        except mysql.connector.errors.OperationalError:
            pass
        finally:
            if self.sshserver is not None:
                self.sshserver.stop()

    def __init__(self):
        self.conn = None
        self.sshserver = None
        if bc_info.env['outside']=='y' and bc_info.env['env'] == 'dev':
            self.sshserver = sshserver()
            self.sshserver.start()

    def __del__(self):
        # print('del db')
        try:
            # mysql.connector.errors.OperationalError: MySQL Connection not available.
            self.conn.database
            # database connection valid
            # print('safety close')
            self.close_conn()
        except mysql.connector.errors.OperationalError:
            pass
        finally:
            if self.sshserver is not None:
                self.sshserver.stop()

class sshserver_es():
    def __init__(self):
        from sshtunnel import SSHTunnelForwarder
        self.sshserver = SSHTunnelForwarder(
            ('193.112.209.138', 12342),  # B机器的配置
            # ssh_password="sshpasswd",
            ssh_username="root",
            ssh_pkey='/Users/kelly/Desktop/车挣/id_rsa' if bc_info.env['env'] == 'dev' else '/root/.ssh/id_rsa',
            remote_bind_address=(bc_info.env['db_es']['host'], bc_info.env['db_es']['port']))  # A机器的配置

    def start(self):
        self.sshserver.start()
    def stop(self):
        self.sshserver.stop()

class db_es():
    def __init__(self):
        self.conn = None
        self.sshserver_es = None
        if bc_info.env['outside'] == 'y' and bc_info.env['env'] == 'dev':
            self.sshserver_es = sshserver_es()
            self.sshserver_es.start()

    def __del__(self):
        try:
            if self.conn is not None:
                self.close_conn()
        except AttributeError:
            pass

    def create_conn(self):
        if self.conn:
            return self.conn

        if self.sshserver_es is not None:
            # mock prd environment
            sshserver_es = self.sshserver_es.sshserver
        try:
            if bc_info.env['outside'] == 'y' and bc_info.env['env'] == 'dev':
                config = {'host': '127.0.0.1',
                          'port': sshserver_es.local_bind_port,  # 默认即为3306
                          }
                self.conn = elasticsearch.Elasticsearch([config])
            else:
                self.conn = elasticsearch.Elasticsearch([bc_info.env['db_es']])
        except mysql.connector.Error as e:
            basic_config.bc_log.debug().print_debug(bc_info.position(), e, )
            basic_config.bc_log.debugfile().printlog2(basic_config.bc_info.position(), e, )
            raise err.MyError(basic_config.bc_info.position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        return self.conn

    def close_conn(self):
        try:
            if self.conn:
                self.conn = None
                del(self.conn)
        except Exception:
            pass
        finally:
            if self.sshserver_es is not None:
                self.sshserver_es.stop()