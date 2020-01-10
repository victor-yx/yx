#!/bin/env python3
#coding=utf-8
# @Time    : 2020/1/1 13:45
# @Author  : Victor
# @Site    :
# @File    : l_offlineData2mysql.py 即离线数据导入MySQL的开发脚本
# @Software: PyCharm
import uuid
import pymysql
import csv
import datetime
from basic_config.bc_offlinedata2mysql import Config


def gene_uuid():
    uuid_ = str(uuid.uuid4()).replace("-", "") + '000'
    return uuid_

def connect_mysql(mysql_conf):
    db = pymysql.connect(mysql_conf['addr'], mysql_conf['user'], mysql_conf['pswd'], charset="utf8",port=mysql_conf['port'])
    cur = db.cursor()
    cur.execute("use {}".format(mysql_conf['usedb']))
    return db, cur

uuid_train_type = gene_uuid()
uuid_trian_no = gene_uuid()
uuid_real_time = gene_uuid()
m = 'TEST001###TEST_TYPE_01###1001'
list_split = m.split("###")
sql_cd_train_type = Config.sql_cd_train_type.format(uuid_train_type, list_split[0], list_split[1],
                                                    Config.list_basic_attr['create_by'],
                                                    Config.list_basic_attr['create_date'],
                                                    Config.list_basic_attr['update_by'],
                                                    Config.list_basic_attr['update_date'],
                                                    Config.list_basic_attr['remarks'],
                                                    Config.list_basic_attr['del_flag'])
# print(sql_cd_train_type)

db,cur = connect_mysql(Config.mysql_config)
cur.execute(Config.exec_judge_aff_app.format('E28','250公里统型','2216'))
# data = cur.fetchall()
print(cur.fetchall())
db.commit()
db.close()










