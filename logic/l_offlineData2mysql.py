#!/bin/env python3
#coding=utf-8
# @Time    : 2020/1/1 13:45
# @Author  : Victor
# @Site    :
# @File    : l_offlineData2mysql.py 即离线数据导入MySQL的开发脚本
# @Software: PyCharm
import os
import sys
import uuid
import pymysql
import csv
import datetime
from basic_config.bc_offlinedata2mysql import Config

class offline_data_to_mysql_func:
    def root_dir_judge(self,path_root_dir):
        list_dirs = os.listdir(path_root_dir)
        list_prewrite = []
        for i in range(0,len(list_dirs)):
            if list_dirs[i][:-4] == "train_info":
                path = os.path.join(path_root_dir, list_dirs[i])
                list_prewrite.append(path)
        sys.exit(1) if len(list_prewrite) == 0 else 0  #  假设一定有"train_info.csv"这个文件，不然会退出
        for i in range(0,len(list_dirs)):
            if list_dirs[i][-9:-4] != "_over" and list_dirs[i][:-4] != "train_info":
                path = os.path.join(path_root_dir,list_dirs[i])
                list_prewrite.append(path)
            else:
                continue
        return list_prewrite

    def read_data(self,path_read):
        out_data = []
        try:
            with open(path_read, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    out_data.append(line.split(","))
        except UnicodeDecodeError:
            with open(path_read, 'r', encoding='gbk') as f:
                for line in f:
                    out_data.append(line.split(","))
        return out_data

    def train_info_tran2dict(self,list_):
        dict_ = {}
        for i in list_:
            if i[0] != "":
                train_name = "###".join(i[:3])
                if train_name not in dict_.keys():
                    dict_[train_name] = []
                dict_[train_name].append(i[3:])
        return dict_

    def write2CsvByrows(self,list_columns,list_tmp,path_csv):
        with open(path_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_columns)
            writer.writerows(list_tmp)

    def write_2_txt(self, path,str_):
        with open(path, "w", encoding='utf-8') as f:
            f.write(str_)

    def connect_mysql(self, mysql_conf):
        db = pymysql.connect(mysql_conf['addr'], mysql_conf['user'], mysql_conf['pswd'], charset="utf8",port=mysql_conf['port'])
        cur = db.cursor()
        cur.execute("use {}".format(mysql_conf['usedb']))
        return db, cur

    def gene_uuid(self):
        uuid_ = str(uuid.uuid4()).replace("-", "") + '000'
        return uuid_

    def merge_sql(self,sql_one,str_all):
        str_all += sql_one
        str_all += '\n'
        return str_all

    def str_2_datetime(self,str_):
        try:
            if len(str_) <= 10:
                datetime_ = datetime.datetime.strptime(str_.replace("/","-"),'%Y-%m-%d')
            else:
                datetime_ = datetime.datetime.strptime(str_.replace("/","-"), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print("datatime type is wrong!")
            sys.exit(1)
        return datetime_

    def trainNo_realTime_unique_judge_update_version(self, key, dict_all_train_uuid, str_insert, str_delete, cur):
        if key in dict_all_train_uuid.keys():
            list_trainName = key.split("###")
            cur.execute(Config.exec_judge_aff_app.format(list_trainName[0],list_trainName[1],list_trainName[2]))
            data = cur.fetchall()
            if data[0][0] != 0:
                update_flag = input("{}_{}_{}该列车的配属/使用信息已存在，是否更新？".format(list_trainName[0],list_trainName[1],list_trainName[2]))
                if update_flag in ["Y","y","yes","YES","是","是的"]:
                    #  假如需要更新，则把原来cd_real_time和cd_affiliated和cd_application中的数据复制一份做备份
                    #  并且把原来cd_real_time中的"数据更新时间"的那一列做更新，并把新的配置/使用信息插入进去。
                    cur.execute(Config.exec_query_realTime_uuid.format(list_trainName[0],list_trainName[1],list_trainName[2]))
                    uuid_cd_real_time = cur.fetchall()[0][0]     #   未完待续……

                    cur.execute(Config.exec_del_time_aff_app.format(data[0][1],data[0][1],data[0][1]))
                    uuid_real_time = self.gene_uuid()
                    update_cd_train_no = Config.update_cd_train_no.format(uuid_real_time, dict_all_train_uuid[key])
                    str_insert = self.merge_sql(update_cd_train_no, str_insert)
                    sql_cd_real_time = Config.sql_cd_train_real_time.format(uuid_real_time, dict_all_train_uuid[key],
                                                                            Config.nowTime, "运行",
                                                                            Config.list_basic_attr['create_by'],
                                                                            Config.list_basic_attr['create_date'],
                                                                            Config.list_basic_attr['update_by'],
                                                                            Config.list_basic_attr['update_date'],
                                                                            Config.list_basic_attr['remarks'],
                                                                            Config.list_basic_attr['del_flag'])
                    str_insert = self.merge_sql(sql_cd_real_time, str_insert)
                    del_cd_real_time = Config.del_cd_train_real_time.format(uuid_real_time)
                    str_delete = self.merge_sql(del_cd_real_time, str_delete)
                else:
                    uuid_real_time = data[0][1]
            else:
                uuid_real_time = self.gene_uuid()
                update_cd_train_no = Config.update_cd_train_no.format(uuid_real_time,dict_all_train_uuid[key])
                str_insert = self.merge_sql(update_cd_train_no,str_insert)
                sql_cd_real_time = Config.sql_cd_train_real_time.format(uuid_real_time,dict_all_train_uuid[key],
                                                                        Config.nowTime,"运行",
                                                                        Config.list_basic_attr['create_by'],
                                                                        Config.list_basic_attr['create_date'],
                                                                        Config.list_basic_attr['update_by'],
                                                                        Config.list_basic_attr['update_date'],
                                                                        Config.list_basic_attr['remarks'],
                                                                        Config.list_basic_attr['del_flag'])
                str_insert = self.merge_sql(sql_cd_real_time,str_insert)
                del_cd_real_time = Config.del_cd_train_real_time.format(uuid_real_time)
                str_delete = self.merge_sql(del_cd_real_time,str_delete)
        else:
            list_trainName = key.split("###")
            uuid_train_type = self.gene_uuid()
            uuid_trian_no = self.gene_uuid()
            uuid_real_time = self.gene_uuid()
            sql_cd_train_type = Config.sql_cd_train_type.format(uuid_train_type,list_trainName[0],list_trainName[1],
                                                                Config.list_basic_attr['create_by'],
                                                                Config.list_basic_attr['create_date'],
                                                                Config.list_basic_attr['update_by'],
                                                                Config.list_basic_attr['update_date'],
                                                                Config.list_basic_attr['remarks'],
                                                                Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_train_type,str_insert)
            sql_cd_train_no = Config.sql_cd_train_no.format(uuid_trian_no,uuid_train_type,list_trainName[-1],
                                                            uuid_real_time,
                                                            Config.list_basic_attr['create_by'],
                                                            Config.list_basic_attr['create_date'],
                                                            Config.list_basic_attr['update_by'],
                                                            Config.list_basic_attr['update_date'],
                                                            Config.list_basic_attr['remarks'],
                                                            Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_train_no,str_insert)
            sql_cd_real_time = Config.sql_cd_train_real_time.format(uuid_real_time, dict_all_train_uuid[key],
                                                                    Config.nowTime, "运行",
                                                                    Config.list_basic_attr['create_by'],
                                                                    Config.list_basic_attr['create_date'],
                                                                    Config.list_basic_attr['update_by'],
                                                                    Config.list_basic_attr['update_date'],
                                                                    Config.list_basic_attr['remarks'],
                                                                    Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_real_time, str_insert)

            del_cd_train_type = Config.del_cd_train_type.format(uuid_train_type)
            str_delete = self.merge_sql(del_cd_train_type, str_delete)
            del_cd_train_no = Config.del_cd_train_no.format(uuid_trian_no)
            str_delete = self.merge_sql(del_cd_train_no, str_delete)
            del_cd_real_time = Config.del_cd_train_real_time.format(uuid_real_time)
            str_delete = self.merge_sql(del_cd_real_time, str_delete)

        return uuid_real_time,str_insert,str_delete

    def trainNo_realTime_unique_judge(self, key, dict_all_train_uuid, str_insert, str_delete,dict_prewrite_train_real_time_uuid):
        #  该步骤建立在"train_info.csv"肯定有的基础上
        if key in dict_all_train_uuid.keys():
            #  若该列车已经存在基础数据中，则会生成一套新的cd_real_time/cd_affiliated/cd_application/cd_mileage/cd_fault_object/cd_fault_pattern
            #  此处可以加入判断，是否对该列车原有affiliated/applicaion数据是否更新？！
            uuid_real_time = self.gene_uuid()
            update_cd_train_no = Config.update_cd_train_no.format(uuid_real_time,dict_all_train_uuid[key])
            str_insert = self.merge_sql(update_cd_train_no,str_insert)
            sql_cd_real_time = Config.sql_cd_train_real_time.format(uuid_real_time,dict_all_train_uuid[key],
                                                                    Config.nowTime,"运行",
                                                                    Config.list_basic_attr['create_by'],
                                                                    Config.list_basic_attr['create_date'],
                                                                    Config.list_basic_attr['update_by'],
                                                                    Config.list_basic_attr['update_date'],
                                                                    Config.list_basic_attr['remarks'],
                                                                    Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_real_time,str_insert)
            del_cd_real_time = Config.del_cd_train_real_time.format(uuid_real_time)
            str_delete = self.merge_sql(del_cd_real_time,str_delete)
        else:
            list_trainName = key.split("###")
            uuid_train_type = self.gene_uuid()
            uuid_trian_no = self.gene_uuid()
            uuid_real_time = self.gene_uuid()
            sql_cd_train_type = Config.sql_cd_train_type.format(uuid_train_type,list_trainName[0],list_trainName[1],
                                                                Config.list_basic_attr['create_by'],
                                                                Config.list_basic_attr['create_date'],
                                                                Config.list_basic_attr['update_by'],
                                                                Config.list_basic_attr['update_date'],
                                                                Config.list_basic_attr['remarks'],
                                                                Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_train_type,str_insert)
            sql_cd_train_no = Config.sql_cd_train_no.format(uuid_trian_no,uuid_train_type,list_trainName[-1],
                                                            uuid_real_time,
                                                            Config.list_basic_attr['create_by'],
                                                            Config.list_basic_attr['create_date'],
                                                            Config.list_basic_attr['update_by'],
                                                            Config.list_basic_attr['update_date'],
                                                            Config.list_basic_attr['remarks'],
                                                            Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_train_no,str_insert)
            sql_cd_real_time = Config.sql_cd_train_real_time.format(uuid_real_time, uuid_trian_no,
                                                                    Config.nowTime, "运行",
                                                                    Config.list_basic_attr['create_by'],
                                                                    Config.list_basic_attr['create_date'],
                                                                    Config.list_basic_attr['update_by'],
                                                                    Config.list_basic_attr['update_date'],
                                                                    Config.list_basic_attr['remarks'],
                                                                    Config.list_basic_attr['del_flag'])
            str_insert = self.merge_sql(sql_cd_real_time, str_insert)

            del_cd_train_type = Config.del_cd_train_type.format(uuid_train_type)
            str_delete = self.merge_sql(del_cd_train_type, str_delete)
            del_cd_train_no = Config.del_cd_train_no.format(uuid_trian_no)
            str_delete = self.merge_sql(del_cd_train_no, str_delete)
            del_cd_real_time = Config.del_cd_train_real_time.format(uuid_real_time)
            str_delete = self.merge_sql(del_cd_real_time, str_delete)
        dict_prewrite_train_real_time_uuid[key] = uuid_real_time
        return dict_prewrite_train_real_time_uuid,str_insert,str_delete

    def sql_write_train_info(self, uuid_real_time, list_value, file_flag, str_insert, str_delete, uuid_train_no):
        #  以下过程为生成对应表格的sql语句，并保存成文件
        if file_flag == "train_info":
            for i in list_value:
                uuid_affiliated = self.gene_uuid()
                uuid_application = self.gene_uuid()
                i[2] = self.str_2_datetime(i[2])
                sql_cd_affiliated = Config.sql_cd_affiliated.format(uuid_affiliated, uuid_real_time, i[0], i[1], i[2], i[3],
                                                                    Config.list_basic_attr['create_by'],
                                                                    Config.list_basic_attr['create_date'],
                                                                    Config.list_basic_attr['update_by'],
                                                                    Config.list_basic_attr['update_date'],
                                                                    Config.list_basic_attr['remarks'],
                                                                    Config.list_basic_attr['del_flag'], "NULL", "NULL")
                str_insert = self.merge_sql(sql_cd_affiliated, str_insert)
                sql_cd_application = Config.sql_cd_application.format(uuid_application, uuid_real_time, i[-2],
                                                                      i[-1].strip("\n "), "NULL",
                                                                      Config.list_basic_attr['create_by'],
                                                                      Config.list_basic_attr['create_date'],
                                                                      Config.list_basic_attr['update_by'],
                                                                      Config.list_basic_attr['update_date'],
                                                                      Config.list_basic_attr['remarks'],
                                                                      Config.list_basic_attr['del_flag'], "NULL")
                str_insert = self.merge_sql(sql_cd_application, str_insert)

                del_cd_affiliated = Config.del_cd_affiliated.format(uuid_affiliated)
                str_delete = self.merge_sql(del_cd_affiliated, str_delete)
                del_cd_application = Config.del_cd_application.format(uuid_application)
                str_delete = self.merge_sql(del_cd_application, str_delete)

        elif file_flag == "train_mileage":
            for i in list_value:
                uuid_mileage = self.gene_uuid()
                i[1] = self.str_2_datetime(i[1])
                sql_cd_mileage = Config.sql_cd_mileage.format(uuid_mileage, uuid_real_time, 0, i[2].strip("\n "), i[1],
                                                              "", Config.list_basic_attr['create_by'],
                                                              Config.list_basic_attr['create_date'],
                                                              Config.list_basic_attr['update_by'],
                                                              Config.list_basic_attr['update_date'],
                                                              Config.list_basic_attr['remarks'],
                                                              Config.list_basic_attr['del_flag'])
                str_insert = self.merge_sql(sql_cd_mileage, str_insert)
                del_cd_mileage = Config.del_cd_mileage.format(uuid_mileage)
                str_delete = self.merge_sql(del_cd_mileage, str_delete)
        elif file_flag == "train_repair":
            for i in list_value:
                uuid_repair = self.gene_uuid()
                i[2] = self.str_2_datetime(i[2])
                i[3] = self.str_2_datetime(i[3])
                sql_cd_repair = Config.sql_cd_repair_process.format(uuid_repair, uuid_real_time, "NULL", "NULL", i[0], i[1],
                                                                    i[2], i[3], i[2], i[3], "NULL", "NULL",
                                                                    Config.list_basic_attr['create_by'],
                                                                    Config.list_basic_attr['create_date'],
                                                                    Config.list_basic_attr['update_by'],
                                                                    Config.list_basic_attr['update_date'],
                                                                    Config.list_basic_attr['del_flag'],
                                                                    Config.list_basic_attr['remarks'])
                str_insert = self.merge_sql(sql_cd_repair, str_insert)
                del_cd_repair = Config.del_cd_repair_process.format(uuid_repair)
                str_delete = self.merge_sql(del_cd_repair, str_delete)

        elif file_flag == "train_object":
            dict_object = {}
            for i in list_value:
                uuid_object = self.gene_uuid()
                dict_object[i[0]] = uuid_object
                sql_cd_fault_object = Config.sql_cd_fault_object.format(uuid_object, i[1], i[1], "NULL", "NULL", "NULL", "Y",
                                                                        "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", i[2],
                                                                        "NULL", "NULL", 1, "NULL", "NULL", "NULL",
                                                                        Config.list_basic_attr['create_by'],
                                                                        Config.list_basic_attr['create_date'],
                                                                        Config.list_basic_attr['update_by'],
                                                                        Config.list_basic_attr['update_date'],
                                                                        Config.list_basic_attr['del_flag'],
                                                                        Config.list_basic_attr['remarks'],
                                                                        uuid_train_no, uuid_object, i[0], "NULL")
                str_insert = self.merge_sql(sql_cd_fault_object, str_insert)
                del_cd_fault_object = Config.del_cd_fault_object.format(uuid_object)
                str_delete = self.merge_sql(del_cd_fault_object, str_delete)

                parent_id = dict_object[i[3]] if i[3] != "" else "NULL"
                parend_ids = ",".join([dict_object[i] for i in i[-1].strip("\n ").split("/")]) if i[3] != "" else "NULL"
                if parent_id == "NULL":
                    sql_cd_fault_object_tree = Config.sql_cd_fault_object_tree_1.format(i[1], parent_id, parend_ids,uuid_object)
                else:
                    sql_cd_fault_object_tree = Config.sql_cd_fault_object_tree_2.format(i[1], parent_id, parend_ids,uuid_object)
                str_insert = self.merge_sql(sql_cd_fault_object_tree, str_insert)
                del_cd_fault_object_tree = Config.del_cd_fault_object_tree.format(uuid_object)
                str_delete = self.merge_sql(del_cd_fault_object_tree, str_delete)

        elif file_flag == "train_pattern":
            for i in list_value:
                uuid_pattern = self.gene_uuid()
                sql_cd_object_object = Config.sql_cd_fault_pattern.format(uuid_pattern, i[0], i[1], i[3], "NULL", "NULL", "NULL",
                                                                          Config.list_basic_attr['create_by'],
                                                                          Config.list_basic_attr['create_date'],
                                                                          Config.list_basic_attr['update_by'],
                                                                          Config.list_basic_attr['update_date'],
                                                                          Config.list_basic_attr['remarks'],
                                                                          Config.list_basic_attr['del_flag'], i[1], i[2])
                str_insert = self.merge_sql(sql_cd_object_object, str_insert)
                del_cd_object_pattern = Config.del_cd_fault_pattern.format(uuid_pattern)
                str_delete = self.merge_sql(del_cd_object_pattern, str_delete)
        else:
            pass
        return str_insert,str_delete


    def sql_write_fault_order(self,list_,str_insert,str_delete):
        NowTime = int(datetime.datetime.now().strftime('%Y%m%d%H%M%S')[4:])
        uuid_header = self.gene_uuid()
        uuid_detail = self.gene_uuid()
        uuid_op_train = self.gene_uuid()
        uuid_real = self.gene_uuid()
        uuid_intuitive = self.gene_uuid()
        uuid_handle = self.gene_uuid()
        uuid_associated = self.gene_uuid()

        list_[3] = self.str_2_datetime(list_[3])
        sql_op_header = Config.sql_op_fault_order_header.format(uuid_header, 100, NowTime, list_[0], list_[10],
                                                                list_[3], list_[1], list_[2],
                                                                Config.list_basic_attr['create_by'],
                                                                Config.list_basic_attr['create_date'],
                                                                Config.list_basic_attr['update_by'],
                                                                Config.list_basic_attr['update_date'],
                                                                Config.list_basic_attr['remarks'],
                                                                Config.list_basic_attr['del_flag'])
        str_insert = self.merge_sql(sql_op_header, str_insert)
        del_op_header = Config.del_op_fault_order_header.format(uuid_header)
        str_delete = self.merge_sql(del_op_header, str_delete)

        sql_op_detail = Config.sql_op_fault_order_detail.format(uuid_detail, uuid_header, list_[3], list_[4],Config.nowTime,
                                                                "NULL", "NULL", list_[5], "NULL", list_[7],list_[8], "NULL",
                                                                Config.list_basic_attr['create_by'],
                                                                Config.list_basic_attr['create_date'],
                                                                Config.list_basic_attr['update_by'],
                                                                Config.list_basic_attr['update_date'],
                                                                Config.list_basic_attr['remarks'],
                                                                Config.list_basic_attr['del_flag'],
                                                                "NULL", "NULL", "NULL", list_[6],"NULL", "NULL", "NULL", "NULL")
        str_insert = self.merge_sql(sql_op_detail, str_insert)
        del_op_detail = Config.del_op_fault_order_detail.format(uuid_detail)
        str_delete = self.merge_sql(del_op_detail, str_delete)

        list_[12] = float(list_[12]) if list_[12] != "" else 0
        list_[13] = float(list_[13]) if list_[13] != "" else 0
        sql_op_train = Config.sql_op_train.format(uuid_op_train, Config.list_basic_attr['create_by'],
                                                  Config.list_basic_attr['create_date'],
                                                  Config.list_basic_attr['update_by'],
                                                  Config.list_basic_attr['update_date'],
                                                  Config.list_basic_attr['remarks'], Config.list_basic_attr['del_flag'],
                                                  uuid_detail, list_[9], list_[15], "01", "NULL", "NULL", list_[11], list_[11],
                                                  list_[12], list_[13], list_[10], list_[14], "NULL")
        str_insert = self.merge_sql(sql_op_train, str_insert)
        del_op_train = Config.del_op_train.format(uuid_op_train)
        str_delete = self.merge_sql(del_op_train, str_delete)

        pattern_chain = (len(list_[17].split(".")) - 2) * "." + list_[19]
        sql_op_intuitive = Config.sql_op_fault_intuitive.format(uuid_intuitive, Config.list_basic_attr['create_by'],
                                                                Config.list_basic_attr['create_date'],
                                                                Config.list_basic_attr['update_by'],
                                                                Config.list_basic_attr['update_date'],
                                                                Config.list_basic_attr['remarks'],
                                                                Config.list_basic_attr['del_flag'], uuid_detail,
                                                                list_[17],list_[17], list_[16], "NULL", list_[19],
                                                                list_[18], "NULL", 1, pattern_chain)
        str_insert = self.merge_sql(sql_op_intuitive, str_insert)
        del_op_intuitive = Config.del_op_fault_intuitive.format(uuid_intuitive)
        str_delete = self.merge_sql(del_op_intuitive, str_delete)

        pattern_chain = (len(list_[21].split(".")) - 2) * "." + list_[19]
        sql_op_real = Config.sql_op_fault_real.format(uuid_real, Config.list_basic_attr['create_by'],
                                                      Config.list_basic_attr['create_date'],
                                                      Config.list_basic_attr['update_by'],
                                                      Config.list_basic_attr['update_date'],
                                                      Config.list_basic_attr['remarks'],
                                                      Config.list_basic_attr['del_flag'], uuid_detail, list_[21],
                                                      list_[20], "NULL","NULL", list_[22], "NULL", list_[24], list_[25],
                                                      "NULL", 1, list_[23], pattern_chain)
        str_insert = self.merge_sql(sql_op_real, str_insert)
        del_op_real = Config.del_op_fault_real.format(uuid_real)
        str_delete = self.merge_sql(del_op_real, str_delete)

        sql_op_handle = Config.sql_op_fault_handle.format(uuid_handle, Config.list_basic_attr['create_by'],
                                                          Config.list_basic_attr['create_date'],
                                                          Config.list_basic_attr['update_by'],
                                                          Config.list_basic_attr['update_date'],
                                                          Config.list_basic_attr['remarks'],
                                                          Config.list_basic_attr['del_flag'], uuid_real, "NULL", list_[2],
                                                          Config.nowTime, "NULL", list_[26], 1, "NULL", list_[27], 1,
                                                          list_[28], list_[29], "NULL", list_[6])
        str_insert = self.merge_sql(sql_op_handle, str_insert)
        del_op_handle = Config.del_op_fault_handle.format(uuid_handle)
        str_delete = self.merge_sql(del_op_handle, str_delete)

        sql_op_associated = Config.sql_op_fault_associated_subject.format(uuid_associated,
                                                                          Config.list_basic_attr['create_by'],
                                                                          Config.list_basic_attr['create_date'],
                                                                          Config.list_basic_attr['update_by'],
                                                                          Config.list_basic_attr['update_date'],
                                                                          Config.list_basic_attr['remarks'],
                                                                          Config.list_basic_attr['del_flag'],
                                                                          uuid_detail, list_[30], list_[31].strip("\n "), "NULL")
        str_insert = self.merge_sql(sql_op_associated, str_insert)
        del_op_associated = Config.del_op_fault_associated_subject.format(uuid_associated)
        str_delete = self.merge_sql(del_op_associated, str_delete)

        return str_insert,str_delete

    def main(self):
        list_preread_dirs = self.root_dir_judge(Config.read_path_root)
        print(list_preread_dirs)

        db,cur = self.connect_mysql(Config.mysql_config)
        str_insert_ti = ""
        str_delete_ti = ""
        str_insert_fo = ""
        str_delete_fo = ""
        today = datetime.datetime.today().strftime('%Y%m%d')

        cur.execute(Config.exec_all_train_name)
        dict_all_train_uuid = {x[0]: x[1] for x in cur.fetchall()}
        dict_train_info = self.train_info_tran2dict(self.read_data(list_preread_dirs[0])[2:])
        dict_prewrite_train_real_time_uuid ={}
        for k,v in dict_train_info.items():
            dict_prewrite_train_real_time_uuid, str_insert_ti, str_delete_ti = \
                self.trainNo_realTime_unique_judge(k, dict_all_train_uuid, str_insert_ti, str_delete_ti,dict_prewrite_train_real_time_uuid)
        for i in str_insert_ti.split(";"):
            cur.execute(i) if i.strip("\n ") != "" else 0

        cur.execute(Config.exec_all_train_name)
        dict_all_train_uuid = {x[0]: x[1] for x in cur.fetchall()}
        for path in list_preread_dirs:
            file_name = path.replace("\\","/").split("/")[-1][:-4]
            if file_name in ("train_info","train_mileage", "train_repair", "train_object", "train_pattern"):
                dict_file_info = self.train_info_tran2dict(self.read_data(path)[2:])
                for k,v in dict_file_info.items():
                    sys.exit(1) if k not in dict_prewrite_train_real_time_uuid.keys() else 0  # 此处加入日志记录
                    str_insert_ti, str_delete_ti = \
                        self.sql_write_train_info(dict_prewrite_train_real_time_uuid[k], v, file_name, str_insert_ti, str_delete_ti, dict_all_train_uuid[k])
                path_write = os.path.join(Config.write_path_root,"{}_train_info_insert.sql".format(today))
                path_delete = os.path.join(Config.write_path_root,"{}_train_info_delete.sql".format(today))
                self.write_2_txt(path_write, str_insert_ti)
                self.write_2_txt(path_delete, str_delete_ti)



            elif file_name in ("fault_order"):
                list_fault_order = self.read_data(Config.read_path_fault_order)[2:]
                for i in list_fault_order:
                    cur.execute(Config.exec_judge_fault_no.format(i[0]))
                    # if cur.fetchall()[0][0] != 0:
                    str_insert_fo, str_delete_fo = self.sql_write_fault_order(i, str_insert_fo, str_delete_fo)
                    # else:
                    #     continue
                path_write = os.path.join(Config.write_path_root, "{}_fault_order_insert.sql".format(today))
                path_delete = os.path.join(Config.write_path_root,"{}_fault_order_delete.sql".format(today))
                self.write_2_txt(path_write, str_insert_fo)
                self.write_2_txt(path_delete, str_delete_fo)


            # dict_train_mileage = self.train_info_tran2dict(self.read_data(Config.read_path_train_mileage)[2:])
            # print(dict_train_mileage)
            # dict_train_repair = self.train_info_tran2dict(self.read_data(Config.read_path_train_repair)[2:])
            # print(dict_train_repair)
            # dict_train_object = self.train_info_tran2dict(self.read_data(Config.read_path_train_object)[2:])
            # print(dict_train_object)
            # dict_object_pattern = self.train_info_tran2dict(self.read_data(Config.read_path_train_pattern)[2:])
            # print(dict_object_pattern)
            # list_train_key = self.gene_unique_train_key(dict_train_info, dict_train_mileage, dict_train_repair)
            # print(list_train_key)
            # self.sql_write_train_info(list_train_key,dict_train_info,dict_train_mileage,dict_train_repair,dict_train_object,dict_object_pattern)

            # list_fault_order = self.read_data(Config.read_path_fault_order)[2:]
            # print(list_fault_order)
            # self.sql_write_fault_order(list_fault_order)

            # path_new = path[:-4] + "_over.csv"
            # os.rename(path,path_new)

        # db.commit()
        db.close()

offline_data_to_mysql_func().main()











