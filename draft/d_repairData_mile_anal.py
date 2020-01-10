#!/bin/env python3
#coding=utf-8
# @Time    : 2019/10/23 下午15:00
# @Author  : Victor
# @Site    : 
# @File    : d_repairData_mile_anal.py
# @Software: PyCharm
import datetime

import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import matplotlib
import basic_config.bc_log as log
import basic_config.bc_info as config
import component.c_dbconn as dbconn
import component.c_sql as sql
import entity.e_db_dml as db_dml
import component.c_error as err
import entity.e_normal_calc as normal_calc
import csv

matplotlib.rcParams['font.family']=['SimHei'] #用来正常显示中文标签
matplotlib.rcParams['axes.unicode_minus']=False #用来正常显示负号

class array_deal():
    def array_contain(self,base_array,ref_array,axis=0):
        """
        数组包含检查
        :param base_array:
        :type base_array:
        :param ref_array:
        :type ref_array:
        :param axis: 0，按行  1，按列
        :type axis:
        :return: 0，不包含， 1，包含   -1，错误
        :rtype:
        """
        if len(np.shape(ref_array))==1:
            if axis == 0:
                ref_array = np.reshape(ref_array,(1,len(ref_array)))
            elif axis == 1:
                ref_array = np.reshape(ref_array, (len(ref_array,1)))

        if axis==0:
            if np.shape(base_array)[1] == np.shape(ref_array)[1]:
                for array in base_array:
                    if (array==ref_array).all():
                        return 1
                return 0
            else:
                return -1
        elif axis==1:
            if np.shape(base_array)[0] == np.shape(ref_array)[0]:
                for array in base_array.T:
                    if (array==ref_array.T).all():
                        return 1
                return 0
            else:
                return -1
        else:
            return -1

    def contain_count(self, base_array, ref_array, axis=0):
        """
        数组包含次数统计
        :param base_array:
        :type base_array:
        :param ref_array:
        :type ref_array:
        :param axis: 0，按行  1，按列
        :type axis:
        :return: x，包含次数   -1，错误
        :rtype:
        """
        i = 0
        if axis == 0:
            if np.shape(base_array)[1] == np.shape(ref_array)[1]:
                for array in base_array:
                    if (array == ref_array).all():
                        i += 1
                return i
            else:
                return -1
        elif axis == 1:
            if np.shape(base_array)[0] == np.shape(ref_array)[0]:
                for array in base_array.T:
                    if (array == ref_array.T).all():
                        i += 1
                return i
            else:
                return -1
        else:
            return -1

    def array_filter(self,target_array=np.array(None),ref_array=np.array(None),target_cols=(),ref_cols=()):
        try:
            if np.ndarray==type(ref_array) and ref_array.shape[1]>0:
                pass
            else:
                ref_array=np.reshape(ref_array,(1,len(ref_array)))
        except IndexError as e:
            ref_array = np.reshape(ref_array, (1, len(ref_array)))

        for ndim in range(ref_array.shape[0]):
            target_array=target_array[np.unique(np.where(target_array[:, target_cols] != ref_array[ndim,ref_cols])[0]),:]
        return target_array

    def array_unique(self,base_array):
        """

        :param base_array:
        :type base_array:
        :return: unique array, duplicate array
        :rtype:
        """
        A = base_array.copy()
        # Perform lex sort and get the sorted array version of the input
        sorted_idx = np.lexsort(A.T)
        sorted_Ar = A[sorted_idx, :]

        # Mask of start of each unique row in sorted array
        mask = np.append(True, np.any(np.diff(sorted_Ar, axis=0), 1))

        # Get counts of each unique row
        unq_count = np.bincount(mask.cumsum() - 1)

        # Compare counts to 1 and select the corresponding unique row with the mask
        # unq_count == 1 means you just want an array appear once
        # unq_count == 2 means you just want an array appear twice
        out = sorted_Ar[mask][np.nonzero(unq_count >= 1)[0]]
        dup = sorted_Ar[mask][np.nonzero(unq_count >= 2)[0]]
        return out,dup

class deal_data():
    def Write2CsvByrows(self, list_contents, list_column, path_csv):
        with open(path_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_column)
            writer.writerows(list_contents)

    def get_data(self,carriage):
        # sql = 'select date_format(mile_time,"%Y/%m/%d %H:%i:%s"),mile from interface.if_train_mile_history where carriage="0201" order by 1,2'
        sql = 'select mile_time,mile,source from interface.if_train_mile_history where carriage="%s" and deal_type is null  order by 1,2' % carriage
        try:
            db = dbconn.db()
            conn = db.create_conn()
            cursor = conn.cursor()
            # print(sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            conn.rollback()
            cursor.close()
            conn.close()
            db.close_conn()
        except Exception as e:
            if 'db' in locals().keys():
                db.close_conn()
            raise
        return results

    def get_carriage_data(self,data=None):
        if data is None or (type(data) == str and data.lower() == 'all'):
            # sql = 'select distinct train_no from darams.cd_train_no '
            sql = 'select distinct carriage from interface.if_train_mile_history_yx '  ##yx_1023
        else:
            if type(data)==str or type(data) == int:
                sql = 'select distinct train_no from darams.cd_train_no where train_no = "%s"' % data
            else:
                sql = 'select distinct train_no from darams.cd_train_no where train_no in ("%s")' % '","'.join(data)
        try:
            db = dbconn.db()
            conn = db.create_conn()
            cursor = conn.cursor()
            # print(sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            conn.rollback()
            cursor.close()
            conn.close()
            db.close_conn()
        except Exception as e:
            if 'db' in locals().keys():
                db.close_conn()
            raise
        return results

    def get_mile_info_mysql(self,carriage):
        if config.env['env']=='prd':
            get_sql = sql.darams_data_clear().get_a_train_mile_info % (carriage,carriage)
        # elif config.env['hostname']==config.dev_pc[0]:
        #     get_sql = sql.darams_data_clear().get_train_mile_info_debug % carriage
        else:
            # todo debug need to be modify
            # get_sql = sql.darams_data_clear().get_a_train_mile_info % (carriage, carriage)
            get_sql = sql.darams_data_clear().get_train_mile_info_debug % carriage
        try:
            db = dbconn.db()
            conn = db.create_conn()
            cursor = conn.cursor()
            # print(get_sql)
            cursor.execute(get_sql)
            results = cursor.fetchall()
            conn.rollback()
            cursor.close()
            conn.close()
            db.close_conn()
        except Exception as e:
            if 'db' in locals().keys():
                db.close_conn()
            log.log(config.set_loglevel().error(),get_sql, e)
            raise e
        return results

    def get_noise_data(self,data):
        """
        获得噪音数据
        :param data:  待处理数据集合，车辆、时间、里程、类型（故障单or列车）、表名、主键值
        :type data: list
        :return: 待删除数据集合
        :rtype: list
        """
        # data none value check
        a_data = np.array(data)
        # array判断为空
        d_data = a_data[a_data[:,1]==[None],:]
        a_data = np.delete(a_data, np.where(a_data[:, 1]==[None]), 0)
        if len(d_data)==0:
            d_data = a_data[a_data[:, 2] == [None], :]
        else:
            d_data = np.row_stack((d_data,a_data[a_data[:,2]==[None],:]))
        a_data = np.delete(a_data, np.where(a_data[:, 2] == [None]), 0)
        # a_data = a_data[a_data[:, 1].argsort()]
        # a_data = a_data[a_data[:, 1].argsort()].tolist()

        # data align
        # bugfix base date set 20190108 luqian start
        base_time = a_data[0][1].replace(hour=0, minute=0, second=0)
        # bugfix base date set 20190108 luqian end
        aa_data = a_data.copy()
        # for i in aa_data:
        #     i[1] = (i[1] - base_time).days
        mile = 0
        datedelta= 0
        index = 0
        while index < len(aa_data):
            aa_data[index][1] = (aa_data[index][1]-base_time).days
            if (aa_data[index][2] == mile and datedelta == aa_data[index][1]):   # del_logic_1
                aa_data = np.delete(aa_data,index,0)
                a_data = np.delete(a_data,index,0)
                index -= 1
            else:
                mile = aa_data[index][2]
                datedelta = aa_data[index][1]
            index += 1

        a_d_data = aa_data.copy()
        aa_data = np.delete(aa_data,(0,4,5),1)
        aa_data = aa_data.astype(np.float64)
        # data noise check
        clear = mile_verify(aa_data)
        # clear.set_data(aa_data)
        clear_data,delete_data = clear.main_logic()  # del_logic_2
        # return noise data
        if delete_data is None or type(delete_data) == str or delete_data.max()==-1:
            delete_data=[]
        else:
            delete_data = np.delete(delete_data,0,0)
            delete_data = delete_data.astype(np.int64)

        if len(delete_data) !=0:
            if len(d_data) == 0:
                d_data = a_data[delete_data[:,3]]
            else:
                d_data = np.row_stack((d_data,a_data[delete_data[:,3]]))

        # enhancement if clear_data = -1 ,all the data should be marked as noisy data start
        if type(clear_data) == int and clear_data == -1:
            # bugfix type error, need to be aligned as numpy array 20190116 luqian start
            # d_data = data
            d_data = np.array(data)
            # bugfix type error, need to be aligned as numpy array 20190116 luqian end
        # enhancement if clear_data = -1 ,all the data should be marked as noisy data end
        return d_data.tolist() if len(d_data)!=0 else []

    def noise_data_delete(self,data,clean_data,commit_flag=False):
        """
        data and clean_data like [['0201' datetime.datetime(2017, 8, 1, 0, 0) 1001355.0 1 'darams.cd_mileage' 'cec56021ecf744abbfa73473f5d63d11000'] ... ]
        :param data:
        :type data:
        :param clean_data:
        :type clean_data:
        :param commit_flag:
        :type commit_flag:
        :return:
        :rtype:
        """
        noise_data_train_his = data[np.where(data[:, 3] == 1)]
        noise_data_train_order = data[np.where(data[:, 3] == 0)]
        # delete from train_his
        del_cnt = self.noise_data_delete_train_his(noise_data_train_his[:, (4, 5)].tolist(),commit_flag)
        # delete from train_order
        del_cnt += self.noise_data_delete_train_order(noise_data_train_order,clean_data,commit_flag)
        del noise_data_train_his
        del noise_data_train_order
        return del_cnt

    def noise_data_delete_train_his(self,data,commit_flag):
        """
        删除噪音数据
        :param data: （表名,id) , true/false
        :type data: list
        :return:
        :rtype:
        """
        try:
            db = dbconn.db()
            conn = db.create_conn()
            cursor = conn.cursor()
            if config.env['env'] =='prd':
                column_name = ('id','del_flag','update_by','update_date')
            else:
                column_name = ('keyid', 'deal_type')
                # column_name = ('id','del_flag','update_by','update_date')
            delete_cnt = 0
            for t_data in data:
                if config.env['env'] == 'prd':
                    column_value = (t_data[1],2,log.debug().function_name(),datetime.datetime.now().strftime(config.datetime_style().long_style()))
                    table_name = t_data[0]
                elif config.env['hostname'] == config.dev_pc[0]:
                    column_name = list(column_name).append('deal_type')
                    column_value = (t_data[1],'no_action')
                    table_name = 'interface.if_train_mile_history'
                else:
                    column_value = (t_data[1],2, log.debug().function_name(),datetime.datetime.now().strftime(config.datetime_style().long_style()))
                    table_name = t_data[0]
                delete_cnt += db_dml.db_dml().update_table_record(table_name,column_name,column_value,conn)
            # results = cursor.fetchall()
            if commit_flag:
                conn.commit()
            else:
                conn.rollback()
            cursor.close()
            conn.close()
            db.close_conn()
        except Exception as e:
            if 'conn' in locals().keys():
                conn.rollback()
                conn.close()
            if 'db' in locals().keys():
                db.close_conn()
            log.log(config.set_loglevel().error(), log.debug().position(), e)
            raise e

        return delete_cnt

    def noise_data_delete_train_order(self,noise_data,clean_data,commit_flag):
        """
        data and clean_data like [['0201' datetime.datetime(2017, 8, 1, 0, 0) 1001355.0 1 'darams.cd_mileage' 'cec56021ecf744abbfa73473f5d63d11000'] ...]
        :param noise_data:
        :type noise_data:
        :param clean_data:
        :type clean_data:
        :param commit_flag:
        :type commit_flag:
        :return:
        :rtype:
        """
        con_t = datetime.datetime(2015,1,1,0,0,1)
        t_noise_data = noise_data.copy()
        t_clean_data = clean_data.copy()
        # convert datetime to timedelta by double type
        t_noise_data[:,1] = (t_noise_data[:,1]-con_t).astype(np.timedelta64)/np.timedelta64(1,'D')
        t_clean_data[:, 1] = (t_clean_data[:, 1] - con_t).astype(np.timedelta64) / np.timedelta64(1, 'D')
        for noise_data_1 in t_noise_data:
            left_cnt = 0
            right_cnt = 0
            left_p = None
            right_p = None
            ret_mile = None
            # find the linear point
            left_cnt = t_clean_data[np.where(t_clean_data[:,1]<=noise_data_1[1])[0][:1]].shape[0]
            right_cnt = t_clean_data[np.where(t_clean_data[:,1]>=noise_data_1[1])[0][:1]].shape[0]

            if left_cnt == 1 and right_cnt == 1:
                left_p = t_clean_data[np.where(t_clean_data[:,1]<=noise_data_1[1])[0][-1]]
                right_p = t_clean_data[np.where(t_clean_data[:,1]>=noise_data_1[1])[0][0]]
            elif left_cnt == 0 and right_cnt == 1:
                if t_clean_data[np.where(t_clean_data[:, 1] >= noise_data_1[1])[0][:2]].shape[0] == 2:
                    left_p = t_clean_data[np.where(t_clean_data[:, 1] >= noise_data_1[1])[0][0]]
                    right_p = t_clean_data[np.where(t_clean_data[:, 1] >= noise_data_1[1])[0][1]]
                else:
                    pass
            elif left_cnt == 1 and right_cnt == 0:
                if t_clean_data[np.where(t_clean_data[:, 1] <= noise_data_1[1])[0][:2]].shape[0] == 2:
                    left_p = t_clean_data[np.where(t_clean_data[:, 1] <= noise_data_1[1])[0][-2]]
                    right_p = t_clean_data[np.where(t_clean_data[:, 1] <= noise_data_1[1])[0][-1]]
            else:
                pass

            # linear points exist, to get the interpolation
            if left_p is not None and right_p is not None:
                ret_mile = normal_calc.interpolate_standard().interpolation(left_p[1],right_p[1],left_p[2],right_p[2],noise_data_1[1])

            # new mile is caculated
            if ret_mile is not None and commit_flag:
                table_name = noise_data_1[4]
                column_name = ('id','accumulated_mileage')
                if np.isnan(ret_mile):
                    if noise_data_1[1] == left_p[1]:
                        ret_mile = left_p[2]
                    elif noise_data_1[1] == right_p[1]:
                        ret_mile = right_p[2]
                    else:
                        ret_mile = 0
                        log.log(log.set_loglevel().error(),log.debug().position(),'interpolate error %s %s %s %s %s' %(left_p[1],right_p[1],left_p[2],right_p[2],noise_data_1[1]))
                if ret_mile < 0:
                    ret_mile = 0
                column_value = (noise_data_1[5],int(ret_mile))
                db_dml.db_dml().update_table_record(table_name,column_name,column_value)
        return t_noise_data.shape[0]

    def all_carriage_noise_deal(self,data=None):
        cur_time = datetime.datetime.now()
        carriages=self.get_carriage_data(data)
        all_records = 0
        all_noise_records=0
        all_delete_records=0
        commit_flag = True
        # esdb=dbconn.db_es()
        # esconn=esdb.create_conn()
        count = 0
        for carriage in carriages:
            count += 1
            p=1
            try:
                total_record = 0
                noise_record = 0
                delete_record = 0
                log.log(log.set_loglevel().warning(),log.debug().position(),'deal %s starting' % carriage)
                all_data = self.get_mile_info_mysql(carriage[0])
                if len(all_data)==0:
                    continue
                # dtype=[('train_no','U40'),('mile_time','datetime64[s]'),('mile',np.float64),('type',np.int64),('source','U40'),('keyid','U40')]
                # all_data_np = np.array(all_data,dtype=dtype)
                # 20180916 luqian start bugfix data need to be ordered
                # all_data_np = all_data_np[all_data_np[:,1].argsort()]
                # all_data_np = np.sort(all_data_np,order=['mile_time','mile'])
                # 20180916 luqian end bugfix data need to be ordered
                all_data_np = np.array(all_data)
                clean_data_np=all_data_np.copy()
                noise_data_del=np.array(None)
                noise_data_set=noise_data_del
                total_record = len(all_data)
                p=2
                if len(all_data)>2:
                    noise_data = self.get_noise_data(all_data)
                    noise_record = len(noise_data)
                    if noise_record > 0:
                        noise_data_del = np.array(noise_data)
                        # print(noise_data)
                        # noise_data_del = noise_data_del[:, (4, 5)].tolist()
                        noise_data_set = noise_data_del
                        # delete_record += self.noise_data_delete(noise_data_del[:, (4, 5)].tolist(),False)
                        # 比较列的序号集合
                        col_compare = (1,2,3)
                        col_compare_num = len(col_compare)
                        p=3
                        # 同日同时间的数据删除
                        for noise_data_1 in noise_data_del:
                            # 得到原数组中相较比较列，是否相同的数组点信息
                            row_no, col_no = np.where(all_data_np[:, col_compare] == noise_data_1[1:col_compare_num + 1])
                            # 还原符合要求的点信息所对应的数组集合
                            del_data_set = all_data_np[np.where(np.bincount(row_no) == col_compare_num), :]
                            # 清洗后数据，含过滤
                            # clean_data_np=clean_data_np[np.where(clean_data_np[:,(0,1,2)]!=noise_data_1[0:3]),:].reshape(-1,col_compare_num)
                            clean_data_np=array_deal().array_filter(clean_data_np,noise_data_1,(0,1,2),(0,1,2))
                            # 判断集合数量是否大于1
                            if len(del_data_set[0]) == 1:
                                continue

                            noise_data_set = np.row_stack((noise_data_set,del_data_set[0]))
                            # delete_record += self.noise_data_delete(del_data_set[0][:, (4, 5)].tolist(), False)

                            # 循环外的已删除的补偿
                            delete_record -= 1

                self.Write2CsvByrows(clean_data_np[:,(5,0,1,2)],["ori_id","carrige","report_time","mileage"],'F:/XJ_Meeting/7.检修数据处理需求ForDrLi/3.outputFile/{}.csv'.format(carriage[0]))  ##yx_1024
                print("{}/{} is finished!".format(count,len(carriages)))

                # reserve all the data deleted by
                # self.reserve_train_data(carriage[0])  ##yx_1024
                # deal noise_data_set

        #         ## yx_1024
        #         if not np.array_equal(noise_data_set,np.array(None)):
        #             delete_record += self.noise_data_delete(noise_data_set,clean_data_np, commit_flag)
        #         p=4
        #         log.log(log.set_loglevel().debug(),log.debug().position(),'carriage:',carriage[0] if type(carriage)==list or type(carriage) == tuple else carriage,type(carriage))
        #         log.log(log.set_loglevel().debug(), log.debug().position(), 'total_record:', total_record)
        #         log.log(log.set_loglevel().debug(), log.debug().position(), 'noise_record:', noise_record)
        #         log.log(log.set_loglevel().debug(), log.debug().position(), 'delete_record:', delete_record)
        #         # log.log(log.set_loglevel().info(), log.debug().position(), 'deal %s ended，total：%s , noise: %s , noise duplicated: %s ,deleted: %s ' % (carriage[0] if type(carriage)==list or type(carriage) == tuple else carriage,total_record,noise_record,delete_record-noise_record,delete_record))
        #         log.log(log.set_loglevel().info(), log.debug().position(),'carriage,total,noise,noise duplicated,deleted',carriage, total_record,noise_record, delete_record - noise_record, delete_record)
        #         col_name, train_type = db_dml.db_dml().query_table_record(sql.darams_data_clear.get_train_type_by_no % carriage)
        #         es_value={'train_type_code': train_type[0][0],'train_type_desc': train_type[0][1] ,'train_no':'%s' % carriage,'deal_time':cur_time,'deal_result':{'total_dp':total_record,'noise_dp':noise_record,'noise_dup_dp':delete_record-noise_record,'delete_dp':delete_record}}
        #         db_dml.db_es_dml().new_doc(index_name='train_mile_statistics',type_name='train_mile_statistics',doc_value=es_value)
        #         p=5
        #         all_data_np = all_data_np[:,(1,2)]
        #         all_data_es=[]
        #         for all_data_1 in all_data_np:
        #             all_data_es.append({'col_time':all_data_1[0],'col_mile':all_data_1[1]})
        #         # all_data_np = np.dstack((np.datetime_as_string(all_data_np[:,0].astype('datetime64[s]'),unit='s'),all_data_np[:,1].astype('U25')))
        #
        #         clean_data_np = clean_data_np[:, (1, 2)]
        #         clean_data_es = []
        #         p=6
        #         for clean_data_1 in clean_data_np:
        #             clean_data_es.append({'col_time': clean_data_1[0], 'col_mile': clean_data_1[1]})
        #         # clean_data_np = np.dstack((np.datetime_as_string(clean_data_np[:, 0].astype('datetime64[s]'), unit='s'),clean_data_np[:, 1].astype('U25')))
        #
        #         noise_data_es=[]
        #         if noise_data_del.shape != np.array(None).shape:
        #             noise_data_del = noise_data_del[:, (1, 2)]
        #             for noise_data_1 in noise_data_del:
        #                 noise_data_es.append({'col_time': noise_data_1[0], 'col_mile': noise_data_1[1]})
        #             # noise_data_del = np.dstack((np.datetime_as_string(noise_data_del[:, 0].astype('datetime64[s]'), unit='s'),noise_data_del[:, 1].astype('U25')))
        #         p=7
        #         es_detail_value = {'train_type_code': train_type[0][0],'train_type_desc': train_type[0][1] ,'train_no': '%s' % carriage, 'deal_time': cur_time, 'all_data': all_data_es, 'clean_data': clean_data_es, 'noise_data': noise_data_es}
        #         # es_detail_value={'train_no':'%s' % carriage,'deal_date':cur_time,'all_data':all_data_np[0].tolist(),'clean_data':clean_data_np[0].tolist(),'noise_data':None if noise_data_del.shape == np.array(None).shape else noise_data_del[0].tolist()}
        #
        #         # es_detail_value = {'train_no': '%s' % carriage, 'deal_date': cur_time, 'all_data': all_data_np[:, (1, 2)].tolist(), 'clean_data': clean_data_np[:, (1, 2)].tolist(), 'noise_data': None if noise_data_del.shape == np.array(None).shape else noise_data_del[:, (1, 2)].tolist()}
        #
        #         db_dml.db_es_dml().new_doc(index_name='train_mile_detail', type_name='train_mile_detail', doc_value=es_detail_value)
        #
        #         all_records += total_record
        #         all_noise_records += noise_record
        #         all_delete_records += delete_record
        #         # break
            except Exception as e:
                log.log(log.set_loglevel().error(), log.debug().position(),p,e,carriage)
                raise e
        # # 总数据，噪音数据，噪音数据的重复点，被删除的数据
        # log.log(log.set_loglevel().info(), log.debug().position(),
        #         'all deal completed，total：%s, noise: %s, noise duplicated:%s ,deleted: %s' % (
        #             all_records, all_noise_records, all_delete_records-all_noise_records, all_delete_records))
        #
        # es_value = {'train_no': '%s' % 'all', 'deal_time': cur_time, 'deal_result': {'total_dp': all_records, 'noise_dp': all_noise_records, 'noise_dup_dp': all_delete_records - all_noise_records, 'delete_dp': all_delete_records}}
        # db_dml.db_es_dml().new_doc(index_name='train_mile_statistics', type_name='train_mile_statistics', doc_value=es_value)
        #
        # # esdb.close_conn()
        # return all_records,all_noise_records,all_delete_records-all_noise_records,all_delete_records

    def get_train_mile_detail(self,train_nos=None):
        if train_nos is None:
            log.log(log.set_loglevel().error(),err.MyError.errcode[4], err.MyError.errmsg[4] )
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if train_nos is not None and type(train_nos) != list:
            train_nos=list(str(train_nos))
        ret_val = []
        for train_no in train_nos:
            res=db_dml.db_es_dml().query_doc('train_mile_detail', 'train_mile_detail',{'query':{'term':{'train_no.keyword':train_no}},'sort':[{'deal_time':{'order':'desc'}}],'size':1,'version':True})
            if len(res['hits']['hits'])>0:
                rec = res['hits']['hits'][0]['_source']
                ret_val.append(rec)
        return ret_val

    def get_train_mile_statistic(self, train_nos=None,period_from=None,period_to=None):
        if train_nos is None:
            log.log(log.set_loglevel().error(), err.MyError.errcode[4], err.MyError.errmsg[4])
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if period_from is not None and datetime.datetime.strptime(period_from,'%Y-%m-%d') > datetime.datetime.now():
            return ''
        if train_nos == 'all':
            train_nos = self.get_carriage_data()
        elif train_nos is not None and type(train_nos) != list:
            train_nos = [str(train_nos)]
        ret_val = []
        for train_no in train_nos:
            rec = res = {}
            if type(train_no) == tuple or type(train_no) == list:
                train_no = train_no[0]
            if period_from is None and period_to is None:
                res = db_dml.db_es_dml().query_doc('train_mile_statistics', 'train_mile_statistics',
                                                   {'query': {'term': {'train_no.keyword': train_no}}, 'sort': [
                                                       {'deal_time': {'order': 'desc'}}], 'size': 1, 'version': True})
                if len(res['hits']['hits']) > 0:
                    rec = res['hits']['hits'][0]['_source']
            else:
                # period paramater
                rec_from = rec_to = {}
                if period_from is not None:
                    res = db_dml.db_es_dml().query_doc('train_mile_statistics', 'train_mile_statistics',{'query': {'bool': {'must': [{'match': {'train_no': train_no}},{'range': {'deal_time': {'gte':period_from}}}]}}, 'size': 1, 'sort': [{'deal_time': {'order': 'asc'}}],'version':True})
                # else:
                #     res = db_dml.db_es_dml().query_doc('train_mile_statistics', 'train_mile_statistics',
                #                                        {'query': {'term': {'train_no.keyword': train_no}}, 'sort': [
                #                                            {'deal_time': {'order': 'asc'}}], 'size': 1, 'version': True})
                if res.__len__()>0 and len(res['hits']['hits']) > 0:
                    rec_from = res['hits']['hits'][0]['_source']

                if period_to is not None:
                    res = db_dml.db_es_dml().query_doc('train_mile_statistics', 'train_mile_statistics',{'query': {'bool': {'must': [{'match': {'train_no': train_no}},{'range': {'deal_time': {'lte':period_to}}}]}}, 'size': 1, 'sort': [{'deal_time': {'order': 'desc'}}], 'version': True})
                else:
                    res = db_dml.db_es_dml().query_doc('train_mile_statistics', 'train_mile_statistics',
                                                       {'query': {'term': {'train_no.keyword': train_no}}, 'sort': [
                                                           {'deal_time': {'order': 'desc'}}], 'size': 1, 'version': True})
                if len(res['hits']['hits']) > 0:
                    rec_to = res['hits']['hits'][0]['_source']

                if rec_from.__len__()>0 and rec_to.__len__()>0:
                    rec = rec_to
                    for i in rec_from['deal_result'].keys():
                        rec['deal_result'][i] = rec_to['deal_result'][i]-rec_from['deal_result'][i]
                else:
                    rec = rec_to if rec_to.__len__()>0 else rec_from

            if rec.__len__()>0:
                ret_val.append(rec)
        return ret_val

    def reserve_train_data(self,in_train_no):
        if in_train_no is None:
            raise err.MyError(config.position(),err.MyError.errcode[4],err.MyError.errmsg[4])
        db_conn = dbconn.db()
        conn = db_conn.create_conn()
        cursor = conn.cursor()
        cursor.execute(sql.darams_data_clear.reserve_mile_fault_order % in_train_no)
        cursor.execute(sql.darams_data_clear.reserve_mile_train_his % in_train_no)
        conn.commit()
        conn.close()
        db_conn.close_conn()
        del db_conn
        # db_dml.db_dml().query_table_record(sql.darams_data_clear.reserve_mile_fault_order % in_train_no)
        # db_dml.db_dml().query_table_record(sql.darams_data_clear.reserve_mile_train_his % in_train_no)

class show_plot:
    def Write2CsvByrows(self, list_contents, list_column, path_csv):
        with open(path_csv, 'a+', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_column)
            writer.writerows(list_contents)

    def show_all(self):
        carriages = deal_data().get_carriage_data()
        count = 1
        # for j in range(0,len(carriages),2):
        # for j in range(5,8, 2):
        for carriage in carriages:
            # data = get_data().get_data(carriages[j])
            data = deal_data().get_data(carriage)
            o_data = data
            new_data = []
            x_base = data[0][0]
            previous = data[0][0]
            for i in range(0, len(data)):
                current = data[i][0]
                if i == 0:
                    a = 0
                else:
                    current = current.replace(hour=0, minute=0, second=0)
                    x_base = x_base.replace(hour=0, minute=0, second=0)
                    a = (current - x_base).days

                if data[i][2] == 'TRAIN_DIST' or data[i][2] == 'darams.cd_train_real_time':
                    b = 1
                else:
                    b= 0
                previous = current
                new_data.append([float(a), float(data[i][1]),b])
            # print(new_data)
            data = new_data
            data = sp.reshape(data, (len(data), 3))
            # print(data)
            x = data[:, 0]
            y = data[:, 1]
            target = data[:,2]
            plt.title("%s 里程信息" % carriage)
            plt.xlabel("天数间隔")
            plt.ylabel("里程数")
            # plt.figure(count)
            # plt.subplot(211)
            # plt.plot( color="blue", linewidth=2.5, linestyle="x", label="列车信息")
            # plt.plot( color="red", linewidth=2.5, linestyle="o", label="故障单")

            for t, marker, c  in zip(sp._lib.six.xrange(2), "ox", "rb"):
                plt.scatter(data[target == t, 0],
                            data[target == t, 1],
                            marker=marker,
                            c=c)
            # plt.scatter(x, y, data=data)
            # plt.plot(x, y, color='r', markerfacecolor='blue', marker='o')
            plt.legend(labels = ['故障单', '列车数据'], loc = 'best',title='数据来源')
            plt.grid()
            plt.savefig('../logs/%s.png' % carriage)
            # plt.show()
            plt.close()
            # data = get_data().get_data(carriages[j+1])
            # new_data = []
            # x_base = data[0][0]
            # previous = data[0][0]
            # for i in range(0, len(data)):
            #     current = data[i][0]
            #     if i == 0:
            #         a = 0
            #     else:
            #         current = current.replace(hour=0, minute=0, second=0)
            #         x_base = x_base.replace(hour=0, minute=0, second=0)
            #         a = (current - x_base).days
            #     previous = current
            #     new_data.append([int(a), int(data[i][1])])
            # # print(new_data)
            # data = new_data
            # data = sp.reshape(data, (len(data), 2))
            # # print(data)
            # x = data[:, 0]
            # y = data[:, 1]
            # plt.figure(count)
            # plt.subplot(212)
            # plt.scatter(x, y, data=data)
            # plt.plot(x, y, color='r', markerfacecolor='blue', marker='o')
            # count += 1
            # if count == 2:
            #     break
        # data = get_data().get_data()
        # new_data = []
        # x_base = data[0][0]
        # previous = data[0][0]
        # for i in range(0,len(data)):
        #     current = data[i][0]
        #     if i == 0:
        #         a = 0
        #     else:
        #         current = current.replace(hour=0,minute=0,second=0)
        #         x_base = x_base.replace(hour=0,minute=0,second=0)
        #         a = (current - x_base).days
        #     previous = current
        #     new_data.append([int(a),int(data[i][1])])
        # print(new_data)
        # data = new_data
        # data = sp.reshape(data,(len(data),2))
        # print(data)
        # x = data[:, 0]
        # y = data[:, 1]
        # plt.figure(1)
        # plt.scatter(x, y, data=data)
        # plt.title("里程信息")
        # plt.xlabel("天数间隔")
        # plt.ylabel("里程数")
        # fp1, residuals, rank, sv, rcond = sp.polyfit(x, y, 1, full=True)
        # f1 = sp.poly1d(fp1)
        # f2p = sp.polyfit(x, y, 2)  # 2阶曲线
        # f2 = sp.poly1d(f2p)
        # f3p = sp.polyfit(x, y, 3)  # 2阶曲线
        # f3 = sp.poly1d(f3p)
        # # fx = sp.linspace(0, x[-1], 500)
        # fx= sp.arange(0,x[-1],1)
        # plt.subplot(211)
        # plt.scatter(x, y, data=data)
        # plt.plot(fx,f1(fx),linewidth=1)
        # plt.subplot(212)
        # plt.scatter(x, y, data=data)
        # plt.plot(fx, f2(fx), linewidth=1)
        # # for i in range(len(data)):
        # #     plt.plot(x[i],y[i], color='r')
        # # print(error(f1,x,y))
        # # plt.legend("d=%i" % f1.order)
        # # plt.autoscale(tight=True)
        # plt.figure(2)
        # plt.subplot(211)
        # plt.scatter(x, y, data=data)
        # plt.plot(fx, f3(fx), linewidth=1)
        # plt.subplot(212)
        # plt.plot(x, y, color='r', markerfacecolor='blue', marker='o')
        # for a, b in zip(x, y):
        #     plt.text(a, b, (a, b), ha='center', va='bottom', fontsize=10)
        # plt.grid()
        # plt.show()


    def test(self,i_carriage=None):
        carriage= '4114' if i_carriage is None else i_carriage
        # data = deal_data().get_data(carriage)
        data = deal_data().get_mile_info_mysql(i_carriage)
        data = np.array(data)[:,1:4].tolist()
        new_data = []
        x_base = data[0][0]
        previous = data[0][0]
        for i in range(0, len(data)):
            current = data[i][0]
            if i == 0:
                a = 0
            else:
                current = current.replace(hour=0, minute=0, second=0)
                x_base = x_base.replace(hour=0, minute=0, second=0)
                a = (current - x_base).days
            if data[i][2] == 'TRAIN_DIST':
                b = 1
            else:
                b = 0
            previous = current
            new_data.append([float(a), float(data[i][1]), b,])
        # print(new_data)
        data = new_data
        data = sp.reshape(data, (len(data),3))
        # print(data)
        x = data[:, 0]
        y = data[:, 1]
        target = data[:, 2]
        plt.title("%s 里程信息" % carriage)
        plt.xlabel("天数间隔")
        plt.ylabel("里程数")
        for t, marker, c in zip(sp._lib.six.xrange(2), "ox", "rb"):
            plt.scatter(data[target == t, 0],
                        data[target == t, 1],
                        marker=marker,
                        c=c)
        # plt.scatter(x, y, data=data)
        plt.legend(labels=['故障单', '列车数据'], loc='best', title='数据来源')
        fp1, residuals, rank, sv, rcond = sp.polyfit(x, y, 1, full=True)
        f1 = sp.poly1d(fp1)
        fx = sp.arange(0, x[-1], 1)
        plt.plot(fx, f1(fx), linewidth=1,markerfacecolor='red')
        plt.plot(x, y, markerfacecolor='blue')
        plt.grid()
        # plt.savefig('../logs/%s.png' % (carriage))
        # plt.show()
        plt.close()
        return data

    def display(self,data,outtype='P or S'):
        # a = data[0]
        # b = data[1]
        # # c = np.random.randint(0,1,size=[len(data[0]),1])
        # data = np.dstack((a,b))
        # data = data[0]
        x = data[:, 0]
        y = data[:, 1]
        target = np.hstack(np.random.randint(0,1,size=(len(data),1)))
        plt.title("里程信息")
        plt.xlabel("天数间隔")
        plt.ylabel("里程数")
        for t, marker, c in zip(sp._lib.six.xrange(2), "ox", "rb"):
            plt.scatter(data[target == t, 0],
                        data[target == t, 1],
                        marker=marker,
                        c=c)
        plt.scatter(x, y, data=data)
        # plt.legend(labels=['故障单', '列车数据'], loc='best', title='数据来源')
        # plt.plot(x, y, markerfacecolor='blue')
        plt.grid()
        if outtype == 'P':
            plt.show()
        plt.close()
        # return a

    def module_analyze_nouse(self):
        o_data = self.test('2042')
        data = np.delete(o_data,2,axis=1)
        # for test
        from scipy.spatial.distance import pdist
        for i in range(0,len(data)):
            start = min(i,abs(i-3))
            end = min(max(i,i+3),len(data))
            x = data[start:end, 0]
            y = data[start:end, 1]
            X = np.vstack([x,y])
            d2 = pdist(X, 'seuclidean')
            # print('start: %s , end: %s , dist: %s' % (start,end,d2))

        from sklearn.decomposition import PCA
        new_data=data[:,0:2]
        # print(new_data)
        data = new_data
        pca_data = np.array((data[:,0],data[:,1]))
        pca = PCA(n_components=1,copy=False)
        a = pca.fit(pca_data)
        newa = pca.fit_transform(new_data)
        print(newa)
        print(new_data)
        # print(newa)
        # newd = pca.inverse_transform(newa)
        # print(pca.components_,pca.mean_,pca.explained_variance_,pca.explained_variance_ratio_,pca.noise_variance_)
        # print(pca.singular_values_)
        self.display(new_data)

    # def show_difference(self,carriage,outtype='P or S'):
    #     data = deal_data().get_data(carriage)
    #     # data = deal_data().get_mile_info_mysql(carriage)
    #     new_data = []
    #     x_base = data[0][0]
    #     previous = data[0][0]
    #     mile = 0
    #     for i in range(0, len(data)):
    #         current = data[i][0]
    #         if mile==data[i][1]:
    #             continue
    #         else:
    #             mile = data[i][1]
    #         if i == 0:
    #             a = 0
    #         else:
    #             current = current.replace(hour=0, minute=0, second=0)
    #             x_base = x_base.replace(hour=0, minute=0, second=0)
    #             a = (current - x_base).days
    #         if data[i][2] == 'TRAIN_DIST':
    #             b = 1
    #         else:
    #             b = 0
    #         previous = current
    #         new_data.append([float(a), float(data[i][1]), b, ])
    #     # print(new_data)
    #     data = new_data
    #     data = sp.reshape(data, (len(data), 3))
    #     #######
    #     clear = mile_verify()
    #     clear.set_data(data)
    #     clear_data,delete_data = clear.main_logic()
    #     #######
    #     x = data[:, 0]
    #     y = data[:, 1]
    #     target = np.hstack(np.random.randint(0, 1, size=(len(data), 1)))
    #     plt.figure(1,figsize=(8,8))
    #     plt.subplot(211)
    #     plt.title("里程信息")
    #     # plt.xlabel("天数间隔")
    #     plt.ylabel("里程数")
    #     for t, marker, c in zip(sp._lib.six.xrange(2), "ox", "rb"):
    #         plt.scatter(data[target == t, 0],
    #                     data[target == t, 1],
    #                     marker=marker,
    #                     c=c)
    #     # plt.scatter(x, y, data=data)
    #     plt.legend(labels=['故障单', '列车数据'], loc='best', title='数据来源')
    #     plt.plot(x, y, markerfacecolor='blue')
    #     plt.grid()
    #     plt.subplot(212)
    #     plt.title("里程信息")
    #     plt.xlabel("天数间隔")
    #     plt.ylabel("里程数")
    #     x = clear_data[:, 0]
    #     y = clear_data[:, 1]
    #     plt.scatter(x, y, data=clear_data)
    #     plt.plot(x, y, markerfacecolor='red')
    #     plt.grid()
    #     if outtype == 'P':
    #         plt.show()
    #     elif outtype == 'S':
    #         plt.savefig('../logs/%s_compare.png' % carriage)
    #     plt.close()
    def show_difference(self,carriage,outtype='P or S'):
        data = deal_data().get_mile_info_mysql(carriage)
        ori_data = data.copy()
        if data is None:
            print('no data')
            return

        dict_data_date = {i[6]:i[1] for i in data }
        data = np.array(data)[:,(1,2,3,5)]

        # array判断为空
        data = np.delete(data, np.where(data[:, 1] == [None]), 0)
        data = np.delete(data, np.where(data[:, 2] == [None]), 0)

        data = data.tolist()
        new_data = []
        new_data_yx = []   ### yx_191028
        x_base = data[0][0]
        x_base = x_base.replace(hour=0, minute=0, second=0)
        current = None
        # current = 0.0  ##  191028_yx
        x_base = None
        mile = None
        previous = data[0][0]
        mile = 0
        for i in range(0, len(data)):
            if current == data[i][0].replace(hour=0, minute=0, second=0) and mile == data[i][1]:
                continue
            else:
                mile = data[i][1]
                current = data[i][0]
                current_date = data[i][0]  ##  191028_yx
                current_keyid = data[i][3]  ##  191028_yx
                if mile is None or current is None:
                    continue
            if i == 0:
                x_base = current.replace(hour=0, minute=0, second=0)
            current = current.replace(hour=0, minute=0, second=0)
            # x_base = x_base.replace(hour=0, minute=0, second=0)
            a = (current - x_base).days
            # if data[i][2] == 'TRAIN_DIST':
            if (type(data[i][2]) or data[i][2].isnumeric()) and int(data[i][2]) == 1:
                b = 1
            else:
                b = 0
            previous = current
            new_data.append([float(a), float(data[i][1]), b, ])
            new_data_yx.append([float(a), float(data[i][1]), b, current_date, current_keyid])  ## 191028_yx
            # current = data[i][0]
            # if mile==data[i][1]:
            #     continue
            # else:
            #     mile = data[i][1]
            # if i == 0:
            #     a = 0
            # else:
            #     current = current.replace(hour=0, minute=0, second=0)
            #     # x_base = x_base.replace(hour=0, minute=0, second=0)
            #     a = (current - x_base).days
            #     log.log(log.set_loglevel().warning(),a,mile)
            # # if data[i][2] == 'TRAIN_DIST':
            # if data[i][2] == 1:
            #     b = 1
            # else:
            #     b = 0
            # previous = current
            # new_data.append([float(a), float(data[i][1]) , b, ])
        # print(new_data)
        data = new_data
        data = sp.reshape(data, (len(data), 3))
        # data_yx = sp.reshape(new_data_yx, (len(new_data_yx), 4))
        #######
        index = np.fromfunction(lambda i: i % len(new_data_yx) - 1 + 1, (len(new_data_yx),)).reshape((1, len(new_data_yx)))
        data_yx = np.hstack((new_data_yx, index.T))
        #######
        # data = np.reshape(data, (len(data), 4))  ## 191028_yx
        #######
        clear = mile_verify(data)
        clear.set_data(data)
        clear_data,delete_data = clear.main_logic()
        ####### down:191028_yx
        clear_indexs = clear_data[:,3]
        try:
            delete_indexs = delete_data[:,3]
        except TypeError:
            delete_indexs = []
        # ori_data = [list(i) for i in ori_data]
        data_yx_new = []
        for index in range(len(data_yx)):
            if data_yx[index][5] in clear_indexs:
                j = list(np.append(data_yx[index], '0'))
            elif data_yx[index][5] in delete_indexs:
                j = list(np.append(data_yx[index], '1'))
            else:
                j = list(np.append(data_yx[index], '-1'))
            data_yx_new.append(j)
        data_yx_new = sp.reshape(data_yx_new, (len(data_yx_new), 7))
        ####### up:191028_yx
        # x = data[:, 0]
        # y = data[:, 1]
        # target = np.hstack(np.random.randint(0, 1, size=(len(data), 1)))
        # target = data[:,2]
        # plt.figure(1,figsize=(8,8))
        # plt.subplot(211)
        # plt.title("里程信息")
        # # plt.xlabel("天数间隔")
        # plt.ylabel("里程数")
        # for t, marker, c in zip(sp._lib.six.xrange(2), "ox", "rb"):
        #     plt.scatter(data[target == t, 0],
        #                 data[target == t, 1],
        #                 marker=marker,
        #                 c=c)
        # # plt.scatter(x, y, data=data)
        # plt.legend(labels=['故障单', '列车数据'], loc='best', title='数据来源')
        # # plt.plot(x, y, markerfacecolor='blue')
        # plt.grid()
        # plt.subplot(212)
        # plt.title("里程信息")
        # plt.xlabel("天数间隔")
        # plt.ylabel("里程数")
        # if type(clear_data) == int and clear_data == -1:
        #     pass
        # else:
        #     x = clear_data[:, 0]
        #     y = clear_data[:, 1]
        #     plt.scatter(x, y, data=clear_data)
        #     plt.plot(x, y, markerfacecolor='red')
        # plt.grid()
        if outtype == 'P':
            plt.show()
        elif outtype == 'S':
            plt.savefig('../logs/%s_compare.png' % carriage)
        elif outtype == 'W':  ##yx_1024
            # self.Write2CsvByrows(data_yx_new[:, (4, 3, 1, 6,)], ["ori_id", "report_time", "mileage", "dataType(0:CleanedData,1:UselessData)"],
            #                      'F:/XJ_Meeting/7.检修数据处理需求ForDrLi/3.outputFile_v4/{}.csv'.format(carriage))  ##yx_1028
            self.Write2CsvByrows(data_yx_new[:, (4, 3, 1, 6,)], ["ori_id", "report_time", "mileage", "dataType(0:CleanedData,1:UselessData)"],
                                 'F:/XJ_Meeting/7.检修数据处理需求ForDrLi/3.outputFile_v4/{}.csv'.format("all"))  ##yx_1113
            # self.Write2CsvByrows(ori_data[:, (5, 0, 1, 2, 7)], ["ori_id", "carrige", "report_time", "mileage", "dataType"],
            #                      'F:/XJ_Meeting/7.检修数据处理需求ForDrLi/3.outputFile_v2/{}.csv'.format(carriage))  ##yx_1024
            # print("{}/{} is finished!".format(count, len(carriages)))
        # plt.close()
        return clear_data,delete_data


class mile_verify():
    # 检查下一个点位和当前点位的差距
    # 1、下一个点位的日均大于500公里／小时*24小时，需要检查逻辑A
    # 2、下一个点位的里程数小于当前里程，需要检查逻辑A
    # 如果都满足，则当前点位有效，进入下一个点位检查
    # 竞争逻辑A，若下一个点位无效，则重新循环当前节点；若当前节点无效，则重新循环上一节点。
    #   被被删除节点淘汰的节点恢复有效。
    # 竞争逻辑A：
    # 去除当前两点，一阶函数拟合剩余点，斜率FA
    # 加入当前点，一阶函数拟合前后点（去除下一节点），斜率平均FC
    # 加入下一点，一阶函数拟合前后点（去除当前点），斜率平均FN
    # 校验FC、FN哪个更接近FA，取信更近的节点
    # 最终，所有被删除的节点，依次进去做业务校验，业务参考上述描述
    def __init__(self,data=None):
        if data is None:
            data = show_plot().test('2501')  # 4103
        # self.data = np.delete(data, 2, axis=1)
        # 数据来源要考虑，调整权重
        self.data = data
        # self.data = np.array([[0,1],[1,320],[8,1230],[10,233],[4,1],[20,500],[100,100],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],[101,101],])
        # self.data = np.array(
        #     [[0, 1], [1, 320], [8, 1230], [10, 233], [4, 1], [20, 500]])
        if self.data.shape[1] == 3:
        # if self.data.shape[1] == 4:  ## 191028_yx
            # 生成序列
            # index = np.array([range(self.data.shape[0])])
            index = np.fromfunction(lambda i: i % self.data.shape[0]-1 + 1, (self.data.shape[0],)).reshape((1,self.data.shape[0]))
            self.data = np.hstack((self.data, index.T))

        if self.data.shape[1] == 4:  ## 191028_yx
        # if self.data.shape[1] == 5:  ## 191028_yx
            # 增加一列被删除次数
            d_cnt = np.zeros((1,self.data.shape[0]))
            self.data = np.hstack((self.data,d_cnt.T))

        # 日期间隔，距离数，来源，索引值，被删除次数, 原始日期
        self.del_data = np.array((-1,-1,-1,-1,-1))
        # self.del_data = np.array((-1,-1,-1,-1,-1,-1))   ##  191028_yx
        self.del_data = np.reshape(self.del_data,(1,len(self.del_data)))
        self.data_relation = np.array((-1,-1))  # 基点序号，被删除点序号
        self.data_relation = np.reshape(self.data_relation, (1, len(self.data_relation)))
        # 递归次数
        self.recurrence_num = 3
        # 交叉删除的最大次数
        self.max_check_delete_number = 3

    def set_data(self,data=np.array(1)):
        self.data = data
        if self.data.shape[1] == 3:  ## 191028_yx
        # if self.data.shape[1] == 4:  ## 191028_yx
            # 生成序列
            # index = np.array([range(self.data.shape[0])])
            index = np.fromfunction(lambda i: i % self.data.shape[0] - 1 + 1, (self.data.shape[0],)).reshape(
                (1, self.data.shape[0]))
            self.data = np.hstack((self.data, index.T))

        if self.data.shape[1] == 4:
        # if self.data.shape[1] == 5:  ## 191028_yx
            # 增加一列被删除次数
            d_cnt = np.zeros((1, self.data.shape[0]))
            self.data = np.hstack((self.data, d_cnt.T))

    def main_logic(self):
        # 单一点不处理
        if len(self.data) < 2:
            return self.data,None
        # 20180916 luqian start bugfix data must be ordered by collection time
        # self.data = self.data[self.data[:, 0].argsort()]
        # 20180916 luqian end bugfix data must be ordered by collection time
        self.data,self.del_data,self.data_relation = self.data_noise_reduce(self.data)
        # 业务补偿
        # show_plot().display(self.data, 'P')
        if np.ndim(self.data_relation) > 1:
            self.data,self.del_data,self.data_relation = self.dataset_compensation(self.data,self.del_data,self.data_relation)
        # show_plot().display(self.data,'P')
        # 放弃科学技术法表示
        np.set_printoptions(suppress=True)
        return self.data,self.del_data

    def check_logic(self,dataset,i,disturb=-1,disturbed=-1):
        return_val = None
        if dataset[i][2] == 1:
            p_c = 0
        else:
            p_c = 0

        if dataset[i+1][2] == 1:
            p_n = 0
        else:
            p_n = 0

        core_dataset = np.delete(dataset,i+1,0)
        core_dataset = np.delete(core_dataset,i,0)
        if len(core_dataset) <= 1:
            # 待校验集合数量不足
            return -1

        x = core_dataset[:,0]
        y = core_dataset[:,1]
        # 剔除矛盾点的趋势
        z1 = sp.polyfit(x,y,1) #一次多项式拟合，相当于线性拟合
        # print('x:%s,y:%s,z:%s' % (x, y,z1[0]))
        # p1 = sp.poly1d(z1)
        # print(z1[0])    # 斜率
        # get current point 斜率
        x_c = np.hstack((x,dataset[i][0]))
        y_c = np.hstack((y,dataset[i][1]))
        # 含当前点的趋势
        z_c =sp.polyfit(x_c,y_c,1)
        # print('x_c:%s,y_c:%s,z_c:%s' % (x_c, y_c, z_c[0]))

        # get next point 斜率
        x_n = np.hstack((x, dataset[i+1][0]))
        y_n = np.hstack((y, dataset[i+1][1]))
        # 含下一个点的趋势
        z_n = sp.polyfit(x_n, y_n, 1)
        # print('x_n:%s,y_n:%s,z_n:%s' % (x_n, y_n, z_n[0]))
        # 相邻点的趋势
        # with sp.errstate(invalid='ignore'):
        #     z_l = sp.polyfit(np.hstack((dataset[i][0],dataset[i+1][0])),np.hstack((dataset[i][1],dataset[i+1][1])),1)
        # if return_val is None and disturb!=-1:
        #     if z_l[0]<z1[0]:
        #         return_val = i

        # 相比有效初始点，排除斜率是0的
        if return_val is None:
            if dataset[0][0:1]==dataset[i][0:1] or sp.polyfit([dataset[0][0],dataset[i][0]], [dataset[0][1],dataset[i][1]], 1)[0] == 0:
                return_val = i+1
            elif dataset[0][0:1]==dataset[i+1][0:1] or sp.polyfit([dataset[0][0],dataset[i+1][0]], [dataset[0][1],dataset[i+1][1]], 1)[0] == 0:
                return_val = i

        # 当前斜率偏差 小等于 下一点的斜率偏差 则取信当前节点
        # if abs(z_c[0]-z1[0])*p_c <= abs(z_n[0]-z1[0])*p_n:
        if return_val is None:
            if abs(np.arctan(z_c[0])-np.arctan(z1[0])) <= abs(np.arctan(z_n[0])-np.arctan(z1[0])) :
                return_val= i
            else:
                return_val = i+1

        # 两组相比 取总路径短的
        # todo 新的算法预留
        # if disturb == -1:
        #     # 不使用这个算法
        #     pass
        # else:
        #     # 干扰点的路径总和
        #     # 得到干扰点前的点位，和干扰点后的点位
        #     pass

        return return_val

    def dataset_compensation(self,o_dataset,d_dataset,r_dataset):
        # 统计关系中，单点的影响
        log.log(log.set_loglevel().info(),log.debug().position(),'starting')
        affect_allowed = 3
        part_range_points = 9
        extention = 3
        part_range_rate = affect_allowed/part_range_points
        if np.ndim(d_dataset)>1:
            all_dataset = np.row_stack((o_dataset,d_dataset[1:]))
        else:
            all_dataset = o_dataset
        all_dataset = all_dataset[all_dataset[:,3].argsort()]
        del_dataset = d_dataset[:1]
        rel_dataset = r_dataset[:1]
        p_tmp = None
        for tmp in r_dataset:
            if p_tmp is None:
                p_tmp = tmp
            else:
                if p_tmp[0]==tmp[0]:
                    continue
            p_tmp=tmp

            if np.sum(r_dataset[:,0]==tmp[0])>affect_allowed:
                # print('need check ',tmp[0])
                datas = r_dataset[r_dataset[:,0]==tmp[0],:]
                for data_i in datas:
                    r_dataset = np.delete(r_dataset, np.where(r_dataset == data_i), 0)
                deleted_point= np.shape(datas)[0]
                # bugfix 20180615 luqian start   删除局部处理后的全局业务校验逻辑，无法去除其他干扰集合
                # for ext in range(1,extention):
                # bugfix 20180615 luqian end   删除局部处理后的全局业务校验逻辑，无法去除其他干扰集合
                # min_a = 0 if np.min(datas, 0)[1]<ext*deleted_point/part_range_rate else np.min(datas, 0)[1] - ext*deleted_point/part_range_rate
                min_a = 0 if np.min(datas, 0)[1] < deleted_point / part_range_rate else np.min(datas, 0)[1] - deleted_point / part_range_rate
                try:
                    min_index = np.max(np.where(all_dataset[:,3]<=min_a))
                except Exception as e:
                    min_index = np.min(np.where(all_dataset[:, 3] >= min_a))
                # max = tmp[0]+ext*deleted_point/part_range_rate
                max = tmp[0] + deleted_point / part_range_rate
                # max = max if np.max(datas, 0)[1]<max else np.max(datas, 0)[1] + ext*deleted_point/part_range_rate
                max = max if np.max(datas, 0)[1] < max else np.max(datas, 0)[1] + deleted_point / part_range_rate
                try:
                    max_index = np.min(np.where(all_dataset[:, 3] >= max))
                except Exception as e:
                    max_index = np.max(np.where(all_dataset[:, 3] >= tmp[0]))
                # 获得局部的数据集合
                part_dataset = self.part_dataset(all_dataset,min_a,max)
                # 降噪处理
                part_distub_index = np.where(part_dataset[:,3]==tmp[0])[0]
                part_distubed_index = list()
                for distubed_tmp in datas:
                    part_distubed_index.append(np.where(part_dataset[:,3]==distubed_tmp[1])[0])
                part_distubed_index = np.array(part_distubed_index)
                data_ok,data_ng,data_rel = self.data_noise_reduce(part_dataset,part_distub_index,part_distubed_index)
                # bugfix 20180615 luqian start   删除局部处理后的全局业务校验逻辑，无法去除其他干扰集合
                    # 降噪后校验业务关系
                    # if np.ndim(data_ng) > 1:
                    #     need_verify_biz_dataset = all_dataset
                    #     for data_ng_i in data_ng[1:]:
                    #         need_verify_biz_dataset = np.delete(need_verify_biz_dataset, np.where(need_verify_biz_dataset[:, 3] == data_ng_i[3]), 0)
                    #     if self.biz_logic(need_verify_biz_dataset) != 0:
                    #         if ext == 3:
                    #             pass
                    #             # data_ok,data_ng,data_rel = self.dataset_compensation(data_ok,data_ng,data_rel)
                    #         continue
                    #     else:
                    #         break
                # bugfix 20180615 luqian end   删除局部处理后的全局业务校验逻辑，无法去除其他干扰集合

                # log.debug().print_debug(data_ok,data_ng,data_rel)
                log.log(log.set_loglevel().debug(),log.debug().position(),data_ok,data_ng,data_rel)
                if np.ndim(data_ng) > 1:
                    for data_ng_i in data_ng[1:]:
                        all_dataset = np.delete(all_dataset, np.where(all_dataset[:, 3] == data_ng_i[3]), 0)
                        if len(np.where(d_dataset[:,3]==data_ng_i[3])[0])==0:
                            d_dataset = np.row_stack((d_dataset, data_ng_i))
                for data_ok_i in data_ok:
                    d_dataset = np.delete(d_dataset, np.where(d_dataset[:, 3] == data_ok_i[3]), 0)
                d_dataset= d_dataset[d_dataset[:, 3].argsort()]
                # d_dataset = np.row_stack((d_dataset,data_ng))
                # 单维度去重复
                # d_dataset = np.unique(d_dataset)
                r_dataset = np.row_stack((r_dataset,data_rel))

        for d_dataset_i in d_dataset[1:]:
            all_dataset = np.delete(all_dataset, np.where(all_dataset[:, 3] == d_dataset_i[3]), 0)

        # 排序
        all_dataset = all_dataset[all_dataset[:, 3].argsort()]

        if self.biz_logic(all_dataset) != 0:
            if self.recurrence_num > 0:
                rec_dataset,rec_d_dataset,rec_r_dataset = self.data_noise_reduce(all_dataset)
                if type(rec_dataset)==int and rec_dataset==-1:
                    pass
                else:
                    all_dataset,d_dataset,r_dataset = self.dataset_migration(all_dataset,d_dataset,r_dataset,rec_dataset,rec_d_dataset,rec_r_dataset)
                    self.recurrence_num -= 1
            else:
                log.debugfile().printlog2(o_dataset,d_dataset,r_dataset, ' 需要人工干预')

        return all_dataset,d_dataset,r_dataset

    def part_dataset(self,dataset,min_index=0,max_index=0):
        data = dataset[dataset[:,3]>=min_index,:]
        data = data[data[:,3]<=max_index,:]
        return data

    def data_noise_reduce(self,dataset,disturb=-1,disturbed=-1):
        if len(dataset) < 2:
            return dataset, None,None
        # 输入参数缓存
        o_dataset =dataset
        # reset dataset 的被删除次数
        dataset[:,4] = 0
        # dataset[:,5] = 0  ## 191028_yx
        # del_data = np.array((-1, -1, -1, -1))
        del_data = np.ones(self.del_data.shape[1]).reshape(1, self.del_data.shape[1]) * -1
        # data_relation = np.array((-1, -1))
        data_relation = np.ones(self.data_relation.shape[1]).reshape(1,self.data_relation.shape[1]) * -1
        data = dataset
        # 所有删除的历史记录，为了避免死循环
        data_rel_history = data_relation
        i = 0

        while i < len(data) - 1 :
            if i == len(data):
                break
            # 判断下一点和当前点相同时间点，存在数据问题，条件去掉
            # if data[i+1][0] == data[i][0]:
            #     i+=1
            #     continue

            # 判断下一个点位的日均大于500公里／小时*24小时，或者 下一个点位的里程数大于当前点位, 或者 时间点一样且里程数不一样
            if (data[i + 1][1] - data[i][1]) / ((data[i + 1][0] - data[i][0]) if data[i+1][0]!=data[i][0] else 1) >= float(500 * 24) \
                    or data[i + 1][1] < data[i][1] \
                    or (data[i+1][0]==data[i][0] and data[i+1][1]!=data[i][1]):
                # 竞争
                check_result = self.check_logic(data, i,disturb,disturbed)
                # 被删除的点的被删除次数增加 1
                if check_result != -1:
                    data[i if check_result == i + 1 else i + 1][4] += 1
                    # 递归循环后退出
                    if data[i if check_result == i + 1 else i + 1][4] >= self.max_check_delete_number:
                        # 统计被删除次数大于设定值，检查删除记录，被相同点删除的次数
                        deleted_point_datasets = data_rel_history[np.where(data_rel_history[:,1]==data[i if check_result == i + 1 else i + 1][3])]
                        # (array(唯一元素一维集合)，array(一维元素统计重复次数))
                        delete_point_info = np.unique(deleted_point_datasets[:, 0], return_counts=True)
                        delete_point_count = delete_point_info[1]
                        if len(np.where(delete_point_count>=self.max_check_delete_number)[0])>0:
                            # 默认通过
                            # 彻底删除
                            del_data = np.row_stack((del_data, data[i if check_result == i + 1 else i + 1]))
                            # 查找被删除点引起的删除对象，做回复
                            next_data = data[i if check_result == i + 1 else i + 1]
                            get_cur_index = next_data[3]
                            reserve_relation = data_relation[~(data_relation[:, 0] != get_cur_index), :]
                            # 更新点位信息
                            data = np.delete(data, i if check_result == i + 1 else i + 1, 0)
                            # 影响点回复
                            if len(reserve_relation) > 0:
                                # next loop index
                                index_val = -1
                                for tmp in reserve_relation[:, 1]:
                                    if tmp < index_val or index_val == -1:
                                        index_val = tmp
                                    need_reserve_data = del_data[del_data[:, 3] == tmp, :]
                                    # add in to points list
                                    data = np.row_stack((data, need_reserve_data))
                                    # delete from del dataset
                                    del_data = np.delete(del_data, np.where(del_data[:, 3] == tmp), 0)
                                data = data[data[:, 3].argsort()]
                                if index_val != -1:
                                    i = np.where(data[:, 3] == index_val)[0][0]
                                    i = 0 if i == 0 else i - 1

                            # i 保持不变  @20180623
                            # i += 1
                            continue
                        else:
                            # 非同一点删除，重置删除次数
                            data[i if check_result == i + 1 else i + 1][4] = 0
                # 点位集合更新
                if check_result == -1:
                    log.debugfile().printlog2(data, ' 需要人工干预')
                    return  -1,'需要人工干预',None
                elif check_result == i:
                    # 下一个点位不合理
                    # 查找被删除点引起的删除对象，做回复
                    next_data = data[i+1]
                    get_cur_index = next_data[3]
                    reserve_relation = data_relation[~(data_relation[:, 0] != get_cur_index), :]
                    data_relation = data_relation[data_relation[:, 0] != get_cur_index, :]

                    # 更新干扰信息
                    data_relation = np.row_stack((data_relation, (data[i][3], data[i + 1][3])))
                    # 增加删除历史
                    data_rel_history = np.row_stack((data_rel_history, (data[i][3], data[i + 1][3])))
                    # 缓存删除点，当前点失效时需要回复
                    del_data = np.row_stack((del_data, data[i + 1]))
                    # 更新点位集合
                    data = np.delete(data, i + 1, 0)
                    # 影响点回复
                    if len(reserve_relation) > 0:
                        # next loop index
                        index_val = -1
                        for tmp in reserve_relation[:, 1]:
                            if tmp < index_val or index_val == -1:
                                index_val = tmp
                            need_reserve_data = del_data[del_data[:, 3] == tmp, :]
                            # add in to points list
                            data = np.row_stack((data, need_reserve_data))
                            # delete from del dataset
                            del_data = np.delete(del_data, np.where(del_data[:, 3] == tmp), 0)
                        data = data[data[:, 3].argsort()]
                        if index_val != -1:
                            i = np.where(data[:, 3] == index_val)[0][0]
                            i = 0 if i == 0 else i - 1
                    # 重新和下一个点位做竞争
                    continue
                elif check_result == i + 1:
                    # 当前节点不合理
                    # 缓存当前节点信息
                    cur_data = data[i]
                    # 更新干扰信息
                    data_relation = np.row_stack((data_relation, (data[i + 1][3], data[i][3])))
                    # 增加删除历史
                    data_rel_history = np.row_stack((data_rel_history, (data[i + 1][3], data[i][3])))
                    # 缓存删除点
                    del_data = np.row_stack((del_data, data[i]))
                    # 更新点位集合
                    data = np.delete(data, i, 0)

                    # if i == 0:
                    #     # 第一点
                    #     continue
                    # else:
                    #     # 非第一点
                    # 释放由被删除点引起的前序删除
                    # self.data_relation[2][0] = 3
                    # self.data_relation[1][0] = 3

                    # data_relation_list = self.data_relation.tolist()
                    # del_data_list = self.del_data.tolist()

                    # 获得不合理节点的序号
                    get_cur_index = cur_data[3]

                    # start 从关系中，获得被不合理节点删除的，在relation中的位置序号
                    # get_del_index_by_cur_index = np.where(self.data_relation[:,0] == get_cur_index)[0]
                    reserve_relation = data_relation[~(data_relation[:, 0] != get_cur_index), :]
                    data_relation = data_relation[data_relation[:, 0] != get_cur_index, :]
                    # next loop index
                    index_val = -1
                    for tmp in reserve_relation[:, 1]:
                        if tmp < index_val or index_val == -1:
                            index_val = tmp
                        need_reserve_data =del_data[del_data[:, 3] == tmp, :]
                        # add in to points list
                        data = np.row_stack((data, need_reserve_data))
                        # delete from del dataset
                        del_data = np.delete(del_data, np.where(del_data[:, 3] == tmp), 0)

                    data = data[data[:, 3].argsort()]
                    if index_val!=-1:
                        i = np.where(data[:, 3] == index_val)[0][0]
                    # 或者被删除的完整的点位信息
                    # for tmp in get_del_index_by_cur_index:
                    #     # 得到被删除点的点位序号
                    #     del_p_position = self.data_relation[tmp][1]
                    #     # 从删除关系中释放关联信息
                    #     data_relation_list.pop(data_relation_list.index([get_cur_index,del_p_position]))
                    #
                    #     # 获得del_data的位置序号
                    #     get_array_index_from_del_data = np.where(self.del_data[:,2] == del_p_position)
                    #     # 更新需要恢复的列表
                    #     need_reserve_list.append(del_data_list[get_array_index_from_del_data[0][0]])

                    # self.data_relation = np.array(data_relation_list)
                    # end 更新关系

                    # 上一节点校验
                    i = i - 1 if i > 0 else 0
                    continue
            # print(self.data[i])
            i += 1

        return data,del_data,data_relation

    def biz_logic(self,datasets):
        if np.ndim(datasets) <= 1:
            return 0
        i = 0
        for data in datasets:
            if i == 0:
                pass
            else:
                if (data[1] - p_data[1]) / ((data[0] - p_data[0]) if data[0] != p_data[0] else 1) >= float(500 * 24) or data[1] < p_data[1]:
                    return i
            p_data = data
            i += 1

        return 0

    def dataset_migration(self,bs_datasets,bs_d_datasets,bs_r_datasets,m_datasets,m_d_datasets,m_r_datasets):
        array_check = array_deal()
        for m_dataset in m_datasets:
            # 处理OK的集合
            if array_check.array_contain(bs_datasets,m_dataset)==1:
                # 存在
                pass
            else:
                bs_datasets = np.row_stack((bs_datasets,m_dataset))
            bs_d_datasets = np.delete(bs_d_datasets,np.where(bs_d_datasets[:,3] == m_dataset[3]),0)
            bs_r_datasets = np.delete(bs_r_datasets,np.where(bs_d_datasets[:,1] == m_dataset[3]), 0)

        for m_d_dataset in m_d_datasets:
            bs_datasets = np.delete(bs_datasets, np.where(bs_datasets[:,3] == m_d_dataset[3]), 0)
            if array_check.array_contain(bs_d_datasets,m_d_dataset)==1:
                # 存在
                pass
            else:
                bs_d_datasets = np.row_stack((bs_d_datasets, m_d_dataset))

        for m_r_dataset in m_r_datasets:
            if array_check.array_contain(bs_r_datasets,m_r_dataset)==1:
                pass
            else:
                bs_r_datasets = np.row_stack((bs_r_datasets,m_r_dataset))

        bs_datasets = bs_datasets[bs_datasets[:, 3].argsort()]
        bs_d_datasets = bs_d_datasets[bs_d_datasets[:, 3].argsort()]
        bs_r_datasets = bs_r_datasets[bs_r_datasets[:, 0].argsort()]

        return bs_datasets,bs_d_datasets,bs_r_datasets


class test():
    def aaa(self):
        db_dml.db_es_dml().new_doc(index_name='train_mile_statistics', type_name='train_mile_statistics', doc_value=
        {'train_no': 'test', 'deal_result': {'delete_dp': 99, 'noise_dp': 83, 'noise_dup_dp': 16, 'total_dp': 658}, 'train_type_code': 'E01', 'train_type_desc': '2A', 'deal_time': datetime.datetime(
            2019, 6, 5, 13, 35, 56, 898833)})

    def bbb(self):
        res=db_dml.db_es_dml().query_doc('train_mile_statistics', 'train_mile_statistics',
                                       {'aggs': {'group_by_train': {'terms': {'field':'train_no'}}}, 'sort': [
                                           {'deal_time': {'order': 'desc'}}], 'size': 1, 'version': True})
        return res

# show_plot().show()
# print(show_plot().module_analyze())
# 显示某辆车
# print(show_plot().test('2386'))
# 2451 跑测试 2462
# 降噪
# a = mile_verify().main_logic()
# show_plot().display(a)
# @单列车
# data = deal_data().get_data('2386')

# ###---------------------------------------------------
# carriage = '2634'
# clear = mile_verify()
# # clear.set_data(data)
# clear_data = clear.main_logic()
# deal_data().noise_data_delete(clear.del_data)
# raise
# # @比较
# deal_data().all_carriage_noise_deal('10101')
show_plot().show_difference('10101','P')   # 2693
# ###---------------------------------------------------
# ###---------------------------------------------------
# ###---------------------------------------------------YX
# carriages = deal_data().get_carriage_data()
# count = 0
# for carriage in carriages:
#     count += 1
#     show_plot().show_difference('%s'%carriage,'P')   # 2693
#     print("{}/{} is finished!".format(count, len(carriages)))
# ###---------------------------------------------------YX
# ###---------------------------------------------------
# ###---------------------------------------------------
# raise
# carriages = get_data().get_carriage_data()
# continue_flag = False
# for carriage in carriages:
#     if carriage[0] == '2693':
#         continue_flag = True
#     if continue_flag:
#         print(carriage[0])
#         show_plot().show_difference(carriage[0],'S')
##################################
# results = deal_data().all_carriage_noise_deal()

