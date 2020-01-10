#!/bin/env python3
#coding=utf-8
# @Time    : 2019/12/25 下午13:00
# @Author  : Victor
# @Site    :
# @File    : d_tanhuaban_predict.py
# @Software: PyCharm
import basic_config.bc_repairdata as config
import datetime
from scipy import stats
import numpy as np
import csv

class tanhuaban_predict:
    def read_data(self,path_read):
        read_data = []
        try:
            with open(path_read, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    read_data.append(line.split(","))
        except UnicodeDecodeError:
            with open(path_read, 'r', encoding='gbk') as f:
                for line in f:
                    read_data.append(line.split(","))
        return read_data

    def Write2CsvByrows(self,list_columns, list_tmp, path_csv):
        '''
        funcion：把列表中的元素一次性全部写入csv文件中
        return：csv文件
        '''
        with open(path_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_columns)
            writer.writerows(list_tmp)

    def get_trainno_speed(self,trainNo,list_serialNo):
        '''
        根据原始文件中的序号，查找报告时间和完成时间，从而得出该列车所对应的平均时速
        :parameter 列表，数据表示碳滑板的一组序号值
        :returns
        '''
        read_data = self.read_data(config.path_ori_file_datetime)[1:]
        nparray_all_trainNo = np.array(read_data).T[1]
        list_unique_tiainNo = list(set(nparray_all_trainNo.tolist()))
        if trainNo in list_unique_tiainNo:
            list_trainNo = np.array(read_data)[np.where(nparray_all_trainNo == trainNo)]
            dict_all_data = {}
            for i in list_trainNo:
                dict_all_data[i[0]] = i[1:]
            train_down = dict_all_data[list_serialNo[0]][0]   # 换下时的车号
            train_up = dict_all_data[list_serialNo[1]][0]  # 换上时的车号
            datetime_down = datetime.datetime.strptime(dict_all_data[list_serialNo[0]][1].strip("\n ").split(" ")[0],"%Y/%m/%d")  # 换下时列车的日期（取报告时间）
            datetime_up = datetime.datetime.strptime(dict_all_data[list_serialNo[1]][1].strip("\n ").split(" ")[0],"%Y/%m/%d")  # 换上时列车的日期（取报告时间）
            mileage_down = dict_all_data[list_serialNo[0]][-1].strip("\n ")  #  换下时列车的里程
            mileage_up = dict_all_data[list_serialNo[1]][-1].strip("\n ")  #  换上时列车的里程

            cal_hours= 8 * (datetime_down - datetime_up).days  #  !!!计算天数差，并假设列车每天跑8个小时，算出小时差!!!
            cal_mileage = int(mileage_down) - int(mileage_up)

            if train_down == train_up:
                try:
                    speed = cal_mileage / cal_hours
                except:
                    speed = 0
            else:
                return "trainNo different!"
        else:
            return "has no {}'s data for cal speed!".format(trainNo)
        return speed

    def get_train_info(self,trainNo,read_path):
        '''
        :param: 给出具体的某一个车号和某个要查询数据的文件
        从187条认为良好的数据中算出该列车号上的碳滑板回归直线的斜率和截距，如果该列车有多组斜率截距，那么就分别取斜率和截距的平均值
        :return: 输入车号的斜率/截距/平均时速，如dict_all = {"10147":[-0.00089,37.1,28.0034]}
        '''
        read_data = self.read_data(read_path)[1:]
        nparray_all_trainNo = np.array(read_data).T[4]
        list_unique_tiainNo = list(set(nparray_all_trainNo.tolist()))
        if trainNo in list_unique_tiainNo:
            list_trainNo = np.array(read_data)[np.where(nparray_all_trainNo == trainNo)]
            dict_trainNo = {}
            for i in list_trainNo:
                x = np.array([float(i[7]), float(i[2])])
                y = np.array([float(i[8]), float(i[3])])
                list_serialNo = [i[1],i[6]]
                slope, intercept, r_value, p_value, slope_std_error = stats.linregress(x, y)
                train_speed = self.get_trainno_speed(trainNo,list_serialNo)
                if type(train_speed) == 'str':
                    return train_speed
                if i[4] in dict_trainNo.keys():
                    dict_trainNo[i[4]].append([slope, intercept,train_speed])
                else:
                    dict_trainNo[i[4]] = [[slope, intercept,train_speed]]
        else:
            return "has no {}'s data for cal slope and intercept!".format(trainNo)

        #  以下是针对单一车号多组斜率截距分别求平均值的过程
        dict_res = {}
        for k,v in dict_trainNo.items():
            if len(v) > 1:
                v = np.transpose(v).tolist()
                a = np.mean(v[0])
                b = np.mean(v[1])
                c = np.mean(v[2])
            else:
                a = v[0][0]
                b = v[0][1]
                c = v[0][2]
            dict_res[k] = [a,b,c]
        return dict_res

    def line_predict_by_y(self,list_line,y_start,y_end):
        x_start = (y_start - list_line[1]) / list_line[0]
        x_end = (y_end - list_line[1]) / list_line[0]
        pass
    def line_predict_by_x(self,list_line,y_start,train_run_time):
        x_start = (y_start - list_line[1]) / list_line[0]
        x_end = x_start + list_line[2] * train_run_time
        y_end = x_end * list_line[0] + list_line[1]
        return y_end

    def get_predict_by_train(self,trianNo,y_start,y_set_end_normal,y_set_end_abnormal):
        line_normal = self.get_train_info(trianNo,config.path_result_file_1)
        # print(line_normal)
        # line_abnormal = self.get_train_info(trianNo,config.path_result_file_2)
        # print(line_abnormal)
        # y_start = 37   #  假设起始换上时的厚度
        day_interval = 1
        day_run_hours = 8

        list_res_normal = []
        day_count_normal = 0
        y_end_normal = y_start
        while y_end_normal >= y_set_end_normal:  #  假设正常换下时的厚度
            list_res_normal.append([day_count_normal,y_end_normal])
            y_end_normal = self.line_predict_by_x(line_normal[trianNo],y_end_normal,day_interval*day_run_hours)  #  正常线性曲线进行拟合
            day_count_normal += 1

        list_res_abnormal = []
        day_count_abnormal = 0
        y_end_abnormal = y_start
        while y_end_abnormal >= y_set_end_abnormal:  #  假设非正常换下时的厚度
            list_res_abnormal.append([day_count_abnormal, y_end_abnormal])
            y_end_abnormal = self.line_predict_by_x(line_normal[trianNo], y_end_abnormal, day_interval * day_run_hours)  #  正常线性曲线进行拟合
            day_count_abnormal += 1
        return list_res_normal,list_res_abnormal,day_count_normal-1,day_count_abnormal-1

    def period_analysis_material(self,period_normal,period_abnormal,material_unit_price):
        #  一周年物料费分析
        material_change_number_normal = 365 // period_normal
        material_change_number_abnormal = 365 // period_abnormal
        material_save = (material_change_number_abnormal - material_change_number_normal) * material_unit_price
        material_save_percent = '{:.2%}'.format(((material_change_number_abnormal - material_change_number_normal) * material_unit_price) / (material_change_number_abnormal * material_unit_price))
        return material_change_number_normal,material_change_number_abnormal,material_save,material_save_percent

    def period_analysis_labor(self,list_normal,list_abnormal,labor_unit_price,period_normal,period_abnormal):
        #  一周年人工费分析
        nparray_normal = np.array(list_normal).T
        index_normal = np.where(nparray_normal[1] <= 28)  #  取出厚度值大于等于28的下标值
        labor_cost_normal = labor_unit_price * (len(index_normal[0]) // 2)  #  优化策略：从预测碳滑板厚度28mm开始介入监测，并且保持两天一测的频率
        labor_sum_normal_1 = (365 // period_normal) * labor_cost_normal
        remaining_days = 365 - (365 // period_normal) * len(list_normal)
        labor_sum_normal_2 = 0 if remaining_days <= len(list_normal) - len(index_normal[0]) else labor_unit_price * (remaining_days // 2)
        labor_sum_normal = labor_sum_normal_1 + labor_sum_normal_2

        nparray_abnormal = np.array(list_abnormal).T
        index_abnormal_1 = np.where(nparray_abnormal[1] > 28)
        index_abnormal_2 = np.where(nparray_abnormal[1] <= 28)
        labor_cost_abnormal = labor_unit_price * (len(index_abnormal_1[0])//3 + len(index_abnormal_2[0])//2)  #  计划策略：厚度值大于28mm时3天一监修，厚度值小于28之后2天一监修
        labor_sum_abnormal_1 = (365 // period_abnormal) * labor_cost_abnormal
        remaining_days = 365 - (365 // period_abnormal) * len(list_abnormal)
        labor_sum_abnormal_2 = labor_unit_price * (remaining_days // 3) if remaining_days <= len(index_abnormal_1[0]) else labor_unit_price * (len(index_abnormal_1[0])//3 + (remaining_days-len(index_abnormal_1[0]))//2)
        labor_sum_abnormal = labor_sum_abnormal_1 + labor_sum_abnormal_2

        labor_cost_save = labor_sum_abnormal - labor_sum_normal
        labor_percent_save = '{:.2%}'.format((labor_sum_abnormal - labor_sum_normal) / labor_sum_abnormal)
        return labor_cost_save,labor_percent_save

    def main(self):
        read_data = self.read_data(config.path_result_file_1)
        list_unique_train = list(set(np.array(read_data[1:]).T[4].tolist()))
        # print(list_unique_train)
        list_all_res = []
        for trainNo in list_unique_train:
            list_tmp = []
            list_tmp.append(trainNo)
            list_res_normal, list_res_abnormal,period_normal,period_abnormal = self.get_predict_by_train(trainNo,37,25,26.5)
            # print(list_res_normal)
            # print(list_res_abnormal)

            # self.Write2CsvByrows(config.columns_name_predict,list_res_normal,config.path_predict_normal)
            # self.Write2CsvByrows(config.columns_name_predict,list_res_abnormal,config.path_predict_abnormal)

            material_change_number_normal, material_change_number_abnormal,material_save_cost,material_save_percent = \
                self.period_analysis_material(period_normal,period_abnormal,1000)
            list_tmp.append(material_change_number_normal)
            list_tmp.append(material_change_number_abnormal)
            list_tmp.append(material_save_cost)
            list_tmp.append(material_save_percent)
            labor_save_cost,labor_save_percent = self.period_analysis_labor(list_res_normal,list_res_abnormal,period_normal,period_abnormal,300)
            list_tmp.append(labor_save_cost)
            list_tmp.append(labor_save_percent)
            # print(list_tmp)
            list_all_res.append(list_tmp)

        self.Write2CsvByrows(config.columns_name_results,list_all_res,config.path_analisis_res)

tanhuaban_predict().main()





























