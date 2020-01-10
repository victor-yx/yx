# -*- coding:utf-8 -*-
# @Time    : 2019/11/13 上午9:35
# @Author  : Victor
# @Site    :
# @File    : d_tanhuaban_analysis.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import csv
import matplotlib.pyplot as plt
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
import scipy as sp

class tanhuabanDataAnalysis():
    def read_csv(self, path_csv,start_num,end_num):
        inp_data = []
        count =1
        with open(path_csv, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                if  count >= start_num \
                and count <= end_num:
                    data = [substr.strip("\n ") for substr in line.split(',')]
                    inp_data.append(data)
                count += 1
        inp_data = np.array(inp_data)
        return inp_data

    def Write2CsvByrows(self, list_contents, list_column, path_csv):
        with open(path_csv, 'w', newline='',encoding="utf-8-sig") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_column)
            writer.writerows(list_contents)

    def dictWrite2Csv(self,dict_data,list_columns,path):
        list_write_all = []
        for k,v in dict_data.items():
            list_tmp = [k]
            for i in v:
                for j in i:
                    list_tmp.append(j)
            list_write_all.append(list_tmp)
        self.Write2CsvByrows(list_write_all,list_columns,path)

    def deal_data_step_one(self,readData,list_column):
        # res_old = pd.value_counts(list_ori_seNo_new)
        # print(res_old)

        list_ori_data_old,list_ori_data_new = [],[]
        for line in readData:
            for column in list_column:
                if line[column] != "" and (line[column+1] != "" or line[column+2] != "" or line[column+3] != ""):
                    list_tmp_houdu = [line[column+1],line[column+2],line[column+3]]
                    list_tmp_merge = [line[0],line[column],line[2],list_tmp_houdu,line[1]]
                    list_ori_data_old.append(list_tmp_merge)if column in [3,11] else list_ori_data_new.append(list_tmp_merge)
                else:
                    continue
        # print(list_ori_data_old)
        # list_ori_data_old = np.array(list_ori_data_old)
        # list_ori_data_new = np.array(list_ori_data_new)
        # list_uni_seNo_old = list(set(list_ori_data_old[:,0]))
        # list_uni_seNo_new = list(set(list_ori_data_new[:,0]))

        dict_data_old,dict_data_new,dict_data_pre = {},{},{}
        for i in list_ori_data_old:
            i_3 = [float(i) if i != "" else float("999") for i in i[3]]
            value = [i[0], float(i[2]), min(i_3), i[-1]]
            if i[1] not in dict_data_old.keys():
                dict_data_old[i[1]] = [value]
            else:
                dict_data_old[i[1]].append(value)
        for i in list_ori_data_new:
            i_3 = [float(i) if i != "" else float("999") for i in i[3]]
            value = [i[0], float(i[2]), min(i_3), i[-1]]
            if i[1] not in dict_data_new.keys():
                dict_data_new[i[1]] = [value]
            else:
                dict_data_new[i[1]].append(value)
        # print(dict_data_old)

        # for i in dict_data_old.keys():
        #     if i in dict_data_new.keys():
        #         for j in dict_data_old[i]:
        #             j.append("换下")
        #         for j in dict_data_new[i]:
        #             j.append("换上")
        #         dict_data_pre[i] = dict_data_old[i] + dict_data_new[i]

        for i in dict_data_old.keys():
            if i in dict_data_new.keys():
                if len(dict_data_old[i]) == 1 and len(dict_data_new[i]) == 1 and dict_data_old[i][0][-1] == dict_data_new[i][0][-1]:
                    dict_data_old[i][0].append("换下")
                    dict_data_new[i][0].append("换上")
                    dict_data_pre[i] = dict_data_old[i] + dict_data_new[i]
                elif len(dict_data_old[i]) == 1 and len(dict_data_new[i]) != 1:
                    for j in dict_data_new[i]:
                        if dict_data_old[i][0][-1] == j[-1]:
                            dict_data_old[i][0].append("换下")
                            j.append("换上")
                            dict_data_pre[i] = dict_data_old[i] + [j]
                        else:
                            continue
                elif len(dict_data_old[i]) != 1 and len(dict_data_new[i]) == 1:
                    for j in dict_data_old[i]:
                        if dict_data_new[i][0][-1] == j[-1]:
                            j.append("换下")
                            dict_data_new[i][0].append("换上")
                            dict_data_pre[i] =[j] + dict_data_new[i]
                        else:
                            continue
                else:
                    for j in dict_data_old[i]:
                        count = 1
                        for k in dict_data_new[i]:
                            if j[-1] == k[-1]:
                                j.append("换下")
                                k.append("换上")
                                new_key = i + "_" + str(count)
                                dict_data_pre[new_key] = [j] + [k]
                            count += 1

        # list_write_all = []
        # for k,v in dict_data_pre.items():
        #     list_tmp = [k]
        #     for i in v:
        #         for j in i:
        #             list_tmp.append(j)
        #     list_write_all.append(list_tmp)
        # path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/test.csv"
        # list_columns = ["序列号","原始序号","里程","最小厚度值/mm","车号","动作","原始序号","里程","最小厚度值/mm","车号","动作","原始序号","里程","最小厚度值/mm","车号","动作","原始序号","里程","最小厚度值/mm","车号","动作"]
        # self.Write2CsvByrows(list_write_all,list_columns,path)
        return dict_data_pre

    def deal_data_step_two(self, dict_data_pre):
        # ----------------------------------------------------------------------------------------
        for k,v in dict_data_pre.items():
            v_lc_max = max(v[0][1], v[-1][1])
            v_lc_min = min(v[0][1], v[-1][1])
            v[0][1] = v_lc_max - v_lc_min if v[0][1] == v_lc_max else 0
            v[-1][1] = v_lc_max - v_lc_min if v[-1][1] == v_lc_max else 0
        # path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/test.csv"
        # list_columns = ["序列号","原始序号","里程","最小厚度值/mm","车号","动作","原始序号","里程","最小厚度值/mm","车号","动作"]
        # self.dictWrite2Csv(dict_data_pre,list_columns,path)
        # ----------------------------------------------------------------------------------------
        dict_data_final_all,dict_data_error = {},{}
        for k,v in dict_data_pre.items():
            if (v[0][1] > v[-1][1] and v[0][2] < v[-1][2]) or \
               (v[0][1] < v[-1][1] and v[0][2] > v[-1][2]):
                dict_data_final_all[k] = v
            else:
                dict_data_error[k] = v
        # path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/test.csv"
        # list_columns = ["序列号","原始序号","里程","最小厚度值/mm","车号","动作","原始序号","里程","最小厚度值/mm","车号","动作"]
        # self.dictWrite2Csv(dict_data_final_all,list_columns,path)
        # ----------------------------------------------------------------------------------------
        dict_data_final_1,dict_data_final_2,dict_data_final_3,dict_data_final_4 = {},{},{},{}
        for k,v in dict_data_final_all.items():
            if v[0][2] >= 25 and v[-1][2] <= 36:
                dict_data_final_4[k] = v
            elif v[0][2] < 25 and v[-1][2] <= 36:
                dict_data_final_3[k] = v
            elif v[0][2] >= 25 and v[-1][2] > 36:
                dict_data_final_2[k] = v
            else:
                dict_data_final_1[k] = v
        # path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/finalData_2.csv"
        # list_columns = ["序列号","原始序号","里程","最小厚度值/mm","车号","动作","原始序号","里程","最小厚度值/mm","车号","动作"]
        # self.dictWrite2Csv(dict_data_final_2,list_columns,path)
        # path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/finalData_3.csv"
        # list_columns = ["序列号", "原始序号", "里程", "最小厚度值/mm", "车号", "动作", "原始序号", "里程", "最小厚度值/mm", "车号", "动作"]
        # self.dictWrite2Csv(dict_data_final_3, list_columns, path)
        # path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/finalData_4.csv"
        # list_columns = ["序列号", "原始序号", "里程", "最小厚度值/mm", "车号", "动作", "原始序号", "里程", "最小厚度值/mm", "车号", "动作"]
        # self.dictWrite2Csv(dict_data_final_4, list_columns, path)
        # ----------------------------------------------------------------------------------------

        return dict_data_final_all,dict_data_final_1,dict_data_final_2,dict_data_final_3,dict_data_final_4

    def deal_data_step_two_old(self,dict_data_pre):
        dict_data_final_1,dict_data_final_2,dict_data_ana_1,dict_data_ana_2,dict_data_error = {},{},{},{},{}
        for k,v in dict_data_pre.items():  #  统一成最大/最小厚度值的两组数据
            if len(v) == 2:
                if (float(v[0][-1]) > float(v[-1][-1]) and float(v[0][0]) < float(v[-1][0])) or \
                   (float(v[0][-1]) < float(v[-1][-1]) and float(v[0][0]) > float(v[-1][0])):
                    dict_data_ana_1[k] = v
                else:
                    dict_data_error[k] = v
            else:
                np_v = np.array(v)
                v_max = np_v[np.where(np_v[:, 1] == max(np_v[:, 1]))].tolist()[0]
                v_min = np_v[np.where(np_v[:, 1] == min(np_v[:, 1]))].tolist()[0]
                v = [v_max,v_min]
                if (float(v[0][-1]) > float(v[-1][-1]) and float(v[0][0]) < float(v[-1][0])) or \
                   (float(v[0][-1]) < float(v[-1][-1]) and float(v[0][0]) > float(v[-1][0])):
                    dict_data_ana_1[k] = v
                else:
                    dict_data_error[k] = v
        # ----------------------------------------------------------------------------------------
        for k,v in dict_data_ana_1.items():  #  把两组的里程规范化，统一成0/差值的格式
            v_lc_max = max(float(v[0][0]),float(v[-1][0]))
            v_lc_min = min(float(v[0][0]),float(v[-1][0]))
            v[0][0] = v_lc_max-v_lc_min if float(v[0][0]) == v_lc_max else 0
            v[-1][0] = v_lc_max-v_lc_min if float(v[-1][0]) == v_lc_max else 0
        for k,v in dict_data_ana_1.items():  #  调整两个列表的位置
            if v[0][0] != 0:
                v_ = v[0]
                v[0] = v[-1]
                v[-1] = v_
        for k,v in dict_data_ana_1.items():  #  去掉不好看的数据
            if v[-1][0] <= 60000:
                dict_data_final_1[k] = v
            else:
                continue
        for k,v in dict_data_final_1.items():
            if float(v[0][1]) >= 36:
                dict_data_final_2[k] = v
            else:
                continue
        x_1,y_1 = [],[]
        for k,v in dict_data_final_2.items():  #  生成x,y两个坐标轴的值
            for i in range(2):
                x_1.append(v[i][0])
                y_1.append(float(v[i][1]))
        # ----------------------------------------------------------------------------------------
        # for k,v in dict_data_ana_2.items():  #  把两组的里程/厚度值规范化，统一成0/差值的格式
        #     v_lc_max = max(float(v[0][0]),float(v[-1][0]))
        #     v_lc_min = min(float(v[0][0]),float(v[-1][0]))
        #     v_hd_max = max(float(v[0][1]),float(v[-1][1]))
        #     v_hd_min = min(float(v[0][1]),float(v[-1][1]))
        #     v[0][0] = v_lc_max-v_lc_min if float(v[0][0]) == v_lc_max else 0
        #     v[-1][0] = v_lc_max-v_lc_min if float(v[-1][0]) == v_lc_max else 0
        #     v[0][1] = float(v_hd_min / v_hd_max) if float(v[0][1]) == v_hd_min else 10
        #     v[-1][1] = float(v_hd_min / v_hd_max) if float(v[-1][1]) == v_hd_min else 10
        # for k,v in dict_data_ana_2.items():  #  调整两个列表的位置
        #     if v[0][0] != 0:
        #         v_ = v[0]
        #         v[0] = v[-1]
        #         v[-1] = v_
        # for k,v in dict_data_ana_2.items():  #  去掉不好看的数据
        #     if v[-1][0] <= 60000:
        #         dict_data_final_2[k] = v
        #     else:
        #         continue
        # x_2,y_2 = [],[]
        # for k,v in dict_data_final_2.items():  #  生成x,y两个坐标轴的值
        #     for i in range(2):
        #         x_2.append(v[i][0])
        #         y_2.append(float(v[i][1]))
        # ----------------------------------------------------------------------------------------
        return dict_data_final_2,x_1,y_1

    def show_plot(self,dict_data_final_all, dict_data_final_1, dict_data_final_2,dict_data_final_3,dict_data_final_4):
        # ----------------------------------------------------------------------------------------
        # x_all, y_all = [], []
        # for k, v in dict_data_final_all.items():  # 生成x,y两个坐标轴的值
        #     for i in range(2):
        #         x_all.append(v[i][1])
        #         y_all.append(v[i][2])
        # # ----------------------------------------------------------------------------------------
        # x_1, y_1 = [], []
        # for k, v in dict_data_final_1.items():  # 生成x,y两个坐标轴的值
        #     for i in range(2):
        #         x_1.append(v[i][1])
        #         y_1.append(v[i][2])
        # ----------------------------------------------------------------------------------------
        # x_2, y_2 = [], []
        # for k, v in dict_data_final_2.items():  # 生成x,y两个坐标轴的值
        #     for i in range(2):
        #         x_2.append(v[i][1])
        #         y_2.append(v[i][2])
        # ----------------------------------------------------------------------------------------
        # x_3, y_3 = [], []
        # for k, v in dict_data_final_3.items():  # 生成x,y两个坐标轴的值
        #     for i in range(2):
        #         x_3.append(v[i][1])
        #         y_3.append(v[i][2])
        # ----------------------------------------------------------------------------------------
        x_4, y_4 = [], []
        for k, v in dict_data_final_4.items():  # 生成x,y两个坐标轴的值
            for i in range(2):
                x_4.append(v[i][1])
                y_4.append(v[i][2])
        # ----------------------------------------------------------------------------------------
        plt.figure('Scatter', facecolor='lightgray')
        plt.title('碳滑板磨耗散点图', fontsize=20)
        plt.xlabel('里程/km', fontsize=14)
        plt.ylabel('厚度/mm', fontsize=14)
        plt.tick_params(labelsize=10)  # 记号参数
        # plt.subplot(211)
        # plt.scatter(x_1, y_1)
        # plt.subplot(212)
        plt.scatter(x_4, y_4)
        for k,v in dict_data_final_4.items():
            plt.plot([v[0][1], v[-1][1]],[v[0][2], v[-1][2]])
        plt.grid(linestyle=':')
        # ----------------------------------------------------------------------------------------
        # fp1, residuals, rank, sv, rcond = sp.polyfit(x_1, y_1, 1, full=True)  # 1阶直线
        # f1 = sp.poly1d(fp1)
        # fx = sp.linspace(0, x_1[-1], 500)
        # plt.plot(fx,f1(fx),linewidth=1)
        #
        # fp1, residuals, rank, sv, rcond = sp.polyfit([0,16982],[36.7,24.6], 1, full=True)  # 2阶曲线
        # f1 = sp.poly1d(fp1)
        # # fx = sp.linspace(0, x_1[-1], 500)
        # plt.plot(fx, f1(fx), linewidth=1)
        #
        # fp1, residuals, rank, sv, rcond = sp.polyfit([0,43096],[37.3,28.5], 1, full=True)  # 2阶曲线
        # f1 = sp.poly1d(fp1)
        # # fx = sp.linspace(0, x_1[-1], 500)
        # plt.plot(fx, f1(fx), linewidth=1)

        # f2p = sp.polyfit([0,43096,46096],[37.3,28.5, 27.5], 2)  # 2阶曲线
        # f2 = sp.poly1d(f2p)
        # fx = sp.arange(0,x_1[-1],1)
        # plt.plot(fx, f2(fx), linewidth=1)
        #
        # f2p_ = sp.polyfit([0,16982],[36.7,24.6], 2)  # 2阶曲线
        # f2_ = sp.poly1d(f2p_)
        # fx = sp.arange(0,x_1[-1],1)
        # plt.plot(fx, f2_(fx), linewidth=1)
        # plt.grid(linestyle=':')

        plt.show()

    def main(self,readPath):
        readData = self.read_csv(readPath,2,600)
        # print(readData[0])
        dict_data_pre = self.deal_data_step_one(readData,[3,7,11,15])
        print(dict_data_pre)
        dict_data_final_all, dict_data_final_1, dict_data_final_2,dict_data_final_3,dict_data_final_4\
            = self.deal_data_step_two(dict_data_pre)
        # print(dict_data_final_all)
        # self.show_plot(dict_data_final_all, dict_data_final_1, dict_data_final_2,dict_data_final_3,dict_data_final_4)


readPath = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/4.analyseFiles/data_thickness_read.csv"
analReadPath = "C:/Users/samsung/Desktop/7.检修数据处理需求ForDrLi/6.人工费物料费分析/read_data.csv"
tanhuabanDataAnalysis().main(readPath)



































