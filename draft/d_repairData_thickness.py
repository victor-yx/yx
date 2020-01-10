# -*- coding:utf-8 -*-
# @Time    : 2019/10/25 上午13:55
# @Author  : Victor
# @Site    :
# @File    : d_repairData_serialNo.py
# @Software: PyCharm

import numpy as np
import jieba
jieba.load_userdict('userdict.txt')  # 载入自定义词典
# import jieba.analyse
# jieba.analyse.set_stop_words('stop_word.txt')  # 载入自定义停止词
import jieba.posseg
import re
import csv
import basic_config.bc_repairdata as re_config
from math import floor

class cleanMeasuredValue():
    def read_csv(self, path_csv,start_num,end_num):
        inp_data = []
        count =1
        with open(path_csv, 'r', encoding='utf-8-sig') as f:
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

    def partition(self,ls, size):
        """
        Returns a new list with elements
        of which is a list of certain size.
        """
        list_ = [ls[i:i + size] for i in range(0, len(ls), size)]
        list__ = [tuple(i) for i in list_]
        return list__

    def jieba_all_model(self,string):
        # 全模式（把句子在所有可以成词的成语都扫描出来，速度非常快，但是不能解决歧义的问题）
        gen_jiebawords = jieba.cut(str(string).replace('\\n','').replace('/n','').replace('\\','').replace('/','').replace('[','').replace(']',''),cut_all=True)
        str_jiebawords = " ".join(gen_jiebawords).lstrip(" ").rstrip(" ")
        list_jiebawords = str_jiebawords.split(" ")
        return list_jiebawords

    def jieba_accurate_model(self,string):
        # 精确模式（试图将句子最精确的切开，适合文本分析）
        gen_jiebawords = jieba.cut(str(string).replace('\\n','').replace('/n','').replace('\\','').replace('/','').replace('[','').replace(']',''),cut_all=False)
        str_jiebawords = " ".join(gen_jiebawords).lstrip(" ").rstrip(" ")
        list_jiebawords = str_jiebawords.split(" ")
        return list_jiebawords

    def jieba_searchEngine_model(self,string):
        # 搜索引擎模式（在精确模式的基础上，对长词再次切分，提高召回率）
        gen_jiebawords =  jieba.cut_for_search(string)
        str_jiebawords = " ".join(gen_jiebawords).lstrip(" ").rstrip(" ")
        list_jiebawords = str_jiebawords.split(" ")
        return list_jiebawords

    def jieba_pseg_model(self,string):
        # 词性标注模式
        gen_jiebawords = jieba.posseg.cut(string)
        str_jiebawords = " ".join(["{0}/{1}".format(w, t) for w, t in gen_jiebawords])
        list_jiebawords = str_jiebawords.split(" ")
        return list_jiebawords

    def stop_pooling_gene(self, list_):
        # 去除数字/大小写英文/停用词文本中的词
        save_word_path = r'save_word.txt'
        save_word = {x.replace('\n', '') for x in open(save_word_path, 'r', encoding='utf8').readlines()}
        p = re.compile(r'[a-zA-Z0-9]*')
        gen_jiebawords = [p.sub('', x) for x in list_ if x not in save_word]
        str_jiebawords = " ".join(gen_jiebawords).lstrip(" ").rstrip(" ")
        list_jiebawords = str_jiebawords.split(" ")
        return list_jiebawords

    def list_filter_pooling(self,list_acc,list_pool):
        # 比较两个列表，根据需求选出需要的数据
        new_list = []
        stop_word_path = r'stop_word.txt'
        stop_word = {x.replace('\n', '') for x in open(stop_word_path, 'r', encoding='utf8').readlines()}
        for i in list_acc:
            if i not in list_pool and i not in stop_word and i is not "":
                new_list.append(i)
        return new_list

    def get_branket_contents(self,string):
        list_res = [i.replace(" ", "") for i in re.findall("（([\s\S]*?)）", string)]
        return list_res

    def deal_fenci_str_1(self,string):
        list_ed = []
        string += " 结尾"
        string = string.replace("旧 新 前","旧新前").replace("新 前","新前").replace("旧 前","旧前")
        list_ = string.split(" ")
        list_.append("test")
        # print("checkPoiet_1：",list_)
        for index in range(len(list_[:-1])):
            if list_[index] in re_config.stop_strs:
                if re.match("[23]{1}[0-9]{1}\.[0-9]",list_[index+1]) != None or re.match("[23]{1}[0-9]{1}\.[0-9]",list_[index]) != None or re.match("[123]",list_[index]) or re.match("[123]",list_[index+1]):
                    list_ed.append(list_[index])
            else:
                list_ed.append(list_[index])
        # print("checkPoiet_2：",list_ed)
        index_jiuxinqian = list_ed.index("旧新前") if "旧新前" in list_ed else None
        index_jiuqian = list_ed.index("旧前") if "旧前" in list_ed else None
        index_xinqian = list_ed.index("新前") if "新前" in list_ed else None
        index_jiuxinhou = list_ed[index_jiuxinqian:].index("后") if index_jiuxinqian is not None and "后" in list_ed[index_jiuxinqian:] else None
        index_jiuhou = list_ed[index_jiuqian:].index("后") if index_jiuqian is not None and "后" in list_ed[index_jiuqian:] else None
        index_xinhou = list_ed[index_xinqian:].index("后") if index_xinqian is not None and "后" in list_ed[index_xinqian:] else None
        index_jiuxinhou_all = index_jiuxinqian + index_jiuxinhou if index_jiuxinhou is not None else -1
        index_jiuhou_all = index_jiuqian + index_jiuhou if index_jiuhou is not None else -1
        index_xinhou_all = index_xinqian + index_xinhou if index_xinhou is not None else -1
        list_ed[int(index_jiuxinhou_all)] = "旧新后" if index_jiuxinhou_all is not None else list_ed[int(index_jiuxinhou_all)]
        list_ed[int(index_jiuhou_all)] = "旧后" if index_jiuhou_all is not None else list_ed[int(index_jiuhou_all)]
        list_ed[int(index_xinhou_all)] = "新后" if index_xinhou_all is not None else list_ed[int(index_xinhou_all)]
        string_ = " ".join(list_ed[:-1])
        return string_

    def deal_fenci_str_2(self,string):
        list_ed = []
        string += " 结尾"
        string = string.replace("旧 新 前","旧新前").replace("新 前","新前").replace("旧 前","旧前").replace("旧 新 后","旧新后").replace("新 后","新后").replace("旧 后","旧后t")
        list_ = string.split(" ")
        list_.append("test")
        # print("checkPoiet_1：",list_)
        for index in range(len(list_[:-1])):
            if list_[index] in re_config.stop_strs:
                if re.match(re_config.serialNo,list_[index+1]) != None or re.match(re_config.serialNo,list_[index]) != None:
                    list_ed.append(list_[index])
            else:
                list_ed.append(list_[index])
        # print("checkPoiet_2：",list_ed)
        index_jiuxinqian = list_ed.index("旧新前") if "旧新前" in list_ed else None
        index_jiuqian = list_ed.index("旧前") if "旧前" in list_ed else None
        index_xinqian = list_ed.index("新前") if "新前" in list_ed else None
        index_jiuxinhou = list_ed[index_jiuxinqian:].index("后") if index_jiuxinqian is not None and "后" in list_ed[index_jiuxinqian:] else None
        index_jiuhou = list_ed[index_jiuqian:].index("后") if index_jiuqian is not None and "后" in list_ed[index_jiuqian:] else None
        index_xinhou = list_ed[index_xinqian:].index("后") if index_xinqian is not None and "后" in list_ed[index_xinqian:] else None
        index_jiuxinhou_all = index_jiuxinqian + index_jiuxinhou if index_jiuxinhou is not None else -1
        index_jiuhou_all = index_jiuqian + index_jiuhou if index_jiuhou is not None else -1
        index_xinhou_all = index_xinqian + index_xinhou if index_xinhou is not None else -1
        list_ed[int(index_jiuxinhou_all)] = "旧新后" if index_jiuxinhou_all is not None else list_ed[int(index_jiuxinhou_all)]
        list_ed[int(index_jiuhou_all)] = "旧后" if index_jiuhou_all is not None else list_ed[int(index_jiuhou_all)]
        list_ed[int(index_xinhou_all)] = "新后" if index_xinhou_all is not None else list_ed[int(index_xinhou_all)]
        string_ = " ".join(list_ed[:-1])
        return string_

    def get_serial_contents_123(self,string):
        list_all = []
        for i in ["1","2","3"]:
            list_res_ = [string.split(" ")[j + 1] for j in [index for index, value in enumerate(string.split(" ")) if value == i]]
            list_all.append(list_res_)
        return list_all

    def get_serial_contents_hanzi(self,string):
        dict_all = {}
        string += " 结尾"
        for i in ["旧前","旧后","新前","新后"]:
            list_res_ = re.findall(r"{}([\s\S]*?)[\u4e00-\u9fa5]".format(i),string)
            list_res_ = list_res_[0].lstrip(" ").rstrip(" ").split(" ") if len(list_res_) != 0 else ""
            list_tmp = []
            list_tmp.append(list_res_[list_res_.index("1") + 1] if list_res_.index("1")+1 < len(list_res_) else None) if '1' in list_res_ else list_tmp.append(None)
            list_tmp.append(list_res_[list_res_.index("2") + 1] if list_res_.index("2")+1 < len(list_res_) else None) if '2' in list_res_ else list_tmp.append(None)
            list_tmp.append(list_res_[list_res_.index("3") + 1] if list_res_.index("3")+1 < len(list_res_) else None) if '3' in list_res_ else list_tmp.append(None)
            if len(list_res_) in [3,6]:
                # print(list_res_)
                list_res_ = list_res_[::2] if len(list_res_) == 6 else list_res_
                list_tmp.append(list_res_[0])
                list_tmp.append(list_res_[1])
                list_tmp.append(list_res_[2])
            elif list_res_ == '':
                list_tmp.append(None)
                list_tmp.append(None)
                list_tmp.append(None)
            dict_all[i] = list_tmp
        return dict_all

    def get_serial_contents_none(self,_):
        dict_all = {}
        for i in ["旧前", "旧后", "新前", "新后"]:
            list_tmp = [None,None,None]
            dict_all[i] = list_tmp

        return dict_all

    def get_trainno_contents(self,id,string,ori_handle_data):
        list_res = re.findall(re_config.carriage,string)
        if list_res == []:
            list_res = re.findall(re_config.carriage,ori_handle_data[ori_handle_data[:,0].tolist().index(id)][7])
            if list_res == []:
                list_res = re.findall(re_config.carriage, ori_handle_data[ori_handle_data[:, 0].tolist().index(id)][8])
        # str_res = ",".join(list_res) if len(list_res) != 0 else None
        return list_res

    def deal_ori_data(self,ori_handle_data, list_column_nums):
        new_ori_data = ori_handle_data.tolist()
        for line in new_ori_data:
            for column in list_column_nums:
                # 去除回车行等符号
                line[column] = line[column].replace('\\n', '').replace('/n', '').replace('\r', '').replace('\n', '')
                # 车厢号统一
                line[column] = line[column].replace("mp2车", "mp2").replace("MP2车", "MP2").replace("mp1车", "mp1").replace("MP1车", "MP1")
                # 统一单位
                line[column] = line[column].replace("mmm","*&*&*").replace("MMM","*&*&*").replace("mm","*&*&*").replace("MM","*&*&*").replace("*&*&*","mm")
                # 清洗"两厚度值连体"的情况
                for i in re.findall('[234]{1}[0-9]{1}\.[0-9][234]{1}[0-9]{1}\.[0-9]', line[column]):
                    j = i[:4] + ' ' + i[4:]
                    line[column] = line[column].replace(i,j)
                # 清洗"序号点连体"的情况
                for i in re.findall('[123]\.[0-9]{2,3}\.[0-9]',line[column]):
                    j = i[0] + ' ' + i[2:]
                    line[column] = line[column].replace(i,j)
                # 清洗"相似字替换"的情况
                for k,v in re_config.replace_str.items():
                    line[column] = line[column].replace(k, v)
                for k,v in {"新前前":"新前","新后后":"新后"}.items():
                    line[column] = line[column].replace(k, v)
                # 清洗"数据之间用斜杠分开"的情况
                line[column] = line[column].replace("/"," ")
                # 清洗"单位数字连体"的情况
                line[column] = line[column].replace("mm2","mm 2",).replace("mm.","mm ",).replace("mm3","mm 3",).replace("mm4","mm 4",).replace("mm(2)","mm (2)",).replace("mm(3)","mm (3)",).replace("mm(4)","mm (4)",)
        new_ori_data = np.array(new_ori_data)
        return new_ori_data

    def deal_serialNo_info(self,serialNoInfo,num):
        info = serialNoInfo.replace(" ","").replace("-","").replace("旧前","前").replace("旧后","后").replace("新前","前").replace("新后","后")
        if num == 14:
            info = info.replace("前","新前").replace("后","新后")
        elif num == 15:
            info = info.replace("前","旧前").replace("后","旧后")
        return info

    def check_thickness(self,thickness):
        length_ = len(thickness) / 3
        thickness_ = self.partition(thickness, floor(len(thickness)/length_))
        # thickness_ = list(set(thickness_))
        return thickness_

    def set_label_value(self,distNo,mark):
        if mark == 1:
            if len(distNo) == 24:
                label = 4
            elif len(distNo) == 12:
                label = 2
            elif len(distNo) == 6:
                label = 1
            elif len(distNo) == 0:
                label = 0
            else:
                label = -1
        elif mark == 2:
            if len(distNo) == 8:
                label = 4
            elif len(distNo) == 4:
                label = 2
            elif len(distNo) == 2:
                label = 1
            elif len(distNo) == 0:
                label = 0
            else:
                label = -1
        return label

    def set_dict_res(self,label,line,dict_):
        if label == 4:
            dict_["4"].append(line[0])
        elif label == 2:
            dict_["2"].append(line[0])
        elif label == 1:
            dict_["1"].append(line[0])
        elif label == 0:
            dict_["0"].append(line[0])
        elif label == -1:
            dict_["-1"].append(line[0])
        else:
            pass
        return dict_

    def nlp_step_one(self,ori_data, list_column_nums):
        list_exc_one = []
        dict_no_all = {}
        dict_no_all["-1"] = []
        dict_no_all["0"] = []
        dict_no_all["1"] = []
        dict_no_all["2"] = []
        dict_no_all["4"] = []
        dict_serial_9 = {}
        dict_serial_9["-1"] = []
        dict_serial_9["0"] = []
        dict_serial_9["1"] = []
        dict_serial_9["2"] = []
        dict_serial_9["4"] = []
        for line in ori_data:
            list_tmp_1 = []
            for column in list_column_nums:
                carriage = re.findall(re_config.carriage,line[column])  # 提取数字
                thickness = re.findall(re_config.t_v_2,line[column]) if column == 9 else  re.findall(re_config.t_v_1,line[column]) # 提取厚度值
                serialNo_9 = re.findall(re_config.serialNo,line[column])  # 提取序列号

                label_t_v = self.set_label_value(thickness,1) if column == 9 else -99
                label_s_n = self.set_label_value(serialNo_9,2) if column == 9 else -99

                dict_no_all = self.set_dict_res(label_t_v,line,dict_no_all)
                dict_serial_9 = self.set_dict_res(label_s_n,line,dict_serial_9)

                list_tmp_2 = [carriage,thickness,serialNo_9]
                list_tmp_1.append(list_tmp_2)
            list_exc_one.append(list_tmp_1)
        return dict_no_all,dict_serial_9

    def nlp_step_two(self, ori_handle_data, list_column_nums):
        dict_fenci_all = {}
        ori_p0_data = self.deal_ori_data(ori_handle_data, [7,8,9])
        for line in ori_p0_data:
            str_tmp = ""
            for column in list_column_nums:
                # list_allCuted = self.jieba_all_model(line[column])
                # list_srcCuted = self.jieba_searchEngine_model(line[column])
                # list_psgCuted = self.jieba_pseg_model(line[column])
                list_accCuted = self.jieba_accurate_model(line[column])
                list_pooling = self.stop_pooling_gene(list_accCuted)
                res = self.list_filter_pooling(list_accCuted,list_pooling)
                res = " ".join(i for i in res)
                # print("%-10s --> "%"finalRes",res)
                str_tmp += res
                str_tmp += " "
                # print("--" * 100)
            dict_fenci_all[line[0]] = (str_tmp.strip(" "))
        return dict_fenci_all,ori_p0_data

    def nlp_step_2point5(self, oriData, list_column_nums):
        dict_serial_14, dict_serial_15,dict_serial_14_15 = {},{},{}
        for line in oriData:
            for column in list_column_nums:
                str_tmp = ""
                ori_serialNo_info_ed = self.deal_serialNo_info(line[column], column)
                list_accCuted = self.jieba_accurate_model(ori_serialNo_info_ed)
                list_pooling = self.stop_pooling_gene(list_accCuted)
                res = self.list_filter_pooling(list_accCuted,list_pooling)
                res = " ".join(i for i in res)
                str_tmp += res
                str_tmp += " "
                if column == 14:
                    dict_serial_14[line[0]] = (str_tmp.strip(" "))
                elif column == 15:
                    dict_serial_15[line[0]] = (str_tmp.strip(" "))
        for k,v in dict_serial_14.items():
            dict_serial_14_15[k] = v + " " + dict_serial_15[k]
        return dict_serial_14_15

    def self_check_1(self,list_):
        list_ed = []
        list_.append("test")
        for index in range(len(list_[:-1])):
            if list_[index] in re_config.stop_strs:
                if re.match("[23]{1}[0-9]{1}\.[0-9]",list_[index+1]) != None:
                    list_ed.append(list_[index])
            else:
                list_ed.append(list_[index])
        return list_ed

    def self_check_2(self,list_):
        list_ed = []
        list_.append("test")
        for index in range(len(list_[:-1])):
            if list_[index] in re_config.stop_strs:
                if re.match(re_config.serialNo,list_[index+1]) != None:
                    list_ed.append(list_[index])
            else:
                list_ed.append(list_[index])
        return list_ed

    def get_houdu_1(self, res_fenci, list_trainNo):
        dict_res_final_1 = {}
        res_fenci = "开头 " + res_fenci + " 结尾"
        list_1 = re.findall('开头 ([\s\S]*?) 结尾', res_fenci)
        res_1 = self.deal_fenci_str_1(list_1[0].strip(" "))
        # print(res_1)
        list_final_res_old,list_final_res = [res_1],[]
        for i in list_final_res_old[0].split(" "):
            if re.match("[23]{1}[0-9]{1}\.[0-9]",i) != None or i in re_config.stop_strs:
                list_final_res.append(i)
        list_final_res = self.self_check_1(list_final_res)
        list_final_res = [" ".join(i for i in list_final_res)]
        # print(list_final_res)
        list_trainNo = ["NotMentioned"] if len(list_trainNo) == 0 else list_trainNo
        list_trainNo = list(set(list_trainNo))
        for i in range(len(list_trainNo)):
            dict_res_final_1[list_trainNo[i]] = {}
            try:
                list_res_tmp = list_final_res[i].split(" ")
                for j in ["旧前", "旧后", "新前", "新后"]:
                    if j not in list_res_tmp:
                        continue
                    dict_res_final_1[list_trainNo[i]][j] = []
                    for k in range(1, 4):
                        dict_res_final_1[list_trainNo[i]][j].append(list_res_tmp[list_res_tmp.index(j) + k])
            except IndexError:
                continue
        return dict_res_final_1

    def get_serial_1(self, res_fenci, list_trainNo):
        dict_res_final_1 = {}
        res_fenci = "开头 " + res_fenci + " 结尾"
        list_1 = re.findall('开头 ([\s\S]*?) 结尾', res_fenci)
        res_1 = self.deal_fenci_str_2(list_1[0].strip(" "))
        # print(res_1)
        list_final_res_old,list_final_res = [res_1],[]
        for i in list_final_res_old[0].split(" "):
            if re.match(re_config.serialNo,i) != None or i in re_config.stop_strs:
                list_final_res.append(i)
        list_final_res = self.self_check_2(list_final_res)
        list_final_res = [" ".join(i for i in list_final_res)]
        # print(list_final_res)
        list_trainNo = ["NotMentioned"] if len(list_trainNo) == 0 else list_trainNo
        list_trainNo = list(set(list_trainNo))
        for i in range(len(list_trainNo)):
            dict_res_final_1[list_trainNo[i]] = {}
            try:
                list_res_tmp = list_final_res[i].split(" ")
                for j in ["旧前", "旧后", "新前", "新后"]:
                    if j not in list_res_tmp:
                        continue
                    dict_res_final_1[list_trainNo[i]][j] = []
                    for k in range(1):
                        dict_res_final_1[list_trainNo[i]][j].append(list_res_tmp[list_res_tmp.index(j) + 1])
            except IndexError:
                continue
        return dict_res_final_1

    def get_houdu_2(self, res_fenci, list_trainNo):
        dict_res_final_2 = {}
        res_fenci = "开头 " + res_fenci + " 结尾"
        list_1 = re.findall('开头 ([\s\S]*?) 结尾', res_fenci)
        res_1 = self.deal_fenci_str_1(list_1[0].strip(" "))
        # print(res_1)
        list_final_res_old, list_final_res = [res_1], []
        for i in list_final_res_old[0].split(" "):
            if re.match("[23]{1}[0-9]{1}\.[0-9]",i) != None or i in re_config.stop_strs:
                list_final_res.append(i)
        list_final_res = self.self_check_1(list_final_res)
        list_final_res = [" ".join(i for i in list_final_res)]
        # print(list_final_res)
        list_trainNo = ["NotMentioned"] if len(list_trainNo) == 0 else list_trainNo
        list_trainNo = list(set(list_trainNo))
        for i in range(len(list_trainNo)):
            dict_res_final_2[list_trainNo[i]] = {}
            try:
                list_res_tmp = list_final_res[i].split(" ")
                if "旧新前" in list_res_tmp:
                    for j in ["旧前","旧后", "新前",  "新后"]:
                        dict_res_final_2[list_trainNo[i]][j] = []
                    for k in range(1, 6, 2):
                        dict_res_final_2[list_trainNo[i]]["旧前"].append(list_final_res[i].split(" ")[list_final_res[i].split(" ").index("旧新前") + k])
                        dict_res_final_2[list_trainNo[i]]["旧后"].append(list_final_res[i].split(" ")[list_final_res[i].split(" ").index("旧新后") + k])
                    for k in range(2, 7, 2):
                        dict_res_final_2[list_trainNo[i]]["新前"].append(list_final_res[i].split(" ")[list_final_res[i].split(" ").index("旧新前") + k])
                        dict_res_final_2[list_trainNo[i]]["新后"].append(list_final_res[i].split(" ")[list_final_res[i].split(" ").index("旧新后") + k])
                    continue
                for j in ["旧前", "旧后", "新前", "新后"]:
                    if j not in list_res_tmp:
                        continue
                    dict_res_final_2[list_trainNo[i]][j] = []
                    for k in range(1, 4):
                        dict_res_final_2[list_trainNo[i]][j].append(list_res_tmp[list_res_tmp.index(j) + k])
            except IndexError:
                continue
        return dict_res_final_2

    def get_serial_2(self, res_fenci, list_trainNo):
        dict_res_final_2 = {}
        res_fenci = "开头 " + res_fenci + " 结尾"
        list_1 = re.findall('开头 ([\s\S]*?) 结尾', res_fenci)
        res_1 = self.deal_fenci_str_2(list_1[0].strip(" "))
        # print("point_1:",res_1)
        list_final_res_old, list_final_res = [res_1], []
        for i in list_final_res_old[0].split(" "):
            if re.match(re_config.serialNo,i) != None or i in re_config.stop_strs:
                list_final_res.append(i)
        list_final_res = self.self_check_2(list_final_res)
        list_final_res = [" ".join(i for i in list_final_res)]
        print("point_2:",list_final_res[0].split(" "))
        list_trainNo = ["NotMentioned"] if len(list_trainNo) == 0 else list_trainNo
        list_trainNo = list(set(list_trainNo))
        for i in range(len(list_trainNo)):
            dict_res_final_2[list_trainNo[i]] = {}
            try:
                list_res_tmp = list_final_res[i].split(" ")
                if "旧新前" in list_res_tmp:
                    for j in ["旧前","旧后", "新前",  "新后"]:
                        dict_res_final_2[list_trainNo[i]][j] = []
                    for k in range(1,2):
                        dict_res_final_2[list_trainNo[i]]["旧前"].append(list_res_tmp[list_res_tmp.index("旧新前") + k])
                        dict_res_final_2[list_trainNo[i]]["旧后"].append(list_res_tmp[list_res_tmp.index("旧新后") + k])
                    for k in range(2,3):
                        dict_res_final_2[list_trainNo[i]]["新前"].append(list_res_tmp[list_res_tmp.index("旧新前") + k])
                        dict_res_final_2[list_trainNo[i]]["新后"].append(list_res_tmp[list_res_tmp.index("旧新后") + k])
                    continue
                for j in ["旧前", "旧后", "新前", "新后"]:
                    if j not in list_res_tmp:
                        # if len(list_res_tmp) == 2 and re.match(re_config.serialNo,list_res_tmp[0])!= None and re.match(re_config.serialNo,list_res_tmp[-1]) != None:
                        #     dict_res_final_2[list_trainNo[i]]["新后"] = [list_res_tmp[0]]
                        #     dict_res_final_2[list_trainNo[i]][""]
                        continue
                    dict_res_final_2[list_trainNo[i]][j] = [] if j in dict_res_final_2[list_trainNo[i]].keys() else dict_res_final_2[list_trainNo[i]][j]
                    for k in range(1):
                        dict_res_final_2[list_trainNo[i]][j].append(list_res_tmp[list_res_tmp.index(j) + 1])
            except IndexError:
                continue
        return dict_res_final_2

    def get_houdu_4(self,res_fenci,list_trainNo):
        dict_res_final_4 = {}
        res_fenci += " 结尾"
        list_1 = re.findall('{}([\s\S]*?){}'.format(list_trainNo[0],list_trainNo[-1]),res_fenci)
        res_1 = self.deal_fenci_str_1(list_1[0].strip(" "))
        list_2 = re.findall('{}([\s\S]*?) 结尾'.format(list_trainNo[-1]),res_fenci)
        res_2 = self.deal_fenci_str_1(list_2[0].strip(" "))
        list_final_res = [res_1,res_2]
        list_trainNo = ["Notmentioned_1","NotMentioned_2"] if len(list_trainNo) == 0 else list_trainNo
        list_trainNo = list(set(list_trainNo))
        for i in range(len(list_trainNo)):
            dict_res_final_4[list_trainNo[i]] = {}
            for j in ["旧前", "旧后", "新前", "新后"]:
                dict_res_final_4[list_trainNo[i]][j] = []
                for k in range(1,4):
                    dict_res_final_4[list_trainNo[i]][j].append(list_final_res[i].split(" ")[list_final_res[i].split(" ").index(j) + k])
        return dict_res_final_4

    def get_serial_4(self,res_fenci,list_trainNo):
        dict_res_final_4 = {}
        res_fenci += " 结尾"
        list_1 = re.findall('{}([\s\S]*?){}'.format(list_trainNo[0],list_trainNo[-1]),res_fenci)
        res_1 = self.deal_fenci_str_2(list_1[0].strip(" "))
        list_2 = re.findall('{}([\s\S]*?) 结尾'.format(list_trainNo[-1]),res_fenci)
        res_2 = self.deal_fenci_str_2(list_2[0].strip(" "))
        list_final_res = [res_1,res_2]
        list_trainNo = ["Notmentioned_1","NotMentioned_2"] if len(list_trainNo) == 0 else list_trainNo
        list_trainNo = list(set(list_trainNo))
        for i in range(len(list_trainNo)):
            dict_res_final_4[list_trainNo[i]] = {}
            list_res_tmp = list_final_res[i].split(" ")
            for j in ["旧前", "旧后", "新前", "新后"]:
                dict_res_final_4[list_trainNo[i]][j] = []
                for k in range(1):
                    dict_res_final_4[list_trainNo[i]][j].append(list_res_tmp[list_res_tmp.index(j) + 1])
        return dict_res_final_4

    def get_final_value(self,k,v,list_trainNo,dict_all,mark):
        if k in dict_all["4"]:
            dict_final_value = self.get_houdu_4(v, list_trainNo) if mark == 1 else self.get_serial_4(v, list_trainNo)
        elif k in dict_all["2"]:
            dict_final_value = self.get_houdu_2(v, list_trainNo) if mark == 1 else self.get_serial_2(v, list_trainNo)
        elif k in dict_all["1"]:
            dict_final_value = self.get_houdu_1(v, list_trainNo) if mark == 1 else self.get_serial_1(v, list_trainNo)
        else:
            dict_final_value = {}
        return dict_final_value

    def nlp_step_three(self,dict_fenci_all,dict_no_all,ori_p0_data):
        dict_final_value = {}
        for k,v in dict_fenci_all.items():
            list_trainNo = self.get_trainno_contents(k,v,ori_p0_data)
            dict_digit_all = self.get_final_value(k,v,list_trainNo,dict_no_all,1)
            dict_final_value[k] = dict_digit_all
        return dict_final_value

    def nlp_step_3point5(self,dict_serial_14_15):
        dict_final_value = {}
        for k,v in dict_serial_14_15.items():
            list_trainNo = ["serialNoTest"]
            dict_serial_pre = self.get_serial_2(v,list_trainNo)
            dict_final_value[k] = dict_serial_pre
        return dict_final_value

    def nlp_step_four(self,dict_fenci_all,dict_serial_all,ori_p0_data):
        dict_final_value = {}
        for k,v in dict_fenci_all.items():
            list_trainNo = self.get_trainno_contents(k,v,ori_p0_data)
            dict_serial_value = self.get_final_value(k,v,list_trainNo,dict_serial_all,2)
            dict_final_value[k] = dict_serial_value
        return dict_final_value

    def merge_final_data(self,list_tmp,list_tmp_tmp):
        for i in range(3):
            list_tmp.append(list_tmp_tmp[0][i])
            list_tmp.append(list_tmp_tmp[2][i])
        for i in range(3):
            list_tmp.append(list_tmp_tmp[1][i])
            list_tmp.append(list_tmp_tmp[3][i])
        return list_tmp

    def get_reg_no(self,id_,dict_no):
        for i in dict_no.keys():
            if id_ in dict_no[i]:
                return i
        return -999

    def get_real_no(self,id_,dict_final):
        if dict_final[id_] == {}:
            return 0
        else:
            for carriage in dict_final[id_].keys():
                if  dict_final[id_][carriage] == {}:
                    return 0
                elif len(dict_final[id_][carriage].keys()) == 1:
                    return -1
                elif len(dict_final[id_][carriage].keys()) == 2:
                    return 1
                elif len(dict_final[id_][carriage].keys()) == 4:
                    return 2
                elif len(dict_final[id_][carriage].keys()) == 8:
                    return 4
                else:
                    return -99

    def get_data_by_dict(self,dict_final,key):
        if key in dict_final.keys():
            return dict_final[key]
        else:
            return ["","",""]

    def gene_final_input(self,dict_digit_final,dict_serial_final,dict_digit_no,dict_serial_no,dict_serial_final_14_15,oriData):
        oriData = oriData.tolist()
        for i in oriData:
            for k in ["旧前", "新前", "旧后", "新后"]:
                try:
                    i.append(self.get_data_by_dict(dict_serial_final_14_15[i[0]]['serialNoTest'], k)[0])
                except KeyError:
                    i.append("")
            i.append(self.get_reg_no(i[0],dict_serial_no))
            i.append(self.get_real_no(i[0],dict_serial_final))
            i.append(self.get_reg_no(i[0],dict_digit_no))
            i.append(self.get_real_no(i[0],dict_digit_final))
            for j in dict_digit_final[i[0]].keys():
                i.append(j)
                for k in ["旧前", "新前", "旧后", "新后"]:
                    try:
                        i.append(self.get_data_by_dict(dict_serial_final[i[0]][j],k)[0])
                    except KeyError:
                        i.append("")
                    try:
                        list_tmp = self.get_data_by_dict(dict_digit_final[i[0]][j],k)
                    except KeyError:
                        list_tmp = ["","",""]
                    for m in list_tmp:
                        i.append(m)
        oriData = np.array(oriData)
        return oriData

    def main(self, path_csv):
        oriData = self.read_csv(path_csv,re_config.bgnLine,re_config.endLine)
        # print(oriData)
        ori_handle_data = oriData[:,[0,1,2,3,4,7,8,9,10,11,12,13,14,15]]
        # print(ori_handle_data)
        dict_digit_no,dict_serial_no = self.nlp_step_one(ori_handle_data, [9])
        # print(dict_digit_no)
        # print(dict_serial9_no)
        dict_fenci_all,ori_p0_data = self.nlp_step_two(ori_handle_data, [9])
        # print(dict_fenci_all)
        dict_serial_14_15 = self.nlp_step_2point5(oriData,[14,15])
        print(dict_serial_14_15)
        dict_digit_final = self.nlp_step_three(dict_fenci_all,dict_digit_no,ori_handle_data)
        # print(dict_digit_final)
        dict_serial_final_14_15 = self.nlp_step_3point5(dict_serial_14_15)
        print(dict_serial_final_14_15)
        # dict_serial_final = self.nlp_step_four(dict_fenci_all,dict_serial_no,ori_handle_data)
        # print(dict_serial_final)
        # list_final_input = self.gene_final_input(dict_digit_final,dict_serial_final,dict_digit_no,dict_serial_no,dict_serial_final_14_15,oriData)
        # print(list_final_input)
        # self.Write2CsvByrows(list_final_input, re_config.columns_name,re_config.save_path)

cleanMeasuredValue().main(re_config.read_path)





