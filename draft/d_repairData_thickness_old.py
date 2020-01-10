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
# jieba.analyse.set_stop_words('stop_words.txt')  # 载入自定义停止词
import jieba.posseg
import re
import csv
import sys

class cleanMeasuredValue():
    def read_csv(self, path_csv,start_num,end_num):
        '''
        funcion：从csv文件中读取数据
        return：含有训练数据的array数组
        '''
        inp_data = []
        count = 2
        with open(path_csv, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                if  count >= start_num \
                and count <= end_num:
                    data = [substr.strip("\n ") for substr in line.split(',')]
                    inp_data.append(data)
                count += 1
        inp_data = np.array(inp_data)
        return inp_data

    def Write2CsvByrows(self, list_tmp, list_cloumns, path_csv):
        '''
        funcion：把列表中的元素一次性全部写入csv文件中
        return：csv文件
        '''
        with open(path_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_cloumns)
            writer.writerows(list_tmp)

    def clearSen(self,comment):
        # 去掉中英文状态下的逗号、句号
        comment = comment.strip()
        comment = comment.replace('、', '')
        comment = comment.replace('，', '。')
        comment = comment.replace('《', '。')
        comment = comment.replace('》', '。')
        comment = comment.replace('～', '')
        comment = comment.replace('…', '')
        comment = comment.replace('\r', '')
        comment = comment.replace('\t', ' ')
        comment = comment.replace('\f', ' ')
        comment = comment.replace('/', '')
        comment = comment.replace('、', ' ')
        comment = comment.replace('/', '')
        comment = comment.replace('。', '')
        comment = comment.replace('（', '')
        comment = comment.replace('）', '')
        comment = comment.replace('_', '')
        comment = comment.replace('?', ' ')
        comment = comment.replace('？', ' ')
        comment = comment.replace('了', '')
        comment = comment.replace('➕', '')
        comment = comment.replace('：', '')
        return comment

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

    def Write2CsvByrows(self, list_contents, list_column, path_csv):
        with open(path_csv, 'w', newline='',encoding="utf-8-sig") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(list_column)
            writer.writerows(list_contents)

    def get_branket_contents(self,string):
        list_res = [i.replace(" ", "") for i in re.findall("（([\s\S]*?)）", string)]
        return list_res

    def deal_fenci_str(self,string):
        string += " 结尾"
        string = string.replace("新 前","新前").replace("旧 前","旧前")
        list_ = string.split(" ")
        index_jiuqian = list_.index("旧前") if "旧前" in list_ else None
        index_xinqian = list_.index("新前") if "新前" in list_ else None
        index_jiuhou = list_[index_jiuqian:].index("后") if index_jiuqian is not None and "后" in list_[index_jiuqian:] else None
        index_xinhou = list_[index_xinqian:].index("后") if index_xinqian is not None and "后" in list_[index_xinqian:] else None
        index_jiuhou_all = index_jiuqian + index_jiuhou if index_jiuhou is not None else -1
        index_xinhou_all = index_xinqian + index_xinhou if index_xinhou is not None else -1
        list_[int(index_jiuhou_all)] = "旧后" if index_jiuhou_all is not None else list_[int(index_jiuhou_all)]
        list_[int(index_xinhou_all)] = "新后" if index_xinhou_all is not None else list_[int(index_xinhou_all)]
        string_ = " ".join(list_[:-1])
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

    def get_trainno_contents(self,string):
        list_res = re.findall("MP[0-9]|[0-9]车",string)
        list_res = list(set(list_res))
        str_res = ",".join(list_res) if len(list_res) != 0 else None
        return str_res

    def nlp_step_one(self,ori_data, list_column_nums):
        for line in ori_data:
            for column in list_column_nums:
                #
                line[column] = line[column].replace('\\n', '').replace('/n', '').replace('\r', '').replace('\n', '')
                # 统一单位
                line[column] = line[column].replace("mmm","m").replace("MMM","m").replace("mm","m").replace("MM","m").replace("N","m").replace("n","m").replace("m","mm")
                # 清洗"数据之间用斜杠分开"的情况
                line[column] = line[column].replace("/"," ")
                # 清洗"序号点连体"的情况
                for i in re.findall('[0-9]\.[0-9]{2,3}\.[0-9]',line[column]):
                    j = i[0] + ' ' + i[2:]
                    line[column] = line[column].replace(i,j)
                # 清洗"相似字替换"的情况
                for k,v in {"换上":"新","换下":"旧","调整前":"新前","调整后":"新后","新件厚度":"新","旧件厚度":"旧"}.items():
                    line[column] = line[column].replace(k, v)
                for k,v in {"新前前":"新前","新后后":"新后"}.items():
                    line[column] = line[column].replace(k, v)
                # 清洗"单位数字连体"的情况
                line[column] = line[column].replace("mm2","mm 2",).replace("mm3","mm 3",).replace("mm4","mm 4",).replace("mm(2)","mm (2)",).replace("mm(3)","mm (3)",).replace("mm(4)","mm (4)",)

        return ori_data

    def nlp_step_two(self, ori_p0_data, list_column_nums):
        list_all = []
        for line in ori_p0_data:
            str_tmp = ""
            for cloumn in list_column_nums:
                # list_allCuted = self.jieba_all_model(line[cloumn])
                # list_srcCuted = self.jieba_searchEngine_model(line[cloumn])
                # list_psgCuted = self.jieba_pseg_model(line[cloumn])
                list_accCuted = self.jieba_accurate_model(line[cloumn])
                list_pooling = self.stop_pooling_gene(list_accCuted)
                res = self.list_filter_pooling(list_accCuted,list_pooling)
                res = " ".join(i for i in res)
                # print("%-10s --> "%"finalRes",res)
                str_tmp += res
                str_tmp += " "
                # print("--" * 100)
            list_all.append(str_tmp.strip(" "))
        list_all = np.array(list_all)
        return list_all

    def nlp_step_three(self,list_step_one):
        list_final_all = []
        for i in list_step_one:
            list_tmp = []
            # 对分词之后的字符串进行预处理
            i = self.deal_fenci_str(i)
            # print(i)
            str_res = self.get_trainno_contents(i)
            list_tmp.append(str_res)
            if ("（" and "）") in i.split(" "):
                list_res_branket = self.get_branket_contents(i)
            if "旧前" or "旧后" or "新前" or "新后" in i.split(" "):
                dict_res_serial_all = self.get_serial_contents_hanzi(i)
            elif ("1" or "2" or "3") in i.split(" "):
                dict_res_serial_all = self.get_serial_contents_123(i)
            else:
                dict_res_serial_all = self.get_serial_contents_none(i)
            for j in ["旧前", "旧后", "新前", "新后"]:
                for k in range(3):
                    list_tmp.append(dict_res_serial_all[j][k] if dict_res_serial_all[j] != [] else None)
            list_final_all.append(list_tmp)
        list_final_all = np.array(list_final_all)
        return list_final_all

    def main(self, path_csv):
        oriData = self.read_csv(path_csv,533,533)
        ori_handle_data = oriData[:,[0,1,2,3,4,7,8,9,10,11,12,13,14,15]]
        print(ori_handle_data)
        # ori_p0_data = self.nlp_step_one(ori_handle_data, [7,8,9])
        # print(ori_p0_data)
        # ori_p1_data = self.nlp_step_two(ori_p0_data, [7,8,9])
        # print(ori_p1_data)
        # ori_p2_data = self.nlp_step_three(ori_p1_data)
        # print(ori_p2_data)
        # list_final = np.delete(ori_p0_data,[7,8,9],1)
        # print(list_final)
        # list_final = np.column_stack((list_final,ori_p2_data))
        # # print(list_final)
        # list_final = list_final.tolist()
        self.Write2CsvByrows(list_final, ["原始序号","报告时间","车号","车厢号","运行里程(km)","所属系统","子系统","换件名称","换上件编号","换下件编号","完成时间","信息提取-车厢号","信息提取-厚度(mm)-旧前1","信息提取-厚度(mm)--旧前2","信息提取-厚度(mm)--旧前3","信息提取-厚度(mm)--旧后1","信息提取-厚度(mm)--旧后2","信息提取-厚度(mm)--旧后3","信息提取-厚度(mm)--新前1","信息提取-厚度(mm)--新前2","信息提取-厚度(mm)--新前3","信息提取-厚度(mm)--新后1","信息提取-厚度(mm)--新后2","信息提取-厚度(mm)--新后3"], \
                             "F:\\XJ_Meeting\\7.检修数据处理需求ForDrLi\\3.outputFile_v3\\test.csv")


path_csv = 'D:/XJ_Meeting/7.检修数据处理需求ForDrLi/2.DataPreHandle/preData_2.csv'
cleanMeasuredValue().main(path_csv)





