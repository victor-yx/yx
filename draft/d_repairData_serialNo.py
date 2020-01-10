# -*- coding:utf-8 -*-
# @Time    : 2019/10/25 上午13:55
# @Author  : Victor
# @Site    :
# @File    : d_repairData_serialNo.py
# @Software: PyCharm

import numpy as np

class cleanSerialNo():
    def read_csv(self, path_csv):
        '''
        funcion：从csv文件中读取数据
        return：含有训练数据的array数组
        '''
        inp_data = []
        count = 0
        with open(path_csv, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                if  count >= 1 \
                and count <= 30:
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

res = cleanSerialNo().read_csv('F:/XJ_Meeting/7.检修数据处理需求ForDrLi/2.DataPreHandle/preData_3.csv')
print(res)