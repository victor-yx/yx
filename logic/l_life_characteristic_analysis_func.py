'''
create at 2019/6/11
'''
import math
from sklearn import linear_model
import pandas as pd
import numpy as np

class life_characteristic_analysis_func:
    def get_i(self,evaluate_list, result_map):
        for index in range(len(evaluate_list)):
            RR = len(evaluate_list) - index
            pre_index = index - 1
            while pre_index >= 0 and evaluate_list[pre_index][1] == "S":
                pre_index -= 1
            x = result_map.get(pre_index, 0)
            result_map[index] = (RR * x + (len(evaluate_list) + 1)) / (RR + 1)

    def evaluate(self,list_, list_size):
        i_map = {}
        x_list = []
        y_list = []
        MR_list = []
        t_list = []
        self.get_i(list_, i_map)
        for index in range(len(list_)):
            if list_[index][1] == "S":
                continue
            i = i_map.get(index)
            MR = (i - 0.3) / (list_size + 0.4)
            X = self.ln(self.ln(1 / (1 - MR)))
            t0 = 0
            Y = self.ln(list_[index][0] - t0)
            x_list.append(X)
            y_list.append(Y)
            MR_list.append(MR)
            t_list.append(list_[index][0])
        result = self.polyfit(x_list, y_list, 1)

        k = result['polynomial'][0]
        b = result['polynomial'][1]
        r2 = result['determination']
        return {"β": 1 / k, "η": math.e ** b, "r2": r2, "MR": MR_list, "t": t_list}

    def polyfit(self,x, y, degree):

        result = {}  # 定义一个字典
        coeffs = np.polyfit(x, y, degree)  # 直接求出b0 b1 b2 b3 ..的估计值
        result["polynomial"] = coeffs.tolist()

        p = np.poly1d(coeffs)  # 返回预测值
        yhat = p(x)  # 传入x 返回预测值
        ybar = np.sum(y) / len(y)  # 求均值
        ssreg = np.sum((yhat - ybar) ** 2)
        sstot = np.sum((y - ybar) ** 2)
        result["determination"] = ssreg / sstot

        return result

    def ln(self,x):
        return math.log(x, math.e)