#!/bin/env python3
#coding=utf-8
# @Time    : 16/12/5 18:15
# @Author  : kelly
# @Site    : 
# @File    : multithread.py
# @Software: PyCharm

import sys
sys.path.append("./")
sys.path.append("../")

import threading
import basic_config.bc_info as config
import draft.d_mile_analyze

if config.env['prd'] == 'prd':
    threading_num = 30
else:
    threading_num = 10

class myThread:
    # todo 多线程封装
    def set_parameter(self,parameter):
        self.p = [[] for a in range(threading_num)]
        i = 0
        parameter = list(parameter)
        while len(parameter) > 0:
            i = 0
            while i < threading_num:
                if len(parameter) == 0:
                    break
                self.p[i].append(parameter.pop())
                i += 1

    def set(self,program):
        self.program = program
        if self.program == 'mile_analyze':
            request_program = ins_logic.refresh_quote()
            request_program.get_quotable_objects()
            self.set_parameter(request_program.quotable_result)

    def main(self):
        threads = []
        while len(threads) < threading_num:
            if self.program == 'mile_analyze':
                threads.append(threading.Thread(target=ins_logic.refresh_quote().refresh_objects(self.p[len(threads)])))
            else:
                break

        # print(config.position(),'%s threads ready...go' % len(threads))
        for t in threads:
            # t.setDaemon(True)
            t.start()

        for t in threads:
            t.join()

        # print(config.position(), "all over")
