#!/bin/env python3
#coding=utf-8
# @Time    : 2018/6/20 下午11:26
# @Author  : kelly
# @Site    : 
# @File    : c_basic.py
# @Software: PyCharm

import re
import urllib
import basic_config.bc_log as log
import random
import string

class re_tools():
    def re_expr(self,re_word='',string=''):
        log.log(log.set_loglevel().debug(),re_word,string)
        pattern = re.compile(r'%s' % re_word)
        match_result = re.findall(pattern,string)
        return match_result

class str_tools():
    def rand_string(self, str_length=8):
        salt = ''.join(random.sample(string.ascii_letters + string.digits, str_length))
        return salt

class http_tools():
    def httpserver(self):
        """

        :param ordercode:
        :type ordercode:
        :return:
        :rtype:
        """
        # 不再需要消息解析，纯保险公司查询
        url = "%s/py/pressure" % (1) #(config.env['domain_address'])
        header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko'}
        req = urllib.request.Request(url=url, headers=header_dict)

        try:
            f = urllib.request.urlopen(req, timeout=15)
            return str(f.read().decode('utf-8'))
        except Exception as e:
            if str(e).__contains__('500'):
                pass
            else:
                log.log(log.set_loglevel().error(), log.debug().position(), req.data, req.full_url, req.headers, e)
            return '{"code":"-1","message":"%s","info":%s}' % (e, log.debug().position())