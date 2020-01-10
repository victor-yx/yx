#!/bin/env python3
#coding=utf-8
# @Time    : 2018/7/5 下午10:58
# @Author  : kelly
# @Site    :
# @File    : webserver.py
# @Software: PyCharm

# import sys
# import socket
# import gevent
import time
from gevent import socket,spawn,sleep
from urllib.parse import unquote, quote,parse_qs
import gevent
import basic_config.bc_info as config
import basic_config.bc_log as log
import module.m_route as route
# import ssl

class start_check():
    def port_open(self,ip,port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((ip, int(port)))
            s.shutdown(2)
        except Exception:
            return False
        finally:
            s.close()

class Request:
    def __init__(self, r):
        self.content = r
        self.method = r.split()[0]
        self.path = r.split()[1]
        try:
            self.body = r.split('\r\n\r\n', 1)[1]
        except:
            self.body = ''

    def form_body(self):
        return self._parse_parameter(self.body)

    def parse_path(self):
        index = self.path.find('?')
        if index == -1:
            # return self.path, {}
            return self.path, eval(self.body) if self.body != '' else None
        else:
            path, query_string = self.path.split('?', 1)
            query = self._parse_parameter(query_string)
            return path, query

    @property
    def headers(self):
        header_content = self.content.split('\r\n\r\n', 1)[0].split('\r\n')[1:]
        result = {}
        for line in header_content:
            k, v = line.split(': ')
            # result[quote(k)] = quote(v)
            result[quote(k)] = v
        return result

    @staticmethod
    def _parse_parameter(parameters):
        args = parameters.split('&')
        query = {}
        for arg in args:
            k, v = arg.split('=')
            query[k] = unquote(v)
        return query


def server(port):
    s = socket.socket()
    # s = ssl.wrap_socket(s,certfile='./https_svr_key.pem', server_side=True)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', port))
        # max listener number
        s.listen(1500)
        while True:
            cli, addr = s.accept()
            # gevent.spawn(handle_request, cli, gevent.sleep)
            spawn(handle_request, cli, sleep)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        if e.errno==48:
            print('ERROR! waiting for tcp port closing. try it later')
        else:
            print(e)
    finally:
        s.close()

def handle_request(s, sleep):
    try:
        if (config.env['env']=='dev' and config.env['whiteip']==[]) or s.getpeername()[0] in config.env['whiteip']:
            pass
        else:
            s.send(b'HTTP/1.1 400 NG ')
            s.send(bytes('\r\n\r\n%s\n' % s.getpeername()[0]+'not in white ip list', encoding='utf8'))
            s.shutdown(socket.SHUT_WR)
            return
        data=b''
        MSGLEN=1024
        # solution 1 start
        # i=0
        # s.shutdown(socket.SHUT_RD)
        # while True:
        #     data_tmp = s.recv(MSGLEN)
        #     if data_tmp.__len__()==0 and i>0:  # for post request, the 1st line is None
        #         break
        #     else:
        #         i=1
        #         data += data_tmp
        #     sleep(0.1)   # 必须要，否则会快速满足条件退出
        # solution 1 end
        # solution 2 start
        while True:
            data_tmp = b''   # 必须要保留，不懂为啥
            with gevent.Timeout(0.5, False) as timeout:
                data_tmp = s.recv(MSGLEN)
            # check=s.recv(MSGLEN,socket.MSG_PEEK)
            data+=data_tmp
            if data_tmp.__len__()==0:
                break
        # solution 2 end
            # if len(data)<79:
            #     break
            # else:
            #     if data.decode().endswith('\r\n\r\n'):
            #         break
        # sleep(0.1)
        data = data.decode()
        log.log(log.set_loglevel().debug(),data)
        a = Request(data)
        # path,query = a.parse_path()
        response = route.route_main().main(a.parse_path())
        log.log(log.set_loglevel().debug(),log.debug().position(),len(response))
        # print('header:',a.headers)
        # print('body:',a.body)
        # print('path:',a.path)
        # print('query:',a.parse_path())
        # # method=unquote(a.path.split('?', 1)[0][1:])
        # # parameter=unquote(a.path.split('?', 1)[1])
        # # query_string=parse_qs(parameter)
        # # print(method)
        # # print(parameter)
        # # print(query_string)
        # print('method:',a.method)
        # print('content:',a.content)
        # request_string = "GET %(request)s HTTP/1.1\r\nHost: %(hostip)s\r\n\r\n%(content)s\n" % {'request':'index.html','hostip':hostip,'content':11111}

        request_string = '\r\n\r\n%s\n' % response
        log.log(log.set_loglevel().debug(),log.debug().position(),len(request_string))
        s.send(b'HTTP/1.1 200 OK')
        s.send(b"\r\nAccess-Control-Allow-Origin:*")
        s.sendall(bytes(request_string,encoding='utf8'))
        log.log(log.set_loglevel().debug(), log.debug().position(), 'send over.')
        s.shutdown(socket.SHUT_WR)
        log.log(log.set_loglevel().debug(), log.debug().position(), 'socket closed')
        # print('.','be killed')
    except KeyboardInterrupt:
        raise
    except Exception as ex:
        print(time.asctime(),log.debug().position(), 'ERROR:',data,ex)
    finally:
        s.close()

if __name__ == '__main__':
    starttime = time.time()
    try:
        portNum=config.env['port']
        hostname=socket.gethostname()
        hostip=socket.gethostbyname(socket.gethostname())
        log.log(log.set_loglevel().top(),time.asctime(), "Server Starts - %s,%s:%s" % (hostname,hostip, portNum))
        # print(time.asctime(), "Server Starts - %s,%s:%s" % (hostname,hostip, portNum))
        # while ~start_check().port_open(hostip,portNum):
        #     print('waiting for port release...')
        #     time.sleep(1)
        server(portNum)
    finally:
        endtime = time.time()
        log.log(log.set_loglevel().top(), "kept alive %d seconds" % (endtime - starttime))
        log.log(log.set_loglevel().top(), time.asctime(), "Server Stops - %s,%s:%s" % (hostname,hostip, portNum))
        # print("kept alive %d seconds" % (endtime - starttime))
        # print(time.asctime(), "Server Stops - %s,%s:%s" % (hostname,hostip, portNum))