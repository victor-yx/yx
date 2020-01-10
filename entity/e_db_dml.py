#!/bin/env python3
#coding=utf-8
# @Time    : 2018/6/22 上午10:14
# @Author  : kelly
# @Site    : 
# @File    : e_db_dml.py
# @Software: PyCharm
import time
import datetime
import basic_config.bc_log as log
import mysql
import elasticsearch
import elasticsearch.helpers
import basic_config.bc_info as config
import component.c_error as err
import component.c_db_dml
import component.c_dbconn as dbconn

# todo add query definition
class db_dml():

    def new_table_record(self, table_name=None, column_name=None, column_value=None, p_conn=None):
        if column_name is None or column_value is None or table_name is None:
            log.log(log.set_loglevel().error(), table_name, column_name, column_value, p_conn)
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])

        if p_conn is None:
            try:
                # conn = mysql.connector.connect(**config.env['db_connection'])
                db = dbconn.db()
                conn = db.create_conn()
            except mysql.connector.Error as e:
                log.log(log.set_loglevel().error(),log.debug().position(), e, )
                log.debugfile().printlog2(log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        insert_sql, column_value = component.c_db_dml.mysql().new_record_sql(table_name, column_name,column_value)

        cursor = conn.cursor()
        try:
            if config.env['env'] == 'dev':
                log.log(log.set_loglevel().debug(), log.debug().position(),insert_sql, column_value)
            cursor.execute(insert_sql, column_value)
            new_id = cursor._last_insert_id
        except Exception as e:
            log.log(log.set_loglevel().error(), log.debug().position(), e, insert_sql, column_value)
            raise err.MyError(log.debug().position(), err.MyError.errcode[1],
                              err.MyError.errmsg[1] + insert_sql + ' ' + column_value)
        finally:
            if cursor:
                cursor.close()
            if p_conn is None:
                if conn:
                    conn.commit()
                    conn.close()
                    db.close_conn()
            if 'new_id' in locals():
                return new_id


    def update_table_record(self, table_name=None, column_name=None, column_value=None, p_conn=None):
        if column_name is None or column_value is None or table_name is None:
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                # conn = mysql.connector.connect(**config.env['db_connection'])
                db = dbconn.db()
                conn = db.create_conn()
            except mysql.connector.Error as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                log.debugfile().printlog2(log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        update_sql, column_value = component.c_db_dml.mysql().update_record_sql(table_name, column_name,column_value)

        cursor = conn.cursor()
        try:
            try_count = 1
            while try_count <= 5:
                try:
                    if config.env['env']== 'dev':
                        log.log(log.set_loglevel().debug(), log.debug().position(),update_sql,column_value)
                    cursor.execute(update_sql, column_value)
                    update_count = cursor._rowcount
                    break
                except Exception as e:
                    # log.log(log.set_loglevel().error(),log.debug().position(),err.err_catch().catch(sys.exc_info()), update_sql, column_name, column_value,e)
                    log.log(log.set_loglevel().error(), log.debug().position(), update_sql, column_name, column_value, e)
                    process_db = dbconn.db()
                    process_conn = process_db.create_conn()
                    process_cursor = process_conn.cursor()
                    process_cursor.execute("show full processlist;")
                    process_lists = process_cursor.fetchall()
                    log.log(log.set_loglevel().error(), log.debug().position(), process_lists)
                    process_cursor.close()
                    process_conn.close()
                    process_db.close_conn()
                    cursor.close()
                    cursor = conn.cursor()
                    if str(e).count('Lock wait timeout exceeded') > 0:
                        try_count += 1
                        time.sleep(5)
                        if try_count > 5:
                            raise
                    else:
                        raise
        finally:
            if cursor:
                cursor.close()
            if p_conn is None:
                if conn:
                    conn.commit()
                    conn.close()
                    db.close_conn()
            if 'update_count' in locals():
                return update_count

    def update_table_record_x(self, table_name=None, whr_col_name=None, whr_col_value=None,upd_col_name=None, upd_col_value=None, p_conn=None):
        if whr_col_name is None or whr_col_value is None or table_name is None or upd_col_name is None or upd_col_value is None:
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                # conn = mysql.connector.connect(**config.env['db_connection'])
                db = dbconn.db()
                conn = db.create_conn()
            except mysql.connector.Error as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                log.debugfile().printlog2(log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        update_sql, column_value = component.c_db_dml.mysql().update_record_sql_x(table_name, whr_col_name,whr_col_value,upd_col_name,upd_col_value)

        cursor = conn.cursor()
        try:
            try_count = 1
            while try_count <= 5:
                try:
                    if config.env['env'] == 'dev':
                        log.log(log.set_loglevel().debug(), log.debug().position(), update_sql, column_value)
                    cursor.execute(update_sql, column_value)
                    update_count = cursor._rowcount
                    break
                except Exception as e:
                    # log.log(log.set_loglevel().error(),log.debug().position(),err.err_catch().catch(sys.exc_info()), update_sql, column_name, column_value,e)
                    log.log(log.set_loglevel().error(), log.debug().position(), update_sql, whr_col_name, whr_col_value,upd_col_name,upd_col_value,e)
                    process_db = dbconn.db()
                    process_conn = process_db.create_conn()
                    process_cursor = process_conn.cursor()
                    process_cursor.execute("show full processlist;")
                    process_lists = process_cursor.fetchall()
                    log.log(log.set_loglevel().error(), log.debug().position(), process_lists)
                    process_cursor.close()
                    process_conn.close()
                    process_db.close_conn()
                    cursor.close()
                    cursor = conn.cursor()
                    if str(e).count('Lock wait timeout exceeded') > 0:
                        try_count += 1
                        time.sleep(5)
                        if try_count > 5:
                            raise
                    else:
                        raise
        finally:
            if cursor:
                cursor.close()
            if p_conn is None:
                if conn:
                    conn.commit()
                    conn.close()
                    db.close_conn()
            if 'update_count' in locals():
                return update_count

    def delete_table_record(self, table_name=None, column_name=None, column_value=None, p_conn=None):
        if column_name is None or column_value is None or table_name is None:
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                # conn = mysql.connector.connect(**config.env['db_connection'])
                db = dbconn.db()
                conn = db.create_conn()
            except mysql.connector.Error as e:
                log.log(log.set_loglevel().error(),log.debug().position(), e, )
                log.debugfile().printlog2(log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        delete_sql = component.c_db_dml.mysql().delete_record_sql(table_name, column_name,
                                                                                     column_value)

        cursor = conn.cursor()
        try:
            if config.env['env'] == 'dev':
                log.log(log.set_loglevel().debug(), log.debug().position(),delete_sql)
            cursor.execute(delete_sql)
            delete_count = cursor._rowcount
        except Exception as e:
            log.log(log.set_loglevel().error(), log.debug().position(), e, delete_sql)
            raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1], delete_sql)
        finally:
            if cursor:
                cursor.close()
            if p_conn is None:
                if conn:
                    conn.commit()
                    conn.close()
                    db.close_conn()
            if 'delete_count' in locals():
                return delete_count

    def query_table_record(self,sql=None,p_conn=None):
        if sql is None:
            log.log(log.set_loglevel().error(), sql, p_conn)
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                # conn = mysql.connector.connect(**config.env['db_connection'])
                db = dbconn.db()
                conn = db.create_conn()
            except mysql.connector.Error as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                log.debugfile().printlog2(log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        cursor = conn.cursor()
        try:
            if config.env['env'] == 'dev':
                log.log(log.set_loglevel().debug(), log.debug().position(), sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            column_names=cursor.column_names
        except Exception as e:
            log.log(log.set_loglevel().error(), log.debug().position(), e, sql)
            raise err.MyError(log.debug().position(), e, sql)
        finally:
            if cursor:
                cursor.close()
            if p_conn is None:
                if conn:
                    conn.commit()
                    conn.close()
                    db.close_conn()
            if 'results' in locals():
                return column_names,results


    def add_memo(self, ordercode='', memo='', operator='', remark=None, p_conn=None):
        """
        暂时不生效
        :param ordercode:
        :type ordercode:
        :param memo:
        :type memo:
        :param operator:
        :type operator:
        :param remark:
        :type remark:
        :param p_conn:
        :type p_conn:
        :return:
        :rtype:
        """
        if ordercode == '' or ordercode is None or memo == '' or memo is None or operator == '' or operator is None:
            log.log(log.set_loglevel().error(), log.debug().position(), err.MyError.errcode[4],
                         err.MyError.errmsg[4])
            raise err.MyError(log.set_loglevel().error(), log.debug().position(), err.MyError.errcode[4],
                              err.MyError.errmsg[4])

        if p_conn is None:
            vipdb = dbconn.vipdb()
            conn = vipdb.create_conn()
        else:
            conn = p_conn

        table_name = '%s.t_insurance_memo' % config.env['vipschema']
        column_name = ('ordercode', 'memo', 'operator', 'remark', 'cdt', 'udt')
        str_curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        column_value = (ordercode, memo, operator, remark, str_curr_time, str_curr_time)
        memo_id = self.new_table_record(table_name, column_name, column_value, conn)

        if p_conn is None:
            conn.rollback()
            del conn
            vipdb.close_conn()

        return memo_id

class db_es_dml():
    def new_doc(self,index_name=None,type_name=None,doc_value=None,p_conn=None):
        """
        ES insert
        :param index_name:
        :type index_name: string
        :param type_name:
        :type type_name: string
        :param doc_value:
        :type doc_value: dict  or  list for bulk
        :param p_conn:
        :type p_conn: es object
        :return: (created True or False, id , version)
        :rtype: array
        """
        if index_name is None or type_name is None or doc_value is None:
            log.log(log.set_loglevel().error(), index_name, type_name, doc_value, p_conn)
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                esdb = dbconn.db_es()
                conn = esdb.create_conn()
            except Exception as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        result = {'created':False,'result':'created','_id':None,'_version':None}
        try:
            if len(doc_value)==0 or type(doc_value)==dict:
                result = conn.index(index=index_name, doc_type=type_name, body=doc_value)
                result['created'] = True
                # '_op_type':'delete'
                # result = elasticsearch.helpers.bulk(conn,[{'_index':index_name,'_type':type_name,'_source':doc_value}],raise_on_error=True)

            elif type(doc_value)==list:
                docs = []
                for doc in doc_value:
                    one_row = {'_index':index_name,'_type':type_name,'_source':doc}
                    docs.append(one_row)
                result = elasticsearch.helpers.bulk(conn,docs,raise_on_error=True)
                result = {'created':True,'result':'created','_id':'' if result[0]>0 else None,'_version':'' if result[0]>0 else None}
            else:
                log.log(log.set_loglevel().error(), index_name, type_name, doc_value, p_conn)
                raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
            # {'created': True, 'result': 'created', '_shards': {'successful': 1, 'failed': 0, 'total': 2}, '_version': 1, '_type': 'python_core', '_id': 'AWVqIzR35SxcWh31QmD1', '_index': 'log_test'}
        except Exception as e:
            log.log(log.set_loglevel().error(),log.debug().position(), e, index_name, type_name, doc_value, p_conn)
            raise e
        finally:
            if p_conn is None:
                if 'esdb' in locals().keys():
                    esdb.close_conn()
                    del esdb
        return result['created'], result['_id'], result['_version']

    def update_doc(self,index_name=None,type_name=None,id=None,version=None,doc_value=None,cond_value=None,p_conn=None):
        """
        ES update  id version同时有值则会校验version；id有值，condvalue没值，更新对象为id；condvalue有值，更新对象为condvalue，和id无关  a=db_es_dml().update_doc('log_test','python_core','AWVqRo0K5SxcWh31QmD5',version=None,cond_value=None,doc_value={'aaa':47})
        :param index_name:
        :type index_name: string
        :param type_name:
        :type type_name: string
        :param id:
        :type id:  string
        :param version:
        :type version:  integer
        :param doc_value:
        :type doc_value:  dict
        :param p_conn:
        :type p_conn:  object
        :return:
        :rtype:
        """
        if index_name is None or type_name is None or doc_value is None or (version is not None and id is None) or ( cond_value is not None and type(cond_value)!=dict) or (len(doc_value) > 0 and type(doc_value) != dict):
            log.log(log.set_loglevel().error(), index_name, type_name, id,version,doc_value,cond_value, p_conn)
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                esdb = dbconn.db_es()
                conn = esdb.create_conn()
            except Exception as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        body_value={}
        continue_flag=True
        if id is not None:
            body_value['query'] = {'term': {'_id': id}}
            body_value['version']=True
            if version is not None:
                results = conn.search(index_name,type_name,body_value)
                result_version = results['hits']['hits'][0]['_version']
                if result_version != version:
                    continue_flag = False
                    ret_val=0

        if cond_value is not None:
            body_value.pop('query')
        elif id is None:
            body_value['query']={'match_all':{}}
        if continue_flag:
            body_value_tmp = component.c_db_dml.es().update_record_sql(index_name,type_name,doc_value,cond_value)
            if 'query' in body_value_tmp.keys():
                if 'query' in body_value.keys():
                    body_value['query'].update(body_value_tmp['query'])
                else:
                    body_value['query'] = body_value_tmp['query']
            if 'script' in body_value.keys():
                body_value['script'].update(body_value_tmp['script'])
            else:
                body_value['script'] = body_value_tmp['script']
            if 'version' in body_value.keys():
                try:
                    body_value['version'].update(body_value_tmp['version'])
                except AttributeError as e:
                    body_value['version'] = body_value_tmp['version']
            else:
                body_value['version'] = body_value_tmp['version']

            result = conn.update_by_query(index=index_name, doc_type=type_name,body=body_value)
            ret_val = result['updated']

        if p_conn is None:
            if 'esdb' in locals().keys():
                esdb.close_conn()
                del esdb
        return ret_val

    def delete_doc(self, index_name=None, type_name=None, id=None, cond_value=None,p_conn=None):
        """
        ES delete  id有值，condvalue没值，删除对象为id；condvalue有值，删除对象为condvalue，和id无关  a=db_es_dml().update_doc('log_test','python_core','AWVqRo0K5SxcWh31QmD5',version=None,cond_value=None,doc_value={'aaa':47})
        :param index_name:
        :type index_name: string
        :param type_name:
        :type type_name: string
        :param id:
        :type id:  string
        :param p_conn:
        :type p_conn:  object
        :return:
        :rtype:
        """
        if index_name is None or type_name is None or (cond_value is not None and type(cond_value) != dict):
            log.log(log.set_loglevel().error(), index_name, type_name, id, cond_value, p_conn)
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                esdb = dbconn.db_es()
                conn = esdb.create_conn()
            except Exception as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        body_value = {}
        continue_flag = True
        if id is not None:
            body_value['query'] = {'term': {'_id': id}}
            body_value['version'] = True

        if cond_value is not None:
            body_value.pop('query')
        elif id is None:
            body_value['query'] = {'match_all': {}}
        body_value_tmp = component.c_db_dml.es().delete_record_sql(index_name, type_name, cond_value)
        if 'query' in body_value_tmp.keys():
            if 'query' in body_value.keys():
                body_value['query'].update(body_value_tmp['query'])
            else:
                body_value['query'] = body_value_tmp['query']

        result = conn.delete_by_query(index=index_name, doc_type=type_name, body=body_value)
        ret_val = result['deleted']

        if p_conn is None:
            if 'esdb' in locals().keys():
                esdb.close_conn()
                del esdb
        return ret_val

    def query_doc(self,index_name=None,type_name=None,cond_value=None,params=None,p_conn=None):
        if index_name is None or type_name is None or cond_value is None or (cond_value is not None and type(cond_value) != dict):
            log.log(log.set_loglevel().error(), index_name, type_name, cond_value, p_conn)
            raise err.MyError(log.debug().position(), err.MyError.errcode[4], err.MyError.errmsg[4])
        if p_conn is None:
            try:
                esdb = dbconn.db_es()
                conn = esdb.create_conn()
            except Exception as e:
                log.log(log.set_loglevel().error(), log.debug().position(), e, )
                raise err.MyError(log.debug().position(), err.MyError.errcode[1], err.MyError.errmsg[1])
        else:
            conn = p_conn

        cond_value['version'] = True
        ret_val=conn.search(index_name,type_name,cond_value)
        if p_conn is None:
            if 'esdb' in locals().keys():
                esdb.close_conn()
                del esdb
        return ret_val