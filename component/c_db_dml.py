#!/bin/env python3
__author__ = 'kelly'
# import basic_config.bc_log as log
# log.log(log.set_loglevel().info(),'imported')

class mysql():
    def new_record_sql(self, table_name, column_name, column_value) -> str:
        """

        :param table_name:
        :param column_name:
        :param column_value:
        :return:
        """
        if table_name is None or column_name is None or column_value is None:
            return None
        column_name = list(column_name)
        column_value = list(column_value)
        try:
            column_value.pop(column_name.index('id'))
            column_name.pop(column_name.index('id'))
        except:
            pass
        insert_sql = "insert into %s (`" % table_name
        insert_sql += '`,`'.join(column_name) + '`) values ('
        # insert_sql += ','.join(["'%s'" for a in range(len(column_name))]) + ')'
        insert_sql += ','.join(["%s" for a in range(len(column_name))]) + ')'

        column_value = tuple(column_value)
        # insert_sql = insert_sql % column_value
        # insert_sql = insert_sql.replace("\'None\'", 'null')
        # insert_sql = insert_sql.replace('None', 'null')
        return insert_sql,column_value

    def update_record_sql(self, table_name, column_name, column_value):
        """

        :param table_name:
        :param column_name:
        :param column_value:
        :return:
        """
        if table_name is None or column_name is None or column_value is None:
            return None
        column_name = list(column_name)
        column_value = list(column_value)
        if column_name.count('id'):
            # id exists
            update_sql = "update %s set " % table_name
            update_where_sql = " where id = '%s'" % column_value.pop(column_name.index('id'))
        else:
            # no id, use the first column
            update_sql = "update %s set " % table_name
            update_where_sql = " where `%s` = '%s'" % (column_name[0],column_value[0])
        try:
            column_name.pop(column_name.index('id'))
        except:
            pass

        update_set_sql = ""
        if len(column_name) > 0:
            update_set_sql = '`=%s,`'.join(column_name)
            update_set_sql = '`' + update_set_sql + '`=%s'
        column_value = tuple(column_value)
        # while len(column_name) > 0:
        #     update_set_sql += "`" + column_name.pop() + "`= '%s' " % column_value.pop()
        #     if len(column_name) == 0:
        #         pass
        #     else:
        #         update_set_sql += ","
        update_sql = update_sql + update_set_sql + update_where_sql
        # update_sql = update_sql.replace("\'None\'", 'null')
        # update_sql = update_sql.replace('None', 'null')
        return update_sql, column_value

    def update_record_sql_x(self, table_name, where_col_name, where_col_value,upd_col_name,upd_col_value):
        """

        :param table_name:
        :param column_name:
        :param column_value:
        :return:
        """
        if table_name is None or where_col_name is None or where_col_value is None or upd_col_name is None or upd_col_value is None:
            return None
        if len(where_col_name) != len(where_col_value):
            raise
        if len(upd_col_name) != len(upd_col_value):
            raise
        where_col_name = list(where_col_name)
        where_col_value = list(where_col_value)
        upd_col_name = list(upd_col_name)
        upd_col_value = list(upd_col_value)
        update_sql = "update %s set " % table_name
        update_where_sql = " where `%s` = '%s'" % (where_col_name[0], where_col_value[0])
        i = 0
        while i<len(where_col_name):
            update_where_sql += " and `%s` = '%s'" % (where_col_name[i], where_col_value[i])
            i += 1

        update_set_sql = ""
        if len(upd_col_name) > 0:
            update_set_sql = '`=%s,`'.join(upd_col_name)
            update_set_sql = '`' + update_set_sql + '`=%s'
        column_value = tuple(upd_col_value)
        # while len(column_name) > 0:
        #     update_set_sql += "`" + column_name.pop() + "`= '%s' " % column_value.pop()
        #     if len(column_name) == 0:
        #         pass
        #     else:
        #         update_set_sql += ","
        update_sql = update_sql + update_set_sql + update_where_sql
        # update_sql = update_sql.replace("\'None\'", 'null')
        # update_sql = update_sql.replace('None', 'null')
        return update_sql, column_value

    def delete_record_sql(self, table_name, column_name, column_value) -> str:
        """

        :param table_name:
        :param column_name:
        :param column_value:
        :return:
        """
        if table_name is None or column_name is None or column_value is None:
            return None
        column_name = list(column_name)
        column_value = list(column_value)
        if column_name.count('id'):
            # id exists
            delete_sql = "delete from %s " % table_name
            delete_where_sql = " where id = '%s' " % column_value.pop(column_name.index('id'))
        else:
            # no id, use the first column
            delete_sql = "delete from %s " % table_name
            delete_where_sql = " where `%s` = '%s' " % (column_name.pop(0), column_value.pop(0))
        try:
            column_name.pop(column_name.index('id'))
        except:
            pass

        while len(column_name) > 0:
            delete_where_sql += " and `" + column_name.pop() + "` = '%s' " % column_value.pop()

        delete_sql = delete_sql + delete_where_sql

        delete_sql = delete_sql.replace("= \'None\'", 'is null')
        delete_sql = delete_sql.replace('= None', 'is null')
        return delete_sql

class es():
    def update_record_sql(self,index_name,type_name,doc_value,cond_value) -> dict:
        """
        es update doc_value migration
        :param index:
        :type index: basestring
        :param type:
        :type type: basestring
        :param doc_value:
        :type doc_value: dict
        :param cond_value:
        :type cond_value: dict
        :return:
        :rtype:dict
        """
        if index_name is None or type_name is None:
            return None
        if doc_value is not None and type(doc_value) != dict:
            return None
        if cond_value is not None and type(cond_value) != dict:
            return None
        value = {}
        if cond_value is not None:
            value['query']=cond_value
        # POST
        # hockey / player / 1 / _update
        # {
        #     "script": {
        #         "lang": "painless",
        #         "source": "ctx._source.last = params.last; ctx._source.nick = params.nick",
        #         "params": {
        #             "last": "gaudreau",
        #             "nick": "hockey"
        #         }
        #     }
        # }
        value['script']={'lang':'painless','source':''}
        value['version']=True
        value['script']['source']=tools().iter_dict('',doc_value.copy())
        value['script']['params']=doc_value
        return value

    def delete_record_sql(self,index_name,type_name,cond_value) -> dict:
        """
        es delete doc_value migration
        :param index:
        :type index: basestring
        :param type:
        :type type: basestring
        :param doc_value:
        :type doc_value: dict
        :param cond_value:
        :type cond_value: dict
        :return:
        :rtype:dict
        """
        if index_name is None or type_name is None:
            return None
        if cond_value is not None and type(cond_value) != dict:
            return None
        value = {}
        if cond_value is not None:
            value['query']=cond_value
        else:
            value['query']={'match_all':{}}
        return value

class tools():
    def iter_dict(self,parent_key,dict_object,str_t=''):
        while len(dict_object)>0:
            (k,v) = dict_object.popitem()
            str_t += "ctx._source.%s=params.%s;" % ((parent_key + '.' if parent_key != '' else parent_key) + k,
                                                    (parent_key + '.' if parent_key != '' else parent_key) + k)
            # if type(v)==dict:
            #     str_t += self.iter_dict((parent_key+'.' if parent_key!='' else parent_key)+k,v,)
            # else:
            #     str_t += "ctx._source.%s=params.%s;"% ((parent_key+'.' if parent_key!='' else parent_key)+k,(parent_key+'.' if parent_key!='' else parent_key)+k)
        return str_t