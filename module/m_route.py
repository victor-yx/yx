# import requests
import json
import sys
import draft.d_mile_analyze
import basic_config.bc_log as log
# import logic.l_nhpp_func
import logic
import component.c_error as err

class route_main():
    def parse_path_query(self,path_query):
        method = path_query[0]
        params = path_query[1]
        return method,params

    def main(self,path_query):
        rel_type='00'  # normal
        rel_value=''
        try:
            method,params = self.parse_path_query(path_query)
            log.log(log.set_loglevel().debug(),log.debug().position(),method,params)
            if method=='/redirect':
                pass
                # return requests.get('http://127.0.0.1:10008/cross')
            elif method == '/cross':
                rel_type,rel_value='00','OK'
            elif method == '/rgegeg':
                if params is None:
                    rel_type,rel_value = -1,'paramater error'
                else:
                    rel_type,rel_value= 0,'this request method is %s, params is %s ' % (method,params)
            elif method == '/es_insert_check':
                draft.d_mile_analyze.test().aaa()
                rel_type,rel_value=0,''
            elif method == '/es_select_check':
                rel_type,rel_value=0,draft.d_mile_analyze.test().bbb()
            elif method == '/data_masking':
                if params is None:
                    rel_type, rel_value = -1, 'paramater error'
                elif params['key'] != '1234':
                    rel_type, rel_value = -1, 'authorization error'
            elif method == '/train_mile_filter_collect':
                if type(params.get('trainlist')) == type(None) or 'trainlist' not in params.keys():
                    rel_type, rel_value = '100', 'paramater error'
                else:
                    if params.get('trainlist') == 'all':
                        t_r, n_r, du_r, de_r = draft.d_mile_analyze.deal_data().all_carriage_noise_deal()
                    else:
                        t_r,n_r,du_r,de_r=draft.d_mile_analyze.deal_data().all_carriage_noise_deal(params.get('trainlist').split(','))
                    rel_type,rel_value = '00',{'all_records':t_r,'noise_records':n_r,'noise_duplicated_records':du_r,'delete_records':de_r}
            elif method == '/train_mile_detail':
                if type(params.get('train_no')) == type(None) or 'train_no' not in params.keys():
                    rel_type, rel_value = '100', 'paramater error'
                else:
                    result = draft.d_mile_analyze.deal_data().get_train_mile_detail(
                            params.get('train_no').split(','))
                    rel_type, rel_value = '00', result
            elif method == '/train_mile_statistic':
                rel_type, rel_value = route_tools().param_keys_check(params, 'train_no', 'period_from', 'period_to')
                # if type(params.get('train_no')) == type(None) or 'train_no' not in params.keys():
                #     rel_type, rel_value = '100', 'paramater error'
                # else:
                if rel_type == '':
                    if params.get('train_no') == 'all':
                        result = draft.d_mile_analyze.deal_data().get_train_mile_statistic(
                            'all')
                    else:
                        result = draft.d_mile_analyze.deal_data().get_train_mile_statistic(
                            params.get('train_no').split(','))
                    rel_type, rel_value = '00', result
            elif method == '/nhpp_coefficient':
                log.log(log.set_loglevel().debug(), log.debug().position(),
                        'this is %s request' % method)
                if 'train' not in params.keys() or type(params.get('train')) != list:
                    log.log(log.set_loglevel().error(), log.debug().position(),
                            'this is %s request, parameter error. parameter: %s' % (method,params))
                    rel_type, rel_value = '100', 'paramater error'
                else:
                    log.log(log.set_loglevel().debug(), log.debug().position(),
                            'nhpp_coefficient start')
                    result = logic.l_nhpp_func.nhpp_func().main(params)
                    log.log(log.set_loglevel().debug(), log.debug().position(),
                            'nhpp_coefficient end')
                    rel_type , rel_value = '00',result
            elif method == '/excel_func':
                if 'params' not in params.keys() or 'func_name' not in params.keys() or type(params.get('params')) != list:
                    rel_type, rel_value = '100', 'paramater error, paramater name are params and func_name, and must be set to a value, and params should be list type.'
                else:
                    result = logic.l_excel_func.excel_func().run(params.get('func_name'),params.get('params'))
                    rel_type, rel_value = '00', result
            elif method == '/life_analysis_func':
                if 'data' not in params.keys() or type(params.get('data')) != list:
                    rel_type, rel_value = '100', 'paramater error, paramater name are params and func_name, and must be set to a value, and data should be list type.'
                else:
                    result = logic.l_life_characteristic_analysis_func.life_characteristic_analysis_func().evaluate(sorted(params.get('data')),len(params.get('data')))
                    rel_type, rel_value = '00', result
            else:
                rel_type,rel_value = 'EPYC99999','Wrong request method'
            log.log(log.set_loglevel().debug(),log.debug().position(),'logic finished. return migrating......')
        except Exception as e:
            log.log(log.set_loglevel().error(),err.err_catch().catch(sys.exc_info()),e,path_query)
            rel_type,rel_value='9x99999',str(e.args[0])+' '+'<<<<<-----'.join(err.err_catch().catch(sys.exc_info()))
        finally:
            try:
                return route_tools().return_json(rel_type,rel_value)
            except Exception as e:
                return json.dumps({'errcode':'999','errmsg':e,'result':None})
            finally:
                log.log(log.set_loglevel().debug(), log.debug().position(), rel_type, rel_value)
                log.log(log.set_loglevel().debug(), log.debug().position(), 'return migrate over.')

class route_tools():
    def return_json(self,type,value):
        returns = {'errcode':'00',
                   'errmsg':'',
                   'result':''}
        if type is not None and type != '00':
            returns['errcode']=type
            returns['errmsg']=value
        else:
            returns['result']=value
        return json.dumps(returns)

    def param_keys_check(self,params,*args):
        if type(params)==dict:
            pass
        else:
            return '100', 'paramater error'

        for arg in args:
            if arg not in params.keys():
                return '100', 'paramater error'

        return '',''