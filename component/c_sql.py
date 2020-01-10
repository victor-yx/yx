#!/bin/env python3
#coding=utf-8
# @Time    : 2018/6/20 下午10:36
# @Author  : kelly
# @Site    : 
# @File    : c_sql.py
# @Software: PyCharm

class darams_data_clear():
    # 对象是有效，或者由算法引起的删除
    get_a_train_mile_info = 'select ot.train_no ,od.occurrence_time mile_time,ot.origin_accu_mileage mile,0 type,"darams.op_train" source,ot.id keyid from darams.op_fault_order_detail od, darams.op_train ot where od.id =ot.fault_detail_id /*and od.occurrence_time is not null and ot.accumulated_mileage is not null*/ and ot.train_no ="%s" and (ot.del_flag is null or ot.del_flag = 0 or ot.del_flag = 2) and od.occurrence_time is not null union ALL select cdn.train_no,cm.mileage_time mile_time,cm.current_mileage mile, 1 type,"darams.cd_mileage" source, cm.id keyid from darams.cd_train_real_time ctt, darams.cd_mileage cm, darams.cd_train_no cdn where cm.train_real_time_id = ctt.id /*and ctt.data_update_time is not null and cm.current_mileage is not null*/ and cdn.id = ctt.train_no_id and cdn.train_no ="%s" and (cm.del_flag is null or cm.del_flag = 0 or cm.del_flag = 2) and cm.mileage_time is not null/*union all select null as train_no,null as mile_time, null as mile ,null as type , null as source, null as keyid*/ order by mile_time,mile'
    # get_train_mile_info_debug = 'select carriage,mile_time,mile,source,0,keyid from interface.if_train_mile_history where carriage="%s" and deal_type is null'
    # get_train_mile_info_debug = 'select carriage,mile_time,mile,source,0,keyid from interface.if_train_mile_history_yx where carriage="%s" and deal_type is null ORDER BY mile_time,mile;'   ##yx_1024
    get_train_mile_info_debug = 'SELECT carriage,mile_time,mile,source,0,keyid, (@i:=@i+1) AS seq FROM interface.if_train_mile_history_yx_old,(select @i:=-1)j WHERE carriage = "%s" AND deal_type IS NULL ORDER BY mile_time,mile;'   ##191028_yx
    get_train_type_by_no='select train_type_code,train_type_desc from cd_train_type tp,cd_train_no ct where ct.train_type_id = tp.id and ct.train_no = "%s"'
    reserve_mile_fault_order = 'update darams.op_train set accumulated_mileage = origin_accu_mileage , del_flag = 0 where train_no = "%s"'
    reserve_mile_train_his = 'update darams.cd_train_real_time ctt, darams.cd_mileage cm, darams.cd_train_no cdn set cm.del_flag = 0 where cm.train_real_time_id = ctt.id and cdn.id = ctt.train_no_id and cdn.train_no ="%s" and cm.del_flag = 2 '