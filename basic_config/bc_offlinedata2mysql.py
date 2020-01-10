#coding=utf-8
# @Time    : 2020/1/1 13:45
# @Author  : Victor
# @Site    :
# @File    : bc_offlineData2mysql.py
# @Software: PyCharm
import datetime
class Config:
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mysql_config = {'addr':"localhost",
                    'port':3306,
                    'user':'root',
                    'pswd':"123456",
                    "usedb":"darams"}

    list_basic_attr = {"create_by":"YX_SIMU",
                       "create_date": nowTime,
                       "update_by": "YX_SIMU",
                       "update_date":nowTime ,
                       "remarks": "YX_GENE",
                       "del_flag": "0"}

    read_path_root = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files"
    read_path_train_info = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files/train_info.csv"
    read_path_train_mileage = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files/train_mileage.csv"
    read_path_train_repair = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files/train_repair.csv"
    read_path_train_object = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files/train_object.csv"
    read_path_train_pattern = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files/train_pattern.csv"
    read_path_fault_order = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/2.input_files/fault_order.csv"

    write_path_root = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/3.output_files"
    write_path_train_info_delete = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/3.output_files/train_info_delete.sql"
    write_path_train_info_insert = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/3.output_files/train_info_insert.sql"
    write_path_fault_order_delete = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/3.output_files/fault_order_delete.sql"
    write_path_fault_order_insert = "G:/XJ_Meeting/11.darams业务离线数据导入MySQL/3.output_files/fault_order_insert.sql"

    sql_cd_train_type = "INSERT INTO `darams`.`cd_train_type`(`id`, `train_type_code`, `train_type_desc`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}');"
    sql_cd_train_no = "INSERT INTO `darams`.`cd_train_no`(`id`, `train_type_id`, `train_no`, `latest_real_time_id`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}');"
    sql_cd_train_real_time = "INSERT INTO `darams`.`cd_train_real_time`(`id`, `train_no_id`, `data_update_time`, `status`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}');"
    sql_cd_mileage = "INSERT INTO `darams`.`cd_mileage`(`id`, `train_real_time_id`, `initial_mileage`, `current_mileage`, `mileage_time`, `transferred_mileage`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`) VALUES ('{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}');"
    sql_cd_affiliated = "INSERT INTO `darams`.`cd_affiliated`(`id`, `train_real_time_id`, `company`, `affiliation`, `assignment_time`, `assignment_status`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `company_code`, `affiliation_code`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}',{}, {});"
    sql_cd_application = "INSERT INTO `darams`.`cd_application`(`id`, `train_real_time_id`, `application_location`, `application_time`, `move_type`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `application_location_code`) VALUES ('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}','{}',{});"
    sql_cd_repair_process = "INSERT INTO `darams`.`cd_repair_process`(`id`, `train_real_time_id`, `project`, `repair_location`, `repair_process`, `start_repair_miles`, `actual_entry_time`, `actual_delivery_time`, `plan_entry_time`, `plan_delivery_time`, `access_name`, `access_status`, `create_by`, `create_date`, `update_by`, `update_date`, `del_flag`, `remarks`) VALUES ('{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}','{}', '{}', '{}', '{}','{}');"

    sql_cd_fault_object = "INSERT INTO `darams`.`cd_fault_object`(`id`, `fault_object_code`, `fault_object_desc`, `show_order`, `effective_time_from`, `effective_time_to`, `is_effective`, `other_fault_object_desc`, `unit_type`, `is_universal_parts`, `is_duplicate_system`, `is_outsourcing`, `is_calculate_reliability_index`, `train_counts`, `is_lru`, `is_sru`, `version`, `parent_id`, `parent_ids`, `sort`, `create_by`, `create_date`, `update_by`, `update_date`, `del_flag`, `remarks`, `train_no_id`, `relationship_id`, `fault_object_id`, `cd_conf_id`) VALUES ('{}', '{}', '{}', {}, {}, {}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {});"
    sql_cd_fault_object_tree_1 = "INSERT INTO `darams`.`cd_fault_object_tree`(`fault_node`, `parent_fault_node`, `parent_fault_nodes`, `id`) VALUES ('{}', {}, {}, '{}');"
    sql_cd_fault_object_tree_2 = "INSERT INTO `darams`.`cd_fault_object_tree`(`fault_node`, `parent_fault_node`, `parent_fault_nodes`, `id`) VALUES ('{}', '{}', '{}', '{}');"
    sql_cd_fault_pattern = "INSERT INTO `darams`.`cd_fault_pattern`(`id`, `fault_object_id`, `fault_pattern_code`, `fault_pattern_desc`, `other_fault_pattern_desc`, `fault_pattern_class`, `unit_type`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `fault_pattern_id`, `fault_pattern_name`) VALUES ('{}', {}, '{}', '{}', {}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}');"

    sql_op_fault_order_header = "INSERT INTO `darams`.`op_fault_order_header`(`id`, `org_id`, `if_fault_id`, `fault_no`, `train_no`,`occurrence_time`,`status`, `fault_order_type`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`) VALUES ('{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');"
    sql_op_fault_order_detail = "INSERT INTO `darams`.`op_fault_order_detail`(`id`, `fault_id`, `occurrence_time`, `fault_level`, `report_time`, `report_person`, `report_part`, `fault_property`, `contact_tel`, `fault_brief`, `fault_desc`, `in_repair`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `occurrence_place`, `accident_level`, `report_position`, `cause_class`, `initail_cause_analysis`, `initial_treatment_measures`, `final_cause_analysis`, `final_treatment_measures`) VALUES ('{}', '{}', '{}', '{}', '{}', {},{}, '{}', {}, '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}',{}, {}, {}, {});"
    sql_op_train = "INSERT INTO `darams`.`op_train`(`id`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `fault_detail_id`, `train_type_desc`, `railway`, `car_no`, `running_way`, `car_trips`, `accumulated_mileage`, `origin_accu_mileage`, `start_late`, `end_late`, `train_no`, `service_fault_class`, `fault_time_flag`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}', {});"
    sql_op_fault_intuitive = "INSERT INTO `darams`.`op_fault_intuitive`(`id`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `fault_detail_id`, `intuitive_fault_object`, `intuitive_fault_object_handled`, `intuitive_fault_object_id`, `intuitive_fault_object_desc`, `intuitive_fault_pattern`, `intuitive_fault_pattern_id`, `intuitive_fault_pattern_desc`, `version`, `intuitive_fault_pattern_chain`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', {}, {}, '{}', '{}');"
    sql_op_fault_real = "INSERT INTO `darams`.`op_fault_real`(`id`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `fault_detail_id`, `real_fault_object`, `real_fault_object_id`, `real_other_fault_object_desc`, `real_fault_desc`, `real_fault_pattern_id`, `real_other_fault_pattern_desc`, `special_fault`, `safety_supervisio_fault`, `startup_fault_analysis`, `version`, `real_fault_pattern`, `real_fault_pattern_chain`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', {}, '{}', '{}', {}, '{}', '{}', '{}');"
    sql_op_fault_handle = "INSERT INTO `darams`.`op_fault_handle`(`id`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `real_fault_id`, `service_station`, `fault_order_type`, `processing_date`, `processing_result`, `diagnostic_time`, `replace_parts`, `repair_location`, `debugging_time`, `total_repair_counts`, `repair_time`, `total_downtime`, `repair_properety`, `cause_class`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, '{}');"
    sql_op_fault_associated_subject = "INSERT INTO `darams`.`op_fault_associated_subject`(`id`, `create_by`, `create_date`, `update_by`, `update_date`, `remarks`, `del_flag`, `fault_detail_id`, `responsibility_class`, `main_responsibility`, `other_responsibility`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {});"

    del_cd_train_type = "DELETE FROM `darams`.`cd_train_type` WHERE `id` = '{}';"
    del_cd_train_no = "DELETE FROM `darams`.`cd_train_no` WHERE `id`='{}';"
    del_cd_train_real_time = "DELETE FROM `darams`.`cd_train_real_time` WHERE `id`='{}';"
    del_cd_mileage = "DELETE FROM `darams`.`cd_mileage` WHERE `id`='{}';"
    del_cd_affiliated = "DELETE FROM `darams`.`cd_affiliated` WHERE `id`='{}';"
    del_cd_application = "DELETE FROM `darams`.`cd_application` WHERE `id`='{}';"
    del_cd_repair_process = "DELETE FROM `darams`.`cd_repair_process` WHERE `id`='{}';"

    del_cd_fault_object = "DELETE FROM `darams`.`cd_fault_object` WHERE `id`='{}';"
    del_cd_fault_object_tree = "DELETE FROM `darams`.`cd_fault_object_tree` WHERE `id`='{}';"
    del_cd_fault_pattern = "DELETE FROM `darams`.`cd_fault_pattern` WHERE `id`='{}';"

    del_op_fault_order_header = "DELETE FROM `darams`.`op_fault_order_header` WHERE `id`='{}';"
    del_op_fault_order_detail = "DELETE FROM `darams`.`op_fault_order_detail` WHERE `id`='{}';"
    del_op_train = "DELETE FROM `darams`.`op_train` WHERE `id`='{}';"
    del_op_fault_intuitive = "DELETE FROM `darams`.`op_fault_intuitive` WHERE `id`='{}';"
    del_op_fault_real = "DELETE FROM `darams`.`op_fault_real` WHERE `id`='{}';"
    del_op_fault_handle = "DELETE FROM `darams`.`op_fault_handle` WHERE `id`='{}';"
    del_op_fault_associated_subject = "DELETE FROM `darams`.`op_fault_associated_subject` WHERE `id`='{}';"

    exec_all_train_name = "SELECT CONCAT( p.train_type_code,'###',p.train_type_desc,'###',n.train_no),n.id AS mz FROM	cd_train_type p INNER JOIN cd_train_no n ON n.train_type_id = p.id;"
    update_cd_train_no = "UPDATE `darams`.`cd_train_no` SET `latest_real_time_id` = '{}' WHERE `id` = '{}';"
    exec_judge_fault_no = "select count(*) from `darams`.`op_fault_order_header` where fault_no ='{}';"

    exec_judge_aff_app = "SELECT COUNT(*),r.id FROM cd_train_type y INNER JOIN cd_train_no n ON n.train_type_id = y.id INNER JOIN cd_train_real_time r ON r.train_no_id = n.id INNER JOIN cd_affiliated f ON f.train_real_time_id = r.id INNER JOIN cd_application p ON p.train_real_time_id = r.id WHERE y.train_type_code = '{}'  AND y.train_type_desc = '{}' AND n.train_no = '{}';"
    exec_del_time_aff_app = "DELETE FROM `darams`.`cd_train_real_time` WHERE id = {};DELETE FROM `darams`.`cd_affiliated` WHERE train_real_time_id = {};DELETE FROM `darams`.`cd_application` WHERE train_real_time_id = {};"
    exec_query_realTime_uuid = "SELECT r.id FROM cd_train_type p INNER JOIN cd_train_no n ON n.train_type_id = p.id INNER JOIN cd_train_real_time r ON r.train_no_id = n.id WHERE p.train_type_code = '{}' AND p.train_type_desc = '{}' AND n.train_no = '{}' ORDER BY r.create_date LIMIT 1;"






































