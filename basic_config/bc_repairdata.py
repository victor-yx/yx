
# thickness_value  厚度值
t_v_1 = '[23]{1}[0-9]{1}\.?[0-9]?'
t_v_2 = '[23]{1}[0-9]{1}\.[0-9]'
#车厢号
carriage = 'mp[12]|MP[12]|[25]车'
#编号
serialNo = '[0-9a-zA-Z]{13,18}'

#替换字符
replace_str = {"换上编号":"新","换下编号":"旧","新编号":"新","旧编号":"旧","换上":"新","换下":"旧","调整前":"旧","调整后":"新","新件厚度":"新","旧件厚度":"旧","更换前":"旧","更换后":"新","更换":"新","新件数据":"新","旧件数据":"旧","新件编号":"新","旧件编号":"旧","配对后":"新"}

#阻塞字符
stop_strs = ["旧前","旧后", "新前",  "新后", "旧新前", "旧新后"]

#文件读入开始行与结束行
bgnLine = 329
endLine = 329

#excel列名称
columns_name = ["原始序号","报告时间","车号","车厢号","运行里程（km）","故障来源","修程","所属系统","子系统","问题描述","故障原因","处理方案","问题分类","换件名称","换上件编号","换下件编号","完成时间","前_旧编号1","前_新编号1","后_旧编号1","后_新编号1","编号识别类型","编号结果类型","磨耗识别类型","磨耗结果类型","信息提取-车厢号1","前_旧编号2","前_旧1","前_旧2","前_旧3","前_新编号2","前_新1","前_新2","前_新3","后_旧编号2","后_旧1","后_旧2","后_旧3","后_新编号2","后_新1","后_新2","后_新3","信息提取-车厢号2","前_旧编号2","前_旧1","前_旧2","前_旧3","前_新编号2","前_新1","前_新2","前_新3","后_旧编号2","后_旧1","后_旧2","后_旧3","后_新编号2","后_新1","后_新2","后_新3"]
columns_name_predict = ['day', 'thickness']
columns_name_results = ["车号","一般情况下更换次数","优化情况下更换次数","物料费节省/RMB","物料费节省百分比/%","人工费节省/RMB","人工费节省百分比/%"]
save_path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/3.outputFile_v3/test_all_191112.csv"
read_path = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/2.DataPreHandle/preData_3.csv"

#文件读取保存路径
path_ori_file_datetime = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/2.input_files/ori_file_datetime.csv"
path_result_file_1 = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/2.input_files/read_data_1.csv"
path_result_file_2 = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/2.input_files/read_data_2.csv"
path_result_file_3 = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/2.input_files/read_data_3.csv"
path_predict_normal = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/3.output_files/normal.csv"
path_predict_abnormal = 'F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/3.output_files/abnormal.csv'
path_analisis_res = "F:/XJ_Meeting/7.检修数据处理需求ForDrLi/6.人工费物料费分析/3.output_files/THB_analisis_191230.csv"














