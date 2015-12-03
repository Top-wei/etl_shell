#!/bin/bash 

# 导入数据到 hive 
# 参数 [conf] 
#   解释 (数据库服务器,有dw、slave)-(mysql的数据库、表名称)-(hive的数据库、表名称)-(map reduce数量)-[分割字符 (只有使用 sqoop 导入生效)]
# 案例 ./m2h.sh dw-dw_db.dw_scheduler_task-dw_db_temp.dw_scheduler_task-1-\001

export master_run_pid=$$;

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;


format_sqoop_table="dw_db.dw_basis_dimen_action_page_lkp,dw_db.dw_basis_dimension_delivery_channels_package,dw_db.dw_basis_dimension_pagename_lkp";


function run () {

    #输入的参数
    import_table_config=${1};

    if [ -z $import_table_config ];then

        #库类型-mysql数据库表-hive数据库表-mp数量
        table_config=(
            #dw-dw_monitor.mini_report-db_sync.dw_monitor__mini_report-0
            #dw-dw_monitor.dw_monitor_user-db_sync.dw_monitor__dw_monitor_user-0
            #dw-dw_monitor.mini_history-db_sync.dw_monitor__mini_history-0
        );

    else

        table_config=(${import_table_config});

    fi


    for now_info in ${table_config[@]};
    do
        arr_now_info=(${now_info//-/ });
    
        db_type="${arr_now_info[0]}";

        #数据 库表名
        mysql_db_and_tb_name="${arr_now_info[1]}";

        #是否需要快照
        hive_db_and_tb_name="${arr_now_info[2]}";

        #启用的 map_reduce_num 数量
        map_reduce_num="${arr_now_info[3]}";

        #字段分隔符(仅使用 sqoop 有效)
        fields_terminated_by="${arr_now_info[4]}";

        ${toolpath}/connect-base-hive-server-hql.sh "DROP TABLE IF EXISTS ${hive_db_and_tb_name};";

        #sqoop 导入
        if [ `$toolpath/in-array.sh ${hive_db_and_tb_name} ${format_sqoop_table}` -eq 1 ];then
            echo "--------- 强制 sqoop 导入 ---------"
            ${toolpath}/mysql-to-hive.sh db_type=${db_type} mysql=${mysql_db_and_tb_name} hive=${hive_db_and_tb_name}  num_mappers="${map_reduce_num}";

        #sqoop 方式导入
        elif [ ${map_reduce_num} -gt 0 ];then
            if [ -n "${fields_terminated_by}" ];then
                ${toolpath}/mysql-to-hive.sh db_type=${db_type} mysql=${mysql_db_and_tb_name} hive=${hive_db_and_tb_name}  num_mappers="${map_reduce_num}" fields_terminated_by="${fields_terminated_by}";
            else
                ${toolpath}/mysql-to-hive.sh db_type=${db_type} mysql=${mysql_db_and_tb_name} hive=${hive_db_and_tb_name}  num_mappers="${map_reduce_num}";
            fi

        #mysql dump 方式导入
        else
            ${toolpath}/mysql-to-hive-dump.sh db_type=${db_type} mysql=${mysql_db_and_tb_name} hive=${hive_db_and_tb_name};
        fi

    done;
}

run ${1};