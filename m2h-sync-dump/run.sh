#!/bin/bash

#当前脚本进程 PID
export master_run_pid=$$;

# 导入 msyql 到 同步库中

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

monitor_dir="${basepath}/../monitor";

tmp_dir="${basepath}/../.tmp";

#引入配置文件
source ${confpath}/conf.sh;


#日期
last_date=$(date -d last-day +%Y%m%d); #默认是昨天的日期
import_date=$1;
m_date=${import_date:-${last_date}};


#需要同步表的配置，公式：[数据库.表]-[是否快照(1是0否)]-[map reduce 数量(如果填写了此项，则使用 sqoop 方式导入)]
table_config=(
    #angejia
    angejia.call_relation_with_inventory-0

);


#统计当前表 mysql 条数，写入到文件
monitor_mysql_count_log="${tmp_dir}/${M2H_SYNC_MONITOR_SYNC_MYSQL_COUNT_LOG_NAME}";



#获取 mysql 记录条数
function getMysqlCountFn () {

    fn_gmc_get_count_sql="SELECT COUNT(*) FROM ${1};";

    #获取表的数据行数
    echo `${toolpath}/connet-mysql-server.sh "slave" "${fn_gmc_get_count_sql};"`;

}


function getNowTime () {
    echo $(date -d "today" "+%F %T");
}


# sync 流程执行
function runSyncTableDump() {
    echo "-------------- `getNowTime` sync-run start  -------------";

    #mysql 记录条数保存文件
    rm -rf ${monitor_mysql_count_log};

    for now_info in ${table_config[@]};
    do
        arr_now_info=(${now_info//-/ });

        #数据 库表名
        db_and_tb_name="${arr_now_info[0]}";

        #是否需要快照
        is_snapshoot="${arr_now_info[1]}";

        #启用的 map_reduce_num 数量
        map_reduce_num="${arr_now_info[2]}";

        #统计 mysql 记录条数,写入到文件
        echo "--------------- `getNowTime` run getMysqlCountFn  ${db_and_tb_name} ---------------"；
        #mysql 条数
        now_mysql_table_count=`getMysqlCountFn "${db_and_tb_name}"`;
        #写入文件
        mysql_db_and_table_format_fn="${M2H_SYNC_HIVE_DATABASE_SYNC_NAME}.${db_and_tb_name//./${M2H_SYNC_HIVE_SEPARATOR}}";
        echo "${mysql_db_and_table_format_fn} ${now_mysql_table_count}" >> ${monitor_mysql_count_log};

        #当写了 map reduce 数的时候，则使用 sqoop 方式导入
        if [ ${map_reduce_num} -gt 0 ];then
            echo "--------------- `getNowTime` sqoop 导入  ---------------"；
            ${basepath}/m2h-sync.sh ${db_and_tb_name} sqoop ${map_reduce_num};

        #否则使用 mysql dump 方式导入
        else
            echo "--------------- `getNowTime` mysql_dump 导入  ---------------"；
            ${basepath}/m2h-sync.sh ${db_and_tb_name} mysql_dump;
        fi

        # 聚合
        if [ ${arr_now_info[1]} -eq 1 ];then
            #写了 map reduce 方式,使用查询 hive table 方式导入
            if [ ${map_reduce_num} -gt 0 ];then
                echo "--------------- `getNowTime` run (query_load) gather-table.sh ---------------"；
                ${basepath}/gather-table.sh "${db_and_tb_name}" "${m_date}" query_load;

            #没写 map reduce 方式,使用查询 hive table 写入到本地文档，再上传到 hive table 中
            else
                echo "--------------- `getNowTime` run (file_load) gather-table.sh ---------------"；
                ${basepath}/gather-table.sh "${db_and_tb_name}" "${m_date}" file_load;
            fi
        fi
    done;

    echo "-------------- `getNowTime` sync-run end  -------------";
}

runSyncTableDump;
