#!/bin/bash
#监控MySQL to hive 日志
#调用方式 mysql-to-hive-sync.sh date[日期,可选 20150320]

#当前shell文件执行路径
basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

tmp_dir="${basepath}/../.tmp";

#引入配置文件
source ${confpath}/conf.sh;


hadoop_bin=${SYSTEM_HADOOP_BIN};
hive_bin=${SYSTEM_HIVE_BIN};

hive_path="/${SYSTEM_HADOOP_USER}/hive";
db_sync=${M2H_SYNC_HIVE_DATABASE_SYNC_DIR};


# m2h sync 监控 mysql 记录条数文件
mysql_count_log="${tmp_dir}/${M2H_SYNC_MONITOR_SYNC_MYSQL_COUNT_LOG_NAME}";


#监控的日期
last_date=$(date -d last-day +%Y%m%d); #默认是昨天的日期
import_date=$1;
m_date=${import_date:-${last_date}};


#日志文件
log_name="sync_monitor_log";
log_file="${SYSTEM_LOG_DIR}/${log_name}.${m_date}";

sendMail() {
    ${toolpath}/send-mail.sh "m2h-sync" "${1}" "${2}";
}


#4.监控 mysql2db_sync sychronized result
mysql2dbSyncFn() {
    
    #开始的当前时间戳
    start_timestamp=$(date -d today +"%s");
    #开始的格式化后的日期
    start_time_date=$(date -d "@${start_timestamp}" +"%F %T");

    #记录开始执行时间
    log_run_log+="<div style=color:black>开始：${start_time_date};</div>";

    echo "--------------- 监控 mysql2db_sync 日志 ---------------";
    #读取mysql_table_rows.txt，获得表名，查hdfs上对应文件的修该时间，文件行数是否匹配 
    if [ -f  ${mysql_count_log} ]; then
        echo "${M2H_SYNC_MONITOR_SYNC_MYSQL_COUNT_LOG_NAME} file exists";
        while read line;
        do
            arr=($line)
            #获得表名
            table_name=${arr[0]}
            
            #查hdfs上对应文件的修该时间
            #db_sync_path_modify_time=`${hadoop_bin}/hadoop fs -ls ${hive_path}|grep  'db_sync'|awk '{print $6}'` #2015-04-30
            #sc_time=${db_sync_path_modify_time//-/} #20150430

            #if  [ ${sc_time} -eq ${modify_date} ]; then

                #文件行数是否匹配

                mysql_rows=${arr[1]}

                echo ${table_name};
                echo ${mysql_rows};

                #db_sync_table_rows=`${hive_bin}/hive -e " select count(*) from ${table_name}"`;
                db_sync_table_rows=`${toolpath}/get-table-count.sh "SELECT COUNT(*) FROM ${table_name};"`;

                db_sync_table_rows=${db_sync_table_rows:-0}
                mysql_table_name=`echo ${table_name}|cut -d "." -f2`;

                #mysql > 0 && hive = 0 
                if [ ${mysql_rows} -gt 0 ] && echo "" && [ ${db_sync_table_rows} -eq 0 ]; then
                    log_run_log+="<div style=color:red>db_sync_action : error ->mysql ${mysql_table_name}:${mysql_rows} , hive ${table_name}:${db_sync_table_rows}</div>";
                #hive > mysql 
                elif [ ${db_sync_table_rows} -gt ${mysql_rows} ]; then
                    log_run_log+="<div style=color:orange>db_sync_action : warning ->mysql ${mysql_table_name}:${mysql_rows} , hive ${table_name}:${db_sync_table_rows}</div>";
                #hive = mysql
                elif [ ${db_sync_table_rows} -eq ${mysql_rows} ]; then
                    log_run_log+="<div style=color:green>db_sync_action : success ->mysql ${mysql_table_name}:${mysql_rows} , hive ${table_name}:${db_sync_table_rows}</div>";
                #mysql < hive
                else 
                    log_run_log+="<div style=color:red>db_sync_action : error ->mysql ${mysql_table_name}:${mysql_rows} , hive ${table_name}:${db_sync_table_rows}</div>";
                fi

            #else
                #log_run_log+="<div style=color:red>db_sync_action : error ->db_sync time dosen't match !</div>"
            #fi

        done < ${mysql_count_log} 
    else
        log_run_log+="<div style=color:red>db_sync_action : error -> ${M2H_SYNC_MONITOR_SYNC_MYSQL_COUNT_LOG_NAME} dosen't exist </div>"
    fi


    #结束的时间戳
    over_timestamp=$(date -d today +"%s");
    #结束的格式化后的日期
    over_time_date=$(date -d "@${over_timestamp}" +"%F %T");

    #结束标记
    log_run_log+="<div style=color:black>结束：${over_time_date};</div>";

    #耗时
    elapsed_time=$(($over_timestamp-$start_timestamp));
    log_run_log+="<div style=color:black>耗时(秒)：${elapsed_time};</div>";

}

#5.监控 db_sync2db_snapshoot  sychronized result
db_sync2db_snapshoot() {
    echo "--------------- 监控 db_sync2db_snapshoot 日志 ---------------";
    #new tables exists or not?
    db_snapshoot_path_modify_time=`${hadoop_bin}/hadoop fs -ls ${hive_path}|grep  'db_snapshoot'|awk '{print $6}'` #2015-04-30
    ss_time=${db_sync_path_modify_time//-/} #20150430
    if [ ${ss_time} -eq ${modify_date} ]; then
        # Does table which in msyql_table_rows.txt  exsit in db_snapshoot? 
            snap_table_name=`echo ${table_name}|cut -d "." -f2`
        echo ${snap_table_name}
        db_snapshoot_table=${snap_table_name}"_"${import_date}
        table_file=`${hadoop_bin}/hadoop fs -ls ${db_snapshoot}"/"${db_snapshoot_table}`
        tmp_db_snapshoot="db_snapshoot."
        if  [ -n "$table_file" ]; then
            #Is corresponde db_snapshoot table rows correct?
            db_snapshoot_table_rows=`${hive_bin}/hive -e " select count(*) from ${tmp_db_snapshoot}${db_snapshoot_table}"`
                   if [ ${db_snapshoot_table_rows} -ne ${mysql_rows} ];then
                log_run_log+="<div style=color:red>db_sync_action : error ->db_snapshoot  ${table_name}"__"${m_date} rows dosen't match with mysql table !</div>"
            else 
                log_run_log+="<div style=color:green>db_sync_action : ok ->db_snapshoot  ${table_name}"__"${m_date} mysql to hive complete !</div>"
            fi
        fi
    else
        log_run_log+="<div style=color:red>db_sync_action : error ->db_snapshoot  time dosen't match! </div>"
    fi
}


#run
#mysql2dbSyncFn >> ${log_file} 2>&1;
mysql2dbSyncFn;

#发送邮件
sendMail "${log_run_log}" "${m_date}";
