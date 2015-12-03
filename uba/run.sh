#!/bin/bash

#当前脚本进程 PID
export master_run_pid=$$;

basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

import_date=$1;
date=$(date -d last-day +%Y%m%d);
m_date=${import_date:-${date}};


function getNowTime () {
    echo $(date -d "today" "+%F %T");
}

function run () {

    #创建 hive 表结构
    echo "--------------- `getNowTime` run creat-daily-table.sh ---------------"；
    ${basepath}/creat-daily-table.sh ${m_date};

    #导入日志到 hive 表
    echo "--------------- `getNowTime` run log-to-hdfs.sh ---------------"；
    ${basepath}/log-to-hdfs.sh ${m_date} ;

    #分析源 hive 表，生成 hive result 表
    echo "--------------- `getNowTime` run analysis-hive-to-result-hive.sh ---------------"；
    ${basepath}/analysis-hive-to-result-hive.sh ${m_date};

    #分析源 hive 表，生成 hive detail 表
    echo "--------------- `getNowTime` run analysis-action-detail-log-table.sh ---------------"；
    ${basepath}/analysis-action-detail-log-table.sh ${m_date};

}

run;
