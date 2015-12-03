#!/bin/bash
#写日志脚本

#调用方式 : ./log.sh "日志内容" "日志名" "日志日期"，注意：日志内容一定用双引号括起来
#1.把 shell 执行的所有日志写如到 shell 中 ：如 :${basepath}/log.sh "`${basepath}/creat-daily-table.sh ${m_date} 2>&1`"  "uba_log" "${m_date}"; (注意 shell 要 2>&1)
#2.直接日志 ./log.sh "日志内容"

basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

#配置文件
source ${confpath}/conf.sh;

#日志内容
log_content="${1}";

#日志文件名称,默认是 dw_log
log_name=${2-:dw_log};

#日志日期,默认当天
import_date=${3};
date=$(date -d today +%Y%m%d);
m_date=${import_date:-${date}};

#写日志
echo -e "${log_content}" >> ${SYSTEM_LOG_DIR}/${log_name}.${m_date};



