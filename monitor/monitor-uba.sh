#!/bin/bash
#监控 uba 日志脚本

#调用方式 monitor-uba.sh date[日期,可选 20150320]

#当前shell文件执行路径
basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

hadoop_bin=${SYSTEM_HADOOP_BIN};

uba_basic_dir=${UBA_HIVE_BASIC_DIR};
access_log_basic_dir=${UBA_ACCESS_LOG_BASIC_DIR};

sendMail() {
   ${toolpath}/send-mail.sh "uba" "${1}" "${2}";
}

#监控的日期
last_date=$(date -d last-day +%Y%m%d);
import_date=$1;
m_date=${import_date:-${last_date}};


#1.监控 uba_web_visit 日志
ubaWebVisitFn () {
    echo "--------------- 监控 uba_web_visit 日志 ---------------";

    #文件规则
    uba_web_visit_rule="uba_web_visit_log_${m_date}";

    #本地文件目录和文件
    uba_web_visit_local_dir="/var/log/uba";   #uba_web_visit_log 本地日志文件目录
    uba_web_visit_local_file="${uba_web_visit_local_dir}/uba_web_visit_${m_date}.log";#uba_web_visit_log 本地日志文件路径

    #hdfs文件目录和文件
    uba_web_visit_hdfs_dir="${uba_basic_dir}/uba_web_log/uba_web_visit_log/${uba_web_visit_rule}";
    uba_web_visit_hdfs_file="${uba_web_visit_hdfs_dir}/uba_web_visit_${m_date}.log";


    #if [ -f $uba_web_visit_local_file ]; then

      #读取 hdfs 文件
      is_uba_web_visit_hdfs_file=$($hadoop_bin/hadoop dfs -ls ${uba_web_visit_hdfs_file});

      if [ -n "$is_uba_web_visit_hdfs_file" ]; then
            #hdfs 中日志文件记录条数
            uba_web_visit_hdfs_file_count=$($hadoop_bin/hadoop dfs -cat ${uba_web_visit_hdfs_file} | wc -l);

            #本地日志文件记录条数
            uba_web_visit_local_file_count=($(ssh ${LOG_SERVER_ADDRESS} "wc -l ${uba_web_visit_local_file}"));

            #验证 hdfs 日志文件行数和 本地日志文件行数是否相同
            if [ ${uba_web_visit_hdfs_file_count} -eq ${uba_web_visit_local_file_count[0]} ];then
                #log_run_log+=" uba_web_visit : success -> hdfs 条数 ${uba_web_visit_hdfs_file_count},本地条数 ${uba_web_visit_local_file_count[0]}";
                log_run_log+="<div style=color:green>uba_web_visit : success -> hdfs 条数 ${uba_web_visit_hdfs_file_count},本地条数 ${uba_web_visit_local_file_count[0]}</div>";

            else
                log_run_log+="<div style=color:red>uba_web_visit : error -> hdfs 条数 ${uba_web_visit_hdfs_file_count},本地条数 ${uba_web_visit_local_file_count[0]}</div>";
            fi
        else
           log_run_log+="<div style=color:red>uba_web_visit : error -> uba_web_visit_hdfs_file not find</div>";
        fi

    #else
    #   log_run_log+="<div style=color:red>uba_web_visit : error -> uba_web_visit_local_file not find</div>";
    #fi

}


#2.监控 access 日志
accessLogFn () {
    echo "--------------- 监控 access 日志 ---------------";

    access_log_rule="access.${m_date}.log";  #access_log 文件命名规则

    access_log_local_dir="/var/log/uba/lb";#access_log 本地日志文件目录
    access_log_local_file="${access_log_local_dir}/${access_log_rule}";#access_log 本地日志文件路径

    access_log_hdfs_dir="${access_log_basic_dir}/access_log_${m_date}";#access_log hdfs 目录
    access_log_hdfs_file="${access_log_hdfs_dir}/${access_log_rule}";#access_log 本地日志文件路径

    #if [ -f $access_log_local_file ]; then

      #读取 hdfs 文件
      is_access_log_hdfs_file=$($hadoop_bin/hadoop dfs -ls ${access_log_hdfs_file});

      if [ -n "$is_access_log_hdfs_file" ]; then
            #hdfs 中日志文件记录条数
            access_log_hdfs_file_count=$($hadoop_bin/hadoop dfs -cat ${access_log_hdfs_file} | wc -l);

            #本地日志文件记录条数
            access_log_local_file_count=($(ssh ${LOG_SERVER_ADDRESS} "wc -l ${access_log_local_file}"));

            #验证 hdfs 日志文件行数和 本地日志文件行数是否相同
            if [ ${access_log_hdfs_file_count} -eq ${access_log_local_file_count[0]} ];then
                log_run_log+="<div style=color:green>access_log : success -> hdfs 条数 ${access_log_hdfs_file_count},本地条数 ${access_log_local_file_count[0]}</div>";
            else
                log_run_log+="<div style=color:red>access_log : error -> hdfs 条数 ${access_log_hdfs_file_count},本地条数 ${access_log_local_file_count[0]}</div>";
            fi
        else
           log_run_log+="<div style=color:red>access_log : error -> access_log_hdfs_file not find</div>";
        fi

    #else
  #     log_run_log+="<div style=color:red>access_log : error -> access_log_local_file not find</div>";
    #fi


}


#3.监控 app_aciton_log 日志
appActionLogFn () {
    echo "--------------- 监控 app_aciton_log 日志 ---------------";
    #文件规则
    uba_app_action_rule="uba_app_action_log_${m_date}";

    #本地文件目录和文件
    uba_app_action_local_dir="/var/log/uba";   #uba_app_action_log 本地日志文件目录
    uba_app_action_local_file="${uba_app_action_local_dir}/uba_app_action_${m_date}.log";#uba_app_action_log 本地日志文件路径

    #hdfs文件目录和文件
    uba_app_action_hdfs_dir="${uba_basic_dir}/uba_app_log/uba_app_action_log/${uba_app_action_rule}";
    uba_app_action_hdfs_file="${uba_app_action_hdfs_dir}/uba_app_action_${m_date}.log";

    #if [ -f $uba_app_action_local_file ]; then

      #读取 hdfs 文件
      is_uba_app_action_hdfs_file=$($hadoop_bin/hadoop dfs -ls ${uba_app_action_hdfs_file});

      if [ -n "$is_uba_app_action_hdfs_file" ]; then
            #hdfs 中日志文件记录条数
            uba_app_action_hdfs_file_count=$($hadoop_bin/hadoop dfs -cat ${uba_app_action_hdfs_file} | wc -l);

            #本地日志文件记录条数
            uba_app_action_local_file_count=($(ssh ${LOG_SERVER_ADDRESS} "wc -l ${uba_app_action_local_file}"));

            #验证 hdfs 日志文件行数和 本地日志文件行数是否相同
            if [ ${uba_app_action_hdfs_file_count} -eq ${uba_app_action_local_file_count[0]} ];then
                #log_run_log+=" uba_app_action : success -> hdfs 条数 ${uba_app_action_hdfs_file_count},本地条数 ${uba_app_action_local_file_count[0]}";
                log_run_log+="<div style=color:green>uba_app_action : success -> hdfs 条数 ${uba_app_action_hdfs_file_count},本地条数 ${uba_app_action_local_file_count[0]}</div>";

            else
                log_run_log+="<div style=color:red>uba_app_action : error -> hdfs 条数 ${uba_app_action_hdfs_file_count},本地条数 ${uba_app_action_local_file_count[0]}</div>";
            fi
        else
           log_run_log+="<div style=color:red>uba_app_action : error -> uba_app_action_hdfs_file not find</div>";
        fi

    #else
    #   log_run_log+="<div style=color:red>uba_app_action : error -> uba_app_action_local_file not find</div>";
    #fi

}



#4.监控 uba_web_action_log 日志
ubaWebActionFn () {
    echo "--------------- 监控 uba_web_action 日志 ---------------";

    #文件规则
    uba_web_action_rule="uba_web_action_log_${m_date}";

    #本地文件目录和文件
    uba_web_action_local_dir="/var/log/uba";   #uba_web_action_log 本地日志文件目录
    uba_web_action_local_file="${uba_web_action_local_dir}/uba_web_action_${m_date}.log";#uba_web_action_log 本地日志文件路径

    #hdfs文件目录和文件
    uba_web_action_hdfs_dir="${uba_basic_dir}/uba_web_log/uba_web_action_log/${uba_web_action_rule}";
    uba_web_action_hdfs_file="${uba_web_action_hdfs_dir}/uba_web_action_${m_date}.log";


    #if [ -f $uba_web_action_local_file ]; then

      #读取 hdfs 文件
      is_uba_web_action_hdfs_file=$($hadoop_bin/hadoop dfs -ls ${uba_web_action_hdfs_file});

      if [ -n "$is_uba_web_action_hdfs_file" ]; then
            #hdfs 中日志文件记录条数
            uba_web_action_hdfs_file_count=$($hadoop_bin/hadoop dfs -cat ${uba_web_action_hdfs_file} | wc -l);

            #本地日志文件记录条数
            uba_web_action_local_file_count=($(ssh ${LOG_SERVER_ADDRESS} "wc -l ${uba_web_action_local_file}"));

            #验证 hdfs 日志文件行数和 本地日志文件行数是否相同
            if [ ${uba_web_action_hdfs_file_count} -eq ${uba_web_action_local_file_count[0]} ];then
                #log_run_log+=" uba_web_action : success -> hdfs 条数 ${uba_web_action_hdfs_file_count},本地条数 ${uba_web_action_local_file_count[0]}";
                log_run_log+="<div style=color:green>uba_web_action : success -> hdfs 条数 ${uba_web_action_hdfs_file_count},本地条数 ${uba_web_action_local_file_count[0]}</div>";

            else
                log_run_log+="<div style=color:red>uba_web_action : error -> hdfs 条数 ${uba_web_action_hdfs_file_count},本地条数 ${uba_web_action_local_file_count[0]}</div>";
            fi
        else
           log_run_log+="<div style=color:red>uba_web_action : error -> uba_web_action_hdfs_file not find</div>";
        fi

    #else
    #   log_run_log+="<div style=color:red>uba_web_action : error -> uba_web_action_local_file not find</div>";
    ##fi

}


#run
ubaWebVisitFn;
accessLogFn;
appActionLogFn;
ubaWebActionFn;

#发送邮件
sendMail "${log_run_log}" "${m_date}";
