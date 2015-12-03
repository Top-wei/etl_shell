#!/bin/bash
#导入本地日志文件到 hdfs 中

#调用方式 log-to-hdfs.sh date[日期,可选,20150320] reset[重跑标致,可选,1]

#文档:http://git.corp.angejia.com/dw/uba/blob/master/docs/design/create-hive-table.md
#作者jason@angejia.com

basepath=$(cd `dirname $0`; pwd)

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

#日期
import_date=$1;
date=$(date -d last-day +%Y%m%d);
m_date=${import_date:-${date}};


#hadoop
hadoop_bin=${SYSTEM_HADOOP_BIN};

#hdfs uba_log 目录
uba_basic_dir=${UBA_HIVE_BASIC_DIR};


#hdfs access_log 目录
access_log_basic_dir=${UBA_ACCESS_LOG_BASIC_DIR};


#上传文件到 hdfs
function fileToHdfsFn () {
    fn_fth_local_file=${1};
    fn_fth_hdfs_file=${2};

    ssh ${LOG_SERVER_ADDRESS} "
    bash -i $hadoop_bin/hadoop dfs -rm ${fn_fth_hdfs_file};
    bash -i $hadoop_bin/hadoop dfs -put ${fn_fth_local_file} ${fn_fth_hdfs_file};
    ";
}


#******开始日志处理*******

#1.uba_web_visit 日志处理
ubaWebVisitFn () {

    #文件规则
    uba_web_visit_rule="uba_web_visit_log_${m_date}";
    
    #本地文件目录和文件   
    uba_web_visit_local_dir=${UBA_WEB_VISIT_LOCAL_DIR}; #uba_web_visit_log 本地日志文件目录
    uba_web_visit_local_file="${uba_web_visit_local_dir}/uba_web_visit_${m_date}.log";#uba_web_visit_log 本地日志文件路径
    
    #hdfs文件目录和文件
    uba_web_visit_hdfs_dir="${uba_basic_dir}/uba_web_log/uba_web_visit_log/${uba_web_visit_rule}";
    uba_web_visit_hdfs_file="${uba_web_visit_hdfs_dir}/uba_web_visit_${m_date}.log";

    echo "--------------- uba_web_visit 上传 ${uba_web_visit_local_file} 到 ${uba_web_visit_hdfs_file} ---------------";

    fileToHdfsFn ${uba_web_visit_local_file} ${uba_web_visit_hdfs_file};

}



#2.access_log 日志处理
accessLogFn () {

    #access_log 文件命名规则
    access_log_rule="access.${m_date}.log";

    access_log_local_dir=${UBA_ACCESS_LOG_LOCAL_DIR};   #access_log 本地日志文件目录
    access_log_local_file="${access_log_local_dir}/${access_log_rule}";#access_log 本地日志文件路径
    
    access_log_hdfs_dir="${access_log_basic_dir}/access_log_${m_date}";#access_log hdfs 目录
    access_log_hdfs_file="${access_log_hdfs_dir}/${access_log_rule}";#access_log 本地日志文件路径

    echo "--------------- access_log 上传 ${access_log_local_file} 到 ${access_log_hdfs_file} ---------------";

    fileToHdfsFn ${access_log_local_file} ${access_log_hdfs_file};

}



#3.uba_app_action_log 
function ubaAppActionLogFn () {
    #文件规则
    uba_app_action_rule="uba_app_action_log_${m_date}";
    
    #本地文件目录和文件
    uba_app_action_local_dir=${UBA_APP_ACTION_LOCAL_DIR};   #uba_app_action_log 本地日志文件目录
    uba_app_action_local_file="${uba_app_action_local_dir}/uba_app_action_${m_date}.log";#uba_app_action_log 本地日志文件路径
    
    #hdfs文件目录和文件
    uba_app_action_hdfs_dir="${uba_basic_dir}/uba_app_log/uba_app_action_log/${uba_app_action_rule}";
    uba_app_action_hdfs_file="${uba_app_action_hdfs_dir}/uba_app_action_${m_date}.log";

    echo "--------------- uba_app_action_log 上传 ${uba_app_action_local_file} 到 ${uba_app_action_hdfs_file} ---------------";

    fileToHdfsFn ${uba_app_action_local_file} ${uba_app_action_hdfs_file};

}


#4.uba_web_action_log 日志处理
ubaWebActionLogFn () {

    #文件规则
    uba_web_action_rule="uba_web_action_log_${m_date}";

    #本地文件目录和文件
    uba_web_action_local_dir=${UBA_WEB_ACTION_LOCAL_DIR};   #uba_web_action_log 本地日志文件目录
    uba_web_action_local_file="${uba_web_action_local_dir}/uba_web_action_${m_date}.log";#uba_web_action_log 本地日志文件路径
    
    #hdfs文件目录和文件
    uba_web_action_hdfs_dir="${uba_basic_dir}/uba_web_log/uba_web_action_log/${uba_web_action_rule}";
    uba_web_action_hdfs_file="${uba_web_action_hdfs_dir}/uba_web_action_${m_date}.log";

    echo "--------------- uba_web_action_log 上传 ${uba_web_action_local_file} 到 ${uba_web_action_hdfs_file} ---------------";

    fileToHdfsFn ${uba_web_action_local_file} ${uba_web_action_hdfs_file};

}



#执行脚本

ubaWebVisitFn;

accessLogFn;

ubaAppActionLogFn;

ubaWebActionLogFn;



