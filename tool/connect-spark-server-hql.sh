#!/bin/bash

#连接 spark server 执行 hql 语句 

#调用方式 ./connect-hive-server-hql.sh [hql 语句]

basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

#临时存放文件目录
tmp_file_dir="${basepath}/../.tmp";

#配置文件
source ${confpath}/conf.sh;

if [ ${system_env} = "offline" ]; then

    beeline_conf="${SYSTEM_BEELINE_IP} hadoop\n\n";

elif [ ${system_env} = "online" ]; then

    beeline_conf="uhadoop-ociicy-master2:10002/default hadoop\n\n";

fi


# hql 命令
hive_hql_import=$1;
hive_hql_default="show databases;"; #默认不需要
hive_hql=${hive_hql_import:-${hive_hql_default}};


#当前执行的 hive pid , 默认为 0 
now_run_pid=${master_run_pid:-$$};


#hive 临时文件
connect_hive_server_tmp_file="${tmp_file_dir}/connect_hive_server_tmp_file_${now_run_pid}.hql";


#连接 hive server 执行 hql 语句
function connectHiveServer () {
    fn_hql=$1;

    #连接进入 hive 语句
    echo -e "!connect jdbc:hive2://${beeline_conf}" > $connect_hive_server_tmp_file;

    #hql 语句
    echo "${fn_hql}" >> $connect_hive_server_tmp_file;

    #打印出操作语句
    #cat $connect_hive_server_tmp_file;

    #hive server 上执行 hql;
    $SYSTEM_BEELINE_PATH/beeline -f $connect_hive_server_tmp_file;

    rm ${connect_hive_server_tmp_file}; 
}

connectHiveServer "${hive_hql}";
