#!/bin/bash

# 通过 hive 方式执行 HQL 
# 语法： ./connect-base-hive-server-hql.sh "HQL";

basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

#临时存放文件目录
tmp_file_dir="${basepath}/../.tmp";

#配置文件
source ${confpath}/conf.sh;


function hiveServerHql() {
    rm -rf $base_hive_server_hql;

    fn_hsh_hql="${1}";

    #进程
    now_run_pid=${master_run_pid:-$$};

    #临时文件
    base_hive_server_hql="${tmp_file_dir}/base_hive_server_hql_${now_run_pid}.hql";

    #default conf
    echo "
-- 关闭打印列名
set hive.cli.print.header=false;
" >  ${base_hive_server_hql};

    #import hql
    echo "${fn_hsh_hql}" >> ${base_hive_server_hql};

    #RUN HQL
    ${SYSTEM_HIVE_BIN}/hive -S -f ${base_hive_server_hql};

    rm -rf $base_hive_server_hql;
}

hiveServerHql "${1}";



