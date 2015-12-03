#!/bin/bash
# 获取 数据表记录条数 
# 用法：./get-table-count.sh [hql语句] ，如：./get-table-count.sh "select count(*) from db_sync.test__test_check;";

basepath=$(cd `dirname $0`; pwd); 

toolpath="${basepath}";

tmp_dir="${basepath}/../.tmp";


hql="${1}";


function getTableCountFn () {
    fn_gtc_hql="${1}";

    #去hive服务器执行
    fn_gtc_run=$(${toolpath}/connect-base-hive-server-hql.sh "${fn_gtc_hql}");

    echo ${fn_gtc_run};
}

getTableCountFn "${hql}";