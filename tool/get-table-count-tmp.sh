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
    fn_gtc_run=$(${toolpath}/connect-spark-server-hql.sh "${fn_gtc_hql}");
    #cn_count=`cat "${fn_gtc_run}" | sed -r '' | cut -f3 -d" "`
    #echo "-----------"
     tmp_a=`echo "${fn_gtc_run}" | sed -n '8p'`
     tmp_a=${tmp_a//|/ }
     echo ${tmp_a}
}

getTableCountFn "${hql}";