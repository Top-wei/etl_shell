#!/bin/bash
#验证数据表是否存在 

#调用方式 check-table-is-exists.sh [db-name] [tb-name]

basepath=$(cd `dirname $0`; pwd);

db_name="$1";
table_name="$2";

function checkTableIsExistsFn () {
    declare -i result;
    result=0;

    fn_ctie_db_name="$1";
    fn_ctie_table_name="$2";

    fn_ctie_fql="
use ${fn_ctie_db_name};
show tables;
";
    declare result;
    query_result=$(${basepath}/connect-base-hive-server-hql.sh "${fn_ctie_fql}");

    declare  -i tables_count;
    tables_count=`echo "${query_result}" | wc -l`;

    #循环
    for (( i=1; i<=$tables_count; i++)); do
        #读取每一行数据
        line=`echo "${query_result}" | sed -n "${i}p"`;
        arr_line=($line);
        if [ "${fn_ctie_table_name}" = "${arr_line[0]}" ];then
            result=1;
        fi
    done;

    echo $result;
}

checkTableIsExistsFn $db_name $table_name;