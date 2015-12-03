#!/bin/bash

#把 mysql 导入到 hive 中

#调用方法 mysql-to-hive-dump.sh  mysql=[mysql数据库名.表名] hive=[hive数据库名.表名] db_type=[slave | dw] fields_terminated_by=[字段分割符号 (默认 \001)]

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

tmp_dir="${basepath}/../.tmp";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;


#解析参数成变量
source ${toolpath}/shell-parameter.sh ${@};


#使用的 MySQL 数据源类型
source_db_type=${db_type:-slave};


#mysql 数据库和表
mysql_import=$mysql;
mysql_test="test.test";
mysql_info=${mysql_import:-${mysql_test}};
mysql_info_arr=(${mysql_info//./ });
mysql_database=${mysql_info_arr[0]};
mysql_table=${mysql_info_arr[1]};


#hive 数据库和表
hive_import=$hive;
hive_test="test.test";
hive_info=${hive_import:-${hive_test}};
hive_info_arr=(${hive_info//./ });
hive_database=${hive_info_arr[0]};
hive_table=${hive_info_arr[1]};


#mysql 结果文件存放目录
now_result_file="${M2H_SYNC_MYSQL_QUERY_RESULT_FILE_DIR}/${mysql_database}.${mysql_table}";


#字段分隔符
fields_terminated_by=${fields_terminated_by:-"001"};
fields_terminated_by="\\${fields_terminated_by}";


function getMysqlResultFn () {
    rm -rf $now_result_file;
    fn_gmr_mysql_sql="SELECT * FROM ${mysql_database}.${mysql_table}";

    #处理换行
    fn_gmr_regexp+="s/[\n|\r\n]//g;";
    #处理 NULL 字符串
    fn_gmr_regexp+="s/NULL/\\\N/g;";
    #处理分隔符 
    fn_gmr_regexp+="s/\t/$(echo -e ${fields_terminated_by})/g;";

    #格式化
    ${toolpath}/connet-mysql-server.sh "${source_db_type}" "${fn_gmr_mysql_sql}" | sed -e "${fn_gmr_regexp}" > ${now_result_file};
}


function getMysqlFieldsFn () {
    fn_gmf_fields_sql="USE ${mysql_database};DESC ${mysql_table};";
    echo "`${toolpath}/connet-mysql-server.sh "${source_db_type}" "${fn_gmf_fields_sql}"`";
}


function createHiveTableForMysqlFn () {
    fn_chtfm_mysql_fields_content="`getMysqlFieldsFn`";

    fn_chtfm_count=`echo "${fn_chtfm_mysql_fields_content}" | wc -l`;

    fn_chtfm_create_hive_table_hql="
CREATE TABLE IF NOT EXISTS ${hive_database}.${hive_table} (
";
 
    for ((i=1;i<=$fn_chtfm_count;i++)) 
    do
        fn_chtfm_now_line=`echo "${fn_chtfm_mysql_fields_content}" | sed -n "${i}p"`
        
        fn_chtfm_hive_fileds_table+="\`$(echo ${fn_chtfm_now_line} | awk '{
            print $1;
        }')\` ";

    done;

    #组合字段结构
    fn_chtfm_create_hive_table_hql+=`${toolpath}/implode.sh " String," ${fn_chtfm_hive_fileds_table}`;

    fn_chtfm_create_hive_table_hql+="
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '${fields_terminated_by}'
  COLLECTION ITEMS TERMINATED BY '\n'
;
";

    echo "${fn_chtfm_create_hive_table_hql}";
}


function mysqlResultToHiveFn () {

    #查询 mysql 数据结构，生成创建 hive table
    fn_mrth_hive_hql+="
-- 创建表结构
`createHiveTableForMysqlFn`
";

    fn_mrth_hive_hql+="
-- 上传文件到 hive table 中
LOAD DATA LOCAL INPATH '${now_result_file}' OVERWRITE INTO TABLE ${hive_database}.${hive_table};
";


    echo "${fn_mrth_hive_hql}";
    ${toolpath}/connect-base-hive-server-hql.sh "${fn_mrth_hive_hql}";
} 


function mysqlToHiveDumpFn () {
    #查询 mysql 结果，放入到本地文件中
    getMysqlResultFn

    #mysql 结果传送到 hive 上
    mysqlResultToHiveFn;
}

mysqlToHiveDumpFn;

