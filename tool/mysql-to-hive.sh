#!/bin/bash

#把 mysql 导入到 hive 基本操作

#调用方法：./mysql-to-hive.sh  mysql=[mysql数据库名.表名] hive=[hive数据库名.表名] db_type=[slave | dw] fields_terminated_by=[字段分割符号 (默认 \001)]  num_mappers=[num (map reduce 数量，默认 1)]


basepath=$(cd `dirname $0`; pwd);

tmp_dir="${basepath}/../.tmp";

confpath="${basepath}/../conf";

toolpath="${basepath}/../tool";


#引入配置文件
source ${confpath}/conf.sh;

#解析参数成变量
source ${toolpath}/shell-parameter.sh ${@};


#sqoop 生成的 java 文件临时存放目录
java_outdir="${tmp_dir}";


#使用的 MySQL 数据源类型
source_db_type=${db_type:-slave};
if [ $source_db_type = slave ];then

    #数据源配置
    mysql_host=${SYSTEM_SLAVE_MYSQL_HOST};
    mysql_user=${SYSTEM_SLAVE_MYSQL_USER};
    mysql_password=${SYSTEM_SLAVE_MYSQL_PASSWORD};

elif [ $source_db_type = dw ];then

    #数据源配置
    mysql_host=${SYSTEM_DW_MYSQL_HOST};
    mysql_user=${SYSTEM_DW_MYSQL_USER};
    mysql_password=${SYSTEM_DW_MYSQL_PASSWORD};

fi


#mysql 数据库和表
mysql_import=$mysql;
mysql_test="xxx.xxx";
mysql_info=${mysql_import:-${mysql_test}};
mysql_info_arr=(${mysql_info//./ });
mysql_database=${mysql_info_arr[0]};
mysql_table=${mysql_info_arr[1]};

#hive 数据库和表
hive_import=$hive;
hive_test="xxx.xxx";
hive_info=${hive_import:-${hive_test}};
hive_info_arr=(${hive_info//./ });
hive_database=${hive_info_arr[0]};
hive_table=${hive_info_arr[1]};


#获取MySQL 字段类型
function getMysqlFieldsFn () {
    fn_gmf_fields_sql="USE ${mysql_database};DESC ${mysql_table};";
    echo "`${toolpath}/connet-mysql-server.sh "${source_db_type}" "${fn_gmf_fields_sql}"`";
}

#把所有字段转换成 String  --map-column-hive
function hiveTableFormatForMysqlFn () {
    fn_htffm_mysql_fields_content="`getMysqlFieldsFn`";

    fn_htffm_count=`echo "${fn_htffm_mysql_fields_content}" | wc -l`;

    for ((i=1;i<=$fn_htffm_count;i++)) 
    do
        fn_htffm_now_line=`echo "${fn_htffm_mysql_fields_content}" | sed -n "${i}p"`

        fn_htffm_hive_fileds_table+="$(echo ${fn_htffm_now_line} | awk '{
            print $1;
        }') ";

    done;
    #--map-column-hive="AAA=String,BBB=STRING"
    #组合字段结构
    fn_htffm_hive_table_fields+=`${toolpath}/implode.sh "=String," ${fn_htffm_hive_fileds_table}`;

    echo $fn_htffm_hive_table_fields;
}

map_column_hive_fields=`hiveTableFormatForMysqlFn`;


#sqoop 参数

# map reduct 数量
num_mappers=${num_mappers:-1};

#字段分隔符
fields_terminated_by=${fields_terminated_by:-"001"};
fields_terminated_by="\\${fields_terminated_by}";


# 删除 java 临时文件
rm ${java_outdir}/${mysql_table}.java;

sqoop_target_dir="/${SYSTEM_HADOOP_USER}/temp/sqoop-target/${hive_table}";

${SYSTEM_SQOOP_BIN}/sqoop import --connect "jdbc:mysql://${mysql_host}:3306/${mysql_database}?useUnicode=true&tinyInt1isBit=false&characterEncoding=utf-8" --username ${mysql_user} --password ${mysql_password} --table ${mysql_table} --hive-table ${hive_database}.${hive_table} --hive-import -hive-delims-replacement '%n&' --fields-terminated-by "${fields_terminated_by}" --lines-terminated-by "\n" --input-null-string '\\N' --input-null-non-string '\\N' --null-string '\\N' --null-non-string '\\N' --map-column-hive=${map_column_hive_fields} --outdir ${java_outdir} --target-dir ${sqoop_target_dir} --delete-target-dir  -m ${num_mappers};
