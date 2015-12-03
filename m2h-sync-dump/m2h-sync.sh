#!/bin/bash

#把 mysql 导入到 hive sync 同步库中。如果需要快照，则创建一份当天的快照

#调用方法 ./m2h-sync.sh [mysql数据库.表] [导入方式:sqoop | mysql_dump] [map reduce 数]
basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

tmp_dir="${basepath}/../.tmp";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

#mysql 数据库、表
mysql_info=${1:-test.test};

#导入方式
to_hive_type=${2:-mysql_dump};

#mp 数量
map_reduce_num=${3:-1};

#hive 数据和表的分隔符 如 db_anme__tb_name;
hive_separator="${M2H_SYNC_HIVE_SEPARATOR}";

#hive sync 数据表
#[数据库名字].[录入mysql同名数据库][分隔符][录入mysql同名数据表]
hive_sync_database_and_table="${M2H_SYNC_HIVE_DATABASE_SYNC_NAME}.${mysql_info//./${hive_separator}}";


#删除数据 hive sync 同步库的数据表
function dropHiveSyncFn () {
    fn_dtf_drop_table_hql="DROP TABLE IF EXISTS ${hive_sync_database_and_table};";
    echo "--------------- 执行删除表 ${hive_sync_database_and_table} ---------------"
    ${toolpath}/connect-base-hive-server-hql.sh "${fn_dtf_drop_table_hql}";
}

#导入指定数据到 hive sync 同步 数据库中
function mysqlToHiveSyncFn () {

    echo "--------------- 执行 mysql result 导入 hive ${fn_sith_hive_database_and_table} ---------------";

    #导入到 mysql 数据到 hive
    
    if [ $to_hive_type = mysql_dump ];then
        ${toolpath}/mysql-to-hive-dump.sh db_type=slave mysql=${mysql_info} hive=${hive_sync_database_and_table};
    elif [ $to_hive_type = sqoop ];then
        ${toolpath}/mysql-to-hive.sh db_type=slave mysql=${mysql_info} hive=${hive_sync_database_and_table} num_mappers="${map_reduce_num}";
    fi
}


#删除 hive sync 数据库中的指定表
dropHiveSyncFn;

#执行导入
mysqlToHiveSyncFn;








