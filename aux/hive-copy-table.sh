#!/bin/bash
# 迁移 hive table

#使用方法
#./hive-copy-table.sh source_table=[db_name.table_name] target_table=[db_name.table_name] table_type=[0 | 1 ,可选 ,0表示普通表 1表示分区表,默认 1] partition_field=[分区字段，默认 p_dt]

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

#解析参数成变量
source ${toolpath}/shell-parameter.sh ${@};

#源数据表
source_info_arr=(${source_table//./ });
source_database=${source_info_arr[0]};
source_table=${source_info_arr[1]};

#目标表
target_info_arr=(${target_table//./ });
target_database=${target_info_arr[0]};
target_table=${target_info_arr[1]};

#是否是分区表(0普通，1分区)
table_type=${table_type:-1};

#分区字段名称，默认 p_dt
partition_field=${partition_field:-p_dt};

#获取元数据表结构
function getSourceTableFiledsFn () {

    #获取表字段
    get_table_field=`${toolpath}/get-table-fields.sh ${source_database} ${source_table} 0`;

    #组合成字符串
    string_fields=`${toolpath}/implode.sh "," ${get_table_field}`;
    
    echo "${string_fields}";
}


#分区表
function partitionTableFn () {
    
    ptfn_get_table_fields=`getSourceTableFiledsFn`;

    ptfn_hql="
-- 创建表结构
CREATE TABLE IF NOT EXISTS ${target_database}.${target_table} LIKE ${source_database}.${source_table};


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;


-- 插入数据
INSERT OVERWRITE TABLE
    ${target_database}.${target_table}
PARTITION (
    ${partition_field}
)
SELECT 
    ${ptfn_get_table_fields}
FROM
    ${source_database}.${source_table}
DISTRIBUTE BY
    ${partition_field}
;
";
    #执行 hql
    ${toolpath}/connect-base-hive-server-hql.sh "${ptfn_hql}";
}


#普通表
function generalTableFn () {

    #获取表结构
    gtfn_get_table_structure=`${toolpath}/get-table-fields.sh ${source_database} ${source_table} 1`;

    #获取表字段
    gtfn_get_table_fields=`getSourceTableFiledsFn`;

    gtfn_hql="

-- 创建表结构
CREATE TABLE IF NOT EXISTS ${target_database}.${target_table} (
    ${gtfn_get_table_structure}
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY '\n'
STORED AS TEXTFILE;

-- 插入数据
INSERT OVERWRITE TABLE
    ${target_database}.${target_table}
SELECT
    ${gtfn_get_table_fields}
FROM
    ${source_database}.${source_table}
;
";

    #执行 HQL
    ${toolpath}/connect-base-hive-server-hql.sh "${gtfn_hql}";
}


#分区表
if [ ${table_type} -eq 1 ];then
    partitionTableFn;
#普通表
elif [ ${table_type} -eq 0 ];then
    generalTableFn;
fi

