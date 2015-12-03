#!/bin/bash

# hive table 重跑脚本 (只重跑指定逻辑的数据)
# 参数
    # reset_db_table=[db.table] 重跑库名表名
    # reset_date=[ymd] 重跑的分区日期
    # reset_table_alias=[重跑表的 alias 别名] 默认 bs
    # extend_sql_file=[file_dir] 重跑的扩展逻辑 (文件中写入 : ${reset_date} 会替换成对应的,重跑的分区日期 y-m-d 。 ${reset_table_alias} : 会替换成源数据表的 alias 别名)
    # map_fields=[old_field:new_field--old_field_two:new_field_two] 需要替换重跑的字段映射关系。(使用函数用 [[ ]] 代替 () ,如COALESCE[[xxx,0]])	
    # is_debug=[1 | 0] (是否调试模式，1 表示只打印不执行，0 表示打印，但不执行，默认 0)
    # is_delete_partition=[1 | 0] (是否删除当前分区，1 是，0 不是 ，默认 0 (除非完成重跑，不然请不要使用修改，使用默认即可)
    # is_use_reset_date_where=[1 | 0] (是否使用重跑日期，作为重跑表的 where 条件。1 使用 ，0 不适用 ，默认 1)
    # temp_db_name=[可选，临时执行数据库名称] 默认 dw_db_temp 库

#案例:
  #1.刷新表 test_db.test_table 表 ,
  # ./hive-rerun-table.sh \
  # reset_db_table=test_db.test_table \
  # reset_date=20150808  \
  # extend_sql_file=/home/dwadmin/test/xyz.sql \
  # reset_table_alias=bs_info \
  # map_fields=old_field_1:COALESCE[[t_1.new_field_1,0]]--old_field_2:t_2.new_field_2 \
  # is_delete_partition=1 \
  # is_use_reset_date_where=1 \
  # is_debug=1

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

#解析参数
source ${toolpath}/shell-parameter.sh ${@};


#重跑日期
reset_date=`date -d ${reset_date} +%Y-%m-%d`;


#重跑表
db_table=(${reset_db_table//./ });
database=${db_table[0]};
table=${db_table[1]};


#分区字段名称，默认 p_dt
partition_field=${partition_field:-p_dt};


#debug 模式 (1:debug 模式,打印 HQL ,不执行  0:打印 + 执行  HQL)
is_debug=${is_debug:-0};


#是否删除对应分区 (1 删除 ，0 不删除)
is_delete_partition=${is_delete_partition:-0};


#是否使用重跑日期，作为重跑表的 where 条件
is_use_reset_date_where=${is_use_reset_date_where:-1}


#临时数据库名
temp_db_name=${temp_db_name:-dw_db_temp};


#获取额外查询 sql
extend_sql_file=${extend_sql_file};


#数据表前缀
reset_table_alias=${reset_table_alias:-"bs"};


#输入字段信息
map_fields_info=${map_fields};
map_fields_format=${map_fields//--/ };
map_fields_arr=(${map_fields_format});


declare -A import_fields_info_map; #定义 map 数组
#抽取原始字段和替换的字段
for now_map_field_info in ${map_fields_arr[@]};
do
    now_map_fields_arr=(${now_map_field_info//:/ });
    now_map_fields_arr_var="${now_map_fields_arr[1]}";
    now_map_fields_arr_var="${now_map_fields_arr_var//[[/(}";
    now_map_fields_arr_var="${now_map_fields_arr_var//]]/)}";
    import_fields_info_map[${now_map_fields_arr[0]}]="${now_map_fields_arr_var}";
done;


#获取表的字段
get_hive_table_fields=`${toolpath}/get-table-fields.sh ${database} ${table} 2`;

#临时表名
temp_db_and_table="${temp_db_name}.${database}__${table}";


#获取查询字段
hql_select_fields="";

declare -i hive_table_fields_i=0;

for now_table_field_info in ${get_hive_table_fields[@]};
do
    #按照 - 转换为数组
    arr_now_info=(${now_table_field_info//-/ });
    now_field_name=${arr_now_info[0]};
    now_field_type=${arr_now_info[1]};

    if [ ${now_field_name} = ${partition_field} ];then
        continue;
    fi

    if [ -z ${import_fields_info_map[${now_field_name}]} ];then
        hql_select_fields+="
    ${reset_table_alias}.${now_field_name},";
    else 
        hql_select_fields+="
    ${import_fields_info_map[${now_field_name}]},";
    fi

    hive_table_fields_i=$(($hive_table_fields_i+1));

    #获取原表查询字段，剔除分区字段
    query_source_fields+="
    ${now_field_name},";

done;

#去除最后逗号
hql_select_fields_format="${hql_select_fields:0:${#hql_select_fields}-1}";
query_source_fields_format="${query_source_fields:0:${#query_source_fields}-1}";

#提交执行的 HQL 

run_hql+="
-- db_table : ${database}.${table} 
-- reset_date : ${reset_date} 
-- map_fields_info : ${map_fields_info}
";


#删除分区流程
if [ $is_delete_partition -eq 1 ];then

   run_hql+="

--- 执行删除分区流程 START ---

-- 创建临时储存分区表
DROP TABLE IF EXISTS ${temp_db_and_table};
CREATE TABLE IF NOT EXISTS ${temp_db_and_table} LIKE ${database}.${table};
INSERT OVERWRITE TABLE
    ${temp_db_and_table}
PARTITION (
    ${partition_field} = '${reset_date}'
) 
SELECT 
    ${query_source_fields_format}
FROM 
   ${database}.${table}
WHERE
   ${partition_field} = '${reset_date}'
; 

-- 删除重跑表分区 : ${partition_field} = '${reset_date}'
ALTER TABLE
  ${database}.${table}
DROP IF EXISTS PARTITION (
  ${partition_field} = '${reset_date}'
);

--- 执行删除分区流程 END ---

";
fi


run_hql+="
INSERT OVERWRITE TABLE
    ${database}.${table}
PARTITION (
    ${partition_field} = '${reset_date}'
)
SELECT
";


run_hql+="${hql_select_fields_format}";



#删除分区流程
if [ $is_delete_partition -eq 1 ];then
    run_hql+="

FROM
    -- 从临时抽取数据
    ${temp_db_and_table} AS ${reset_table_alias}

";
else 
    run_hql+="

FROM
    ${database}.${table} AS ${reset_table_alias}

";

fi


#读取外部 sql 文件
extend_sql_content=`cat ${extend_sql_file}`;

#替换变量
extend_sql_content="${extend_sql_content//\$\{reset_date\}/\'${reset_date}\'}";
extend_sql_content=${extend_sql_content//\$\{reset_table_alias\}/${reset_table_alias}};


#准备执行的 hql
run_hql+="${extend_sql_content}";


#是否使用重跑日期，作为重跑表的 where 条件
if [ $is_use_reset_date_where -eq 1 ];then
    run_hql+="

WHERE
    ${reset_table_alias}.${partition_field} = '${reset_date}'
;
"
fi


if [ $is_debug -eq 1 ];then
    echo "${run_hql}";
else
    echo "${run_hql}";
    ${toolpath}/connect-base-hive-server-hql.sh "${run_hql}";
fi


