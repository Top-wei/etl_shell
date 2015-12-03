#!/bin/bash

#数据表上线流程
# 上线流程：备份 -> 添加字段
    # 调用方式: ./hive-copy-and-create-fields.sh source_table=[db_name.table_name] run_date=[备份运行时间,默认昨天.如(20150701)] add_fields=[需要添加的字段,为空则不追加字段,如(t_1,t_2,t_3,t_4)] table_type=[数据表类型:0普通表，1分区表] partition_field=[分区表字段名:默认:p_dt] source_db_type=[数据库类型:[dw | slave],默认 dw] 

# 回滚流程: 删除添加字段 
    # 回滚方式: ./hive-copy-and-create-fields.sh is_reset=1 source_table=[db_name.table_name] run_date=[备份运行时间,默认昨天.如(20150701)] add_fields=[回滚删除的字段,如(t_1,t_2,t_3,t_4)] table_type=[数据表类型:0普通表，1分区表] partition_field=[分区表字段名:默认:p_dt] source_db_type=[数据库类型:[dw | slave],默认 dw] 


basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

#解析参数成变量
source ${toolpath}/shell-parameter.sh ${@};


#环境日期
last_date=$(date -d last-day +%Y%m%d);
m_date=${run_date:-${last_date}};
format_m_date=$(date -d ${m_date} +%Y-%m-%d);


#源数据表
source_info_arr=(${source_table//./ });
source_database=${source_info_arr[0]};
source_table=${source_info_arr[1]};


#目标表
bakup_table=${bakup_table:-"dw_history_db.${source_table}_${m_date}"};#默认数据表
bakup_info_arr=(${bakup_table//./ });
bakup_database=${bakup_info_arr[0]};
bakup_table=${bakup_info_arr[1]};


#追加的字段
fields_info=${add_fields};
fields_info_format=${fields_info//,/ };
fields_info_arr=(${fields_info_format});


#是否是分区表(0普通，1分区，默认分区表)
table_type=${table_type:-1};


#分区字段名称，默认 p_dt
partition_field=${partition_field:-p_dt};


#数据库类型
source_db_type=${source_db_type:-"dw"};


#是否撤销
is_reset=${is_reset:-0};


#一、备份数据源 
function bakSourceTableFn () {
    # 1、备份 hive 数据表
    ${basepath}/hive-copy-table.sh source_table="${source_database}.${source_table}" target_table="${bakup_database}.${bakup_table}" table_type="${table_type}" partition_field="${partition_field}";

    # 2、备份 mysql 数据表
    mysql_bakup_sql="
CREATE TABLE IF NOT EXISTS ${bakup_database}.${bakup_table} LIKE ${source_database}.${source_table};
INSERT INTO ${bakup_database}.${bakup_table} SELECT * FROM ${source_database}.${source_table};
";

    echo "${mysql_bakup_sql}";

    ${toolpath}/connet-mysql-server.sh "${source_db_type}" "${mysql_bakup_sql}";

}


#二、追加新增字段到表中
function addFieldsToTableFn () {

    #1、hive table 追加字段
    hive_add_field_hql="
-- HIVE 追加新字段
ALTER TABLE 
    ${source_database}.${source_table}
ADD COLUMNS(
";
    for fn_aftt_now_info in ${fields_info_format[@]};
    do
        hive_add_field_hql+="\`${fn_aftt_now_info}\` String COMMENT '${format_m_date} add',
";
    done;

    hive_add_field_hql="${hive_add_field_hql:0:${#hive_add_field_hql}-2}";

    hive_add_field_hql+="
);";

   #2、mysql 添加字段
   get_mysql_fields_sql="USE ${source_database};DESC ${source_table};";
   mysql_fields_content="`${toolpath}/connet-mysql-server.sh "${source_db_type}" "${get_mysql_fields_sql}"`";
   mysql_fields_count=`echo "${mysql_fields_content}" | wc -l`;

   #获取 mysql 最后一个字段
   for ((i=${mysql_fields_count};i>0;i--))  
   do
        now_filed_line=`echo "${mysql_fields_content}" | sed -n "${i}p"`
        last_fileds_name="$(echo ${now_filed_line} | awk '{
           print $1;
        }')";

        if [ ${table_type} -eq 1 ] && echo "" &&  [ ${last_fileds_name} = ${partition_field} ];then
            continue;
        else
            mysql_last_field="${last_fileds_name}";
            break;
        fi
    done;

    mysql_add_field_sql="
ALTER TABLE
    ${source_database}.${source_table}
";

    fields_info_arr_length=${#fields_info_arr[@]};

    for((j=0;j<${fields_info_arr_length};j++)) {

        if [ $j -eq 0 ];then
            mysql_add_field_sql+="
ADD ${fields_info_arr[${j}]} varchar(255) NOT NULL DEFAULT '' AFTER ${mysql_last_field}
";

        elif [ `expr $j + 1` -lt $fields_info_arr_length ];then
             mysql_add_field_sql+="
,ADD ${fields_info_arr[${j}]} varchar(255) NOT NULL DEFAULT '' AFTER ${fields_info_arr[${j}-1]}
";

        else 
            mysql_add_field_sql+="
,ADD ${fields_info_arr[${j}]} varchar(255) NOT NULL DEFAULT '' AFTER ${fields_info_arr[${j}-1]}
";
        fi
    }
    mysql_add_field_sql+=";";

    #执行追加字段
    echo "${hive_add_field_hql}";
    echo "${mysql_add_field_sql}";

    ${toolpath}/connect-base-hive-server-hql.sh "${hive_add_field_hql}";
    ${toolpath}/connet-mysql-server.sh "${source_db_type}" "${mysql_add_field_sql}";

 }



# 三、撤销添加的字段内容
function resetFieldsToTableFn () {


    #1、撤销 hive 添加的字段
    fn_rftt_get_hive_table_fields=`${toolpath}/get-table-fields.sh ${source_database} ${source_table} 2`;

fn_rftt_reset_hive_table_field_hql="
ALTER TABLE ${source_database}.${source_table} replace columns (
";

    for fn_rftt_now_info in ${fn_rftt_get_hive_table_fields[@]};
    do
      #按照 - 转换为数组
      fn_rftt_arr_now_info=(${fn_rftt_now_info//-/ });
      fn_rftt_now_field_name=${fn_rftt_arr_now_info[0]};
      fn_rftt_now_field_type=${fn_rftt_arr_now_info[1]};

      is_fn_rftt_now_field=`${toolpath}/in-array.sh ${fn_rftt_now_field_name} ${fields_info}`

      if [ $is_fn_rftt_now_field -eq 0 ];then
        #过滤分区字段
        if [ $fn_rftt_now_field_name != $partition_field ];then
            fn_rftt_hive_table_fields+="${fn_rftt_now_field_name} ${fn_rftt_now_field_type},";
        fi
      fi

    done;

    fn_rftt_hive_table_fields_format=${fn_rftt_hive_table_fields:0:${#fn_rftt_hive_table_fields}-1}

    fn_rftt_reset_hive_table_field_hql+="
${fn_rftt_hive_table_fields_format} )
;
";


    #2、撤销 mysql 添加的字段
    fn_rftt_reset_mysql_table_field_hql="
USE ${source_database};
";

fn_rftt_reset_mysql_table_field_hql+="
ALTER TABLE \`${source_table}\`
";

    for fn_rftt_now_import_field in ${fields_info_arr[@]};
    do
        fn_rftt_reset_mysql_table_field_hql+="DROP COLUMN \`${fn_rftt_now_import_field}\`,
";
    done;
    fn_rftt_reset_mysql_table_field_hql="${fn_rftt_reset_mysql_table_field_hql:0:${#fn_rftt_reset_mysql_table_field_hql}-2}";
    fn_rftt_reset_mysql_table_field_hql+="
;";

    #执行撤销
    echo "${fn_rftt_reset_hive_table_field_hql}";
    echo "${fn_rftt_reset_mysql_table_field_hql}";

    ${toolpath}/connect-base-hive-server-hql.sh "${fn_rftt_reset_hive_table_field_hql}";
    ${toolpath}/connet-mysql-server.sh "${source_db_type}" "${fn_rftt_reset_mysql_table_field_hql}";

}
 



if [ ${is_reset} -eq 1 ];then
    #撤销修改的表结构
    resetFieldsToTableFn;
else
    #备份数据
    bakSourceTableFn

    #如果填写了追加字段则，走添加流程流程
    if [ -n "${fields_info}" ];then
        addFieldsToTableFn;
    fi
fi








