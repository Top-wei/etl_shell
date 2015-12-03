#!/bin/bash

#获取表字段
#调用方法 get-table-fields.sh [db-name] [table-name] [是否显示字段那类型 可选 默认0]

basepath=$(cd `dirname $0`; pwd);


#数据库名称
db_name="$1";

#表名
table_name="$2";

#是否显示字段类型
show_filed_type_import="$3";
show_filed_type_default=0; 
show_filed_type=${show_filed_type_import:-${show_filed_type_default}};



function getTableFieldsFn () {
    
    fn_gtf_db_name="$1";
    fn_gtf_table_name="$2";

    #执行的sql
    fn_gtf_hql="
set hive.display.partition.cols.separately=false;
use ${fn_gtf_db_name};
desc ${fn_gtf_table_name};
";
    #去hive服务器执行
    fn_gtf_get_fields=$(${basepath}/connect-base-hive-server-hql.sh "${fn_gtf_hql}");

    fn_gtf_fields_count=`echo "${fn_gtf_get_fields}" | wc -l`;

    for (( i=1; i<=$fn_gtf_fields_count; i++)); do
        #读取每一行数据
        fn_gtf_now_fields_line=`echo "${fn_gtf_get_fields}" | sed -n "${i}p"`;

        fn_gtf_now_hive_col_1="$(echo ${fn_gtf_now_fields_line} | awk '{
            print $1;
        }')";

        #字段名称
        fn_gtf_now_hive_field_name="${fn_gtf_now_hive_col_1}";

        #字段类型
        fn_gtf_now_hive_field_type="$(echo ${fn_gtf_now_fields_line} | awk '{
            print $2;
        }')";

        if [ $show_filed_type -eq 0 ];then
             #格式如 : id system_classification channel_name channel_package_code create_time remark p_dt
            fn_gtf_format_hive_field+="${fn_gtf_now_hive_field_name} ";
        elif [ $show_filed_type -eq 1 ];then
            #格式如: id bigint, system_classification string, channel_name string, channel_package_code string, create_time string, remark string, p_dt string
            fn_gtf_format_hive_field+="${fn_gtf_now_hive_field_name} ${fn_gtf_now_hive_field_type},";
        elif [ $show_filed_type -eq 2 ];then
            #格式如: id-bigint system_classification-string channel_name-string channel_package_code-string create_time-string remark-string p_dt-string
            fn_gtf_format_hive_field+="${fn_gtf_now_hive_field_name}-${fn_gtf_now_hive_field_type} ";
        fi

    done;

    #去除结尾字符
    fn_gtf_format_hive_field=${fn_gtf_format_hive_field:0:${#fn_gtf_format_hive_field}-1};

    echo "${fn_gtf_format_hive_field}";
}

getTableFieldsFn $db_name $table_name;