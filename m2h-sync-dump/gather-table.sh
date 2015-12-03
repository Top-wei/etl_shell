#!/bin/bash
#统计快照库，按照每天日期，放到聚合表中

# 调用方式: ./gather-table.sh [hive数据库.表名] [日期] [导入 gather 库方式 (query_load:hive 查出数据，写入文件，上传到 load hive 中,  file_load：查询 hive table 写入到本地文档，再上传到 hive table 中)]
# 如：./gather-table.sh  test.test_check 20150410 （表示这张表 test__test_check_20150415）

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

tmp_file_dir="${basepath}/../.tmp";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;


#hive 数据库配置
hive_import=$1;
hive_test="xxx.xxx";
hive_info=${hive_import:-${hive_test}};
hive_info_arr=(${hive_info//./ });
hive_database=${hive_info_arr[0]};
hive_table=${hive_info_arr[1]};


#日期
last_date=$(date -d last-day +%Y%m%d); #默认是昨天的日期
import_date=$2;
m_date=${import_date:-${last_date}};
new_m_date=`date -d $m_date +%Y-%m-%d`;


#导入方式,默认使用查询原表方式导入
load_data_type=${3:-query_load};


#hive 数据和表的分隔符 如 db_anme__tb_name;
hive_separator="__";

#db 数据配置
hive_database_sync_name=${M2H_SYNC_HIVE_DATABASE_SYNC_NAME};
hive_database_gather_name=${M2H_SYNC_HIVE_DATABASE_GATHER_NAME};

#gather 聚合表
gather_prefix="";
gather_table="${gather_prefix}${hive_database}${hive_separator}${hive_table}";
gather_db_and_table="${hive_database_gather_name}.${gather_table}";

#sync 源表规则, [db-name][分隔符][表名][_日期]
sync_table="${hive_database}${hive_separator}${hive_table}";
sync_db_and_table="${hive_database_sync_name}.${sync_table}";


#hive 结果文件存放目录
hive_query_result_file="${M2H_SYNC_HIVE_QUERY_RESULT_FILE_DIR}/${sync_db_and_table}";

fields_terminated_by=${fields_terminated_by:-"001"};
fields_terminated_by="\\${fields_terminated_by}";

#验证当前聚合表是否存在
function checkGatherTableFn () {
    #验证
    fn_cgt_result=$(${toolpath}/check-table-is-exists.sh ${hive_database_gather_name} ${gather_table}); 

    echo ${fn_cgt_result};
}

#创建一张新的汇总表
function createGatherTableFn () {

    #获取指定快照表字段
    fn_cgt_c2_sync_table_fields=$(${toolpath}/get-table-fields.sh ${hive_database_sync_name} ${sync_table} 0);

    #格式化创建汇总表的字段类型都是 string
    fn_cgt_c2_sync_format_table_fields=`${toolpath}/implode.sh " String," ${fn_cgt_c2_sync_table_fields}`;

    #hql语句
    fn_cgt_cr_hql="
CREATE TABLE IF NOT EXISTS ${gather_db_and_table} (
    ${fn_cgt_c2_sync_format_table_fields}
) PARTITIONED BY  (
    p_dt String
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${fields_terminated_by}'
COLLECTION ITEMS TERMINATED BY '\n';

INSERT OVERWRITE TABLE 
    ${gather_db_and_table} 
PARTITION (
    p_dt = '${new_m_date}'
)
SELECT  
    *
FROM
    ${sync_db_and_table};
";

    echo "--------------- 初始化汇总表 -> ${gather_db_and_table} ---------------";

    #执行创建
    ${toolpath}/connect-base-hive-server-hql.sh  "${fn_cgt_cr_hql}";
}


#验证聚合表 和 同步表的结构 是否有增加
function checkGatherTableAndSyncTableFieldFn () {
    fn_cgtastf_tmpfile="${tmp_file_dir}/gather_table_tmp_file_cgtastf";
    echo "" > ${fn_cgtastf_tmpfile};

    #快照表结构
    fn_cgtastf_sync_fields=$(${toolpath}/get-table-fields.sh ${hive_database_sync_name} ${sync_table} 0);
    fn_cgtastf_format_sync_fields=${fn_cgtastf_sync_fields// /,};

    #聚合表结构
    fn_cgtastf_gather_fields=$(${toolpath}/get-table-fields.sh ${hive_database_gather_name} ${gather_table} 0);
    fn_cgtastf_format_gather_fields=${fn_cgtastf_gather_fields// /,};

    #获取聚合表少的字段
    for now_sync in ${fn_cgtastf_sync_fields}; do
        fn_cgtastf_is_exist=$(${toolpath}/in-array.sh "${now_sync}" "${fn_cgtastf_format_gather_fields}");
        if [ $fn_cgtastf_is_exist -eq 0 ];then
            echo ${now_sync} >> ${fn_cgtastf_tmpfile};
        fi
    done

    fn_cgtastf_gather_not_fileds=`cat ${fn_cgtastf_tmpfile}`;

    echo ${fn_cgtastf_gather_not_fileds};
}


#给聚合表添加 缺少的 快照表字段
function addGatherDiffSyncFn () {
    #当前聚合表没有的字段
    fn_agds_not_field=$@;

    #组合添加字段的 hql 语句
    fn_agds_add_field_hql="
USE ${hive_database_gather_name};

";
    #组合添加的 hql 语句
    for fn_agds_now_field in $fn_agds_not_field;
    do 
        fn_agds_add_field_hql+="
ALTER TABLE ${gather_table} ADD COLUMNS(${fn_agds_now_field} String COMMENT '${new_m_date} add');
";
    done;

    #执行添加字段
    echo "--------------- 聚合表缺少字段 -> ${fn_agds_not_field} ,添加中 ---------------";

    ${toolpath}/connect-base-hive-server-hql.sh  "${fn_agds_add_field_hql}";

}


#看参数注释
function formatToGatherDbFn() {

    #快照表结构
    fn_qltg_sync_fields=$(${toolpath}/get-table-fields.sh ${hive_database_sync_name} ${sync_table} 0);
    fn_qltg_format_sync_fields=${fn_qltg_sync_fields// /,};

    #聚合表结构
    fn_qltg_gather_fields=$(${toolpath}/get-table-fields.sh ${hive_database_gather_name} ${gather_table} 0);
    fn_qltg_format_gather_fields=${fn_qltg_gather_fields// /,};

fn_qltg_select_fields="";
    for fn_qltg_now_gather_field in $fn_qltg_gather_fields;
    do
        if [ ${fn_qltg_now_gather_field} = "p_dt" ];then
            continue;
        fi
        
        #验证字段
        fn_qltg_is_exist=$(${toolpath}/in-array.sh "${fn_qltg_now_gather_field}" "${fn_qltg_format_sync_fields}");
        if [ $fn_qltg_is_exist -eq 1 ];then

            fn_qltg_select_fields+="${fn_qltg_now_gather_field},";

        else

            fn_qltg_select_fields+="'' as ${fn_qltg_now_gather_field},";
        fi
    done;

#去除结尾逗号
fn_qltg_format_select_fields=${fn_qltg_select_fields:0:${#fn_qltg_select_fields}-1};


    if [ ${load_data_type} = query_load ];then

        fn_qltg_run_hql="
INSERT OVERWRITE TABLE 
    ${gather_db_and_table} 
PARTITION (
    p_dt = '${new_m_date}'
) 
SELECT 
    ${fn_qltg_format_select_fields}
FROM
    ${sync_db_and_table}
;
";

    elif [ ${load_data_type} = file_load ];then
        
        #抽取数据写入到
        fn_qltg_query_data_hql="
SELECT 
    ${fn_qltg_format_select_fields}
FROM
    ${sync_db_and_table}
;
";

        #处理换行
        fn_qltg_regexp+="s/[\n|\r\n]//g;";
        #处理 NULL 字符串
        fn_qltg_regexp+="s/NULL/\\\N/g;";
        #处理分隔符 
        fn_qltg_regexp+="s/\t/$(echo -e ${fields_terminated_by})/g;";

        echo "${fn_qltg_query_data_hql}";
        ${toolpath}/connect-base-hive-server-hql.sh "${fn_qltg_query_data_hql}" | sed -e "${fn_qltg_regexp}" > ${hive_query_result_file};

        fn_qltg_run_hql+="
-- 上传文件到 hive table 中
LOAD DATA LOCAL INPATH '${hive_query_result_file}' OVERWRITE INTO TABLE ${gather_db_and_table} PARTITION (p_dt='${new_m_date}');
";

    fi

    echo "--------------- 导入到聚合表 --> ${gather_db_and_table} ---------------";
    echo "${fn_qltg_run_hql}";

    ${toolpath}/connect-base-hive-server-hql.sh "${fn_qltg_run_hql}";
}


#验证聚合表是否存在
is_gather=`checkGatherTableFn`;


if [ ${is_gather} -eq 1 ]; then

    #验证表结构
    checkt_gather_and_sync_result=$(checkGatherTableAndSyncTableFieldFn);

    #有变动
    if [ -z ${checkt_gather_and_sync_result} ];then

        echo "--------------- 字段无增加 ---------------";
        formatToGatherDbFn;

    #有变动
    else
        echo "--------------- 字段有增加，追加字段中... ${checkt_gather_and_sync_result} ---------------";
        addGatherDiffSyncFn ${checkt_gather_and_sync_result};

        formatToGatherDbFn;
    fi

else
    #创建新的聚合表
    createGatherTableFn;
fi







