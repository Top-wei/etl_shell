#!/bin/bash
#把 hive 导入到 mysql

#调用方式 hive-to-mysql.sh date[日期,可选,20150320]

#文档:http://git.corp.angejia.com/dw/uba/blob/master/docs/design/hive-to-mysql.md
#作者jason@angejia.com

basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

tmp_dir="${basepath}/../.tmp";

java_outdir=${tmp_dir};

#引入配置文件
source ${confpath}/conf.sh;


#日期
import_date=$1;
date=$(date -d last-day +%Y%m%d);
m_date=${import_date:-${date}};

format_date=`date -d $m_date +%Y-%m-%d`;

#DW 数据库配置
mysql_host=${SYSTEM_DW_MYSQL_HOST};
mysql_user=${SYSTEM_DW_MYSQL_USER};
mysql_password=${SYSTEM_DW_MYSQL_PASSWORD};
mysql_database="dw_stage";

hadoop_uba_web_visit_dir="/${SYSTEM_HADOOP_USER}/hive/dw_stage";

hadoop_access_log_dir="/${SYSTEM_HADOOP_USER}/hive/dw_stage";


#******开始处理*******

#处理 uba_web_visit_log
ubaWebVisitLog () {

     #mysql 表配置
     uba_web_visit_table="dw_web_visit_traffic_log";  #写数据的表
     
     #hadood hive table 目标结表分区后的${m_date}日期文件存放目录
     hadoop_uba_web_visit_log_table_dir="${hadoop_uba_web_visit_dir}/${uba_web_visit_table}/p_dt=${format_date}";


#创建 mysql 表
mysql -h${mysql_host} -u${mysql_user} -p${mysql_password} <<EOF

use ${mysql_database};

create table if not exists ${uba_web_visit_table} (
    user_id varchar(255) DEFAULT '',
    selection_city_id varchar(255) DEFAULT '',
    client_time varchar(255) DEFAULT '',
    user_based_city_id varchar(255) DEFAULT '',
    referer_full_url varchar(2500) DEFAULT '',
    referer_page varchar(2500) DEFAULT '',
    referer_page_id varchar(255) DEFAULT '',
    referer_page_name varchar(255) DEFAULT '',
    current_full_url varchar(2500) DEFAULT '',
    current_page varchar(255) DEFAULT '',
    current_page_id varchar(255) DEFAULT '',
    current_page_name varchar(255) DEFAULT '',
    channel_code varchar(255) DEFAULT '',
    page_param varchar(255) DEFAULT '',
    client_param varchar(255) DEFAULT '',
    guid varchar(255) DEFAULT '',
    client_ip varchar(255) DEFAULT '',
    os_type varchar(255) DEFAULT '',
    os_version varchar(255) DEFAULT '',
    brower_type varchar(255) DEFAULT '',
    brower_version varchar(255) DEFAULT '',
    phone_type varchar(255) DEFAULT '',
    server_time varchar(255) DEFAULT '',
    key idx_st (server_time(10)) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


EOF
    rm ${java_outdir}/${uba_web_visit_table}.java;
    echo "--------------- uba_web_visit_log 导入到 mysql --------------- ";
    #执行 hive to mysql
    ${SYSTEM_SQOOP_BIN}/sqoop export -connect "jdbc:mysql://${mysql_host}/${mysql_database}?useUnicode=true&characterEncoding=utf-8" -username ${mysql_user} -password ${mysql_password} -table ${uba_web_visit_table} -export-dir ${hadoop_uba_web_visit_log_table_dir} -fields-terminated-by '\001' -input-null-string '\\N' -input-null-non-string '\\N' -outdir ${java_outdir};
}


#处理 access_log
accessLog () {

     #mysql 表配置
     access_log_table="dw_app_access_log";  #写数据的表
     access_log_template="access_log_template";  #模板表，需要提前在 mysql 创建好

    #hadoop 目标结表的目录
     hadoop_access_log_table_dir="${hadoop_access_log_dir}/${access_log_table}/p_dt=${format_date}";

    
#创建 mysql 表
mysql -h${mysql_host} -u${mysql_user} -p${mysql_password} <<EOF

use ${mysql_database};

create table if not exists ${access_log_table} (
    app_name            varchar(255) DEFAULT '',
    app_version         varchar(255) DEFAULT '',
    selection_city_id   varchar(255) DEFAULT '',
    location_city_id    varchar(255) DEFAULT '',
    client_ip           varchar(255) DEFAULT '',
    user_id             varchar(255) DEFAULT '',
    network_type        varchar(255) DEFAULT '',
    platform            varchar(255) DEFAULT '',
    device_type         varchar(255) DEFAULT '',
    os_version          varchar(255) DEFAULT '',
    device_id           varchar(255) DEFAULT '',
    delivery_channels   varchar(255) DEFAULT '',
    channel_name        varchar(255) DEFAULT '',
    hostname            varchar(2500) DEFAULT '',
    request_uri         varchar(2500) DEFAULT '',
    server_date         varchar(255) DEFAULT '',
    server_time         varchar(255) DEFAULT '',
    request_page_id     varchar(255) DEFAULT '',
    request_page_name   varchar(255) DEFAULT '',
    longitude           varchar(255) DEFAULT '',
    latitude            varchar(255) DEFAULT '',
    key idx_sd (server_date(10))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

EOF
    rm ${java_outdir}/${access_log_table}.java;
    echo "--------------- access_log 导入到 mysql --------------- ";
    #执行 hive to mysql
    ${SYSTEM_SQOOP_BIN}/sqoop export -connect "jdbc:mysql://${mysql_host}/${mysql_database}?useUnicode=true&characterEncoding=utf-8" -username ${mysql_user} -password ${mysql_password} -table ${access_log_table} -export-dir ${hadoop_access_log_table_dir} -fields-terminated-by '\001' -input-null-string '\\N' -input-null-non-string '\\N' -outdir ${java_outdir};
}


ubaWebVisitLog;

accessLog;

exit 0;

