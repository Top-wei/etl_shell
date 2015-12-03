#!/bin/bash

# 生成 dw_web_visit_traffic_log (web 用户访问行为 log，根据 uba_web_visit_log_xxxxxx 获取)
# 生成 dw_app_access_log (nginx 访问日志，根据 access_log_xxxxxx 获取)

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;

#临时存放文件目录
tmp_file_dir="${basepath}/../.tmp";


#日期
import_date=$1;
yesterday=$(date -d yesterday +%Y%m%d);
m_date=${import_date:-${yesterday}};


#jars 配置

access_log_jar=${UBA_ACCESS_LOG_JAR};
access_log_token_jar=${UBA_ACCESS_LOG_TOKEN_JAR};

#用到的数据库配置
hive_uba_database=${UBA_HIVE_UBA_DATABASE};
hive_access_log_database=${UBA_HIVE_ACCESS_LOG_DATABASE};
#hive_dw_database=${UBA_HIVE_DW_DATABASE};
hive_dw_db_database=${UBA_HIVE_DW_DB_DATABASE};


#******开始处理*******

ubaWebVisitLog () {

    # hive uba_web_visit_log 表配置
    hive_uba_web_visit_source_table="${hive_uba_database}.uba_web_visit_log_${m_date}";  #hive uba_web_visit 数据源表

    hive_dimension_filter_ip_table="${hive_dw_db_database}.dw_basis_dimension_filter_ip";#过滤IP维表

    hive_dimension_filter_agent_table="${hive_dw_db_database}.dw_basis_dimension_filter_agent";#过滤agent维表

   #hive_dw_uba_web_visit_result_table="${hive_dw_database}.dw_web_visit_traffic_log";  #hive 最终结果表

    hive_dw_uba_web_visit_result_table="${hive_dw_db_database}.dw_web_visit_traffic_log";  #hive 最终结果表

    #创建 hive 表语句#
uba_web_visit_hql="

CREATE  TABLE if not exists ${hive_dw_uba_web_visit_result_table} (
  user_id   string,
  selection_city_id   string,
  client_time   string,
  user_based_city_id   string,
  referer_full_url   string,
  referer_page   string,
  referer_page_id   string,
  referer_page_name   string,
  current_full_url   string,
  current_page   string,
  current_page_id   string,
  current_page_name   string,
  channel_code   string,
  page_param   string,
  client_param   string,
  guid   string,
  client_ip   string,
  os_type   string,
  os_version   string,
  brower_type   string,
  brower_version   string,
  phone_type   string,
  server_time   string)
partitioned by (p_dt string);

create temporary function parse_user_agent as 'com.angejia.hive.udf.useragent.ParseUserAgent';
create temporary function get_page_info as 'com.angejia.hive.udf.pageinfo.CalculatePageInfo';

INSERT overwrite TABLE
    ${hive_dw_uba_web_visit_result_table}
partition(
    p_dt='`date -d $m_date +%Y-%m-%d`'
)
select
    if(length(a.uid)>0,uid,0) AS user_id,
    a.ccid as selection_city_id,
    a.client_time as client_time,
    '' as user_based_city_id,
    if(length(a.referer)>0,referer,'') as referer_full_url,
    coalesce(parse_url(a.referer,'PATH'),'') as referer_page,
    get_page_info(a.referer,'page_id') as referer_page_id,
    get_page_info(a.referer,'page_name') as referer_page_name,
    if(length(a.url)>0,url,'') as current_full_url,
    coalesce(parse_url(a.url,'PATH'),'') as current_page,
    get_page_info(a.url,'page_id') as current_page_id,
    get_page_info(a.url,'page_name') as current_page_name,
    get_page_info(a.url,'platform_id') as channel_code,
    a.page_param as page_param,
    a.client_param as client_param,
    a.guid as guid,
    a.ip as client_ip,
    parse_user_agent(a.agent,0) as os_type,
    parse_user_agent(a.agent,1) as os_version,
    parse_user_agent(a.agent,2) as brower_type,
    parse_user_agent(a.agent,3) as brower_version,
    parse_user_agent(a.agent,4) as phone_type,
    a.server_time as server_time
from
    ${hive_uba_web_visit_source_table} a
left outer join ${hive_dimension_filter_ip_table} b
  on a.ip = b.client_ip
where b.client_ip is null
  and a.ip not like '61.135.190.%'
  and parse_user_agent(a.agent,2) != 'Robot/Spider'
  and a.agent not like '%spider%'
  and a.agent not like '%-broker%'
  and a.ip not like '10.%';
";

    #待执行 hql
    uba_web_visit_run_hql+="
    add jar ${CALCULATE_PAGEINFO_JAR};
    add jar ${PARSE_USER_AGENT_JAR};";
    uba_web_visit_run_hql+=${uba_web_visit_hql};

    #执行
    ${toolpath}/connect-hive-server-hql.sh "${uba_web_visit_run_hql}";

}

# 处理 accesslog 日志
accessLog () {

    # hive access_log 表配置
    hive_access_log_source_table="${hive_access_log_database}.access_log_${m_date}";  #hive access_log 数据源表

   #hive_dw_access_log_result_table="${hive_dw_database}.dw_app_access_log";  #hive 最终结果表

    hive_dw_access_log_result_table="${hive_dw_db_database}.dw_app_access_log";  #hive 最终结果表

    hive_dimension_filter_ip_table="${hive_dw_db_database}.dw_basis_dimension_filter_ip";#过滤IP维表

    hive_dimension_delivery_channels_table="${hive_dw_db_database}.dw_basis_dimension_delivery_channels_package";#渠道包维表

    #创建 hive 表语句#
access_log_hql="

CREATE  TABLE if not exists ${hive_dw_access_log_result_table} (
  app_name   string,
  app_version   string,
  selection_city_id   string,
  location_city_id   string,
  client_ip   string,
  user_id   string,
  network_type   string,
  platform   string,
  device_type   string,
  os_version   string,
  device_id   string,
  delivery_channels   string,
  channel_name string,
  hostname string,
  request_uri  string,
  server_date  string,
  server_time  string,
  request_page_id   string,
  request_page_name   string,
  longitude   string,
  latitude   string
)
partitioned by (p_dt string);

create temporary function parse_mobile_agent as 'com.angejia.hive.udf.parse.ParseMobileAgent';
create temporary function parse_mobile_token as 'com.angejia.hive.udf.parse.ParseMobileToken';
create temporary function get_page_info as 'com.angejia.hive.udf.pageinfo.CalculatePageInfo';

INSERT overwrite TABLE
    ${hive_dw_access_log_result_table}
partition(
   p_dt='`date -d $m_date +%Y-%m-%d`'
)

select
    parse_mobile_agent(a.mobile_agent,'app') as app_name,
    parse_mobile_agent(a.mobile_agent,'av') as app_version,
    parse_mobile_agent(a.mobile_agent,'ccid') as selection_city_id,
    parse_mobile_agent(a.mobile_agent,'gcid') as location_city_id,
    remote_addr as client_ip,
    coalesce(parse_mobile_token(auth,'user_id'),0) as user_id,
    parse_mobile_agent(a.mobile_agent,'net') as network_type,
    parse_mobile_agent(a.mobile_agent,'p') as platform,
    parse_mobile_agent(a.mobile_agent,'pm') as device_type,
    parse_mobile_agent(a.mobile_agent,'osv') as os_version,
    parse_mobile_agent(a.mobile_agent,'dvid') as device_id,
    parse_mobile_agent(a.mobile_agent,'ch') as delivery_channels,
    coalesce(c.channel_name,'') as channel_name,
    hostname as hostname,
    request_uri as request_uri,
    to_date(server_date) as server_date,
    concat(server_date,' ',server_time) as server_time,
    get_page_info(concat('http://',concat(hostname,request_uri)),'page_id') as request_page_id,
    get_page_info(concat('http://',concat(hostname,request_uri)),'page_name') as request_page_name,
    parse_mobile_agent(a.mobile_agent,'lng')as longitude,
    parse_mobile_agent(a.mobile_agent,'lat')as latitude
from
    ${hive_access_log_source_table} a
left outer join ${hive_dimension_filter_ip_table} b
    on a.remote_addr = b.client_ip
left outer join ${hive_dimension_delivery_channels_table} c
    on parse_mobile_agent(a.mobile_agent,'ch') = c.channel_package_code
where mobile_agent <> '-'
  and b.client_ip is null
;
";


    access_log_run_hql+="
add jar ${access_log_jar};
add jar ${access_log_token_jar};
add jar ${CALCULATE_PAGEINFO_JAR};
add jar ${PARSE_USER_AGENT_JAR};
";
    access_log_run_hql+="${access_log_hql}"

    #执行
    ${toolpath}/connect-hive-server-hql.sh "${access_log_run_hql}";

}

ubaWebVisitLog;

accessLog;
