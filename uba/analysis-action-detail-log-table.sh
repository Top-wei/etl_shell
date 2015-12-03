#!/bin/bash

# 生成 dw_web_action_detail_log (web 动作日志,根据 uba_web_action_log_xxxxxx 获取)
# 生成 dw_app_action_detail_log (app 动作日志,根据 uba_app_action_log_xxxxxx 获取)

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
uba_web_visit_jar=${UBA_WEB_VISIT_JAR};

#用到的数据库配置
web_action_database=${UBA_HIVE_WEB_ACTION_DATABASE};
app_action_database=${UBA_HIVE_APP_ACTION_DATABASE};
dimension_database=${UBA_HIVE_DW_DB_DATABASE};

#detail_database=${UBA_HIVE_DW_DATABASE};

detail_database=${UBA_HIVE_DW_DB_DATABASE};

#维表
action_dimension_table="${dimension_database}.dw_basis_dimen_action_id_name_lkp";
# action_page_dimension_table="${dimension_database}.dw_basis_dimen_action_page_lkp";

# 处理 web_action 日志
webActionLogFn () {

  # hive web_action_log 表配置
  web_action_source_table="${web_action_database}.uba_web_action_log_${m_date}";

  #过滤IP维表
  hive_dimension_filter_ip_table="${dimension_database}.dw_basis_dimension_filter_ip";

  #hive 最终结果表
  web_action_result_table="${detail_database}.dw_web_action_detail_log";

  #add jar
  uba_web_action_hql+="
  add jar ${uba_web_visit_jar};
  ";

  #创建 hive 表语句#
  uba_web_action_hql+="
    CREATE  TABLE if not exists ${web_action_result_table}
    (
      user_id            string,
      ccid               string,
      referer_full_url   string,
      referer_page_id    string,
      referer_page       string,
      referer_page_name  string,
      current_full_url   string,
      current_page       string,
      current_page_id    string,
      current_page_name  string,
      guid               string,
      client_time        string,
      page_param         string,
      action_id          string,
      action_name        string,
      action_cname       string,
      client_param       string,
      server_time        string,
      ip                 string,
      os_type            string,
      os_version         string,
      brower_type        string,
      brower_version     string,
      phone_type         string
    )
    partitioned by (p_dt string);

    create temporary function parse_user_agent as 'com.angejia.hadoop.hive_udf.ParseUserAgent';
    create temporary function get_page_info as 'com.angejia.hadoop.hive_udf.CalculatePageInfo';

    INSERT overwrite TABLE ${web_action_result_table}
    partition
      (
        p_dt='`date -d $m_date +%Y-%m-%d`'
      )
    select

      if(length(a.uid)>0,uid,0) AS user_id,
      a.ccid as selection_city_id,
      if(length(a.referer)>0,referer,'') as referer_full_url,
      get_page_info(a.referer,'page_id') as referer_page_id,
      coalesce(parse_url(a.referer,'PATH'),'') as referer_page,
      get_page_info(a.referer,'page_name') as referer_page_name,
      if(length(a.url)>0,url,'') as current_full_url,
      coalesce(parse_url(a.url,'PATH'),'') as current_page,
      get_page_info(a.url,'page_id') as current_page_id,
      get_page_info(a.url,'page_name') as current_page_name,
      a.guid as guid,
      a.client_time as client_time,
      a.page_param as page_param,
      b.action_id as action_id,
      b.action_name as action_name,
      b.action_cname as action_cname,
      a.client_param as client_param,
      a.server_time as server_time,
      a.ip as client_ip,
      parse_user_agent(a.agent,0) as os_type,
      parse_user_agent(a.agent,1) as os_version,
      parse_user_agent(a.agent,2) as brower_type,
      parse_user_agent(a.agent,3) as brower_version,
      parse_user_agent(a.agent,4) as phone_type

    from
        ${web_action_source_table} a
   left outer join ${action_dimension_table} b
    on a.action = b.action_cname
   left outer join ${hive_dimension_filter_ip_table} c
     on a.ip = c.client_ip
  where c.client_ip is null
    and b.flag=1;
  ";

  #执行
  ${toolpath}/connect-hive-server-hql.sh "${uba_web_action_hql}";

}

# 处理 app_action 日志
appActionLogFn () {

  # hive app_action_log 表配置
  app_action_source_table="${app_action_database}.uba_app_action_log_${m_date}";
    
  #过滤IP维表
  hive_dimension_filter_ip_table="${dimension_database}.dw_basis_dimension_filter_ip";

  #hive 最终结果表
  app_action_result_table="${detail_database}.dw_app_action_detail_log";

  #jir hql
  uba_app_action_hql+="
  add jar ${uba_web_visit_jar};
  ";

  #创建 hive 表语句#
  uba_app_action_hql+="
  CREATE  TABLE if not exists ${app_action_result_table}
  (
     mac                string,
     dvid               string,
     model              string,
     os                 string,
     name               string,
     channel            string,
     version            string,
     uid                string,
     net                string,
     ip                 string,
     ccid               string,
     gcid               string,
     longtitude         string,
     latitude           string,
     action_id          string,
     action_name        string,
     action_cname       string,
     current_page_id    string,
     current_page_name  string,
     current_page_cname string,
     click_time         string,
     extend             string,
     bp_id              string,
     bp_name            string,
     server_time        string,
     client_ip          string
  )
  partitioned by (p_dt string);

  INSERT overwrite TABLE ${app_action_result_table}
  partition
    (
      p_dt='`date -d $m_date +%Y-%m-%d`'
    )
  select

   a.mac,
   a.dvid,
   a.model,
   a.os,
   a.name,
   a.ch AS channel,
   a.ver AS version,
   if(length(a.uid)>0,uid,0) AS uid,
   a.net,
   a.ip,
   a.ccid,
   a.gcid,
   split(a.geo,'-')[0] AS longtitude,
   split(a.geo,'-')[1] AS latitude,
   a.action AS action_id,
   b.action_name,
   b.action_cname,
   concat(substr(a.action,1,6),'00') AS currnet_page_id,
   c.action_name AS current_page_name,
   c.action_cname AS current_page_cname,
   a.click_time,
   a.extend,
   get_json_object(a.extend,'$.bp') AS bp_id,
   d.action_name AS bp_name,
   a.server_time,
   a.client_ip

  from
    ${app_action_source_table} a
  left outer join ${action_dimension_table} b
    on a.action=b.action_id
  left outer join ${action_dimension_table} c
    on concat(substr(a.action,1,6),'00') = c.action_id
  left outer join ${action_dimension_table} d
    on get_json_object(a.extend,'$.bp') = d.action_id
  left outer join ${hive_dimension_filter_ip_table} e
     on a.client_ip = e.client_ip
  where b.flag=0
    and c.flag=0
    and e.client_ip is null
  ;
 ";


 #执行
 ${toolpath}/connect-hive-server-hql.sh "${uba_app_action_hql}";

}

webActionLogFn;

appActionLogFn;
