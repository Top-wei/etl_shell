#!/bin/bash

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

input_date=$1;
yesterday=`date -d yesterday +"%Y%m%d"`
m_date=${input_date:-${yesterday}};


log_types='uba_web_visit_log access_log uba_app_action_log uba_web_action_log';

for type in $log_types
do


    create_db_table_hql+="
USE ${type};
CREATE TABLE IF NOT EXISTS ${type}_${m_date} like ${type}_model;
";

done


${toolpath}/connect-hive-server-hql.sh "$create_db_table_hql";