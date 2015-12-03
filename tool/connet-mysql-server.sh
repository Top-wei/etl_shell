#!/bin/bash

# 连接 Mysql 执行 sql
# 调用 ./connet-mysql-server.sh [dw|slave] [sql]

basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

toolpath="${basepath}/../tool";

tmp_dir="${basepath}/../.tmp";


#引入配置文件
source ${confpath}/conf.sh;


source_db_type=${1:-slave};
if [ $source_db_type = slave ];then

    #数据源配置
    mysql_host=${SYSTEM_SLAVE_MYSQL_HOST};
    mysql_user=${SYSTEM_SLAVE_MYSQL_USER};
    mysql_password=${SYSTEM_SLAVE_MYSQL_PASSWORD};

elif [ $source_db_type = dw ];then

    #数据源配置
    mysql_host=${SYSTEM_DW_MYSQL_HOST};
    mysql_user=${SYSTEM_DW_MYSQL_USER};
    mysql_password=${SYSTEM_DW_MYSQL_PASSWORD};

fi


mysql_sql=${2:-show databases;};


${SYSTEM_MYSQL_BIN}/mysql -h"${mysql_host}" -u"${mysql_user}" -p"${mysql_password}" -N -s -e "${mysql_sql}";