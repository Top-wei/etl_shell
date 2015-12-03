#!/bin/bash

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

is_online="${basepath}/../ONLINE";

#线上
if [ -e $is_online ]; then
    #线上标识
    system_env=online
#线下
else
    #线下标志
    system_env=offline 
fi

source ${toolpath}/get-conf.sh "${basepath}/../conf/system-${system_env}.conf";

source ${toolpath}/get-conf.sh "${basepath}/../conf/uba-${system_env}.conf";

source ${toolpath}/get-conf.sh "${basepath}/../conf/m2h-sync-${system_env}.conf";

source ${toolpath}/get-conf.sh "${basepath}/../conf/monitor-${system_env}.conf";
