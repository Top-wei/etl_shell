#!/bin/bash
# 重跑脚本 ，重跑一个时间段脚本
# 使用方法：
# 参数  [脚本地址] (其中 {date} 表示日期出现在脚本的位置,脚本运行时会被替换成指定日期)
#       [开始日期] (20150601) 
#       [结束日期] (20150605) 如果不传入结束时间，则表示一直跑到今天为止

# 案例  ./reset-run "/home/hadoop/app/jason-uba/scripts/shell/uba/run.sh {date}"  20150601 20150605


basepath=$(cd `dirname $0`; pwd);

confpath="${basepath}/../conf";

#引入配置文件
source ${confpath}/conf.sh;


function resetRunFn () {

    #脚本路径
    shell_dir=${1};

    #开始时间
    start_time=${2};

    #结束日期
    over_time=${3};

    #转换日期为时间戳
    start_time_timestamp=$(date -d "${start_time}" +"%s");
    over_time_timestamp=$(date -d "${over_time}" +"%s");

    diff_date=$((($over_time_timestamp-$start_time_timestamp)/86400)); 


    for((i=0; i<=$diff_date; i++));
    do

        now_date=`date -d "${start_time} +${i} day " +%Y%m%d`;

        /bin/bash ${shell_dir/"{date}"/"${now_date}"}

    done;
} 


resetRunFn "${1}" "${2}" "${3}";

