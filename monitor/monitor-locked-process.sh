#!/bin/bash

#监控长时间job
#1、超过1小时发邮件警告
#2、超过3个小时kill掉进程


#当前shell文件执行路径
basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

#引入配置文件
source ${basepath}/../conf/conf.sh;

hadoop_bin=${SYSTEM_HADOOP_BIN};

jobs_line=`$hadoop_bin/hadoop job -list | grep ${SYSTEM_HADOOP_USER}`

#邮件内容
mail_body+="";

count=`echo "$jobs_line"|wc -l`

for (( i=1; i<=$count; i++)); do
    line=`echo "$jobs_line" | sed -n "${i}p"`
    echo "${line}"; #每一行内容
    #只获得job行
    if [[ "${line}" =~ ^job_ ]];then
	array=(${line});
	job_id=${array[0]}
        #job运行开始时间
	start_stamp=${array[2]:0:10}
	#当前时间
	now_stamp=`date +%s`
	#运行时长
	run_duration=`expr $now_stamp - $start_stamp`
	warning_seconds=3600
	killing_seconds=`expr 3600 \* 3`

	#严重超时，没人来处理，杀死
	if [ $run_duration -gt $killing_seconds ];then
	    mail_body+="<div style=color:red>【Job be killed!】JOB_ID : $job_id!  DURATION : ${run_duration} seconds!</div>\n";
	    #杀死无人处理的进程
	    $hadoop_bin/hadoop job -kill $job_id
	#超时，发警告邮件
	elif [ $run_duration -gt $warning_seconds ];then
	    mail_body+="<div style=color:orange>【Job timeout!】JOB_ID : $job_id!  DURATION : ${run_duration} seconds!</div>\n";
	fi

    fi
done


if [ "$mail_body" ];then
    echo -e [`date`] "\n$mail_body"
    mail_title="【!重要】job运行超时"
    ${toolpath}/send-mail.sh "$mail_title" "$mail_body"

else
    echo -e [`date`] "\n ok \n"
fi
