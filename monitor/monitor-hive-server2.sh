#!/bin/bash
# 监控 hiveServer2 状态，不可用时自动重启

#当前脚本进程 PID
export master_run_pid=$$;

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

source ${confpath}/conf.sh;


if [ ${system_env}  = offline ];then

    ssh_command="sudo touch /tmp/aaaaa.txt";

elif [ ${system_env} = online ];then

    ssh_command="sudo /etc/init.d/hive-server2 restart";

fi



ssh_ip=${MONITOR_DW_HADOOP_IP};
ssh_account=${MONITOR_DW_HADOOP_ACCOUNT};
ssh_port=${MONITOR_DW_HADOOP_PORT};


#当前脚本执行时间
now_date=$(date +"%Y-%m-%d %H:%M:%S");

#测试命令
test_connect_command="${toolpath}/connect-hive-server-hql.sh 'show databases;'";

#超时时间，默认 60 秒
timeouts=${1:-60};


function monitoringHiveServer2Fn () {

    fn_mhs2_hql_run=$(${toolpath}/connect-hive-server-hql.sh "show databases;");

    fn_mhs2_format_result=`echo "${fn_mhs2_hql_run}" | grep "database_name"`;

    #成功
    if [ -n "${fn_mhs2_format_result}" ];then
        echo 1;

    #失败
    else
        echo 0;
    fi

}

function errorActionFn () {
    echo "${now_date} hiveServer2 error";
    ssh -t -p ${ssh_port} ${ssh_account}@${ssh_ip} ${ssh_command};
    #${toolpath}/send-mail.sh "<div style=color:red;>${now_date} hive Server 2 挂了，已重启</div>";
}

# 检测是是否超时
is_timeout=`${toolpath}/timeout.sh "${test_connect_command}" "${timeouts}"`;


# 正常
if [ $is_timeout -eq 1 ];then

    is_return_info=`monitoringHiveServer2Fn`;
    if [ $is_return_info -eq 1 ];then
        echo "${now_date} hiveServer2 success";
    else
        errorActionFn;
    fi

# 超时
else
    #超时重启
    errorActionFn;
fi
