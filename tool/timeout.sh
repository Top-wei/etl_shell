#!/bin/bash

# 监控脚本超时
# 调用方法 ./timeout.sh [脚本完整路径] [秒数] 
# return [1成功，没有超时 ， 0失败，超时]

shell_dir=$1;
timeout=$2;


function TimeoutFn () {

    #执行的脚本 $1,放到后台执行
    $1 > /dev/null  2>&1  &
    #$1 &

    #当前执行脚本的 pid 
    now_pid=$!;

    #等待 N 秒后检测
    sleep $2s;

    pid_num=`ps -aux | grep ${now_pid} | wc -l`

    #如果只有 1 表示执行成功，进程释放完毕
    if [ $pid_num -eq 1 ];then
        echo 1;

    #否则进程还没有成功释放，执行超时
    else
        #杀掉进程
        kill -15 $now_pid;
        echo 0;
    fi
}

TimeoutFn "$shell_dir" "$timeout";

