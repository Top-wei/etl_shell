#!/bin/bash

#读取配置文件
#调用: source ${toolpath}/get-conf.sh "配置文件"; 
#访问: 如 echo ${变量名}

conf_file=${1};

if [ -e $conf_file ];then

    while read line;
    do
        if [ -n "$line" ];then
            line_conf=(${line//=/ });

            eval "${line_conf[0]}=${line_conf[1]}";
        fi

    done < $conf_file;

else
    echo "conf file not find";
    exit 0;
fi