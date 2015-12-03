#!/bin/bash

#解析 shell 后的参数，成为变量
#1、书写方式：如 : xxx.shell mysql_user=123 mysql_password=456
#2、xxx.shell 中调用本 shell,如： source ./shell-parameter.sh ${@}; 
#3、xxx.shell 使用， echo ${mysql_user}

#参数个数
parameter_num=$#;
#所有参数
parameter_arr=($@);

for (( i=0; i<${parameter_num}; i++)); do
    eval "${parameter_arr[$i]}";
done;