#!/bin/bash
#转换数组为拼接字符串
#调用方式 implode.sh [拼接字符串  如：,] [数组  如：aaa bbb ccc ddd];



#参数个数
parameter_num=$#;
#所有参数
parameter_arr=($@);

glue=$1;


i=0;

while [ ${i} -lt ${parameter_num} ];
do

    if [ ${i} -gt 0 ];then 
        merge_string+="${parameter_arr[$i]}${glue}"; 
    fi

    #echo ${i};
    #echo ${parameter_arr[$i]}

    i=$(($i+1));

done

#glue_length=${#glue};
merge_string_length=${#merge_string};

echo ${merge_string:0:merge_string_length-1};



