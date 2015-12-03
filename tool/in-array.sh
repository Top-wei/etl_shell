#!/bin/bash
#验证是否包含数组 
#调用方式 in-array [如: aaa ] [如：aaa,bbbb,ccc] ，变成这种格式的可以用  ${aaa bbb cc// /,} 格式化一下;

#搜索的值
search_val=$1;

#验证的数组
check_arr_import=$2;
check_arr=${check_arr_import//,/ };

result=0;

for now_val in ${check_arr}; do
    #echo ${now_val};
    if [ $search_val = $now_val ];then
        result=1;
        break;
    fi

done

echo $result;