#!/bin/bash

#监控uba.php状态是否为200


#当前shell文件执行路径
basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

#引入配置文件
source ${basepath}/../conf/conf.sh;

test_url="http://s.angejia.com/uba?payload=web_visit&heartbeat=1&ray"
code=`curl -I -m 20 -o /dev/null -s -w %{http_code} "$test_url"`

echo -e [`date`] "\nCODE:$code"
if [ $code != 200 ];then
    mail_body="[`date`] <div style=color:red>INFO:请求20秒返回结果, URL:$test_url, CODE:$code</div>"
    mail_title="【!!重要】uba接收日志服务出错"
    ${toolpath}/send-mail.sh "$mail_title" "$mail_body"
fi
