#!/bin/bash

#发送系统邮件

#调用方式 send-mail.sh title[标题] content[邮件内容] date[日期] 

basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

#配置文件
source ${confpath}/conf.sh;

if [ ${system_env} = "offline" ]; then
    type="offline";
elif [ ${system_env} = "online" ]; then
    type="online"
fi

#邮件标题
title="${1}";

#邮件内容
content="${2}";

#日期
import_date=$3;
date=$(date -d last-day +%Y%m%d);
m_date=${import_date:-${date}};



#发给 dl-bi@angejia.com
#echo -e "${content}" | ${SYSTEM_MUTT_BIN}/mutt -s "<${title}>-<${m_date}>-<${type}>" -e 'set content_type="text/html"' jason@angejia.com -c dl-bi@angejia.com;
echo -e "${content}" | ${SYSTEM_MUTT_BIN}/mutt -s "<${title}>-<${m_date}>-<${type}>" -e 'set content_type="text/html"' jason@angejia.com -c dl-bi@angejia.com;