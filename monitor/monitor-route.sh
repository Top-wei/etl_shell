#!/bin/bash

#当前shell文件执行路径
basepath=$(cd `dirname $0`; pwd);

toolpath="${basepath}/../tool";

confpath="${basepath}/../conf";

tmp_dir="${basepath}/../.tmp";

system_bin_path="/usr/bin";

#引入配置文件
source ${confpath}/conf.sh;

#临时日志文件
angejia_route_file="${tmp_dir}/angejia_route_file";

#git 仓库地址
angejia_service_git_url=${MONITOR_ANGEJIA_SERVICE_GIT_URL};

#仓库本地存放目录
angejia_service_git_dir="${tmp_dir}/${MONITOR_ANGEJIA_SERVICE_GIT_NAME}";

#使用的分支
angejia_service_git_branch="${MONITOR_ANGEJIA_SERVICE_GIT_BRANCH}";

#WEB URL
angejia_service_git_web="${MONITOR_ANGEJIA_SERVICE_GIT_WEB}";

#git http 地址规则
angejia_service_git_web_rules="${angejia_service_git_web}/blob/${angejia_service_git_branch}";


#监控的仓库文件
angejia_git_routes=(
    #线下配置
    #aaa.md
    #bbb.md
    #线上配置
    app-site/app/Http/routes.php
    #app-web/app/Http/touchroutes.php
    app-web/app/Http/routes.php
    #经纪人
    app-mobi/app/Http/routes.php
    #用户
    app-mobi-member/app/Http/routes.php
);


#发送邮件
sendMailFn() {
    #当前时间
    now_date=$(date -d today +"%Y-%m-%d %H:%M:%S");

    #发送邮件
    ${toolpath}/send-mail.sh "route" "${1}" "${now_date}";
}


#获取文件修改时间
function getFileTimeFn () {
    fn_gft_file=$1;
    echo `stat ${fn_gft_file} -c %y`;
}

#创建 git 仓库
function initGitDirFn() {
    #指定仓库不存在，创建一个
    if [ ! -e ${angejia_service_git_dir} ] ;then
        #克隆仓库代码
        ${system_bin_path}/git clone ${angejia_service_git_url} -b ${angejia_service_git_branch} ${angejia_service_git_dir};
    fi
}

#格式化验证规则文件
function formatRoutesFileFn () {
    rm -rf ${angejia_route_file};
    for file in ${angejia_git_routes[@]};
    do
        echo `getFileTimeFn ${angejia_service_git_dir}/$file` >> ${angejia_route_file};
    done;
}


#运行初始化
function initRouteFileFn () {

    #初始化仓库
    initGitDirFn;

    formatRoutesFileFn;

    sendMailFn "route 初始化完成";
}



#检测监控的文件是否发生变化
function checkModifyRouteFileFn () {

    initGitDirFn

    #重新获取分支代码
    ${system_bin_path}/git --git-dir=${angejia_service_git_dir}/.git  --work-tree=${angejia_service_git_dir}  pull --rebase origin ${angejia_service_git_branch};

    #上次保存文件时间内容
    angejia_route_file_content=`cat ${angejia_route_file}`;

    i=0;

    is_change=0;
    for file in ${angejia_git_routes[@]};
    do
        i=$(($i+1));

        #仓库文件修改时间
        now_angejia_git_file_time=`getFileTimeFn ${angejia_service_git_dir}/${file}`;

        #上次文件修改时间
        now_angejia_route_file_time=`echo "${angejia_route_file_content}" | sed -n "${i}p"`;

        #不相同则有变动
        if [ "${now_angejia_git_file_time}" != "${now_angejia_route_file_time}" ];then
            is_change=1;

            #查看变化内容
            file_diff="`${system_bin_path}/git --git-dir=${angejia_service_git_dir}/.git --work-tree=${angejia_service_git_dir} show ${angejia_service_git_dir}/${file}`";

            change_content+="
            <div>
                变化文件：<a href='${angejia_service_git_web_rules}/${file}'>${file}</a>
                变化内容：
                <xmp>
${file_diff}
                </xmp>
            </div>
            ";
        fi

    done;

    #有变动时
    if [ $is_change = 1 ];then
        #重新格式化 route 文件
        formatRoutesFileFn;
        send_content="${change_content}";
    else
        send_content="正常无变动";
    fi

    #发送邮件
    sendMailFn "${send_content}";
}


if [ -e "$angejia_route_file" ];then
    checkModifyRouteFileFn;
else
    initRouteFileFn;
fi
