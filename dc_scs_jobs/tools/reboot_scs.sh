#!/bin/bash
source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

function check_pid_num()
{
    num=`ps -ef | grep scs_ | grep python | grep -v 127.0 | wc -l`
    echo $num
}

#执行检测
num=`check_pid_num`
first_check=$(($num%2))
echo $first_check

# 过5秒再检查一次
sleep 5

num2=`check_pid_num`
second_check=$(($num2%2))
echo $second_check

#如果调度任务为偶数就表示，主loop进程挂掉
if [ $first_check -eq 0 ] && [ $second_check -eq 0 ]
then
    #发送短信信息
    cd $SCS_PATH
    /usr/local/python272/bin/python tools/sendmsg.py

    #杀死当前运行子进程
    echo '杀死子进程'
    ps -ef | grep scs_ | grep python | grep -v 127.0 | awk '{print $2}' | xargs kill -9

    #重启调度核心
    cd $DC_SERVER_PATH
    nohup /usr/local/python272/bin/python -u scs_loop.py >> /data/other/scs_log/run.log 2>>/data/other/scs_log/run_err.log &
else
    echo 'so far so good'
fi

