#!/bin/bash
source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

# 杀死所有调度相关任务 （慎用）
ps -ef | grep "scs_loop.py" | grep -v grep | awk '{print $2}' | xargs kill -9

sleep 3
num=`ps -ef | grep "scs_loop.py" | grep -v "grep" | wc -l`

if [ $num == 0 ]
then
    echo 'Done!'
else
    echo "剩余 $num 个没杀死"
fi