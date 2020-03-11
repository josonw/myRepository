#!/bin/sh
#set -x

path=/data/other/scs_log

date=`date -d"1 days ago" +%Y%m%d`

cat /data/other/scs_log/run.log >> /data/other/scs_log/loop_history/run_${date}.log

echo "" > ${path}/run.log

# 保留90天的数据
del_date=`date -d"90 days ago" +%Y%m%d`

#删除主进程的历史记录，

for file in `find /data/other/scs_log/loop_history/ -type f -name "run_*.log"`
do
    log_date=`echo $file | awk -F_ '{print $NF}' | sed 's/.log//g'`

    if [ $log_date -lt $del_date ]
    then
        echo $file
        rm -f $file
    fi
done

# 删除备份的配置文件
for file in `find /data/other/scs_log/config_backup/ -type f -name "config_20*.sql"`
do
    log_date=`echo $file | awk -F_ '{print $NF}' | sed 's/.sql//g' | sed 's/-//g'`

    if [ $log_date -lt $del_date ]
    then
        echo $file
        rm -f $file
    fi
done			

# 删除各个任务日志
for dir_name in `find /data/other/scs_log/jobslog/ -type d -name "dtime_20*"`
do
    log_date=`echo $dir_name | awk -F_ '{print $NF}' | sed 's/-//g'`

    if [ $log_date -lt $del_date ]
    then
        echo $dir_name
        rm -rf $dir_name
    fi
done
