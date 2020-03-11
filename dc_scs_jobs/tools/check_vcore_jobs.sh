#!/bin/sh
#set -x

source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

cd $SCS_PATH
/usr/local/python272/bin/python tools/check_vcore_jobs.py >> /data/other/scs_log/check_vcore_jobs.log 2>&1
