#!/bin/sh
#set -x

source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

cd $SCS_PATH
/usr/local/python272/bin/python tools/checkBind.py > /data/other/scs_log/check_bind.log 2>&1