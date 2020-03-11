#!/bin/sh
#set -x

source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

cd $SCS_PATH
/usr/local/python272/bin/python  tools/check_sync.py  >> /data/other/scs_log/ch_sync_res.log  2>>/data/other/scs_log/ch_sync_err.log
