#!/bin/sh
#set -x

source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

/usr/local/python272/bin/python /data/wwwroot/dc_server_test/dc_scs_jobs/tools/check_daily_keydata_status.py >> /data/other/scs_log/check_daily_keydata_status.log
