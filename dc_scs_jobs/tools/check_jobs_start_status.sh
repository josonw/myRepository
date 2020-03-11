#!/bin/sh
#set -x

source /etc/profile
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:$PATH

/usr/local/python272/bin/python /data/wwwroot/dc_server_test/dc_scs_jobs/tools/check_jobs_start_status.py >> /data/other/scs_log/check_job_start_status.log
