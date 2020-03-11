#! /usr/local/python272/bin/python
# coding: utf-8

import os


def exe_cmd(cmd):
    os.system(cmd)


if __name__ == '__main__':

    cmd = "source /etc/profile; /usr/local/python272/bin/python /data/wwwroot/dc_server_release/data_gp.oa.com/script_cluster/sync_data_night.py >> /data/wwwroot/dc_server_release/data_gp.oa.com/script_cluster/log/sync_dcbase.log 2>&1"
    exe_cmd(cmd)
