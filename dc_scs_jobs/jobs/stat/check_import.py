#! /usr/bin/env python
# coding: utf-8

import os
import requests
import sys
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
# HADOOP_HOST = "HKWF-175-45-5-196.boyaa.com"
import config
HADOOP_HOST = config.HIVE_NODE_SSH_IP
HADOOP_PORT = "50070"

class ImportError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

def check(url):
    resp = requests.get(url)
    json = resp.json()

    if 'RemoteException' in json:
        return False
    if 'FileStatus' in json:
        if json['FileStatus']['childrenNum'] < 1:
            return False
    return True

def yestoday():
    import datetime
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

def gen_url(path):
    base_url = "http://%s:%s/webhdfs/v1" % (HADOOP_HOST, HADOOP_PORT)
    url = "%s%s?op=GETFILESTATUS" % (base_url, path)
    return url

def gen_path(date):
    base_path = "/dw/stage/finish_import_flag"
    path = "%s/%s" % (base_path, date)
    return path

def main():
    '''
    if len(sys.argv) == 1:
        date = yestoday()
    elif len(sys.argv) >= 2:
        date = sys.argv[1]
    else:
        print """usage:\n      python check_import.py [date]\n"""
        sys.exit(2)

    path = gen_path(date)
    url = gen_url(path)
    print url
    res = check(url)
    if not res:
        raise ImportError("importing to hadoop not finished")
    '''
    pass

if __name__ == '__main__':
    main()
