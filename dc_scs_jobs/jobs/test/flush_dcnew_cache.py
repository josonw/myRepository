#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
import requests

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import pg_db


def get_flush_table_config(tablename):
    pgschema = 'dcnew'
    table_dims  = pg_db.query("select dims from dcnew.flush_table_config where pgschema='%s' and pgtable = '%s' "%(pgschema,tablename) )

    dms = '|'.join([str(i['dims']) for i in table_dims])

    headers = {"User-Agent":
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"}

    url = "http://localhost:26813/new/business/reload/mongonew/?time=day\&dms=%s"%dms
    print url
    req = requests.get(url = url, headers = headers, timeout=1800)
    return req.json()



def main():
    if sys.argv[1]:
        tablename = sys.argv[1]
        print get_flush_table_config(tablename)


if __name__ == '__main__':
    main()
