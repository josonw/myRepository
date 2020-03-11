#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path

import datetime
import time
import psycopg2
import os
import sys
import re
from pprint import pprint
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_PostgreSQL import Connection

from PublicFunc import PublicFunc

import config


def check_gpv(pg_db):
    """ 返回sql字符串"""
    keys = ['fbpid','fgamefsk','fplatformfsk','fversion_old','fterminalfsk']
    sql = """
    select * from dcnew.bpid_map a
    join
        (select  fgamefsk,fplatformfsk,fversion_old,fterminalfsk,count(*) cn
        from dcnew.bpid_map
        group by fgamefsk,fplatformfsk,fversion_old,fterminalfsk
        having count(*)>=2) b
    on a.fgamefsk = b.fgamefsk
    and a.fplatformfsk = b.fplatformfsk
    and a.fversion_old = b.fversion_old
    and a.fterminalfsk = b.fterminalfsk
    """
    data=pg_db.query(sql)
    if data:
        print '新后台的gpvt同一个组合有多个fbpid'
        print keys
        for item in  data:
            print [item[k] for k in keys]

def check_gphtv(pg_db):
    """ 返回sql字符串"""
    keys = ['fbpid','fgamefsk','fplatformfsk','fhallfsk','fterminaltypefsk','fversionfsk']
    sql = """
    select * from dcnew.bpid_map a
    join
        (select  fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,count(*) cn
        from dcnew.bpid_map
        group by fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk
        having count(*)>=2) b
    on a.fgamefsk = b.fgamefsk
    and a.fplatformfsk = b.fplatformfsk
    and a.fhallfsk = b.fhallfsk
    and a.fterminaltypefsk = b.fterminaltypefsk
    and a.fversionfsk = b.fversionfsk
    """
    data=pg_db.query(sql)
    if data:
        print '新后台的gphtv同一个组合有多个fbpid'
        print keys
        for item in  data:
            print [item[k] for k in keys]


def check_newbpid_oldbpid(pg_db):
    """ 返回sql字符串"""
    keys = ['fbpid','fgamefsk','fplatformfsk','fhallfsk','fterminaltypefsk','fversionfsk']
    sql = """
    select a.* from analysis.bpid_platform_game_ver_map a
    join dcnew.bpid_map b
      on a.fbpid=b.fbpid
    where a.fgamefsk!=4118194431
      and (b.fversion_old<>a.fversionfsk or
      a.fplatformfsk <> b.fplatformfsk  OR
      a.fgamefsk <> b.fgamefsk or
      a.fterminalfsk<> b.fterminalfsk)
    """
    data=pg_db.query(sql)
    bpid_limit = ['292E243C11FBEF59ADB8AA6E9B233592',
                  '1D006FFA20D96867FE52143E8248C9C5',
                  '3094D95D0E9D52423F094AF690C540D8',
                  '65A51B64A7355FB3ECFF921F4254F17A',
                  '64B73A5A8B1F305199BBA1889CD845E2',
                  '4420D7B4661539F7309C6951F691DF7A',
                  '33EEDC61893DB6852462B2804C9F2540',
                  '6D83C90695CEC20993B1BD3A4C8B5C82']

    data = [item for item in  data if item['fbpid'] not in bpid_limit]
    if data:
        print '新旧后台的gpvt组合不一致'
        print keys
        for item in  data:
            print [item[k] for k in keys]


if __name__ == "__main__":
    pg_db = Connection(config.PG_DB_HOST, config.PG_DB_NAME, config.PG_DB_USER, config.PG_DB_PSWD, debug=True)
    check_gpv(pg_db)
    print '\n'
    check_gphtv(pg_db)
    print '\n'
    check_newbpid_oldbpid(pg_db)

