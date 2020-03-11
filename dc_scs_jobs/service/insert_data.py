#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path

import datetime
import time
import psycopg2
import os
import sys
import re

path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_PostgreSQL import Connection

from PublicFunc import PublicFunc

import config

"""
批量插入pg数据便于前后端开发测试
"""

def insertsql(compose_field, compose_value, query, pg_db):
    """ 执行sql """
    sql_list = []
    if  compose_field:
        for item in compose_value:
            item = [str(i) for i in item]
            tmp = zip(item,compose_field)

            dim_str ='%s\n%s' %(',', ', '.join([' '.join(i) for i in tmp]))
            query.update({'dim_str':dim_str})
            insert_sql = sql_format(query)
            sql_list.append(insert_sql)
            pg_db.execute(insert_sql)

    else:
        insert_sql = sql_format(query)
        sql_list.append(insert_sql)
        pg_db.execute(insert_sql)

    return sql_list

def got_stat_str(public_field, compose_field, query, pg_db):
    """ 获取统计字段位置 返回字符串"""
    public_field.extend(compose_field)

    sql ="""
    select column_name,data_type
    from INFORMATION_SCHEMA.COLUMNS where table_name = '%(tblname)s'
    """ %query
    filed_data = pg_db.query(sql)

    dim_field =[]
    for field in filed_data:
        if field['column_name'] not in public_field:
            dim_field.append(field['column_name'])

    stat_str = ', \n6666 ' + ', 6666 '.join(dim_field)
    return stat_str


def got_public_str(public_field):
    """ 获取公共字段位置 返回字符串 """
    public_str = ',\n'.join(public_field)
    return public_str


def sql_format(query):
    """ sql格式化 """
    insert_sql = """
    insert into %(table_schema)s.%(tblname)s
    select
    %(public_str)s
    %(dim_str)s
    %(stat_str)s
     from dcnew.act_user
    WHERE fdate >= date'%(sdate)s'
      AND fdate <= date'%(edate)s'
    """%query
    return insert_sql


if __name__ == "__main__":
    pg_db = Connection(config.PG_DB_HOST, config.PG_DB_NAME, config.PG_DB_USER, config.PG_DB_PSWD, debug=True)

    """组合维度字段, 注意dim_value内的格式,字符串需要两层引号，数值型需要一层引号"""

    dim_field = ['fact_id','frule_id']
    dim_value = [["6","1"],
                 ["1","2"],
                 ["2","3"],
                 ["3","4"],
                 ["4","5"],
                 ["5","8"]]

    public_field = ['fdate','fgamefsk','fplatformfsk','fhallfsk','fsubgamefsk','fterminaltypefsk','fversionfsk','fchannelcode']

    edate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
    sdate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=7), "%Y-%m-%d")

    query = {'sdate':sdate,
             'edate':edate,
             'tblname':'game_activity_rule',
             'table_schema':'dcnew',
             'public_str':got_public_str(public_field),
             'dim_str':'',
            }

    stat_str = got_stat_str(public_field, dim_field, query, pg_db)

    query.update({'stat_str':stat_str})

    insertsql(dim_field, dim_value, query, pg_db)
