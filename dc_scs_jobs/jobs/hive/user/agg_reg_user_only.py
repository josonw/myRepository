#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const

"""
每日新增注册用户统计
"""
class agg_reg_user_only(BaseStat):

    """
    创建每日新增的注册用户统计结果表
    """
    def create_tab(self):
        """
        fgroupingid 废弃字段
        """
        hql = """create table if not exists dcnew.reg_user_only
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fgroupingid int,
            fdregucnt bigint
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)
        return res


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'','src_tbl_alias':''}
        query = {'statdate':self.stat_date,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reg_user_only
        partition(dt = '%(statdate)s')
        select '%(statdate)s' fdate,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fgame_id fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               fchannel_code fchannelcode,
               %(null_int_report)d fgroupingid,
               count(distinct un.fuid) fdregucnt
          from dim.reg_user_array un
         where un.dt = '%(statdate)s'
        group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,un.fversionfsk,un.fchannel_code
        """ % query

        res = self.hq.exe_sql(hql)
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_reg_user_only(statDate)
a()
