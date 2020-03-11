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

class load_user_act_paid(BaseStat):
    """ 历史付费用户中间表 """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_act_paid
        (
            fbpid varchar(50) comment 'BPID',
            fgame_id bigint comment '子游戏ID',
            fchannel_code bigint comment '渠道ID',
            fuid bigint comment '用户ID'
        )
        partitioned by (dt date)
        stored as orc;
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):

        res = self.hq.exe_sql("""set hive.auto.convert.join=false""")
        if res != 0: return res

        hql = """
        insert overwrite table dim.user_act_paid  partition ( dt='%(statdate)s' )
        select
            ua.fbpid,
            ua.fgame_id,
            ua.fchannel_code,
            ua.fuid
        from
            dim.user_act ua
        join
            dim.user_pay pi
        on ua.fbpid = pi.fbpid
            and ua.fuid = pi.fuid
            and pi.dt<'%(statdate)s'
        where ua.dt = '%(statdate)s'
        """ % {'statdate':self.stat_date}

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
a = load_user_act_paid(statDate)
a()
