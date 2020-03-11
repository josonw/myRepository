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


class load_xxx_user(BaseStat):

    def create_tab(self):
        query = {'statdate': self.stat_date,
            'null_str_report': sql_const.NULL_STR_REPORT,
            'null_int_report': sql_const.NULL_INT_REPORT}

        self.hql_dict.update(query)

        hql = """
        create table if not exists dim.xxx_user
        (
            fbpid           varchar(50),      --BPID
            fuid            bigint,           --用户游戏ID
            fuser_type      varchar(10)   comment '用户类型：ad等',
            fuser_type_id   varchar(100)  comment '用户类型id',
            fchannel_id     varchar(100)  comment '渠道包编号'
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        """ 重要部分，统计内容 """

        hql = """ --当日新增用户的广告id，关联bpid是为了给大厅下同ID的用户都打上bpid
        insert overwrite table dim.xxx_user partition (dt='%(statdate)s')
      select distinct t3.fbpid
             ,t1.fuid
             ,'ad' fuser_type
             ,coalesce(t1.fad_code,'%(null_str_report)s') fuser_type_id
             ,coalesce(t1.fchannel_id,'%(null_str_report)s') fchannel_id
        from stage.user_signup_addition_stg t1
        join dim.bpid_map t2
          on t1.fbpid = t2.fbpid
        join dim.bpid_map t3
          on t2.fgamefsk = t3.fgamefsk
         and t2.fplatformfsk = t3.fplatformfsk
         and t2.fhallfsk = t3.fhallfsk
       where t1.dt='%(statdate)s'

        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res


# 愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    # 没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
else:
    # 从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


# 生成统计实例
a = load_xxx_user(statDate)
a()
