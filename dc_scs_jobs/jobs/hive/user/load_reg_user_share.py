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
创建每日新增主游戏注册用户代理新增
"""


class load_reg_user_share(BaseStat):

    def create_tab(self):
        query = {'statdate': self.stat_date,
            'null_str_report': sql_const.NULL_STR_REPORT,
            'null_int_report': sql_const.NULL_INT_REPORT}

        self.hql_dict.update(query)

        hql = """
        create table if not exists dim.reg_user_share
        (
            fbpid           varchar(50),      --BPID
            fuid            bigint,           --用户游戏ID
            fplatform_uid   varchar(50),      --付费用户的平台uid
            fsource_path    varchar(100),     --来源路径
            fpartner_info   varchar(32),      --代理商
            fpromoter       varchar(100)      --推广员
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        hql = """ --当日新增用户中的代理新增
            drop table if exists work.load_reg_user_share_%(num_begin)s;
          create table work.load_reg_user_share_%(num_begin)s as
          select us.fbpid,
                 us.fuid,
                 max(us.fpartner_info) fpartner_info,
                 max(us.fpromoter) fpromoter,
                 max(us.fsource_path) fsource_path
            from stage.partner_bind_stg us
           where dt = '%(statdate)s'
           group by us.fbpid,
                    us.fuid
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """ --当日新增用户中的代理新增
        insert overwrite table dim.reg_user_share partition (dt='%(statdate)s')
      select t1.fbpid,
             t1.fuid,
             t2.fplatform_uid,
             coalesce(t1.fsource_path, t2.fsource_path) fsource_path,
             t1.fpartner_info,
             t1.fpromoter
        from work.load_reg_user_share_%(num_begin)s t1
        left join (select fbpid,fuid,max(fplatform_uid) fplatform_uid,max(fsource_path) fsource_path
                     from dim.user_login_additional t
                    where dt = '%(statdate)s'
                    group by fbpid,fuid) t2
          on t1.fbpid = t2.fbpid
         and t1.fuid = t2.fuid
       where nvl(t1.fpromoter,'0') <> '0'
         and t1.fpromoter <> ''

        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """ --删除临时表
                drop table if exists work.load_reg_user_share_%(num_begin)s;
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
a = load_reg_user_share(statDate)
a()
