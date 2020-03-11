#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经注释不使用
class agg_pay_channel_dim(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.paycenter_chanel_dim
        (
          sid               int,
          pmode             int,
          pmodename         varchar(256),
          unit              varchar(20),
          rate              decimal(20,10),
          companyid         int,
          tag               int,
          statid            int,
          statname          varchar(100),
          parter_id         varchar(128),
          parter_name       varchar(128),
          companyname       varchar(128),
          company_type      int,
          company_type_name varchar(128)
        );

        create table if not exists analysis.paycenter_chanel_dim_bak
        (
          fdate             date,
          sid               int,
          pmode             int,
          pmodename         varchar(256),
          unit              varchar(20),
          rate              decimal(20,10),
          companyid         int,
          tag               int,
          statid            int,
          statname          varchar(100),
          parter_id         varchar(128),
          parter_name       varchar(128),
          companyname       varchar(128),
          company_type      varchar(128),
          company_type_name varchar(128)
        )partitioned by (dt date);        """

        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0

        hql = """
        insert into table analysis.paycenter_chanel_dim
        select a.sid, a.pmode,
            a.pmodename,
            a.unit,
            a.rate,
            a.companyid,
            a.tag,
            a.statid,
            a.statname,
            a.parter_id,
            a.parter_name,
            a.companyname,
            a.company_type,
            a.company_type_name
        from analysis.paycenter_chanel_dim_bak  a
        left join analysis.paycenter_chanel_dim b
          on a.sid = b.sid
         and a.pmode = b.pmode
        where a.dt = '%(ld_begin)s'
          and b.sid is null
        """ % self.hql_dict
        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_pay_channel_dim(stat_date)
    a()
