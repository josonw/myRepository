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
class agg_pay_rate_dim(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_rate_dim
        (
          pmode int,
          sid   int,
          unit  varchar(20),
          rate  decimal(20,10),
          rname varchar(100)
        );
        -- 从支付中心拉取
        create table if not exists analysis.paycenter_rate_dim
        (
        fdate   date,
        id      int,
        rate    decimal(20,10),
        unit    varchar(10),
        ext     varchar(255)
        )
        partitioned by (dt date);"""
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0

        # 运算汇率
        hql = """
        insert into table analysis.pay_rate_dim
        select a.pmode, a.sid, a.unit, a.rate, null rname
        from
        (
              select -1 as sid, -1 as pmode, unit, rate
              from analysis.paycenter_rate_dim
              where dt='%(stat_date)s'
              union all
              select sid, pmode, '-1' as unit, rate
              from analysis.paycenter_chanel_dim_bak
              where dt='%(stat_date)s'
        ) a
        left join analysis.pay_rate_dim b
        on a.pmode = b.pmode
        and a.sid = b.sid
        and a.unit = b.unit
        where b.pmode is null
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
    a = agg_pay_rate_dim(stat_date)
    a()
