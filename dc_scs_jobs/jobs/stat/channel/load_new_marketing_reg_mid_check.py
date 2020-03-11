#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_new_marketing_reg_mid(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.channel_market_new_reg_mid_check
        (
          fbpid       string,
          fudid       string,
          fuid        decimal(20),
          fchannel_id string,
          fsignup_at  string
        )
        partitioned by (dt date)
        stored as orc;        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []

        hql = """
       insert overwrite table stage.channel_market_new_reg_mid_check partition
       (dt = "%(ld_begin)s")
       select a.fbpid, a.fudid, a.fuid, a.fchannel_id, a.fsignup_at
         from (select *
              from (select fbpid,
                           fudid,
                           fuid,
                           fchannel_id,
                           flts_at fsignup_at,
                           row_number() over(partition by fbpid, fudid order by flts_at, fuid desc, fchannel_id desc) rown
                      from stage.fd_user_channel_stg
                     where fet_id = 2
                       and dt = '%(ld_begin)s'
                       and fudid is not null
                       and fudid != ''
                    ) abc
             where rown = 1
        ) a left outer join stage.channel_market_new_reg_mid  b
           on a.fbpid = b.fbpid and a.fudid = b.fudid
           and b.dt < '%(ld_begin)s'
        where b.fudid is null
        """ % self.hql_dict
        hql_list.append( hql )

        result = 0
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
    a = load_new_marketing_reg_mid(stat_date)
    a()
