#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_channel_active_mid(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists stage.channel_market_active_mid
        (
          fbpid           string,
          fuid            decimal(20),
          fchannel_id     string,
          fdate           string,
          fudid           string,
          factive_cnt     decimal(20),
          fnow_channel_id string,
          fcli_verinfo    string
        )
        partitioned by (dt date);       """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []

        hql = """
        insert overwrite table stage.channel_market_active_mid partition(dt = "%(ld_begin)s")
         select a.fbpid,
                a.fuid,
                max(nvl(b.fchannel_id, a.fchannel_id)) fchannel_id,
                '%(ld_begin)s' fdate,
                max(a.fudid) fudid,
                count(1) factive_cnt,
                max(a.fchannel_id) fnow_channel_id,
                max(a.fcli_verinfo) fcli_verinfo
           from (select fbpid, fchannel_id, fuid, fudid, fcli_verinfo
                   from stage.fd_user_channel_stg a
                  where dt = '%(ld_begin)s'
                    and a.fet_id in (3, 5)
                 ) a
           left outer join stage.channel_market_new_reg_mid b
             on a.fbpid = b.fbpid
            and a.fudid = b.fudid
          group by a.fbpid, a.fuid
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
    a = load_channel_active_mid(stat_date)
    a()