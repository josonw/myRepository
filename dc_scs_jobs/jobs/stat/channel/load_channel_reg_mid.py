#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_channel_reg_mid(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.channel_market_reg_mid
        (
          fbpid           string,
          fuid            decimal(20),
          fplatform_uid   string,
          fchannel_id     string,
          fsignup_at      string,
          fudid           string,
          fcli_verinfo    string,
          fnow_channel_id string
        )
        partitioned by (dt date);        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []
        hql = """
        insert overwrite table stage.channel_market_reg_mid partition
          (dt = "%(ld_begin)s")
        select a.fbpid,
            a.fuid,
            a.fplatform_uid,
            nvl(b.fchannel_id, a.fchannel_id) fchannel_id,
            a.fsignup_at,
            a.fudid,
            a.fcli_verinfo,
            a.fchannel_id fnow_channel_id
       from (select fbpid,
                    fuid,
                    max(fplatform_uid) fplatform_uid,
                    max(fchannel_id) fchannel_id,
                    max(flts_at) fsignup_at,
                    max(fudid) fudid,
                    max(FCLI_VERINFO) FCLI_VERINFO
               from stage.fd_user_channel_stg
              where fet_id = 2
                and dt = '%(ld_begin)s'
              group by fbpid, fuid) a
       left outer join stage.channel_market_new_reg_mid b
         on a.fbpid = b.fbpid
        and a.fudid = b.fudid
       left outer join stage.channel_market_reg_mid c
         on a.fbpid = c.fbpid
        and a.fuid = c.fuid
        and c.dt < '%(ld_begin)s'
      where c.fuid is null
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
    a = load_channel_reg_mid(stat_date)
    a()