_#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class fd_user_channel_stg(BaseStat):


    def create_tab(self):
        hql = """
        create external table if not exists stage.fd_user_channel_stg
        (
          fbpid         varchar(64),
          fuid          bigint,
          fplatform_uid varchar(64),
          fchannel_id   varchar(64),
          fet_id        int,
          flts_at       varchar(64),
          fip           varchar(64),
          fcli_verinfo  varchar(128),
          fcli_os       varchar(128),
          fdevice_type  varchar(128),
          fpixel_info   varchar(64),
          fisp          varchar(64),
          fnw_type      varchar(64),
          fudid         varchar(64),
          fpay_money    decimal(20,2),
          fpay_rate     decimal(25,10),
          fpay_mode     varchar(64),
          fpid          bigint,
          fm_at         varchar(64),
          fvip_type     varchar(100),
          fvip_level    bigint,
          flevel        bigint,
          fimei         varchar(128),
          fremark       varchar(256)
        )
        partitioned by (dt date)
        location '/dw/stage/user_channel/'
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []

        hql = """
        use stage;
        alter table fd_user_channel_stg add if not exists partition(dt='%(ld_begin)s')
        location '/dw/stage/user_channel/%(ld_begin)s'
        """ % self.hql_dict
        result = self.exe_hql(hql)
        if result != 0:
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
    a = fd_user_channel_stg(stat_date)
    a()