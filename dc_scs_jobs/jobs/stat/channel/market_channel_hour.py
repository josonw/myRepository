#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经停止计算
class market_channel_hour(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.hour_dim
        (
        fsk bigint,
        fhour bigint,
        fhourname varchar(20),
        fworkhour varchar(20),
        fperiod varchar(20),
        fhourid varchar(2),
        fzhhourname varchar(20)
        );
        create table if not exists analysis.user_channel_hour_fct
        (
        fdate date,
        fgamefsk bigint,
        fplatformfsk bigint,
        fversionfsk bigint,
        fterminalfsk bigint,
        fchannel_id varchar(100),
        fhourfsk bigint,
        floginusercnt bigint,
        flogincnt bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        # hql结尾不要带分号';'
        hql_list = []

        hql = """
        insert overwrite table analysis.user_channel_hour_fct partition
        (dt = "%(ld_begin)s")
        select a.fdate,
           d.fgamefsk,
           d.fplatformfsk,
           d.fversionfsk,
           d.fterminalfsk,
           a.fchannel_id,
           a.fhourfsk,
           floginusercnt,
           flogincnt
        from (
            select '%(ld_begin)s' fdate,
                   a.fbpid,
                   fchannel_id,
                   b.fsk fhourfsk,
                   count(distinct a.fudid) floginusercnt,
                   count(1) flogincnt
             from stage.fd_user_channel_stg a
                join analysis.hour_dim b
                  on a.flts_at is not null
                  and hour(a.flts_at) = b.fhourid
               where a.fet_id = 3
               and a.dt = '%(ld_begin)s'
               and a.fudid != ''
               and a.fudid is not null
            group by a.fbpid,
                     fchannel_id,
                     b.fsk
         ) a
          join analysis.bpid_platform_game_ver_map d
            on a.fbpid = d.fbpid

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
    a = market_channel_hour(stat_date)
    a()
