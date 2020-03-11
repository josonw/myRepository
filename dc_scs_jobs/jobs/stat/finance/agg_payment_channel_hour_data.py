#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_channel_hour_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.payment_chan_hour_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fversionfsk             bigint,
            fterminalfsk            bigint,
            fhourfsk                bigint,
            fm_id                   varchar(256),
            forder_num              bigint,
            fuser_num               bigint,
            fpay_num                decimal(20,2)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        # 付费渠道，时段用户数，收入，订单数
        hql = """
        insert overwrite table analysis.payment_chan_hour_fct partition
        (dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
               d.fgamefsk,
               d.fplatformfsk,
               d.fversionfsk,
               d.fterminalfsk,
               b.fhour fhourfsk,
               a.fm_id,
               count(1) forder_num,
               count(distinct a.fplatform_uid) fuser_num,
               round(sum(a.fcoins_num * a.frate),2) fpay_num
          from stage.payment_stream_stg a
          join analysis.hour_dim b
            on hour(a.fdate) = b.fhour
          join analysis.payment_channel_dim c
            on a.fm_id = c.fm_id
          join analysis.bpid_platform_game_ver_map d
            on a.fbpid = d.fbpid
         where a.dt = '%(stat_date)s'
         group by b.fhour,
                  d.fgamefsk,
                  d.fplatformfsk,
                  d.fversionfsk,
                  d.fterminalfsk,
                  a.fm_id
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
    a = agg_payment_channel_hour_data(stat_date)
    a()
