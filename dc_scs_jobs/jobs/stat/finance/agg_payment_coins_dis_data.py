#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_coins_dis_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_coins_num
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fversionfsk             bigint,
            fterminalfsk            bigint,
            fcoins_num              decimal(20,2),
            fusercnt                bigint,
            fpaycnt                 bigint
        )
        partitioned by (dt date);

        -- 特殊同步要求,全表
        create table if not exists analysis.coins_num_dim
        (
            fsk                 bigint,
            fname               decimal(20,2)
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        # 订单额度，分布
        hql = """
          insert into table analysis.coins_num_dim
        select a.fsk, a.fname from (
            select 0 fsk, p.fcoins_num fname
                 from stage.payment_stream_stg p
                where p.dt = '%(stat_date)s'
                group by p.fcoins_num
        ) a
        left join analysis.coins_num_dim b
        on a.fname = b.fname
        where b.fname is null
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.pay_coins_num partition
        (dt = "%(stat_date)s")
          select '%(stat_date)s' fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 fcoins_num,
                 count(distinct fplatform_uid) fusercnt,
                 count(1) fpaycnt
            from stage.payment_stream_stg a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           where a.dt = '%(stat_date)s'
           group by b.fgamefsk,
                    b.fplatformfsk,
                    b.fversionfsk,
                    b.fterminalfsk,
                    fcoins_num
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
    a = agg_payment_coins_dis_data(stat_date)
    a()
