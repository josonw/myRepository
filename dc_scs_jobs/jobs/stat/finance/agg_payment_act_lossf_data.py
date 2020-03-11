#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_act_lossf_data(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求 90天
        create table if not exists analysis.user_pu_loss_funnel_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fversionfsk             bigint,
            fterminalfsk            bigint,
            f1dusernum              bigint,
            f2dusernum              bigint,
            f3dusernum              bigint,
            f4dusernum              bigint,
            f5dusernum              bigint,
            f6dusernum              bigint,
            f7dusernum              bigint,
            f14dusernum             bigint,
            f30dusernum             bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        #加上当天的分区
        hql = """
        use analysis;
        alter table user_pu_loss_funnel_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_pu_loss_funnel_fct partition(dt)
        select
                forder_at fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,
                count( distinct case when fdate >= date_add(forder_at, 1) then fuid end ) f1dusernum,
                count( distinct case when fdate >= date_add(forder_at, 2) then fuid end ) f2dusernum,
                count( distinct case when fdate >= date_add(forder_at, 3) then fuid end ) f3dusernum,
                count( distinct case when fdate >= date_add(forder_at, 4) then fuid end ) f4dusernum,
                count( distinct case when fdate >= date_add(forder_at, 5) then fuid end ) f5dusernum,
                count( distinct case when fdate >= date_add(forder_at, 6) then fuid end ) f6dusernum,
                count( distinct case when fdate >= date_add(forder_at, 7) then fuid end ) f7dusernum,
                count( distinct case when fdate >= date_add(forder_at, 14) then fuid end ) f14dusernum,
                count( distinct case when fdate >= date_add(forder_at, 30) then fuid end ) f30dusernum,
                forder_at dt
          from (
          select  a.fbpid, forder_at, b.fdate, b.fuid
                 from ( select  distinct a.fbpid, a.dt forder_at, b.fuid
                                from stage.payment_stream_stg a
                                join stage.pay_user_mid b
                                  on a.fbpid = b.fbpid
                                 and a.fplatform_uid = b.fplatform_uid
                         where a.dt>= '%(ld_90dayago)s' and a.dt < '%(ld_end)s'
                       ) a
                 join stage.active_user_mid b
                   on a.fbpid=b.fbpid
                  and a.fuid=b.fuid
                where b.dt > '%(ld_90dayago)s' and b.dt < '%(ld_end)s'
          ) a join analysis.bpid_platform_game_ver_map b
          on a.fbpid=b.fbpid
          group by forder_at, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_pu_loss_funnel_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        f1dusernum,
        f2dusernum,
        f3dusernum,
        f4dusernum,
        f5dusernum,
        f6dusernum,
        f7dusernum,
        f14dusernum,
        f30dusernum
        from analysis.user_pu_loss_funnel_fct
        where dt>= '%(ld_90dayago)s' and dt < '%(ld_end)s'

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
    a = agg_payment_act_lossf_data(stat_date)
    a()
