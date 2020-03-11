#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_pay_lossf_data(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求,90天
        create table if not exists analysis.user_pay_num_loss_fct
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
        # self.hq.debug = 1

        hql = """
        use analysis;
        alter table user_pay_num_loss_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )

        # 付费用户，付费流失漏斗
        hql = """
        insert overwrite table analysis.user_pay_num_loss_fct partition(dt)
        select fdate,fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,
             count( distinct case when paydate >= date_add(fdate, 1) then fpid end ) f1dusernum,
             count( distinct case when paydate >= date_add(fdate, 2) then fpid end ) f2dusernum,
             count( distinct case when paydate >= date_add(fdate, 3) then fpid end ) f3dusernum,
             count( distinct case when paydate >= date_add(fdate, 4) then fpid end ) f4dusernum,
             count( distinct case when paydate >= date_add(fdate, 5) then fpid end ) f5dusernum,
             count( distinct case when paydate >= date_add(fdate, 6) then fpid end ) f6dusernum,
             count( distinct case when paydate >= date_add(fdate, 7) then fpid end ) f7dusernum,
             count( distinct case when paydate >= date_add(fdate, 14) then fpid end ) f14dusernum,
             count( distinct case when paydate >= date_add(fdate, 30) then fpid end ) f30dusernum,
             fdate dt
             from(
                select a.fdate fdate, b.fdate paydate, a.fbpid, a.fplatform_uid fpid
                from (
                      select fbpid, dt fdate, fplatform_uid
                      from stage.payment_stream_stg
                      where dt >= '%(ld_90dayago)s' and dt < '%(ld_end)s'
                      group by fbpid, dt, fplatform_uid
                ) a join (
                      select fbpid, dt fdate, fplatform_uid
                      from stage.payment_stream_stg
                      where dt >= '%(ld_90dayago)s' and dt < '%(ld_end)s'
                      group by fbpid, dt, fplatform_uid
                ) b
                on a.fbpid = b.fbpid
                and a.fplatform_uid = b.fplatform_uid
             ) p
             join analysis.bpid_platform_game_ver_map bpm
             on bpm.fbpid = p.fbpid
             group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_pay_num_loss_fct partition(dt='3000-01-01')
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
        from analysis.user_pay_num_loss_fct
        where dt >= '%(ld_90dayago)s' and dt < '%(ld_end)s'
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
    a = agg_payment_pay_lossf_data(stat_date)
    a()
