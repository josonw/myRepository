#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_pay_day_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_pay_day_gap_fct
        (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fversionfsk         bigint,
            fterminalfsk        bigint,
            ftype               int,
            f1cnt               bigint,
            f2cnt               bigint,
            f3cnt               bigint,
            f4cnt               bigint,
            f5cnt               bigint,
            f6cnt               bigint,
            f7cnt               bigint,
            f8cnt               bigint,
            f9cnt               bigint,
            f10cnt              bigint,
            f11cnt              bigint,
            f12cnt              bigint,
            f13cnt              bigint,
            f14cnt              bigint,
            f15cnt              bigint,
            f16cnt              bigint,
            f17cnt              bigint,
            f18cnt              bigint,
            f19cnt              bigint,
            f20cnt              bigint,
            f21cnt              bigint,
            f22cnt              bigint,
            f23cnt              bigint,
            f24cnt              bigint,
            f25cnt              bigint,
            f26cnt              bigint,
            f27cnt              bigint,
            f28cnt              bigint,
            f29cnt              bigint,
            f30cnt              bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    # 7,14,30周期内，用户付费天数据
    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        hql = """
        use analysis;
        alter table user_pay_day_gap_fct drop partition(dt="%(stat_date)s");
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_day_gap_fct partition(dt="%(stat_date)s")
        select  '%(stat_date)s' fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, 7 ftype,
            count(distinct case when a.pay_cnt=1 then a.fplatform_uid end) f1cnt, count(distinct case when a.pay_cnt=2 then a.fplatform_uid end) f2cnt,
            count(distinct case when a.pay_cnt=3 then a.fplatform_uid end) f3cnt, count(distinct case when a.pay_cnt=4 then a.fplatform_uid end) f4cnt,
            count(distinct case when a.pay_cnt=5 then a.fplatform_uid end) f5cnt, count(distinct case when a.pay_cnt=6 then a.fplatform_uid end) f6cnt,
            count(distinct case when a.pay_cnt=7 then a.fplatform_uid end) f7cnt,
            0 f8cnt,
            0 f9cnt,
            0 f10cnt,
            0 f11cnt,
            0 f12cnt,
            0 f13cnt,
            0 f14cnt,
            0 f15cnt,
            0 f16cnt,
            0 f17cnt,
            0 f18cnt,
            0 f19cnt,
            0 f20cnt,
            0 f21cnt,
            0 f22cnt,
            0 f23cnt,
            0 f24cnt,
            0 f25cnt,
            0 f26cnt,
            0 f27cnt,
            0 f28cnt,
            0 f29cnt,
            0 f30cnt
            from
         (select b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.fplatform_uid, count(distinct dt) pay_cnt
            from stage.payment_stream_stg a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           where a.dt >= date_add('%(stat_date)s', -7)  and  a.dt < date_add('%(stat_date)s', 1)
           group by b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.fplatform_uid
        ) a
        group by a.fplatformfsk, a.fgamefsk, a.fversionfsk, a.fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert into table analysis.user_pay_day_gap_fct partition(dt="%(stat_date)s")
          select  '%(stat_date)s' fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, 14 ftype,
            count(distinct case when a.pay_cnt=1 then a.fplatform_uid end) f1cnt, count(distinct case when a.pay_cnt=2 then a.fplatform_uid end) f2cnt,
            count(distinct case when a.pay_cnt=3 then a.fplatform_uid end) f3cnt, count(distinct case when a.pay_cnt=4 then a.fplatform_uid end) f4cnt,
            count(distinct case when a.pay_cnt=5 then a.fplatform_uid end) f5cnt, count(distinct case when a.pay_cnt=6 then a.fplatform_uid end) f6cnt,
            count(distinct case when a.pay_cnt=7 then a.fplatform_uid end) f7cnt, count(distinct case when a.pay_cnt=8 then a.fplatform_uid end) f8cnt,
            count(distinct case when a.pay_cnt=9 then a.fplatform_uid end) f9cnt, count(distinct case when a.pay_cnt=10 then a.fplatform_uid end) f10cnt,
            count(distinct case when a.pay_cnt=11 then a.fplatform_uid end) f11cnt, count(distinct case when a.pay_cnt=12 then a.fplatform_uid end) f12cnt,
            count(distinct case when a.pay_cnt=13 then a.fplatform_uid end) f13cnt, count(distinct case when a.pay_cnt=14 then a.fplatform_uid end) f14cnt,
            0 f15cnt,
            0 f16cnt,
            0 f17cnt,
            0 f18cnt,
            0 f19cnt,
            0 f20cnt,
            0 f21cnt,
            0 f22cnt,
            0 f23cnt,
            0 f24cnt,
            0 f25cnt,
            0 f26cnt,
            0 f27cnt,
            0 f28cnt,
            0 f29cnt,
            0 f30cnt
            from
         (select b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.fplatform_uid, count(distinct dt) pay_cnt
            from stage.payment_stream_stg a
            join analysis.bpid_platform_game_ver_map b on a.fbpid = b.fbpid
           where a.dt >= date_add('%(stat_date)s', -14)  and  a.dt < date_add('%(stat_date)s', 1)
           group by b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.fplatform_uid
         ) a
        group by a.fplatformfsk, a.fgamefsk, a.fversionfsk, a.fterminalfsk;
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """
        insert into table analysis.user_pay_day_gap_fct partition(dt="%(stat_date)s")
        select  '%(stat_date)s' fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, 30 ftype,
            count(distinct case when a.pay_cnt=1 then a.fplatform_uid end) f1cnt, count(distinct case when a.pay_cnt=2 then a.fplatform_uid end) f2cnt,
            count(distinct case when a.pay_cnt=3 then a.fplatform_uid end) f3cnt, count(distinct case when a.pay_cnt=4 then a.fplatform_uid end) f4cnt,
            count(distinct case when a.pay_cnt=5 then a.fplatform_uid end) f5cnt, count(distinct case when a.pay_cnt=6 then a.fplatform_uid end) f6cnt,
            count(distinct case when a.pay_cnt=7 then a.fplatform_uid end) f7cnt, count(distinct case when a.pay_cnt=8 then a.fplatform_uid end) f8cnt,
            count(distinct case when a.pay_cnt=9 then a.fplatform_uid end) f9cnt, count(distinct case when a.pay_cnt=10 then a.fplatform_uid end) f10cnt,
            count(distinct case when a.pay_cnt=11 then a.fplatform_uid end) f11cnt, count(distinct case when a.pay_cnt=12 then a.fplatform_uid end) f12cnt,
            count(distinct case when a.pay_cnt=13 then a.fplatform_uid end) f13cnt, count(distinct case when a.pay_cnt=14 then a.fplatform_uid end) f14cnt,
            count(distinct case when a.pay_cnt=15 then a.fplatform_uid end) f15cnt, count(distinct case when a.pay_cnt=16 then a.fplatform_uid end) f16cnt,
            count(distinct case when a.pay_cnt=17 then a.fplatform_uid end) f17cnt, count(distinct case when a.pay_cnt=18 then a.fplatform_uid end) f18cnt,
            count(distinct case when a.pay_cnt=19 then a.fplatform_uid end) f19cnt, count(distinct case when a.pay_cnt=20 then a.fplatform_uid end) f20cnt,
            count(distinct case when a.pay_cnt=21 then a.fplatform_uid end) f21cnt, count(distinct case when a.pay_cnt=22 then a.fplatform_uid end) f22cnt,
            count(distinct case when a.pay_cnt=23 then a.fplatform_uid end) f23cnt, count(distinct case when a.pay_cnt=24 then a.fplatform_uid end) f24cnt,
            count(distinct case when a.pay_cnt=25 then a.fplatform_uid end) f25cnt, count(distinct case when a.pay_cnt=26 then a.fplatform_uid end) f26cnt,
            count(distinct case when a.pay_cnt=27 then a.fplatform_uid end) f27cnt, count(distinct case when a.pay_cnt=28 then a.fplatform_uid end) f28cnt,
            count(distinct case when a.pay_cnt=29 then a.fplatform_uid end) f29cnt, count(distinct case when a.pay_cnt=30 then a.fplatform_uid end) f30cnt  from
         (select b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.fplatform_uid, count(distinct dt) pay_cnt
            from stage.payment_stream_stg a
            join analysis.bpid_platform_game_ver_map b on a.fbpid = b.fbpid
           where a.dt >= date_add('%(stat_date)s', -30)  and  a.dt < date_add('%(stat_date)s', 1)
           group by b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.fplatform_uid
        ) a
        group by a.fplatformfsk, a.fgamefsk, a.fversionfsk, a.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)


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
    a = agg_payment_pay_day_data(stat_date)
    a()
