#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

"""
payment_stream_stg  上个月1号开始要数据完整
pay_user_mid        全量
active_user_mid     上个月1号开始要数据完整
"""

class agg_lastmonth_pu_retained(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        """ 重要部分，统计内容 """
        # self.hq.debug = 1

        hql_list = []

        hql = """
        drop table if exists analysis.user_payment_fct_lastmonth_part_%(num_begin)s;

        create table if not exists analysis.user_payment_fct_lastmonth_part_%(num_begin)s
        (
            fdate          date,
            fgamefsk       bigint,
            fplatformfsk   bigint,
            fversionfsk    bigint,
            fterminalfsk   bigint,
            flastmonthppr  bigint,
            flastmonthpar  bigint
        );
        """ % self.hql_dict
        hql_list.append( hql )

        # 上月（自然月）付费用户，在本月的付费留存，活跃留存
        hql = """
        insert into table analysis.user_payment_fct_lastmonth_part_%(num_begin)s
        select '%(stat_date)s' fdate,
            fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            count(1) flastmonthppr,
            null     flastmonthpar
        from
        (
            select fbpid, fplatform_uid, max(flag1), max(flag2)
            from
            (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                from stage.payment_stream_stg a
                where a.dt >= '%(ld_1month_ago_begin)s' and a.dt < '%(ld_month_begin)s'

                union all

                select fbpid, fplatform_uid, 0 flag1, 1 flag2
                from stage.payment_stream_stg b
                where b.dt >= '%(ld_month_begin)s' and b.dt < '%(ld_1month_after_begin)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
        ) a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_payment_fct_lastmonth_part_%(num_begin)s
        select '%(stat_date)s' fdate,
            fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            null     flastmonthppr,
            count(1) flastmonthpar
        from
        (
            select fbpid, fuid, max(flag1), max(flag2)
            from
            (
                select a.fbpid, b.fuid,  1 flag1, 0 flag2
                from stage.payment_stream_stg a
                join stage.pay_user_mid  b
                on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
                where a.dt >= '%(ld_1month_ago_begin)s' and a.dt < '%(ld_month_begin)s'

                union all

                select fbpid, fuid,  0 flag1, 1 flag2
                from stage.active_user_mid a
                where a.dt >= '%(ld_month_begin)s' and a.dt < '%(ld_1month_after_begin)s'
            ) a
            group by fbpid, fuid
            having max(flag1) = 1 and max(flag2) = 1
        ) a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_payment_fct_lastmonth_part_%(num_begin)s
        select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(flastmonthppr) flastmonthppr,
            max(flastmonthpar) flastmonthpar
        from analysis.user_payment_fct_lastmonth_part_%(num_begin)s
        group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        res = self.exe_hql_list(hql_list)
        return res


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
    a = agg_lastmonth_pu_retained(stat_date)
    a()
