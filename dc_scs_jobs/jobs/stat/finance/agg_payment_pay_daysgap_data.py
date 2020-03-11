#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_pay_daysgap_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_pay_days_gap_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fdaysgap                    bigint,
            fpayusercnt                 bigint
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

        # 付费用户距离注册时间。
        hql = """
        insert overwrite table analysis.user_pay_days_gap_fct partition (dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
              b.fgamefsk,
              b.fplatformfsk,
              b.fversionfsk,
              b.fterminalfsk,
              a.fdaysgap,
              a.fpayusercnt
            from (select fbpid,
                     fdaysgap,
                     count(distinct fplatform_uid) fpayusercnt
               from (select b.fbpid,
                           b.fplatform_uid ,
                           datediff('%(stat_date)s', a.dt) fdaysgap
                    from stage.user_dim a
                    join (select distinct fbpid,
                                 fplatform_uid
                            from stage.payment_stream_stg
                            where dt = '%(stat_date)s'
                    ) b
               on a.fbpid = b.fbpid
               and a.fplatform_uid = b.fplatform_uid ) a
             group by fbpid, fdaysgap
         ) a
         join analysis.bpid_platform_game_ver_map b
         on a.fbpid = b.fbpid
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
    a = agg_payment_pay_daysgap_data(stat_date)
    a()
