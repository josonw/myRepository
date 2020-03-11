#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_by_deferred_bycoin_balance(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 上个月1号
        create table if not exists analysis.by_deferred_bycoin_fct
        (
            fdate                         date,
            fgamefsk                      bigint,
            fplatformfsk                  bigint,
            fbycoin_balance_history       bigint,
            fbycoin_to_gamecoin_num       bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """

        -- 递延收入，博雅币
        insert overwrite table analysis.by_deferred_bycoin_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        select '%(ld_1month_ago_begin)s' fdate,
                fgamefsk,
                fplatformfsk,
                sum(fuser_bycoins_num) fbycoin_balance_history,
                0 fbycoin_to_gamecoin_num
       from (select flts_at fdate,
                    fgamefsk,
                    fplatformfsk,
                    fuid,
                    fuser_bycoins_num,
                    row_number() over(partition by fgamefsk, fplatformfsk, fuid order by flts_at desc,  a.fbpid, a.fuser_bycoins_num) rown
               from stage.pb_bycoins_stream_stg a
               join dim.bpid_map b
                 on a.fbpid = b.fbpid
              where a.dt < '%(ld_month_begin)s'
                and fuser_bycoins_num >= 0) a
          where rown = 1
          group by fgamefsk, fplatformfsk
        """ % self.hql_dict

        # 只有月初日期才会执行
        if self.hql_dict.get('stat_date', '').endswith('-01'):
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
    a = agg_by_deferred_bycoin_balance(stat_date)
    a()
