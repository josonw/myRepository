#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameworld_balance_data(BaseStat):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.gameworld_balance_fct_2
        (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fpaybalance   decimal(20,0),
            fpayusernum   bigint
        )
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 依赖的源表最早是2015-05-31的，早于这个日期从oracle同步
        if self.stat_date < '2015-05-31':
            return -1

        # 如果统计日期不是月底，退出
        if self.stat_date != PublicFunc.last_day(self.stat_date):
            return -1

        self.hq.debug = 0

        args_dic = {
            "ld_month_end": self.stat_date,
            "ld_year_ago": PublicFunc.add_days(self.stat_date, -365)
        }

        hql = "set mapreduce.reduce.shuffle.input.buffer.percent=0.3;"
        # 付费用户改取一年的流水的用户，财务巫晓娟一再坚持这个做法，不能用全量付费用户
        # 付费用户的结余，一定是bpid内有结余且付费，不可用平台的结余和平台的付费用户交叉

        # 付费用户的结余，因为要独立求用户的最后一条记录，所以没法和上面的合到一条sql去处理
        hql += """
        insert overwrite table analysis.gameworld_balance_fct_2
        partition(dt = "%(ld_month_end)s")
        select "%(ld_month_end)s" fdate, fgamefsk, fplatformfsk,
            sum(user_gamecoins_num) fpaybalance, count(fuid) fpayusernum
        from
        (
            select tb.fgamefsk, tb.fplatformfsk, tb.fuid, user_gamecoins_num,
                row_number() over (partition by tb.fgamefsk, tb.fplatformfsk, tb.fuid order by fdate desc, user_gamecoins_num desc) rown
            from (
                select fgamefsk, fplatformfsk, fuid
                from stage.user_order_gp_mid a
                join dim.bpid_map b
                on a.fbpid = b.fbpid
                where dt = "%(ld_month_end)s"
                    and fdate >= "%(ld_year_ago)s" and fdate <= "%(ld_month_end)s"
            ) tb
            join
            (
                select fdate, fgamefsk, fplatformfsk, fuid, user_gamecoins_num
                from stage.user_gamecoins_balance_gp_mid a
                join dim.bpid_map b
                on a.fbpid = b.fbpid
                where dt = "%(ld_month_end)s"
            ) tc
            on tb.fgamefsk = tc.fgamefsk and tb.fplatformfsk = tc.fplatformfsk and tb.fuid = tc.fuid

        ) t
        where rown = 1 and user_gamecoins_num >= 0
        group by fgamefsk, fplatformfsk;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = agg_gameworld_balance_data()
    a()
