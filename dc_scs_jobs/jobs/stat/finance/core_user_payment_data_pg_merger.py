#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class core_user_payment_data_pg_merger(BasePGStat):
    """将核心数据合并入 user_payment_fct 中
    """
    def stat(self):
        sql = """
        with datas as (
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fpayusercnt, fpaycnt, fincome
            from user_payment_fct_core where fdate = date'%(ld_begin)s'
        ), updated as (
            update user_payment_fct as a set
                fpayusercnt = t.fpayusercnt,
                fpaycnt = t.fpaycnt,
                fincome = t.fincome
            from datas as t
            where a.fdate = date'%(ld_begin)s'
            and a.fgamefsk = t.fgamefsk
            and a.fplatformfsk = t.fplatformfsk
            and a.fversionfsk = t.fversionfsk
            and a.fterminalfsk = t.fterminalfsk
        returning t.fdate, t.fgamefsk, t.fplatformfsk, t.fversionfsk, t.fterminalfsk,
            t.fpayusercnt, t.fpaycnt, t.fincome
        )
        insert into user_payment_fct(fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fpayusercnt, fpaycnt, fincome )
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fpayusercnt, fpaycnt, fincome from datas as d
        where not exists (
            select 1 from updated as u
            where d.fgamefsk = u.fgamefsk
            and d.fplatformfsk = u.fplatformfsk
            and d.fversionfsk = u.fversionfsk
            and d.fterminalfsk = u.fterminalfsk
        )
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = core_user_payment_data_pg_merger(stat_date)
    a()
