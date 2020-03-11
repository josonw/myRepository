#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

#这个表必须有自依赖关系

class agg_bank_final_mid_data(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create external table if not exists stage.user_bank_final_mid
        (
            fdate date,
            fbpid string,
            fuid bigint,
            fbank_gamecoins_num bigint
        );
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date
        }

        hql = """
        use stage;

        insert overwrite table stage.user_bank_final_mid
        select fdate, fbpid, fuid, fbank_gamecoins_num
        from
        (
            -- 这里增加一个字段flts_at，防止一个用户一天有多条
            -- 加上fbank_gamecoins_num是防止同一个时刻，有多条
            select fdate, fbpid, fuid, fbank_gamecoins_num,
                row_number() over(partition by fbpid, fuid order by flts_at desc, fbank_gamecoins_num desc) rown
            from
            (
                select "%(ld_begin)s" fdate, fbpid, fuid, fbank_gamecoins_num, flts_at
                from stage.user_bank_stage
                where dt = "%(ld_begin)s"
                and fact_type in (0, 1)
                and coalesce(fcurrencies_type,'0') = '0'

                union all

                select fdate, fbpid, fuid, fbank_gamecoins_num, fdate as flts_at
                from stage.user_bank_final_mid
            ) t
        ) t
        where rown = 1;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


if __name__ == '__main__':
    a = agg_bank_final_mid_data()
    a()
