#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserCurrentCoins(BaseStatModel):

    def create_tab(self):
        hql = """
            create table if not exists stage_dfqp.user_current_coins
            (
              mid bigint comment '用户ID',
              total_silver_coin bigint comment '总银币',
              carrying_silver_coin bigint comment '携带银币',
              safebox_silver_coin bigint comment '保险箱银币',
              total_gold_bar bigint comment '总金条',
              carrying_gold_bar bigint comment '携带金条',
              safebox_gold_bar bigint comment '保险箱金条'
            )
            comment '地方棋牌用户资产结余'
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            with silver_info as
             (select mid, carrying_silver_coin
                from (select fuid as mid,
                             fuser_gamecoins_num as carrying_silver_coin,
                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking
                        from stage_dfqp.pb_gamecoins_stream_stg
                       where dt = '%(statdate)s') m1
               where ranking = 1),
            gold_info as
             (select mid, carrying_gold_bar
                from (select fuid as mid,
                             fcurrencies_num as carrying_gold_bar,
                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking
                        from stage_dfqp.pb_currencies_stream_stg
                       where fcurrencies_type = '11'
                         and dt = '%(statdate)s') m2
               where ranking = 1),
            safebox_silver_info as
             (select mid, safebox_silver_coin
                from (select fuid as mid,
                             fuser_gamecoins_num as safebox_silver_coin,
                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking
                        from stage_dfqp.user_bank_stage
                       where fcurrencies_type = '0'
                         and dt = '%(statdate)s') m3
               where ranking = 1),
            safebox_gold_info as
             (select mid, safebox_gold_bar
                from (select fuid as mid,
                             fuser_gamecoins_num as safebox_gold_bar,
                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking
                        from stage_dfqp.user_bank_stage
                       where fcurrencies_type = '11'
                         and dt = '%(statdate)s') m4
               where ranking = 1),
            target_users as
             (select mid
                from silver_info
              union distinct
              select mid
                from gold_info
              union distinct
              select mid
                from safebox_silver_info
              union distinct
              select mid
                from safebox_gold_info
              union distinct
              select mid
                from stage_dfqp.user_current_coins)
            insert overwrite table stage_dfqp.user_current_coins
            select v0.mid,
                   coalesce(v1.carrying_silver_coin,v5.carrying_silver_coin,0) + coalesce(v3.safebox_silver_coin,v5.safebox_silver_coin,0) as total_silver_coin,
                   coalesce(v1.carrying_silver_coin,v5.carrying_silver_coin,0) as carrying_silver_coin,
                   coalesce(v3.safebox_silver_coin,v5.safebox_silver_coin,0) as safebox_silver_coin,
                   coalesce(v2.carrying_gold_bar,v5.carrying_gold_bar,0) + coalesce(v4.safebox_gold_bar,v5.safebox_gold_bar,0) as total_gold_bar,
                   coalesce(v2.carrying_gold_bar,v5.carrying_gold_bar,0) as carrying_gold_bar,
                   coalesce(v4.safebox_gold_bar,v5.safebox_gold_bar,0) as safebox_gold_bar
              from target_users v0
              left join silver_info v1
                on v0.mid = v1.mid
              left join gold_info v2
                on v0.mid = v2.mid
              left join safebox_silver_info v3
                on v0.mid = v3.mid
              left join safebox_gold_info v4
                on v0.mid = v4.mid
              left join stage_dfqp.user_current_coins v5
                on v0.mid = v5.mid
             order by v0.mid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserCurrentCoins(sys.argv[1:])
a()