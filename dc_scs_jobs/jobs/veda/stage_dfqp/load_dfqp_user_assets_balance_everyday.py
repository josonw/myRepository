#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserAssetsBalanceEveryday(BaseStatModel):

    def create_tab(self):
        hql = """
            create table if not exists stage_dfqp.user_assets_balance_everyday
            (
              mid bigint comment '用户ID',
              total_silver_coin bigint comment '总银币',
              carrying_silver_coin bigint comment '携带银币',
              safebox_silver_coin bigint comment '保险箱银币',
              total_gold_bar bigint comment '总金条',
              carrying_gold_bar bigint comment '携带金条',
              safebox_gold_bar bigint comment '保险箱金条'
            )
            comment '地方棋牌用户每日资产结余'
            partitioned by(dt string comment '日期')
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            with silver_info as
             (select mid, carrying_silver_coin, safebox_silver_coin
                from (select fuid as mid,
                             fuser_gamecoins_num as carrying_silver_coin,
                             fbank_gamecoins as safebox_silver_coin,
                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking
                        from stage_dfqp.pb_gamecoins_stream_stg
                       where dt = '%(statdate)s') m1
               where ranking = 1),
            gold_info as
             (select mid, carrying_gold_bar, safebox_gold_bar
                from (select fuid as mid,
                             fcurrencies_num as carrying_gold_bar,
                             fbank_currencies as safebox_gold_bar,
                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking
                        from stage_dfqp.pb_currencies_stream_stg
                       where fcurrencies_type = '11'
                         and dt = '%(statdate)s') m2
               where ranking = 1),
            user_yesterday_coins as
             (select mid,
                     total_silver_coin,
                     carrying_silver_coin,
                     safebox_silver_coin,
                     total_gold_bar,
                     carrying_gold_bar,
                     safebox_gold_bar
                from stage_dfqp.user_assets_balance_everyday
               where dt = date_sub('%(statdate)s',1)),
            target_users as
             (select mid from silver_info
              union distinct
              select mid from gold_info
              union distinct
              select mid from user_yesterday_coins)
            insert overwrite table stage_dfqp.user_assets_balance_everyday partition (dt='%(statdate)s')
            select v0.mid,
                   coalesce(v1.carrying_silver_coin,v3.carrying_silver_coin,0) + coalesce(v1.safebox_silver_coin,v3.safebox_silver_coin,0) as total_silver_coin,
                   coalesce(v1.carrying_silver_coin,v3.carrying_silver_coin,0) as carrying_silver_coin,
                   coalesce(v1.safebox_silver_coin,v3.safebox_silver_coin,0) as safebox_silver_coin,
                   coalesce(v2.carrying_gold_bar,v3.carrying_gold_bar,0) + coalesce(v2.safebox_gold_bar,v3.safebox_gold_bar,0) as total_gold_bar,
                   coalesce(v2.carrying_gold_bar,v3.carrying_gold_bar,0) as carrying_gold_bar,
                   coalesce(v2.safebox_gold_bar,v3.safebox_gold_bar,0) as safebox_gold_bar
              from target_users v0
              left join silver_info v1
                on v0.mid = v1.mid
              left join gold_info v2
                on v0.mid = v2.mid
              left join user_yesterday_coins v3
                on v0.mid = v3.mid
             order by v0.mid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserAssetsBalanceEveryday(sys.argv[1:])
a()