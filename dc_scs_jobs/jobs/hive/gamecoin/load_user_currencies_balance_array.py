#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel

"""
    货币结余
"""

class load_user_currencies_balance_array(BaseStatModel):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists dim.user_currencies_balance_array
        ( fdate                string,
          fgamefsk             bigint,
          fplatformfsk         bigint,
          fhallfsk             bigint,
          fsubgamefsk          bigint,
          fterminaltypefsk     bigint,
          fversionfsk          bigint,
          fchannelcode         bigint,
          fuid                 bigint,              --用户游戏ID
          fcurrencies_type     varchar(10),         --用户货币类型
          fcurrencies_num      bigint,              --用户货币携带
          fbank_gamecoins_num  bigint               --用户保险箱携带
        )
        partitioned by (dt date)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 不加fcurrencies_num >= 0条件，加了每天的量会少200w左右
        # 但是加了之后，这个表可用的地方就非常有限了

        hql = """--游戏-平台
        insert overwrite table dim.user_currencies_balance_array
        partition(dt="%(statdate)s")
                select '%(statdate)s' fdate
                       ,fgamefsk
                       ,fplatformfsk
                       ,-21379 fhallfsk
                       ,-21379 fsubgamefsk
                       ,-21379 fterminaltypefsk
                       ,-21379 fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,fcurrencies_type
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fuid,
                               fcurrencies_type,
                               fcurrencies_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk, fcurrencies_type, fuid order by fdate desc, fcurrencies_num desc) rown
                        from dim.user_currencies_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                        where dt = '%(statdate)s'

                        union all

                        select fgamefsk,
                               fplatformfsk,
                               fuid,
                               fcurrencies_type,
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '11'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fuid
                          ,fcurrencies_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--游戏-平台-大厅
        insert into table dim.user_currencies_balance_array
        partition(dt="%(statdate)s")
                select '%(statdate)s' fdate
                       ,fgamefsk
                       ,fplatformfsk
                       ,fhallfsk
                       ,-21379 fsubgamefsk
                       ,-21379 fterminaltypefsk
                       ,-21379 fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,fcurrencies_type
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fuid,
                               fcurrencies_type,
                               fcurrencies_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk, fcurrencies_type,fuid order by fdate desc, fcurrencies_num desc) rown
                        from dim.user_currencies_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'

                        union all

                        select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fuid,
                               fcurrencies_type,
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '11'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fuid
                          ,fcurrencies_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--游戏-平台-大厅-终端-版本
        insert into table dim.user_currencies_balance_array
        partition(dt="%(statdate)s")
                select '%(statdate)s' fdate
                       ,fgamefsk
                       ,fplatformfsk
                       ,fhallfsk
                       ,-21379 fsubgamefsk
                       ,fterminaltypefsk
                       ,fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,fcurrencies_type
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fuid,
                               fcurrencies_type,
                               fcurrencies_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk, fcurrencies_type,fuid order by fdate desc, fcurrencies_num desc) rown
                        from dim.user_currencies_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'

                        union all

                        select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fuid,
                               fcurrencies_type,
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '11'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fuid
                          ,fcurrencies_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--游戏-平台-终端-版本
        insert into table dim.user_currencies_balance_array
        partition(dt="%(statdate)s")
                select '%(statdate)s' fdate
                       ,fgamefsk
                       ,fplatformfsk
                       ,-21379 fhallfsk
                       ,-21379 fsubgamefsk
                       ,fterminaltypefsk
                       ,fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,fcurrencies_type
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fuid,
                               fcurrencies_type,
                               fcurrencies_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fterminaltypefsk,fversionfsk,fcurrencies_type,fuid order by fdate desc, fcurrencies_num desc) rown
                        from dim.user_currencies_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 0
                        where dt = '%(statdate)s'

                        union all

                        select fgamefsk,
                               fplatformfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fuid,
                               fcurrencies_type,
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fterminaltypefsk,fversionfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 0
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '0'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fuid
                          ,fcurrencies_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

a = load_user_currencies_balance_array(sys.argv[1:])
a()
