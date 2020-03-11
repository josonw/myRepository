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
该脚本每天都需要计算一份全量的游戏币结余信息
每天都依赖昨天的量，所以必须是自依赖关系
"""

class load_user_gamecoin_balance_array(BaseStatModel):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists dim.user_gamecoin_balance_array
        (
          fgamefsk             bigint,
          fplatformfsk         bigint,
          fhallfsk             bigint,
          fsubgamefsk          bigint,
          fterminaltypefsk     bigint,
          fversionfsk          bigint,
          fchannelcode         bigint,
          fuid                 bigint,              --用户游戏ID
          fgamecoins_num       bigint,              --用户游戏币携带
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
        # 不加user_gamecoins_num >= 0条件，加了每天的量会少200w左右
        # 但是加了之后，这个表可用的地方就非常有限了

        hql = """--游戏-平台
        insert overwrite table dim.user_gamecoin_balance_array
        partition(dt="%(statdate)s")
                select fgamefsk
                       ,fplatformfsk
                       ,-21379 fhallfsk
                       ,-21379 fsubgamefsk
                       ,-21379 fterminaltypefsk
                       ,-21379 fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fuid,
                               user_gamecoins_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fuid order by fdate desc, user_gamecoins_num desc) rown
                        from dim.user_gamecoin_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                        where dt = '%(statdate)s'

                        union all

                        select fgamefsk,
                               fplatformfsk,
                               fuid,
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '0'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--游戏-平台-大厅
        insert into table dim.user_gamecoin_balance_array
        partition(dt="%(statdate)s")
                select fgamefsk
                       ,fplatformfsk
                       ,fhallfsk
                       ,-21379 fsubgamefsk
                       ,-21379 fterminaltypefsk
                       ,-21379 fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fuid,
                               user_gamecoins_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fuid order by fdate desc, user_gamecoins_num desc) rown
                        from dim.user_gamecoin_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'

                        union all

                        select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fuid,
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '0'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--游戏-平台-大厅-终端-版本
        insert into table dim.user_gamecoin_balance_array
        partition(dt="%(statdate)s")
                select fgamefsk
                       ,fplatformfsk
                       ,fhallfsk
                       ,-21379 fsubgamefsk
                       ,fterminaltypefsk
                       ,fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fuid,
                               user_gamecoins_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fuid order by fdate desc, user_gamecoins_num desc) rown
                        from dim.user_gamecoin_balance t1
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
                               0 fgamecoins_num,
                               fbank_gamecoins_num fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fuid order by fdate desc, fbank_gamecoins_num desc) rown
                        from dim.user_bank_balance t1
                        join dim.bpid_map t2
                          on t1.fbpid = t2.fbpid
                         and t2.hallmode = 1
                        where dt = '%(statdate)s'
                          and coalesce(fcurrencies_type,'0') = '0'
                       ) t
                 where rown = 1
                 group by fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--游戏-平台-终端-版本
        insert into table dim.user_gamecoin_balance_array
        partition(dt="%(statdate)s")
                select fgamefsk
                       ,fplatformfsk
                       ,-21379 fhallfsk
                       ,-21379 fsubgamefsk
                       ,fterminaltypefsk
                       ,fversionfsk
                       ,-21379 fchannelcode
                       ,fuid
                       ,sum(fgamecoins_num) fgamecoins_num
                       ,sum(fbank_gamecoins_num) fbank_gamecoins_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fuid,
                               user_gamecoins_num fgamecoins_num,
                               0 fbank_gamecoins_num,
                               row_number() over(partition by fgamefsk, fplatformfsk,fterminaltypefsk,fversionfsk,fuid order by fdate desc, user_gamecoins_num desc) rown
                        from dim.user_gamecoin_balance t1
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
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


a = load_user_gamecoin_balance_array(sys.argv[1:])
a()
