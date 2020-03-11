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
    用户最后一次登录及携带
"""

class load_user_login_last(BaseStatModel):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists dim.user_login_last
        ( fdate                string,
          fbpid                varchar(50),         --BPID
          fgamefsk             bigint,
          fplatformfsk         bigint,
          fhallfsk             bigint,
          fsubgamefsk          bigint,
          fterminaltypefsk     bigint,
          fversionfsk          bigint,
          fchannelcode         bigint,
          fuid                 bigint,         --用户游戏ID
          flogin_at            string,         --用户货币类型
          user_gamecoins       bigint          --用户货币携带
        )
        partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """

        hql = """
        insert overwrite table dim.user_login_last
        partition(dt="%(statdate)s")
                select flogin_at fdate
                       ,tt.fbpid
                       ,tt.fgamefsk
                       ,tt.fplatformfsk
                       ,tt.fhallfsk
                       ,-21379 fsubgamefsk
                       ,tt.fterminaltypefsk
                       ,tt.fversionfsk
                       ,-21379 fchannelcode
                       ,t1.fuid
                       ,t1.flogin_at
                       ,t1.user_gamecoins
                  from (select fbpid,
                                fuid,
                                flogin_at,
                                user_gamecoins,
                                row_number() over(partition by fbpid,fuid order by flogin_at desc) row_num
                           from (select fbpid,
                                        fuid,
                                        flogin_at,
                                        user_gamecoins
                                   from stage.user_login_stg t1
                                  where dt = '%(statdate)s'

                                 union all

                                 select fbpid,
                                        fuid,
                                        flogin_at,
                                        user_gamecoins
                                   from dim.user_login_last
                                  where dt = date_sub('%(statdate)s',1)
                                ) t
                       ) t1
                  join dim.bpid_map tt
                    on t1.fbpid= tt.fbpid
                 where t1.row_num = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

a = load_user_login_last(sys.argv[1:])
a()
