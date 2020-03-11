# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_act_paid_array(BaseStatModel):
    def create_tab(self):

        hql = """-- 历史活跃用户组合
        create table if not exists dim.user_act_paid_array (
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannel_code       bigint,
               fuid                bigint  comment  '用户ID'
               )comment '历史活跃用户组合'
               partitioned by(dt string)
        location '/dw/dim/user_act_paid_array'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuid'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
                select t2.fgamefsk,
                       coalesce(t2.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(t2.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(t1.fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(t2.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(t2.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(t1.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       t1.fuid
                  from dim.user_act_paid t1
                  join dim.bpid_map t2
                    on t1.fbpid = t2.fbpid
                   and t2.hallmode = %(hallmode)s
                 where t1.dt = "%(statdate)s"
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dim.user_act_paid_array
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


# 生成统计实例
a = load_user_act_paid_array(sys.argv[1:])
a()
