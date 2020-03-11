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

class agg_bankrupt_user(BaseStatModel):
    """ 每日破产用户统计 """
    def create_tab(self):
        hql = """create table if not exists dcnew.bankrupt_user
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fgroupingid int,
            fdbrpucnt bigint
        ) partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': [],
                        'groups': []}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0: return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(cast (fchannel_code as bigint),%(null_int_group_rule)d) fchannel_code,
                       -1 fgroupingid,
                       count(distinct gs.fuid) fdbrpucnt
                  from (select fbpid,
                               fuid,
                               frupt_at,
                               fhas_relieve,
                               fuser_grade,
                               coalesce(frelieve_game_coin,0) frelieve_game_coin,
                               coalesce(frelieve_cnt,0) frelieve_cnt,
                               coalesce(fuphill_pouring,0) fuphill_pouring,
                               fplayground_title,
                               fversion_info,
                               fchannel_code,
                               fvip_type,
                               coalesce(fvip_level,0) fvip_level,
                               coalesce(flevel,0) flevel,
                               fpname,
                               fscene,
                               coalesce(fgame_id,cast (0 as bigint)) fgame_id
                          from stage.user_bankrupt_stg
                         where dt = '%(statdate)s'
                        ) gs
                  join dim.bpid_map c
                    on gs.fbpid=c.fbpid
                   and hallmode = %(hallmode)s
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.bankrupt_user
        partition (dt = "%(statdate)s")
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

#生成统计实例
a = agg_bankrupt_user(sys.argv[1:])
a()
