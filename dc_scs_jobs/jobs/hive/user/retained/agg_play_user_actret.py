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

class agg_play_user_actret(BaseStatModel):
    def create_tab(self):
        """玩牌留存用户统计"""

        hql = """create table if not exists dcnew.play_user_actret
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            f1daycnt bigint,
            f2daycnt bigint,
            f3daycnt bigint,
            f4daycnt bigint,
            f5daycnt bigint,
            f6daycnt bigint,
            f7daycnt bigint,
            f14daycnt bigint,
            f30daycnt bigint,
            f60daycnt bigint,
            f90daycnt bigint
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['dt', "retday"],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.play_user_actret_tmp_b_%(statdatenum)s;
        create table work.play_user_actret_tmp_b_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               c.hallmode,
                               uai.fgame_id,
                               uai.fchannel_code,
                               uai.dt,
                               datediff('%(statdate)s', uai.dt) retday,
                               uai.fuid
                          from dim.user_act uai
                          join dim.user_act b
                            on uai.fuid = b.fuid
                           and uai.fbpid = b.fbpid
                           and b.dt = '%(statdate)s'
                           and b.fparty_num > 0
                          join dim.bpid_map c
                            on uai.fbpid = c.fbpid
                         where uai.dt >= '%(ld_90day_ago)s'
                           and uai.dt <= '%(statdate)s'
                           and uai.fparty_num>0
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select dt fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       retday,
                       count(distinct fuid) fdregrtducnt
                  from work.play_user_actret_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       dt, retday
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.play_user_actret_tmp_%(statdatenum)s;
        create table work.play_user_actret_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.play_user_actret
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
            max(f1daycnt) f1daycnt,
            max(f2daycnt) f2daycnt,
            max(f3daycnt) f3daycnt,
            max(f4daycnt) f4daycnt,
            max(f5daycnt) f5daycnt,
            max(f6daycnt) f6daycnt,
            max(f7daycnt) f7daycnt,
            max(f14daycnt) f14daycnt,
            max(f30daycnt) f30daycnt,
            max(f60daycnt) f60daycnt,
            max(f90daycnt) f90daycnt,
            fdate dt
        from (
            select fdate,fgamefsk,fplatformfsk,fhallfsk,fgame_id fsubgamefsk,fterminaltypefsk,fversionfsk,fchannel_code fchannelcode,
                if( retday=1, fdregrtducnt, 0 ) f1daycnt,
                if( retday=2, fdregrtducnt, 0 ) f2daycnt,
                if( retday=3, fdregrtducnt, 0 ) f3daycnt,
                if( retday=4, fdregrtducnt, 0 ) f4daycnt,
                if( retday=5, fdregrtducnt, 0 ) f5daycnt,
                if( retday=6, fdregrtducnt, 0 ) f6daycnt,
                if( retday=7, fdregrtducnt, 0 ) f7daycnt,
                if( retday=14, fdregrtducnt, 0 ) f14daycnt,
                if( retday=30, fdregrtducnt, 0 ) f30daycnt,
                if( retday=60, fdregrtducnt, 0 ) f60daycnt,
                if( retday=90, fdregrtducnt, 0 ) f90daycnt
            from  work.play_user_actret_tmp_%(statdatenum)s
            union all
            SELECT fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                   f1daycnt,
                   f2daycnt,
                   f3daycnt,
                   f4daycnt,
                   f5daycnt,
                   f6daycnt,
                   f7daycnt,
                   f14daycnt,
                   f30daycnt,
                   f60daycnt,
                   f90daycnt
            FROM dcnew.play_user_actret
            WHERE dt >= '%(ld_90day_ago)s'
              AND dt <= '%(statdate)s'
                    ) tmp group by fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.play_user_actret partition(dt='3000-01-01')
        select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
               f1daycnt,
               f2daycnt,
               f3daycnt,
               f4daycnt,
               f5daycnt,
               f6daycnt,
               f7daycnt,
               f14daycnt,
               f30daycnt,
               f60daycnt,
               f90daycnt
        from dcnew.play_user_actret
        where dt >= '%(ld_90day_ago)s'
        and dt <= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.play_user_actret_tmp_b_%(statdatenum)s;
                 drop table if exists work.play_user_actret_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_play_user_actret(sys.argv[1:])
a()
