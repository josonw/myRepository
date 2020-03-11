# -*- coding: UTF-8 -*-
#src     :dim.user_act,dim.bpid_map
#dst     :dcnew.user_true_act_tmp
#authot  :SimonRen
#date    :2016-09-06


# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel_bak import BaseStatModel
import service.sql_const as sql_const


class agg_user_true_act_tmp(BaseStatModel):
    def create_tab(self):

        hql = """--用户阶段活跃数
        create table if not exists dcnew.user_true_act_tmp (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fact_unum           bigint     comment '当日活跃',
               f3dayact_unum       bigint     comment '三日内活跃',
               f7dayact_unum       bigint     comment '7日内活跃',
               f14dayact_unum      bigint     comment '14日内活跃',
               f30dayact_unum      bigint     comment '30日内活跃',
               fweekact_unum       bigint     comment '本自然周内活跃',
               fmonthact_unum      bigint     comment '本自然月内活跃'
               )comment '用户阶段活跃数'
               partitioned by(dt date)
        location '/dw/dcnew/user_true_act_tmp'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--
        drop table if exists work.user_true_act_tmp_tmp_%(statdatenum)s;
        create table work.user_true_act_tmp_tmp_%(statdatenum)s as
               select d.fgamefsk,
                      d.fplatformfsk,
                      d.fhallfsk,
                      d.fterminaltypefsk,
                      d.fversionfsk,
                      d.hallmode,
                      a.fgame_id,
                      a.fchannel_code,
                      a.dt,
                      a.fuid
                 from (select fbpid, fuid, fgame_id, fchannel_code, max(dt) dt
                         from dim.user_act
                        where dt > '%(ld_30day_ago)s'
                          and dt <= '%(statdate)s'
                          and fuid is not null
                        group by fbpid, fuid, fgame_id, fchannel_code
                      ) a
                 join dim.bpid_map d
                   on a.fbpid = d.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       count(distinct case when dt='%(statdate)s' then fuid end) fact_unum,
                       count(distinct case when dt>='%(ld_2day_ago)s' and dt <= '%(statdate)s' then fuid end) f3dayact_unum,
                       count(distinct case when dt>='%(ld_6day_ago)s' and dt <= '%(statdate)s' then fuid end) f7dayact_unum,
                       count(distinct case when dt>'%(ld_14day_ago)s' and dt <= '%(statdate)s' then fuid end) f14dayact_unum,
                       count(distinct case when dt>'%(ld_30day_ago)s' and dt <= '%(statdate)s' then fuid end) f30dayact_unum,
                       count(distinct case when dt>='%(ld_week_begin)s' and dt <= '%(statdate)s' then fuid end) fweekact_unum,
                       count(distinct case when dt>='%(ld_month_begin)s' and dt <= '%(statdate)s' then fuid end) fmonthact_unum
                  from work.user_true_act_tmp_tmp_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,hallmode

        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.user_true_act_tmp
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_true_act_tmp_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_true_act_tmp(sys.argv[1:])
a()
