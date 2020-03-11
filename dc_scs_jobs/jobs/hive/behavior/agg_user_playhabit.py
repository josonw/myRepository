# -*- coding: UTF-8 -*-
#src     :dim.user_act,dim.user_pay,dim.reg_user_main_additional,dim.bpid_map
#dst     :dcnew.user_playhabit
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
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_user_playhabit(BaseStatModel):
    def create_tab(self):

#将周和月数据分开，先计算周数据，然后计算月数据，提高速度
#
#本脚本计算z周数据，月数据为 agg_user_playhabit_mon

        hql = """--各类用户，活跃天数分布
        create table if not exists dcnew.user_playhabit (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fdate_range         string     comment '数据周期:week,month',
               actdays             bigint     comment '数据周期内的活跃天数(不连续)',
               fact_unum           bigint     comment '对应天数的活跃用户数',
               freg_unum           bigint     comment '对应天数的新增用户数',
               fpay_unum           bigint     comment '对应天数的付费用户数',
               fmatch_unum         bigint     comment '对应天数的比赛用户数'
               )comment '游戏习惯_游戏频次'
               partitioned by(dt date)
        location '/dw/dcnew/user_playhabit'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fdate_range','fuid','uid_reg','uid_pay','uid_match'],
                        'groups':[[1,1,1,1,1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0: return res

        hql = """--取各类用户,活跃天数分布
        drop table if exists work.user_playhabit_tmp_%(statdatenum)s;
        create table work.user_playhabit_tmp_%(statdatenum)s as
               select  /*+ MAPJOIN(d) */ distinct d.fgamefsk,
                      d.fplatformfsk,
                      d.fhallfsk,
                      d.fterminaltypefsk,
                      d.fversionfsk,
                      d.hallmode,
                      a.fgame_id,
                      a.fchannel_code,
                      'week' fdate_range,
                      a.dt,
                      a.fuid,
                      c.fuid uid_reg,
                      b.fuid uid_pay,
                      m.fuid uid_match
                 from dim.user_act a
                 join dim.bpid_map d
                   on a.fbpid = d.fbpid
                 left outer join (select distinct fbpid, fuid
                                    from dim.user_pay_day b
                                   where b.dt >= '%(ld_6day_ago)s'
                                     and b.dt <= '%(statdate)s'
                                 ) b
                   on a.fbpid = b.fbpid
                  and a.fuid = b.fuid
                 left outer join dim.reg_user_main_additional c
                   on a.fbpid = c.fbpid
                  and a.fuid = c.fuid
                  and c.dt >= '%(ld_6day_ago)s'
                  and c.dt <= '%(statdate)s'
                 left outer join (select distinct fbpid,fuid from stage.user_gameparty_stg
                                   where dt >= '%(ld_6day_ago)s'
                                     and dt <= '%(statdate)s'
                                     and fmatch_id <> '0'
                                     and fmatch_id is not null
                      ) m
                   on a.fbpid = m.fbpid
                  and a.fuid = m.fuid
                where a.dt >= '%(ld_6day_ago)s'
                  and a.dt <= '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--汇总不同组合下的活跃天数等数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fdate_range,
                       count(distinct dt) actdays,
                       fuid,
                       uid_reg,
                       uid_pay,
                       uid_match
                  from work.user_playhabit_tmp_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,hallmode,
                          fdate_range,
                          fuid,
                          uid_reg,
                          uid_pay,
                          uid_match

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.user_playhabit_tmp_s_%(statdatenum)s;
        create table work.user_playhabit_tmp_s_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--插入目标表
         insert overwrite table dcnew.user_playhabit partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fdate_range,
                       actdays,
                       count(distinct fuid) fact_unum,
                       count(distinct uid_reg) freg_unum,
                       count(distinct uid_pay) fpay_unum,
                       count(distinct uid_match) fmatch_unum
                  from work.user_playhabit_tmp_s_%(statdatenum)s gs
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                          fdate_range,
                          actdays
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_playhabit_tmp_%(statdatenum)s;
                 drop table if exists work.user_playhabit_tmp_s_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_playhabit(sys.argv[1:])
a()
