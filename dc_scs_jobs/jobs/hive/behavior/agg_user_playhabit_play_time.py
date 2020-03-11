# -*- coding: UTF-8 -*-
#src     :stage.user_daytime_stg,dim.user_pay,dim.bpid_map
#dst     :dcnew.loss_user_rupt_dis
#authot  :SimonRen
#date    :2016-09-02


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


class agg_user_playhabit_play_time(BaseStatModel):
    def create_tab(self):

        hql = """--游戏用户游戏时长分布
        create table if not exists dcnew.user_playhabit_play_time (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fdate_range         string     comment '数据周期:day',
               fonline_time        string     comment '平均游戏时长:0-5分钟,5-15分钟,15-30分钟,30-60分钟,1-2小时,2-4小时,4-8小时,8-24小时',
               fact_unum           bigint     comment '对应时长的活跃用户',
               freg_unum           bigint     comment '对应时长的新增用户_留空',
               fpay_unum           bigint     comment '对应时长的付费用户',
               fcmp_unum           bigint     comment '对应时长的比赛用户_留空',
               fplay_unum          bigint     comment '对应时长的玩牌用户',
               fpplay_unum         bigint     comment '对应时长的付费玩牌用户'
               )comment '游戏习惯游戏时长'
               partitioned by(dt date)
        location '/dw/dcnew/user_playhabit_play_time'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fonline_time'],
                        'groups':[[1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.habit_play_time_tmp_b_%(statdatenum)s;
        create table work.habit_play_time_tmp_b_%(statdatenum)s as
         select c.fgamefsk,
                c.fplatformfsk,
                c.fhallfsk,
                c.fterminaltypefsk,
                c.fversionfsk,
                c.hallmode,
                coalesce(aa.fgame_id,%(null_int_report)d) fgame_id,
                coalesce(aa.fchannel_code,%(null_int_report)d) fchannel_code,
                case when fseconds >= 0 and fseconds < 300 then '0-5分钟'
                     when fseconds >= 300 and fseconds < 900 then  '5-15分钟'
                     when fseconds >= 900 and fseconds < 1800 then '15-30分钟'
                     when fseconds >= 1800 and fseconds < 3600 then '30-60分钟'
                     when fseconds >= 3600 and fseconds < 7200 then '1-2小时'
                     when fseconds >= 7200 and fseconds < 14400 then '2-4小时'
                     when fseconds >= 14400 and fseconds < 28800 then '4-8小时'
                else '8-24小时' end fonline_time,
                a.fuid,
                b.fplatform_uid
           from (select fbpid, fuid, sum(fseconds) fseconds
                   from stage.user_daytime_stg a
                  where a.dt = '%(statdate)s'
                    and length(fday_at)=10
                  group by fbpid, fuid
                ) a
           left join dim.user_act aa
             on aa.fbpid = a.fbpid
            and aa.fuid = a.fuid
            and aa.dt = '%(statdate)s'
           left join dim.user_pay b
             on a.fbpid = b.fbpid
            and a.fuid = b.fuid
           join dim.bpid_map c
             on a.fbpid = c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据并导入到正式表上
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fonline_time,
                       count(distinct fuid) fact_unum,
                       0 freg_unum,
                       count(distinct fplatform_uid) fpay_unum,
                       0 fcmp_unum,
                       0 fplay_unum,
                       0 fpplay_unum
                  from work.habit_play_time_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fonline_time

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.habit_play_time_tmp_%(statdatenum)s;
        create table work.habit_play_time_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.habit_play_time_tmp_p_%(statdatenum)s;
        create table work.habit_play_time_tmp_p_%(statdatenum)s as
         select c.fgamefsk,
                c.fplatformfsk,
                c.fhallfsk,
                c.fterminaltypefsk,
                c.fversionfsk,
                c.hallmode,
                coalesce(aa.fgame_id,%(null_int_report)d) fgame_id,
                coalesce(aa.fchannel_code,%(null_int_report)d) fchannel_code,
                case when fseconds >= 0 and fseconds < 300 then '0-5分钟'
                     when fseconds >= 300 and fseconds < 900 then  '5-15分钟'
                     when fseconds >= 900 and fseconds < 1800 then '15-30分钟'
                     when fseconds >= 1800 and fseconds < 3600 then '30-60分钟'
                     when fseconds >= 3600 and fseconds < 7200 then '1-2小时'
                     when fseconds >= 7200 and fseconds < 14400 then '2-4小时'
                     when fseconds >= 14400 and fseconds < 28800 then '4-8小时'
                else '8-24小时' end fonline_time,
                a.fuid,
                case when b.fuid is not null then 1 else 0 end is_pay
           from (select fbpid, fuid, sum(fplay_time) fseconds
                   from dim.user_gameparty a
                  where a.dt = '%(statdate)s'
                  group by fbpid, fuid
                ) a
           left join dim.user_act aa
             on aa.fbpid = a.fbpid
            and aa.fuid = a.fuid
            and aa.dt = '%(statdate)s'
           left join dim.user_pay b
             on a.fbpid = b.fbpid
            and a.fuid = b.fuid
           join dim.bpid_map c
             on a.fbpid = c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据并导入到正式表上
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fonline_time,
                       0 fact_unum,
                       0 freg_unum,
                       0 fpay_unum,
                       0 fcmp_unum,
                       count(distinct fuid) fplay_unum,
                       count(distinct case when is_pay = 1 then fuid end) fpplay_unum
                  from work.habit_play_time_tmp_p_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fonline_time

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table work.habit_play_time_tmp_%(statdatenum)s
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.user_playhabit_play_time
        partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       'day' fdate_range,
                       fonline_time,
                       max(fact_unum) fact_unum,
                       max(freg_unum) freg_unum,
                       max(fpay_unum) fpay_unum,
                       max(fcmp_unum) fcmp_unum,
                       max(fplay_unum) fplay_unum,
                       max(fpplay_unum) fpplay_unum
                  from work.habit_play_time_tmp_%(statdatenum)s gs
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fonline_time
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.habit_play_time_tmp_b_%(statdatenum)s;
                 drop table if exists work.habit_play_time_tmp_p_%(statdatenum)s;
                 drop table if exists work.habit_play_time_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_playhabit_play_time(sys.argv[1:])
a()
