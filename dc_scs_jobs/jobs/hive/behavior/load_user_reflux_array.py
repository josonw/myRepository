#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const

class load_user_reflux_array(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dim.user_reflux_array
              (fgamefsk             bigint      comment '游戏ID',
               fplatformfsk         bigint      comment '平台ID',
               fhallfsk             bigint      comment '大厅ID',
               fgame_id             bigint      comment '子游戏ID',
               fterminaltypefsk     bigint      comment '终端ID',
               fversionfsk          bigint      comment '版本ID',
               fchannel_code        bigint      comment '渠道ID',
               fuid                 bigint      comment '用户ID',
               is_pay               int         comment '是否付费',
               gamecoins            bigint      comment '资产',
               is_play              int         comment '回流当日是否玩牌',
               fgrade               int         comment '等级',
               fparty_num           bigint      comment '牌局数',
               fage                 int         comment '游戏年龄',
               freflux              bigint      comment '回流天数，例如7表示7日回流',
               freflux_type         varchar(32) comment '回流类型，cycle不考虑freflux天的前一天是否活跃，day表示freflux天的前一天必须活跃'
            )
            partitioned by (dt date);
        """

        res = self.hq.exe_sql(hql)
        return res


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'ld_30dayago': PublicFunc.add_days(statDate, -30),
            'group_by_fuid_no_sub':sql_const.HQL_GROUP_BY_FUID_NO_SUB_GAME % {'bpid_tbl_alias':'bm.','src_tbl_alias':'uam.'},
            'group_by':sql_const.HQL_GROUP_BY_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'},
            'group_by_no_sub_game':sql_const.HQL_GROUP_BY_NO_SUB_GAME % alias_dic,
            'group_by_include_sub_game':sql_const.HQL_GROUP_BY_INCLUDE_SUB_GAME % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT,
            'statdatenum':self.stat_date.replace("-", "")
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        date = PublicFunc.date_define(self.stat_date)
        query.update(date)

        hql = """--cycle回流用户
        drop table if exists work.reflux_cycle_tmp_1_%(statdatenum)s;
        create table work.reflux_cycle_tmp_1_%(statdatenum)s
        as
        select fdate,fbpid, fuid, flast_atv_date
          from ( -- 当天的活跃用户，如果最近30天活跃，取最近活跃时间，否则取31天之前那天
                 -- 为用户的最后活跃时间
                select cast('%(ld_daybegin)s' as date) fdate,fbpid,fuid,
                        max(flast_atv_date) flast_atv_date, max(is_ok) is_ok
                  from (select fbpid,fuid,fparty_num, cast ('%(ld_31dayago)s' as date) flast_atv_date, 1 is_ok
                          from dim.user_act
                         where dt =  '%(ld_daybegin)s'
                         union all
                        select fbpid,fuid,0 fparty_num, dt flast_atv_date, 0 is_ok
                          from dim.user_act
                         where dt >= '%(ld_30dayago)s' and dt < '%(ld_daybegin)s'
                         union all
                        select fbpid,fuid,0 fparty_num, cast(dt as date) flast_atv_date, 0 is_ok
                          from dim.reg_user_main_additional
                         where dt >= '%(ld_30dayago)s' and dt <= '%(ld_daybegin)s'
                       ) t
                 group by fbpid,fuid
               ) tt
         where is_ok = 1;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """--当日回流用户属性
        drop table if exists work.reflux_user_tmp_0_%(statdatenum)s;
        create table work.reflux_user_tmp_0_%(statdatenum)s
        as
        select aa.fbpid,
               aa.fuid,
               aa.fgame_id,
               aa.fchannel_code,
               case when aaa.fuid is not null then 1 else 0 end is_pay,
               c.fgamecoins gamecoins,
               case when b.fuid is not null then 1 else 0 end is_play,
               d.fgrade,
               aa.fparty_num,
               datediff('%(ld_daybegin)s', to_date(nvl(e.fsignup_at,'1970-01-01'))) fage
          from dim.user_act aa
          left join dim.user_pay aaa
            on aa.fbpid = aaa.fbpid
           and aa.fuid = aaa.fuid
          left join dim.user_gameparty b
            on aa.fbpid = b.fbpid
           and aa.fuid = b.fuid
           and b.dt = '%(ld_daybegin)s'
          left join (select fbpid, fuid,fgamecoins
                       from (select fbpid, fuid,fgamecoins,
                                    row_number() over(partition by fbpid, fuid order by fdate desc, fgamecoins desc) rown
                               from dim.user_gamecoin_balance_day c
                              where dt = '%(ld_daybegin)s'
                            ) t
                      where rown = 1
                    ) c
            on aa.fbpid = c.fbpid
           and aa.fuid = c.fuid
          left join dim.user_attribute d
            on aa.fbpid = d.fbpid
           and aa.fuid = d.fuid
          left join dim.reg_user_main_additional e
            on aa.fbpid = e.fbpid
           and aa.fuid = e.fuid
         where aa.dt = '%(ld_daybegin)s';
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """--cycle回流用户及相关属性
        drop table if exists work.reflux_cycle_tmp_2_%(statdatenum)s;
        create table work.reflux_cycle_tmp_2_%(statdatenum)s
        as
        select /*+ MAPJOIN(f) */ f.fgamefsk,
               f.fplatformfsk,
               f.fhallfsk,
               f.fterminaltypefsk,
               f.fversionfsk,
               f.hallmode,
               coalesce(aa.fgame_id,%(null_int_report)d) fgame_id,
               coalesce(aa.fchannel_code,%(null_int_report)d) fchannel_code,
               a.fuid,
               a.flast_atv_date,
               aa.is_pay,
               aa.gamecoins,
               aa.is_play,
               aa.fgrade,
               aa.fparty_num,
               aa.fage
          from work.reflux_cycle_tmp_1_%(statdatenum)s a
          join work.reflux_user_tmp_0_%(statdatenum)s aa
            on a.fbpid = aa.fbpid
           and a.fuid = aa.fuid
          join dim.bpid_map f
            on a.fbpid=f.fbpid;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """--组合计算
        drop table if exists work.reflux_cycle_tmp_3_%(statdatenum)s;
        create table work.reflux_cycle_tmp_3_%(statdatenum)s
        as
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                is_pay,
                gamecoins,
                is_play,
                fgrade,
                fparty_num,
                fage,
                max(flast_atv_date) flast_atv_date
           from work.reflux_cycle_tmp_2_%(statdatenum)s gs
          where hallmode = 0
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage
       grouping sets ((fgamefsk,fplatformfsk,fterminaltypefsk,fversionfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage));


       insert into table work.reflux_cycle_tmp_3_%(statdatenum)s
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                is_pay,
                gamecoins,
                is_play,
                fgrade,
                fparty_num,
                fage,
                max(flast_atv_date) flast_atv_date
           from work.reflux_cycle_tmp_2_%(statdatenum)s gs
          where hallmode = 1
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage
       grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fgame_id,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage));

       insert into table work.reflux_cycle_tmp_3_%(statdatenum)s
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                is_pay,
                gamecoins,
                is_play,
                fgrade,
                fparty_num,
                fage,
                max(flast_atv_date) flast_atv_date
           from work.reflux_cycle_tmp_2_%(statdatenum)s gs
          where hallmode = 1
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage
       grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fhallfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage));
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """--插入目标表
        insert overwrite table dim.user_reflux_array
        partition (dt='%(ld_daybegin)s')
        select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
               fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage,
               freflux,
               'cycle' freflux_type
        from
        (
            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 2 freflux
            from work.reflux_cycle_tmp_3_%(statdatenum)s
            where flast_atv_date < '%(ld_2dayago)s'

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 5 freflux
            from work.reflux_cycle_tmp_3_%(statdatenum)s
            where flast_atv_date < '%(ld_5dayago)s'

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 7 freflux
            from work.reflux_cycle_tmp_3_%(statdatenum)s
            where flast_atv_date < '%(ld_7dayago)s'

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 14 freflux
            from work.reflux_cycle_tmp_3_%(statdatenum)s
            where flast_atv_date < '%(ld_14dayago)s'

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 30 freflux
            from work.reflux_cycle_tmp_3_%(statdatenum)s
            where flast_atv_date < '%(ld_30dayago)s'
        ) tmp ;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """--day回流用户及相关属性
        drop table if exists work.reflux_day_tmp_0_%(statdatenum)s;
        create table if not exists work.reflux_day_tmp_0_%(statdatenum)s
        as
        select fbpid,fgame_id, fchannel_code,fuid,
               max(if(a.dt = '%(ld_daybegin)s' , 1, 0 )) begin_day,
               max(if(a.dt >= '%(ld_30dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 )) dayago30_h,
               max(if(a.dt >= '%(ld_14dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 )) dayago14_h,
               max(if(a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 )) dayago7_h,
               max(if(a.dt >= '%(ld_5dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 )) dayago5_h,
               max(if(a.dt >= '%(ld_2dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 )) dayago2_h,
               max(if(a.dt >= '%(ld_3dayago)s' and a.dt < '%(ld_2dayago)s' , 1, 0 )) dayago2,
               max(if(a.dt >= '%(ld_6dayago)s' and a.dt < '%(ld_5dayago)s' , 1, 0 )) dayago5,
               max(if(a.dt >= '%(ld_8dayago)s' and a.dt < '%(ld_7dayago)s' , 1, 0 )) dayago7,
               max(if(a.dt >= '%(ld_15dayago)s' and a.dt < '%(ld_14dayago)s' , 1, 0 )) dayago14,
               max(if(a.dt >= '%(ld_31dayago)s' and a.dt < '%(ld_30dayago)s' , 1, 0 )) dayago30
          from dim.user_act a
         where dt >= '%(ld_31dayago)s' and dt < '%(ld_dayend)s'
         group by fbpid,fgame_id, fchannel_code,fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res



        hql = """--day回流用户及相关属性
        drop table if exists work.reflux_day_tmp_1_%(statdatenum)s;
        create table if not exists work.reflux_day_tmp_1_%(statdatenum)s
        as
        select /*+ MAPJOIN(f) */ f.fgamefsk,
               f.fplatformfsk,
               f.fhallfsk,
               f.fterminaltypefsk,
               f.fversionfsk,
               f.hallmode,
               coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
               coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
               a.fuid,
               aa.is_pay,
               aa.gamecoins,
               aa.is_play,
               aa.fgrade,
               aa.fparty_num,
               aa.fage,
               begin_day,
               dayago2_h,
               dayago5_h,
               dayago7_h,
               dayago14_h,
               dayago30_h,
               dayago2,
               dayago5,
               dayago7,
               dayago14,
               dayago30
          from work.reflux_day_tmp_0_%(statdatenum)s a
          join work.reflux_user_tmp_0_%(statdatenum)s aa
            on a.fbpid = aa.fbpid
           and a.fuid = aa.fuid
          join dim.bpid_map f
            on a.fbpid=f.fbpid;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """--组合计算
        drop table if exists work.reflux_day_tmp_2_%(statdatenum)s;
        create table work.reflux_day_tmp_2_%(statdatenum)s
        as
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                is_pay,
                gamecoins,
                is_play,
                fgrade,
                fparty_num,
                fage,
                max(begin_day) begin_day,
                max(dayago2_h) dayago2_h,
                max(dayago5_h) dayago5_h,
                max(dayago7_h) dayago7_h,
                max(dayago14_h) dayago14_h,
                max(dayago30_h) dayago30_h,
                max(dayago2) dayago2,
                max(dayago5) dayago5,
                max(dayago7) dayago7,
                max(dayago14) dayago14,
                max(dayago30) dayago30
           from work.reflux_day_tmp_1_%(statdatenum)s gs
          where hallmode = 0
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage
       grouping sets ((fgamefsk,fplatformfsk,fterminaltypefsk,fversionfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage));


       insert into table work.reflux_day_tmp_2_%(statdatenum)s
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                is_pay,
                gamecoins,
                is_play,
                fgrade,
                fparty_num,
                fage,
                max(begin_day) begin_day,
                max(dayago2_h) dayago2_h,
                max(dayago5_h) dayago5_h,
                max(dayago7_h) dayago7_h,
                max(dayago14_h) dayago14_h,
                max(dayago30_h) dayago30_h,
                max(dayago2) dayago2,
                max(dayago5) dayago5,
                max(dayago7) dayago7,
                max(dayago14) dayago14,
                max(dayago30) dayago30
           from work.reflux_day_tmp_1_%(statdatenum)s gs
          where hallmode = 1
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage
       grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fgame_id,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage));

       insert into table work.reflux_day_tmp_2_%(statdatenum)s
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                is_pay,
                gamecoins,
                is_play,
                fgrade,
                fparty_num,
                fage,
                max(begin_day) begin_day,
                max(dayago2_h) dayago2_h,
                max(dayago5_h) dayago5_h,
                max(dayago7_h) dayago7_h,
                max(dayago14_h) dayago14_h,
                max(dayago30_h) dayago30_h,
                max(dayago2) dayago2,
                max(dayago5) dayago5,
                max(dayago7) dayago7,
                max(dayago14) dayago14,
                max(dayago30) dayago30
           from work.reflux_day_tmp_1_%(statdatenum)s gs
          where hallmode = 1
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage
       grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fhallfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage),
                      (fgamefsk,fplatformfsk,fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage));

        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        -- 现在进行流失回流用户插入。
        insert into table dim.user_reflux_array
        partition(dt='%(ld_daybegin)s')
        select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
               fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage,
               freflux, 'day' freflux_type
        from
        (
            -- 当天活跃，30天之前活跃，两者之间不活跃
            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 30 freflux
            from work.reflux_day_tmp_2_%(statdatenum)s
            where begin_day=1 and dayago30_h=0 and dayago30=1

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 14 freflux
            from work.reflux_day_tmp_2_%(statdatenum)s
            where begin_day=1 and dayago14_h=0 and dayago14=1

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 7 freflux
            from work.reflux_day_tmp_2_%(statdatenum)s
            where begin_day=1 and dayago7_h=0 and dayago7=1

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 5 freflux
            from work.reflux_day_tmp_2_%(statdatenum)s
            where begin_day=1 and dayago5_h=0 and dayago5=1

            union all

            select fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                   fuid,is_pay,gamecoins,is_play,fgrade,fparty_num,fage, 2 freflux
            from work.reflux_day_tmp_2_%(statdatenum)s
            where begin_day=1 and dayago2_h=0 and dayago2=1
        ) tmp;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        # 用完将临时表清理掉
        hql ="""
        drop table if exists work.reflux_user_tmp_0_%(statdatenum)s;
        drop table if exists work.reflux_cycle_tmp_1_%(statdatenum)s;
        drop table if exists work.reflux_cycle_tmp_2_%(statdatenum)s;
        drop table if exists work.reflux_cycle_tmp_3_%(statdatenum)s;
        drop table if exists work.reflux_day_tmp_0_%(statdatenum)s;
        drop table if exists work.reflux_day_tmp_1_%(statdatenum)s;
        drop table if exists work.reflux_day_tmp_2_%(statdatenum)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_user_reflux_array(statDate)
a()
