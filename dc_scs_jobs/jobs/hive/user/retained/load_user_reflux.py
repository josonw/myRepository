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

class load_user_reflux(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dim.user_reflux
            (
                fbpid varchar(50) comment 'BPID',
                fgame_id bigint comment '子游戏ID',
                fchannel_code bigint comment '渠道ID',
                fuid bigint comment '用户游戏ID',
                freflux bigint comment '回流天数，例如7表示7日回流',
                freflux_type varchar(32) comment '回流类型，cycle不考虑freflux天的前一天是否活跃，day表示freflux天的前一天必须活跃'
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
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        date = PublicFunc.date_define(self.stat_date)
        query.update(date)

        hql = """
        drop table if exists work.reflux_cycle_last_atv;

        create table work.reflux_cycle_last_atv
        as
        select tt.fdate,tt.fbpid, tt.fuid, tt.flast_atv_date,fgame_id,fchannel_code
          from (-- 当天的活跃用户，如果最近30天活跃，取最近活跃时间，否则取31天之前那天
                -- 为用户的最后活跃时间
                select '%(statdate)s' fdate,fbpid,fuid,
                       max(flast_atv_date) flast_atv_date, max(is_ok) is_ok
                  from (select fbpid,fuid, cast ('%(ld_31dayago)s' as date) flast_atv_date, 1 is_ok
                          from dim.user_act
                         where dt =  '%(statdate)s'
                         union all
                        select fbpid,fuid, dt flast_atv_date, 0 is_ok
                          from dim.user_act
                         where dt >= '%(ld_30dayago)s' and dt < '%(statdate)s'
                         union all
                        select fbpid,fuid, cast (dt as date) flast_atv_date, 0 is_ok
                          from dim.reg_user_main_additional
                         where dt >= '%(ld_30dayago)s' and dt <= '%(statdate)s'
                       ) t
                 group by fbpid,fuid
               ) tt
          join dim.user_act a
            on tt.fbpid = a.fbpid
           and tt.fuid = a.fuid
           and a.dt =  '%(statdate)s'
         where is_ok = 1;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table dim.user_reflux
        partition (dt='%(statdate)s')
        select fbpid,
            coalesce(fgame_id,%(null_int_report)d) fgame_id,
            coalesce(fchannel_code,%(null_int_report)d) fchannel_code,
            fuid,
            freflux,
            'cycle' freflux_type
        from
        (
            select fbpid,fgame_id,fchannel_code,fuid, 2 freflux
            from work.reflux_cycle_last_atv
            where flast_atv_date < '%(ld_2dayago)s'

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 5 freflux
            from work.reflux_cycle_last_atv
            where flast_atv_date < '%(ld_5dayago)s'

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 7 freflux
            from work.reflux_cycle_last_atv
            where flast_atv_date < '%(ld_7dayago)s'

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 14 freflux
            from work.reflux_cycle_last_atv
            where flast_atv_date < '%(ld_14dayago)s'

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 30 freflux
            from work.reflux_cycle_last_atv
            where flast_atv_date < '%(ld_30dayago)s'
        ) tmp ;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.user_reflux_day_judge;

        create table if not exists work.user_reflux_day_judge
        as select fbpid,fgame_id, fchannel_code,fuid,
                  case when cast (fdate as string) = '%(ld_3dayago)s' then 1 else 0 end dayago2,
                  case when cast (fdate as string) = '%(ld_6dayago)s' then 1 else 0 end dayago5,
                  case when cast (fdate as string) = '%(ld_8dayago)s' then 1 else 0 end dayago7,
                  case when cast (fdate as string) = '%(ld_15dayago)s' then 1 else 0 end dayago14,
                  case when cast (fdate as string) = '%(ld_31dayago)s' then 1 else 0 end dayago30
             from (select a.fbpid, a.fuid,fgame_id,fchannel_code,f.dt fdate
                     from dim.user_act a
                     join (select a.fbpid, a.fuid, max(dt) dt
                             from dim.user_act a
                            where a.dt between '%(ld_31dayago)s' and '%(ld_1dayago)s'
                            group by a.fbpid, a.fuid
                          ) f
                       on a.fbpid = f.fbpid
                      and a.fuid = f.fuid
                    where a.dt = '%(ld_daybegin)s'
                  ) a
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        -- 现在进行流失回流用户插入。
        insert into table dim.user_reflux
        partition(dt='%(ld_daybegin)s')
        select fbpid,
            coalesce(fgame_id,%(null_int_report)d) fgame_id,
            coalesce(fchannel_code,%(null_int_report)d) fchannel_code,
            fuid, freflux, 'day' freflux_type
        from
        (
            -- 当天活跃，30天之前活跃，两者之间不活跃
            select fbpid,fgame_id,fchannel_code,fuid, 30 freflux
            from work.user_reflux_day_judge
            where dayago30=1

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 14 freflux
            from work.user_reflux_day_judge
            where dayago14=1

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 7 freflux
            from work.user_reflux_day_judge
            where dayago7=1

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 5 freflux
            from work.user_reflux_day_judge
            where dayago5=1

            union all

            select fbpid,fgame_id,fchannel_code,fuid, 2 freflux
            from work.user_reflux_day_judge
            where dayago2=1
        ) tmp;
         """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 用完将临时表清理掉
        hql ="""
        drop table if exists work.reflux_cycle_last_atv;

        drop table if exists work.user_reflux_day_judge;
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
a = load_user_reflux(statDate)
a()
