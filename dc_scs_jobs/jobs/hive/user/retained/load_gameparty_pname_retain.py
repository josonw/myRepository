#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class load_gameparty_pname_retain(BaseStat):
    """
    场次新增用户、玩牌用户，并继续活跃留存、继续玩牌留存
    场次的新增用户是牌局里有个字段来标识的
    """
    def create_tab(self):
        pass

    def stat(self):
        # self.hq.debug = 0
        query = {}

        query.update( PublicFunc.date_define( self.stat_date ) )
        query.update(sql_const.const_dict())

        hql_list=[]

        hql = """
        -- 场次新增用户、玩牌用户，并继续活跃留存
        drop table if exists work.gameparty_pname_play_actret_%(num_begin)s;
        create table work.gameparty_pname_play_actret_%(num_begin)s as
        select
            a.dt fdate,
            a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
            a.fpname,
            datediff('%(ld_daybegin)s', a.dt) retday,
            count(distinct a.fuid) total_user,
            count(distinct b.fuid) as  active_num,
            count(distinct case when a.ffirst_play=1 then b.fuid else null end) as first_play_num,
            count(distinct case when a.ffirst_play=1 then a.fuid else null end) as total_user_first
        from dim.gameparty_pname a
        left join dim.user_act_array b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.fhallfsk = b.fhallfsk
         and a.fsubgamefsk = b.fsubgamefsk
         and a.fterminaltypefsk = b.fterminaltypefsk
         and a.fversionfsk = b.fversionfsk
         and a.fchannelcode = b.fchannelcode
         and a.fuid = b.fuid
         and b.dt = '%(ld_daybegin)s'
       where a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_dayend)s'
          or a.dt='%(ld_14dayago)s'
          or a.dt='%(ld_30dayago)s'
          or a.dt='%(ld_60dayago)s'
          or a.dt='%(ld_90dayago)s'
       group by
             a.dt, a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
             a.fpname, datediff( '%(ld_daybegin)s', a.dt)
            """ % query
        hql_list.append(hql)


        hql = """
        -- 场次新增用户、玩牌用户，并继续玩牌留存
        drop table if exists work.gameparty_pname_play_playret_%(num_begin)s;
        create table work.gameparty_pname_play_playret_%(num_begin)s as
        select
            a.dt fdate,
            a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
            a.fpname,
            datediff('%(ld_daybegin)s', a.dt) retday,
            count(distinct a.fuid) total_user,
            count(distinct b.fuid) as  active_num,
            count(distinct case when a.ffirst_play=1 then b.fuid else null end) as first_play_num,
            count(distinct case when a.ffirst_play=1 then a.fuid else null end) as total_user_first
        from dim.gameparty_pname a
        left join (
                   select fgamefsk,
                          fplatformfsk,
                          fhallfsk,
                          fsubgamefsk,
                          fterminaltypefsk,
                          fversionfsk,
                          fchannelcode,
                          fpname,
                          fuid
                    from dim.gameparty_pname
                   where dt = '%(ld_daybegin)s'
                   ) b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.fhallfsk = b.fhallfsk
         and a.fsubgamefsk = b.fsubgamefsk
         and a.fterminaltypefsk = b.fterminaltypefsk
         and a.fversionfsk = b.fversionfsk
         and a.fchannelcode = b.fchannelcode
         and a.fpname = b.fpname
         and a.fuid = b.fuid
       where a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_dayend)s'
          or a.dt='%(ld_14dayago)s'
          or a.dt='%(ld_30dayago)s'
          or a.dt='%(ld_60dayago)s'
          or a.dt='%(ld_90dayago)s'
         group by
             a.dt, a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
             a.fpname, datediff( '%(ld_daybegin)s', a.dt)
            """ % query
        hql_list.append(hql)

        res = self.exe_hql_list(hql_list)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
#生成统计实例
a = load_gameparty_pname_retain(statDate)
a()
