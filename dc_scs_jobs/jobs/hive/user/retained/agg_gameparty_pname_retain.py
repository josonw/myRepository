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


class agg_gameparty_pname_retain(BaseStat):
    """
    场次新增、玩牌用户，并继续活跃留存、继续玩牌留存
    """
    def create_tab(self):
        hql = """
        -- 场次玩牌用户，并继续活跃留存
        create table if not exists dcnew.gameparty_pname_play_user_actret
        (
                fdate              date,
                fgamefsk           bigint,
                fplatformfsk       bigint,
                fhallfsk           bigint,
                fsubgamefsk        bigint,
                fterminaltypefsk   bigint,
                fversionfsk        bigint,
                fchannelcode       bigint,
                fpname             varchar(100),
                total_user         bigint,
                f1daycnt           bigint,
                f2daycnt           bigint,
                f3daycnt           bigint,
                f4daycnt           bigint,
                f5daycnt           bigint,
                f6daycnt           bigint,
                f7daycnt           bigint,
                f14daycnt          bigint,
                f30daycnt          bigint,
                f60daycnt          bigint,
                f90daycnt          bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        -- 场次新增用户，并继续活跃留存
        create table if not exists dcnew.gameparty_pname_reg_user_actret
        (
                fdate              date,
                fgamefsk           bigint,
                fplatformfsk       bigint,
                fhallfsk           bigint,
                fsubgamefsk        bigint,
                fterminaltypefsk   bigint,
                fversionfsk        bigint,
                fchannelcode       bigint,
                fpname             varchar(100),
                total_user         bigint,
                f1daycnt           bigint,
                f2daycnt           bigint,
                f3daycnt           bigint,
                f4daycnt           bigint,
                f5daycnt           bigint,
                f6daycnt           bigint,
                f7daycnt           bigint,
                f14daycnt          bigint,
                f30daycnt          bigint,
                f60daycnt          bigint,
                f90daycnt          bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        -- 场次玩牌用户，并继续玩牌留存
        create table if not exists dcnew.gameparty_pname_play_user_playret
        (
                fdate              date,
                fgamefsk           bigint,
                fplatformfsk       bigint,
                fhallfsk           bigint,
                fsubgamefsk        bigint,
                fterminaltypefsk   bigint,
                fversionfsk        bigint,
                fchannelcode       bigint,
                fpname             varchar(100),
                total_user         bigint,
                f1daycnt           bigint,
                f2daycnt           bigint,
                f3daycnt           bigint,
                f4daycnt           bigint,
                f5daycnt           bigint,
                f6daycnt           bigint,
                f7daycnt           bigint,
                f14daycnt          bigint,
                f30daycnt          bigint,
                f60daycnt          bigint,
                f90daycnt          bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        -- 场次新增用户，并继续玩牌留存
        create table if not exists dcnew.gameparty_pname_reg_user_playret
        (
                fdate              date,
                fgamefsk           bigint,
                fplatformfsk       bigint,
                fhallfsk           bigint,
                fsubgamefsk        bigint,
                fterminaltypefsk   bigint,
                fversionfsk        bigint,
                fchannelcode       bigint,
                fpname             varchar(100),
                total_user         bigint,
                f1daycnt           bigint,
                f2daycnt           bigint,
                f3daycnt           bigint,
                f4daycnt           bigint,
                f5daycnt           bigint,
                f6daycnt           bigint,
                f7daycnt           bigint,
                f14daycnt          bigint,
                f30daycnt          bigint,
                f60daycnt          bigint,
                f90daycnt          bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


    def stat(self):
        # self.hq.debug = 0
        query = PublicFunc.date_define( self.stat_date )

        args_list = [{'agg_table':'dcnew.gameparty_pname_play_user_actret',
                     'load_table':'work.gameparty_pname_play_actret_%(num_begin)s'%query,
                     'field_total':'total_user',
                     'field':'active_num'},

                     {'agg_table':'dcnew.gameparty_pname_reg_user_actret',
                      'load_table':'work.gameparty_pname_play_actret_%(num_begin)s'%query,
                      'field_total':'total_user_first',
                      'field':'first_play_num'},

                     {'agg_table':'dcnew.gameparty_pname_play_user_playret',
                      'load_table':'work.gameparty_pname_play_playret_%(num_begin)s'%query,
                      'field_total':'total_user',
                      'field':'active_num'},

                     {'agg_table':'dcnew.gameparty_pname_reg_user_playret',
                      'load_table':'work.gameparty_pname_play_playret_%(num_begin)s'%query,
                      'field_total':'total_user_first',
                      'field':'first_play_num'}]

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql_list=[]

        for i in args_list:
            query.update(i)

            hql = """
            insert overwrite table %(agg_table)s
            partition( dt )
            select fdate,
                fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                fpname,
                -- 这个地方用min是因为新增留存表里，该数据老的计算逻辑是错的，比真实的大
                min(total_user) total_user,
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
            from
            (
                select fdate,
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fpname,
                    %(field_total)s total_user,
                    if( retday=1, %(field)s, 0 ) f1daycnt,
                    if( retday=2, %(field)s, 0 ) f2daycnt,
                    if( retday=3, %(field)s, 0 ) f3daycnt,
                    if( retday=4, %(field)s, 0 ) f4daycnt,
                    if( retday=5, %(field)s, 0 ) f5daycnt,
                    if( retday=6, %(field)s, 0 ) f6daycnt,
                    if( retday=7, %(field)s, 0 ) f7daycnt,
                    if( retday=14, %(field)s, 0 ) f14daycnt,
                    if( retday=30, %(field)s, 0 ) f30daycnt,
                    if( retday=60, %(field)s, 0 ) f60daycnt,
                    if( retday=90, %(field)s, 0 ) f90daycnt
                from %(load_table)s

                union all

                select fdate,
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fpname,
                    -- 这个地方要特殊处理，因为历史数据是错的太大了，要用union all上面的正确数据来修复
                    -- 所以这里如果为0的话要特殊处理，怕这里为0外围用min就取到0了
                    case when total_user = 0 then 99999999 else total_user end total_user,
                    f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt,f60daycnt,f90daycnt
                from %(agg_table)s
                where dt >= '%(ld_90dayago)s'
                and dt < '%(ld_dayend)s'
            ) tmp
            group by fdate,
                fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                fpname;


        --最近三十天的数据放到同一个分区中，提高同步效率
        insert overwrite table  %(agg_table)s
            partition(dt='3000-01-01')
                   select fdate,
                fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                fpname,
                total_user,
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
            from %(agg_table)s
            where dt >= '%(ld_90dayago)s' and dt < '%(ld_dayend)s';
            """ % query
            hql_list.append(hql)

        res = self.exe_hql_list(hql_list)
        if res != 0: return res

        hql = """
        drop table if exists work.gameparty_pname_play_playret_%(num_begin)s;
        drop table if exists work.gameparty_pname_play_actret_%(num_begin)s;
        """% query
        res = self.hq.exe_sql(hql)
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
a = agg_gameparty_pname_retain(statDate)
a()
