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


class agg_match_user_playret(BaseStat):
    """比赛场 参赛留存
    """
    def create_tab(self):
        hql = """create table if not exists dcnew.match_user_playret
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
                fsubname           varchar(100),
                total_user         bigint,
                f1daycnt           bigint,
                f2daycnt           bigint,
                f3daycnt           bigint,
                f4daycnt           bigint,
                f5daycnt           bigint,
                f6daycnt           bigint,
                f7daycnt           bigint,
                f14daycnt          bigint,
                f30daycnt          bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res


    def stat(self):
        # self.hq.debug = 0
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}

        query = sql_const.query_list(self.stat_date, alias_dic, None)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql_list=[]
        hql = """
        drop table if exists work.agg_match_user_playret_%(num_begin)s;
        create table work.agg_match_user_playret_%(num_begin)s as
        select
            a.dt fdate,
            a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
            a.fpname, a.fsubname,
            datediff('%(ld_daybegin)s', a.dt) retday,
            count(distinct a.fuid) total_user,
            count(distinct b.fuid) as active_num
        from dim.user_match_party a
        left join dim.user_match_party b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.fhallfsk = b.fhallfsk
         and a.fsubgamefsk = b.fsubgamefsk
         and a.fterminaltypefsk = b.fterminaltypefsk
         and a.fversionfsk = b.fversionfsk
         and a.fchannelcode = b.fchannelcode
         and a.fpname = b.fpname
         and a.fsubname = b.fsubname
         and a.fuid = b.fuid
         and b.dt = '%(ld_daybegin)s'
       where (a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_dayend)s') or a.dt='%(ld_14dayago)s' or a.dt='%(ld_30dayago)s' or a.dt='%(ld_60dayago)s'
       group by
             a.dt, a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
             a.fpname, a.fsubname, datediff( '%(ld_daybegin)s', a.dt)
            """ % query[0]
        hql_list.append(hql)

        hql = """
        insert overwrite table dcnew.match_user_playret
        partition( dt )
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
               fpname, fsubname,
            max(total_user) total_user,
            max(f1daycnt) f1daycnt,
            max(f2daycnt) f2daycnt,
            max(f3daycnt) f3daycnt,
            max(f4daycnt) f4daycnt,
            max(f5daycnt) f5daycnt,
            max(f6daycnt) f6daycnt,
            max(f7daycnt) f7daycnt,
            max(f14daycnt) f14daycnt,
            max(f30daycnt) f30daycnt,
            fdate dt
        from (
            select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                fpname , fsubname,
                total_user total_user,
                if( retday=1, active_num, 0 ) f1daycnt,
                if( retday=2, active_num, 0 ) f2daycnt,
                if( retday=3, active_num, 0 ) f3daycnt,
                if( retday=4, active_num, 0 ) f4daycnt,
                if( retday=5, active_num, 0 ) f5daycnt,
                if( retday=6, active_num, 0 ) f6daycnt,
                if( retday=7, active_num, 0 ) f7daycnt,
                if( retday=14, active_num, 0 ) f14daycnt,
                if( retday=30, active_num, 0 ) f30daycnt
            from work.agg_match_user_playret_%(num_begin)s
            union all
            select  fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fpname, fsubname,
                    total_user total_user,
                    f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt
            from dcnew.match_user_playret
            where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
               ) tmp group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                       fpname, fsubname
        """ % query[0]
        hql_list.append(hql)

        hql = """--最近三十天的数据放到同一个分区中，提高同步效率
        insert overwrite table dcnew.match_user_playret partition(dt='3000-01-01')
            select  fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fpname, fsubname,
                    total_user total_user,
                    f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt
            from dcnew.match_user_playret
            where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
        """% query[0]
        hql_list.append(hql)

        hql = """
        drop table if exists work.agg_match_user_playret_%(num_begin)s"""% query[0]
        hql_list.append(hql)

        res = self.exe_hql_list(hql_list)
        if res != 0: return res


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
a = agg_match_user_playret(statDate)
a()
