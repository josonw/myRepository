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


class load_user_match(BaseStat):
    """ 比赛场用户中间表"""
    def create_tab(self):
        hql = """create table if not exists dim.user_match_join
            (
            fdate                      date,
            fgamefsk                   bigint,
            fplatformfsk               bigint,
            fhallfsk                   bigint,
            fsubgamefsk                bigint,
            fterminaltypefsk           bigint,
            fversionfsk                bigint,
            fchannelcode               bigint,
            fpname                     varchar(100), --比赛场名称 比如“斗地主比赛场”
            fsubname                   varchar(50),  --三级赛名称 比如“XX赛(10点-12点)”
            fuid                       bigint        --用户ID
            )
            partitioned by(dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """create table if not exists dim.user_match_party
            (
            fdate                      date,
            fgamefsk                   bigint,
            fplatformfsk               bigint,
            fhallfsk                   bigint,
            fsubgamefsk                bigint,
            fterminaltypefsk           bigint,
            fversionfsk                bigint,
            fchannelcode               bigint,
            fpname                     varchar(100), --比赛场名称 比如“斗地主比赛场”
            fsubname                   varchar(50),  --三级赛名称 比如“XX赛(10点-12点)”
            fuid                       bigint        --用户ID
            )
            partitioned by(dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}

        GROUPSET1 = {'alias':['src_tbl_alias', 'src_tbl_alias', 'src_tbl_alias'],
                     'field':['fuid', 'fpname', 'fsubname'],
                     'comb_value':[[1, 1, 1],
                                   [1, 1, 0],
                                   [1, 0, 0]]}


        query = sql_const.query_list(self.stat_date, alias_dic, GROUPSET1)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql_list=[]
        for i in range(2):
            hql = """
            select  /*+ MAPJOIN(b) */
                    "%(ld_daybegin)s" fdate,
                    %(select_field_str)s,
                    case when b.fgamefsk = 4132314431 and a.fpname<>'%(null_str_report)s' then '比赛场'
                         else coalesce(a.fpname,'%(null_str_group_rule)s') end fpname,
                    coalesce(a.fsubname,'%(null_str_group_rule)s') fsubname,
                    a.fuid
                from
                    (select a.fbpid,
                        coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                        coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                        coalesce(a.fpname,'%(null_str_report)s') fpname,
                        coalesce(a.fsubname,'%(null_str_report)s') fsubname,
                        a.fuid
                    from stage.join_gameparty_stg a
                    left join analysis.marketing_channel_pkg_info mcp
                        on a.fchannel_code = mcp.fid
                    where a.dt='%(ld_daybegin)s'  and  coalesce(fmatch_id,'0')<>'0'
                        group by a.fbpid, a.fgame_id, mcp.ftrader_id, a.fpname, a.fsubname, a.fuid
                    ) a
                join dim.bpid_map b
                    on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                %(group_by)s
            """% query[i]
            hql_list.append(hql)


        hql = """
        insert overwrite table dim.user_match_join
        partition(dt = '%s')
        %s;
        insert into table dim.user_match_join
        partition(dt = '%s')
        %s;
              """%(self.stat_date, hql_list[0], self.stat_date, hql_list[1])
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql_list=[]
        for i in range(2):
            hql = """
            select  /*+ MAPJOIN(b) */
                    "%(ld_daybegin)s" fdate,
                    %(select_field_str)s,
                    case when b.fgamefsk = 4132314431 and a.fpname<>'%(null_str_report)s' then '比赛场'
                         else coalesce(a.fpname,'%(null_str_group_rule)s') end fpname,
                    coalesce(a.fsubname,'%(null_str_group_rule)s') fsubname,
                    a.fuid
                from
                    (select a.fbpid,
                        coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                        coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                        coalesce(a.fpname,'%(null_str_report)s') fpname,
                        coalesce(a.fgsubname,'%(null_str_report)s') fsubname,
                        a.fuid
                    from stage.user_gameparty_stg a
                    left join analysis.marketing_channel_pkg_info mcp
                        on a.fchannel_code = mcp.fid
                    where a.dt='%(ld_daybegin)s'  and  coalesce(fmatch_id,'0')<>'0'
                        group by a.fbpid, a.fgame_id, mcp.ftrader_id, a.fpname, a.fgsubname, a.fuid
                    ) a
                join dim.bpid_map b
                    on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                %(group_by)s
            """% query[i]
            hql_list.append(hql)

        hql = """
        insert overwrite table dim.user_match_party
        partition(dt = '%s')
        %s;
        insert into table dim.user_match_party
        partition(dt = '%s')
        %s;
              """%(self.stat_date, hql_list[0], self.stat_date, hql_list[1])
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        return res


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_user_match(statDate, eid)
a()
