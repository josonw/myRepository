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


class load_gameparty_pname(BaseStat):
    """
    生成牌局中间表
    markcai建议加上牌局次数、时长、局数等数据
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.gameparty_pname
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
                fuid               bigint,
                ffirst_play        tinyint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res


    def stat(self):
        # self.hq.debug = 0
        alias_dic = {'bpid_tbl_alias':'a.','src_tbl_alias':'a.', 'const_alias':''}

        GROUPSET1 = {'alias':['src_tbl_alias', 'src_tbl_alias', 'src_tbl_alias', 'const_alias'],
                     'field':['fpname', 'fuid', 'ffirst_play','a.dt'],
                     'comb_value':[[1, 1, 1, 1]] }

        query = sql_const.query_list(self.stat_date, alias_dic, GROUPSET1)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql_list=[]
        for i in range(2):
            hql = """
                select
                     a.dt fdate,
                     %(select_field_str)s,
                     a.fpname,
                     a.fuid,
                     a.ffirst_play,
                     a.dt
                from ( select /*+ MAPJOIN(b) */
                              a.dt,
                              a.fuid,
                              max(a.ffirst_play) ffirst_play,
                              coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                              coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                              coalesce(a.fpname,'%(null_str_report)s') fpname,
                              b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk
                         from stage.user_gameparty_stg a
                         left join analysis.marketing_channel_pkg_info mcp
                           on a.fchannel_code = mcp.fid
                         join dim.bpid_map b
                           on a.fbpid = b.fbpid
                        where a.dt = '%(ld_daybegin)s' and b.hallmode = %(hallmode)s
                        group by a.dt, a.fuid, a.fgame_id, mcp.ftrader_id, a.fpname,
                                 b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk
                      ) a
                   %(group_by)s
                """ % query[i]
            hql_list.append(hql)

        hql = """
        insert overwrite table dim.gameparty_pname
        partition( dt )
        %s;
        insert into table dim.gameparty_pname
        partition( dt )
        %s
        """%(hql_list[0], hql_list[1])

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
a = load_gameparty_pname(statDate)
a()