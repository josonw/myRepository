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


class load_user_match_play(BaseStat):
    def create_tab(self):
        hql = """
        create table if not exists dim.user_match_play
        (
          fbpid               varchar(50),         --BPID
          fgame_id            bigint,              --子游戏ID
          fchannel_code       bigint,              --渠道ID
          fuid                bigint,              --用户游戏ID
          fpname              string,              --牌局一级分类名称
          fname               varchar(100),        --牌局二级分类名称
          fsubname            string,              --牌局三级级分类名称
          fparty_type         varchar(100),        --牌局类型
          fmatch_id           string,              --赛事id
          ffirst_match        smallint,            --首次参加比赛场
          ffirst              smallint,            --首次玩牌，
          ffirst_sub          smallint,            --首次二级赛场玩牌，
          ffirst_gsub         smallint,             --首次在三级赛事玩牌,
          fpartycnt           bigint                --参赛次数
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        hql = """
        create table if not exists dim.user_match_total
        (
          fbpid               varchar(50),         --BPID
          fgame_id            bigint,              --子游戏ID
          fchannel_code       bigint,              --渠道ID
          fuid                bigint,              --用户游戏ID
          fpname              string,              --牌局一级分类名称
          fname               varchar(100),        --牌局二级分类名称
          fsubname            string,              --牌局三级级分类名称
          fmode               varchar(100),        --牌局类型
          fmatch_id           string,              --赛事id
          fcause              int,                   --退赛原因
          fio_type            int,                  --消耗发放
          fapplycnt            bigint,              --报名次数
          fpartycnt           bigint               --参赛次数
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.hq.exe_sql(hql)
        if res != 0:return res


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'mcp.','src_tbl_alias':'ug.', 'const_alias':''}
        query = {'statdate':self.stat_date,
                'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],
                'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
                }

        query.update(sql_const.const_dict())
        query.update(PublicFunc.date_define(self.stat_date))


        hql ="""
            insert overwrite table dim.user_match_play
            partition( dt="%(statdate)s" )
            select /*+ MAPJOIN(mcp) */
                    fbpid,
                    fgame_id,
                    fchannel_code,
                    fuid,
                    fpname,
                    fsubname fname,
                    fgsubname fsubname,
                    null fparty_type,
                    fmatch_id,
                    max(ffirst_match) ffirst_match,
                    max(ffirst_play) ffirst,
                    max(ffirst_play_sub) ffirst_sub,
                    max(ffirst_play_gsub) ffirst_gsub,
                    1 fpartycnt
                from dim.user_match_party_stream a
                where dt = "%(statdate)s"
                group by fbpid,
                         fgame_id,
                         fchannel_code,
                         fuid,
                         fpname,
                         fsubname,
                         fgsubname,
                         fmatch_id
        """% query
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.user_match_temp_%(num_begin)s;
        create table work.user_match_temp_%(num_begin)s as
        select fbpid,
                    coalesce(jg.fgame_id,cast (0 as bigint)) fgame_id,
                    coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                    coalesce(jg.fpname,'%(null_str_report)s') fpname,
                    coalesce(jg.fname,'%(null_str_report)s') fname,
                    coalesce(jg.fsubname,'%(null_str_report)s') fsubname,
                    jg.fmode,jg.fuid,jg.fmatch_id,
                    1 fapplycnt
                from stage.join_gameparty_stg jg
                left join analysis.marketing_channel_pkg_info mcp
                on jg.fchannel_code = mcp.fid  where jg.dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                group by fbpid,
                    coalesce(jg.fgame_id,cast (0 as bigint)),
                    coalesce(mcp.ftrader_id,%(null_int_report)d),
                    coalesce(jg.fpname,'%(null_str_report)s'),
                    coalesce(jg.fname,'%(null_str_report)s'),
                    coalesce(jg.fsubname,'%(null_str_report)s'),jg.fmode,jg.fuid,jg.fmatch_id
        """% query
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        hql ="""
            insert overwrite table dim.user_match_total
            partition( dt="%(statdate)s" )
            select
                    a.fbpid,
                    a.fgame_id,
                    a.fchannel_code,
                    a.fuid,
                    a.fpname,
                    a.fname,
                    a.fsubname,
                    a.fmode,
                    a.fmatch_id,
                    c.fcause,
                    d.fio_type,
                    a.fapplycnt,
                    b.fpartycnt
                from work.user_match_temp_%(num_begin)s a
                left join dim.user_match_play b
                  on a.fbpid=b.fbpid and a.fuid=b.fuid and a.fmatch_id=b.fmatch_id
                 and a.fpname=b.fpname and a.fname= b.fname and a.fsubname= b.fsubname
                left join (
                          select fbpid,
                        coalesce(qg.fgame_id,cast (0 as bigint)) fgame_id,
                        coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                        coalesce(qg.fpname,'%(null_str_report)s') fpname,
                        coalesce(qg.fsubname,'%(null_str_report)s') fname,
                        coalesce(qg.fgsubname,'%(null_str_report)s') fsubname,
                        fuid,qg.fmatch_id, fcause
                    from stage.quit_gameparty_stg qg
                    left join analysis.marketing_channel_pkg_info mcp
                    on qg.fchannel_code = mcp.fid  where qg.dt='%(statdate)s' and  coalesce(fmatch_id,'0')<>'0'
                    group by fbpid,
                        coalesce(qg.fgame_id,cast (0 as bigint)),
                        coalesce(mcp.ftrader_id,%(null_int_report)d),
                        coalesce(qg.fpname,'%(null_str_report)s'),
                        coalesce(qg.fsubname,'%(null_str_report)s'),
                        coalesce(qg.fgsubname,'%(null_str_report)s'),qg.fuid,qg.fmatch_id,qg.fcause
                    ) c
                    on a.fbpid=c.fbpid and a.fuid=c.fuid and a.fmatch_id=c.fmatch_id
                   and a.fpname=c.fpname and a.fname= c.fname and a.fsubname= c.fsubname
                left join (
                          select fbpid,
                        coalesce(mg.fgame_id,cast (0 as bigint)) fgame_id,
                        coalesce(mcp.ftrader_id,%(null_int_report)d) fchannel_code,
                        coalesce(mg.fpname,'%(null_str_report)s') fpname,
                        coalesce(mg.fsubname,'%(null_str_report)s') fname,
                        coalesce(mg.fgsubname,'%(null_str_report)s') fsubname,
                        null fmode,fuid,mg.fmatch_id, fio_type
                    from stage.match_economy_stg mg
                    left join analysis.marketing_channel_pkg_info mcp
                    on mg.fchannel_code = mcp.fid  where mg.dt='%(statdate)s'  and  coalesce(fmatch_id,'0')<>'0'
                    group by fbpid,
                        coalesce(mg.fgame_id,cast (0 as bigint)),
                        coalesce(mcp.ftrader_id,%(null_int_report)d),
                        coalesce(mg.fpname,'%(null_str_report)s'),
                        coalesce(mg.fsubname,'%(null_str_report)s'),
                        coalesce(mg.fgsubname,'%(null_str_report)s'),mg.fuid,mg.fmatch_id,mg.fio_type
                     ) d
                     on a.fbpid=d.fbpid and a.fuid=d.fuid and a.fmatch_id=d.fmatch_id
                    and a.fpname=d.fpname and a.fname= d.fname and a.fsubname= d.fsubname
                where dt = "%(statdate)s"
                group by a.fbpid,
                         a.fgame_id,
                         a.fchannel_code,
                         a.fuid,
                         a.fpname,
                         a.fname,
                         a.fsubname,
                         a.fmode,
                         a.fmatch_id,
                         c.fcause,
                         d.fio_type,
                         a.fapplycnt,
                         b.fpartycnt
        """% query
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        res = self.hq.exe_sql("""drop table if exists work.user_match_temp_%(num_begin)s; """%query)
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
a = load_user_match_play(statDate, eid)
a()
