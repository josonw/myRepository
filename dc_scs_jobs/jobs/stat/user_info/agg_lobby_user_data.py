#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_lobby_user_data(BaseStat):
    """游戏大厅数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.lobby_user_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fregcnt bigint,
                fdaucnt bigint,
                fmaucnt bigint,
                flogincnt bigint,
                fvalidcnt bigint,
                fdaylosscnt bigint,
                ffirstlogincnt bigint,
                fdlcnt bigint,
                fver_info varchar(20)
                )
                partitioned by ( dt date )
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql= """create external table if not exists analysis.lobby_user_download_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fgame_id varchar(20),
                fgame_name varchar(50),
                fversion_info varchar(20),
                fdlcnt bigint,
                fregdlcnt bigint
                )partitioned by ( dt date )"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql= """create external table if not exists stage.lobby_user_tmp
                (
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                ftype string,
                fdata bigint
                )"""
        res = self.hq.exe_sql(hql)

        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update(date)
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        insert overwrite table stage.lobby_user_tmp
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                   'fregcnt' ftype, count(distinct a.fdevid) fdata
                from stage.lobby_download_stg a
                join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
                where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        insert into table stage.lobby_user_tmp
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
              'fdlcnt' ftype, count(a.fdevid) fdata
              from stage.lobby_game_download_stg a
              join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
              where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        insert into table stage.lobby_user_tmp
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                'ffirstlogincnt' ftype, count(distinct a.fdevid) fdata
        from (
            select fbpid, fdevid, max(is_old) is_old
            from (
              select fbpid, fdevid, 0 is_old
                from stage.lobby_launch_stg a
               where dt = '%(ld_daybegin)s'
              union all
              select fbpid, fdevid, 1 is_old
                from stage.lobby_launch_stg
               where dt < '%(ld_daybegin)s'
            ) t group by fbpid, fdevid
        ) a join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
          where is_old=0
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        insert into table stage.lobby_user_tmp
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
            'fmaucnt' ftype, count(a.fdevid) fdata
            from (
                select distinct fbpid, fdevid
                from stage.lobby_launch_stg
                where dt >= '%(ld_monthbegin)s'
                and dt < '%(ld_monthend)s'
            ) a
            join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        drop table if exists lobby_launch_data_tmp;
        create table lobby_launch_data_tmp
        as
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                count(1) flogincnt, count(a.fdevid)  fdaucnt
            from stage.lobby_launch_stg a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        insert into table stage.lobby_user_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                'fdaucnt' ftype, fdaucnt fdata
           from lobby_launch_data_tmp;

        insert into table stage.lobby_user_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                'flogincnt' ftype, flogincnt fdata
           from lobby_launch_data_tmp;


        --1日流失用户
        insert into table stage.lobby_user_tmp
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                'fdaylosscnt' ftype, count(a.fdevid) fdata
            from (
                select fbpid, fdevid, max(is_act) is_act from (
                  select fbpid, fdevid, 0 is_act from stage.lobby_launch_stg
                  where dt = '%(ld_1dayago)s'
                  union all
                  select fbpid, fdevid, 1 is_act from stage.lobby_launch_stg
                  where dt = '%(ld_daybegin)s'
                ) t group by fbpid, fdevid
           ) a
           join analysis.bpid_platform_game_ver_map b
           on a.fbpid = b.fbpid
           where a.is_act=0
           group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        insert into table stage.lobby_user_tmp
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
        'fvalidcnt' ftype, count(a.fdevid) fdata
        from (
           select a.fbpid, a.fdevid
           from stage.lobby_download_stg a
           left semi join stage.lobby_launch_stg b
            on a.fbpid = b.fbpid
           and a.fdevid = b.fdevid
           and b.dt >= '%(ld_6dayago)s'
           and b.dt < '%(ld_dayend)s'
           where a.dt = '%(ld_7dayago)s'
           group by a.fbpid, a.fdevid
        )a
        join analysis.bpid_platform_game_ver_map b
        on a.fbpid = b.fbpid
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;

        insert overwrite table analysis.lobby_user_fct
        partition ( dt='%(ld_daybegin)s' )
        select  '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                if(ftype='fregcnt', fdata, 0 ) fregcnt,
                if(ftype='fdaucnt', fdata, 0 ) fdaucnt,
                if(ftype='fmaucnt', fdata, 0 ) fmaucnt,
                if(ftype='flogincnt', fdata, 0 ) flogincnt,
                if(ftype='fvalidcnt', fdata, 0 ) fvalidcnt,
                if(ftype='fdaylosscnt', fdata, 0 ) fdaylosscnt,
                if(ftype='ffirstlogincnt', fdata, 0 ) ffirstlogincnt,
                if(ftype='fdlcnt', fdata, 0 ) fdlcnt,
                0 fver_info
        from lobby_user_tmp;


        insert overwrite table analysis.lobby_user_download_fct
        partition (dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fgame_id,
                fgame_name,
                fversion_info,
                max(dlnum) fdlcnt,
                max(fregdlcnt) fregdlcnt
        from (
                select b.fgamefsk,
                     b.fplatformfsk,
                     b.fversionfsk,
                     b.fterminalfsk,
                     a.fgame_id,
                     a.fgame_full_name fgame_name,
                     a.fversion_info,
                     count(1) dlnum,
                     0 fregdlcnt
                from stage.lobby_game_download_stg a
                join analysis.bpid_platform_game_ver_map b
                  on a.fbpid = b.fbpid
                where a.dt = '%(ld_daybegin)s'
                group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                        a.fgame_id, a.fgame_full_name, a.fversion_info
        union all
                select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
                             b.fgame_id, b.fgame_full_name fgame_name, b.fversion_info,
                             0 dlnum, count(1) fregdlcnt
                        from stage.lobby_download_stg a
                        left join stage.lobby_game_download_stg b
                          on a.fbpid = b.fbpid
                         and a.fdevid = b.fdevid
                         and b.dt = '%(ld_daybegin)s'
                        join analysis.bpid_platform_game_ver_map c
                          on a.fbpid = c.fbpid
                       where a.dt = '%(ld_daybegin)s'
                       group by c.fgamefsk,
                                c.fplatformfsk,
                                c.fversionfsk,
                                c.fterminalfsk,
                                b.fgame_id,
                                b.fgame_full_name,
                                b.fversion_info
        ) tmp group by fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   fterminalfsk,
                   fgame_id,
                   fgame_name,
                   fversion_info

        """ % query
        res = self.hq.exe_sql(hql)
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
a = agg_lobby_user_data(statDate)
a()
