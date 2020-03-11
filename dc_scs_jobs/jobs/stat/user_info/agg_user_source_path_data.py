#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_source_path_data(BaseStat):
    """用户来源路径分析
    """
    def create_tab(self):
        hql = """
        create table if not exists analysis.user_source_path_fct
        (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fsource_path varchar(100),
            factive bigint,
            fregister bigint
        )
        partitioned by (dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # hql = """
        # create table if not exists stage.user_source_path_fct_merge_tmp_%(num_begin)s
        # (
        #     fgamefsk bigint,
        #     fplatformfsk bigint,
        #     fversionfsk bigint,
        #     fterminalfsk bigint,
        #     fsource_path varchar(100),
        #     factive bigint,
        #     fregister bigint
        # )
        # """
        # res = self.hq.exe_sql(hql)
        # if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res

        hql = """
        drop table if exists stage.user_source_path_fct_merge_tmp_%(num_begin)s;
        create table stage.user_source_path_fct_merge_tmp_%(num_begin)s 
        as
            select c.fgamefsk fgamefsk, c.fplatformfsk fplatformfsk, c.fversionfsk fversionfsk,
                c.fterminalfsk fterminalfsk, a.fsource_path fsource_path,
                count(distinct a.fuid) factive, 0 fregister
             from stage.user_login_stg a
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
              and a.fsource_path is not null
            group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, a.fsource_path
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert into table stage.user_source_path_fct_merge_tmp_%(num_begin)s
            select c.fgamefsk fgamefsk, c.fplatformfsk fplatformfsk, c.fversionfsk fversionfsk,
                c.fterminalfsk fterminalfsk, a.fsource_path fsource_path, 0 factive,
                count(distinct fuid) fregister
              from stage.user_dim a
              join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
             where a.dt = '%(ld_daybegin)s'
               and a.fsource_path is not null
             group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, a.fsource_path
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """ -- 临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_source_path_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fsource_path,
            sum(factive) factive,
            sum(fregister) fregister
        from stage.user_source_path_fct_merge_tmp_%(num_begin)s
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fsource_path;
        drop table if exists stage.user_source_path_fct_merge_tmp_%(num_begin)s;
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
a = agg_user_source_path_data(statDate)
a()
