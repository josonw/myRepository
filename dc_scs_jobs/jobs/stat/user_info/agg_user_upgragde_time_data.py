#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_upgragde_time_data(BaseStat):
    """用户升级时间分布
    """
    def create_tab(self):
        hql = """
        create table if not exists analysis.user_upgrade_fct
        (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fversionfsk   bigint,
            fterminalfsk  bigint,
            flevel        varchar(50),
            ftime         decimal(20,2)
        )
        partitioned by (dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.user_upgrade_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, b.fgamefsk, b.fplatformfsk, b.fversionfsk,
            b.fterminalfsk, a.flevel, round(avg(a.ftime), 2)
        from
        (
            -- 这里的等级取升级后的等级
            select a.fbpid, a.fuid, a.flevel,
                -- datediff 算的是两个事件的日期部分的天数差异
                -- min( datediff(a.fgrade_at, b.fgrade_at) ) as ftime
                min( round( (unix_timestamp(a.fgrade_at) - unix_timestamp(b.fgrade_at)) / 86400 ) ) as ftime
            from
            (
                select fbpid, fuid, flevel, flevel-1 as lflevel, max(fgrade_at) as fgrade_at
                from stage.user_grade_stg
                where dt = '%(ld_daybegin)s'
                group by fbpid, fuid, flevel
            ) a
            join stage.user_grade_stg b
            on a.fbpid = b.fbpid
                and a.fuid = b.fuid
                and a.lflevel = b.flevel
                and b.dt >= '%(ld_30dayago)s'
                and b.dt <= '%(ld_daybegin)s'
            group by a.fbpid, a.fuid, a.flevel
        ) a
        join analysis.bpid_platform_game_ver_map b
        on a.fbpid = b.fbpid
        group by b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.flevel;
        """ % query
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
a = agg_user_upgragde_time_data(statDate)
a()
