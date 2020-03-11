#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_game_activity_retent_data(BaseStat):
    """活动数据,时段数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.game_activity_retain_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fact_id varchar(50),
                fusernum bigint,
                f1dru bigint,
                f2dru bigint,
                f3dru bigint,
                f4dru bigint,
                f5dru bigint,
                f6dru bigint,
                f7dru bigint,
                f14dru bigint,
                f30dru bigint
                )
                partitioned by (dt date)
            """
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
        insert overwrite table analysis.game_activity_retain_fct
        partition (dt )
         select a.fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fact_id,
                count( distinct a.fuid) fusernum,
                count( distinct if( datediff( b.dt,a.fdate)=1, a.fuid, null ))  f1dru,
                count( distinct if( datediff( b.dt,a.fdate)=2, a.fuid, null ))  f2dru,
                count( distinct if( datediff( b.dt,a.fdate)=3, a.fuid, null ))  f3dru,
                count( distinct if( datediff( b.dt,a.fdate)=4, a.fuid, null ))  f4dru,
                count( distinct if( datediff( b.dt,a.fdate)=5, a.fuid, null ))  f5dru,
                count( distinct if( datediff( b.dt,a.fdate)=6, a.fuid, null ))  f6dru,
                count( distinct if( datediff( b.dt,a.fdate)=7, a.fuid, null ))  f7dru,
                count( distinct if( datediff( b.dt,a.fdate)=14, a.fuid, null ))  f14dru,
                count( distinct if( datediff( b.dt,a.fdate)=30, a.fuid, null ))  f30dru,
                a.fdate dt
           from (select distinct dt fdate, fbpid, fact_id, fuid
                           from stage.game_activity_stg
                          where dt >= '%(ld_30dayago)s'
                            and dt < '%(ld_dayend)s'
                        ) a
                   left outer join stage.active_user_mid b
                     on a.fbpid = b.fbpid
                    and a.fuid = b.fuid
                    and b.dt >= '%(ld_30dayago)s'
                    and b.dt < '%(ld_dayend)s'
                   join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
              group by a.fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       FTERMINALFSK,
                       fact_id;

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.game_activity_retain_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fact_id,
        fusernum,
        f1dru,
        f2dru,
        f3dru,
        f4dru,
        f5dru,
        f6dru,
        f7dru,
        f14dru,
        f30dru
        from analysis.game_activity_retain_fct
        where dt >= '%(ld_30dayago)s'
          and dt < '%(ld_dayend)s'
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
a = agg_game_activity_retent_data(statDate)
a()
