#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_game_activity_hour_data(BaseStat):
    """活动数据,时段数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.game_activity_hour_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fact_id varchar(50),
                frule_id varchar(50),
                fhour bigint,
                fusercnt bigint,
                fusernum bigint
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
        insert overwrite table analysis.game_activity_hour_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fgamefsk, fplatformfsk, fversionfsk,
             fterminalfsk, fact_id, frule_id, hour(flts_at) + 1 fhour,
             count(distinct concat(flts_at, a.fuid)) fusercnt,
             count(distinct a.fuid) fusernum
        from stage.game_activity_stg a
        join analysis.bpid_platform_game_ver_map d
          on a.fbpid = d.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fact_id,
                frule_id, hour(flts_at) + 1;

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
a = agg_game_activity_hour_data(statDate)
a()
