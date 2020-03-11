#!/user/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_user_vip_day_fct(BaseStat):
    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_vip_day_fct
        (
            fdate   date    ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk bigint  ,
            fterminalfsk    bigint  ,
            fvip_type   int  ,
            fvip_level  int  ,
            foper_type  string  ,
            foper_way   string  ,
            fpay_way    string  ,
            fusercnt    bigint  ,
            fcnt    bigint  ,
            fmoney  decimal(20, 2)
        )
        partitioned by(dt date)
        location '/dw/analysis/user_vip_day_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        query = { 'statdate':statDate, 'ld_1daylater':PublicFunc.add_days(statDate, 1) }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """--
        insert overwrite table analysis.user_vip_day_fct
        partition (dt="%(statdate)s")
        select "%(statdate)s" fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fvip_type,
               fvip_level,
               foper_type,
               foper_way,
               fpay_way,
               coalesce(count(distinct fuid), 0) fusercnt,
               coalesce(count(fuid), 0) fcnt,
               coalesce(round(sum(fmoney), 2), 0) fmoney
          from stage.user_vip_stg a
          left join analysis.bpid_platform_game_ver_map t
            on a.fbpid = t.fbpid
         where a.dt = "%(statdate)s"
           and fdue_at > "%(ld_1daylater)s"
         group by fgamefsk,
                  fplatformfsk,
                  fversionfsk,
                  fterminalfsk,
                  fvip_type,
                  fvip_level,
                  foper_type,
                  foper_way,
                  fpay_way""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
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
a = agg_user_vip_day_fct(statDate, eid)
a()



