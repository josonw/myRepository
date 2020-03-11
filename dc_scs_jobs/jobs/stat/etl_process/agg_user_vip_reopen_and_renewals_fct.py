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


class agg_user_vip_reopen_and_renewals_fct(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_vip_re_open_fct
        (
           fdate    date    ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk bigint  ,
            fterminalfsk    bigint  ,
            re_open_days    bigint  ,
            fcnt    bigint  ,
            fusercnt    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_vip_re_open_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        use analysis;
        create table if not exists analysis.user_vip_renewals_fct
        (
            fdate    date    ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk bigint  ,
            fterminalfsk    bigint  ,
            renewals_days   bigint  ,
            fcnt    bigint  ,
            fusercnt    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_vip_renewals_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        query = { 'statdate':statDate }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_vip_re_open_fct
        partition (dt="%(statdate)s")
           select "%(statdate)s" fdate,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   fterminalfsk,
                   datediff(fvip_at , flast_due_at) re_open_days,
                   count(fuid),count(distinct fuid)
              from stage.user_vip_stg a
              left join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
             where foper_type = '1'
               and fvip_at <> ffirst_at
               and a.dt = "%(statdate)s"
             group by fgamefsk,
                      fplatformfsk,
                      fversionfsk,
                      fterminalfsk,
                      datediff(fvip_at , flast_due_at)""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.user_vip_renewals_fct
        partition (dt="%(statdate)s")
           select "%(statdate)s" fdate,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   fterminalfsk,
                   datediff(flast_due_at,fvip_at) renewals_days,
                   count(fuid),count(distinct fuid)
              from stage.user_vip_stg a
              left join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
             where foper_type = '2'
               and a.dt = "%(statdate)s"
             group by fgamefsk,
                      fplatformfsk,
                      fversionfsk,
                      fterminalfsk,
                      datediff(flast_due_at,fvip_at)""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
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
a = agg_user_vip_reopen_and_renewals_fct(statDate, eid)
a()
