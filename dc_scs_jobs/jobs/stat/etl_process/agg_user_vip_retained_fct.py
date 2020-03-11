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

class agg_user_vip_retained_fct(BaseStat):
    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_vip_retained_fct
        (
            fdate           date    ,
            fgamefsk        bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk     bigint  ,
            fvip_type       int  ,
            fvip_level      int  ,
            flevel          int  ,
            fcnt            bigint  ,
            f1daycnt        bigint  ,
            f7daycnt        bigint  ,
            f30daycnt       bigint )
        partitioned by(dt date)
        location '/dw/analysis/user_vip_retained_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        query = { 'statdate':statDate, 'ld_30daybefore':PublicFunc.add_days(statDate, -30),'ld_1daylater':PublicFunc.add_days(statDate, 1) }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.user_vip_retained_fct
        partition (dt)
        select aa.fdate fdate,
           aa.fgamefsk fgamefsk,
           aa.fplatformfsk fplatformfsk,
           aa.fversionfsk fversionfsk,
           aa.fvip_type fvip_type,
           aa.fvip_level fvip_level,
           aa.flevel flevel,
           coalesce(count(distinct fuid), 0) fcnt,
           coalesce(count(distinct(case when act_date = date_add(fdate, 1) then fuid end)), 0) f1daycnt,
           coalesce(count(distinct(case when act_date = date_add(fdate, 7) then fuid end)), 0) f7daycnt,
           coalesce(count(distinct(case when act_date = date_add(fdate, 30)  then fuid end)), 0) f30daycnt,
           aa.fdate dt
           from (select a.dt fdate,
                   b.dt act_date,
                   c.fgamefsk fgamefsk,
                   c.fplatformfsk fplatformfsk,
                   c.fversionfsk fversionfsk,
                   a.fuid fuid,
                   a.fvip_type fvip_type,
                   a.fvip_level fvip_level,
                   a.flevel flevel
              from stage.user_vip_stg a
              left join stage.active_user_mid b
                on a.fbpid = b.fbpid
               and a.fuid = b.fuid
               and b.dt >= "%(ld_30daybefore)s"
               and b.dt < "%(ld_1daylater)s"
              join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
             where a.dt >= "%(ld_30daybefore)s"
               and a.dt < "%(ld_1daylater)s"
               and foper_type = '1')  aa
         group by aa.fdate,
                  aa.fgamefsk,
                  aa.fplatformfsk,
                  aa.fversionfsk,
                  aa.fvip_type,
                  aa.fvip_level,
                  aa.flevel""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        insert overwrite table analysis.user_vip_retained_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fvip_type,
        fvip_level,
        flevel,
        fcnt,
        f1daycnt,
        f7daycnt,
        f30daycnt
        from analysis.user_vip_retained_fct
        where dt >= "%(ld_30daybefore)s"
          and dt < "%(ld_1daylater)s"
        """ % query
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
a = agg_user_vip_retained_fct(statDate, eid)
a()



