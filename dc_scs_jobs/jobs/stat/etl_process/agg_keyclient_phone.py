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

class agg_keyclient_phone(BaseStat):
    def create_tab(self):


        hql = """
        use analysis;
        create table if not exists analysis.user_lost_keyclient_info
        (
            fdate date,
            fbpid   string,
            fuid    bigint,
            ftype   string,
            fid string,
            fdate_diff  bigint,
            fusd    bigint,
            fstatus string,
            fsenttime   date
        )
        partitioned by(dt date)

        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):

        query = { 'pd_usd':2,'statdate':statDate,'ld_1daylater': PublicFunc.add_days(statDate, 1),
        'ld_7daybefore':PublicFunc.add_days(statDate, -7),'ld_14daybefore':PublicFunc.add_days(statDate, -14)}
        hql_list = []
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql =  """insert overwrite table analysis.user_lost_keyclient_info
                  partition (dt = "%(statdate)s" )
                  select "%(statdate)s" as fdate, aa.fbpid, aa.fuid, ftype, fid, 7 as fdate_diff,fusd,"成功" as fstatus,null
                      from (select a.fbpid, a.fuid
                              from stage.active_user_mid a
                              left outer join (
                              select fbpid,fuid from stage.active_user_mid
                              where fdate >  "%(ld_7daybefore)s"
                               and fdate <  "%(ld_1daylater)s" )b
                                on a.fbpid = b.fbpid
                               and a.fuid = b.fuid
                              join analysis.bpid_platform_game_ver_map c
                                on a.fbpid = c.fbpid
                               and c.fgamefsk = 4125815876
                             where a.fdate = "%(ld_7daybefore)s"
                             and b.fuid is null) aa -- 七天前有活跃，七天后无活跃
                      join (select fbpid, fuid, max(fusd) as fusd
                              from stage.payment_stream_all
                              where fusd >= %(pd_usd)s
                              group by fbpid, fuid) bb -- 付费大于2美金
                        on aa.fbpid = bb.fbpid
                       and aa.fuid = bb.fuid
                      join stage.user_ids_stg cc
                        on aa.fbpid = cc.fbpid
                       and aa.fuid = cc.fuid
                       and ftype = 'phone'
                     group by aa.fbpid, aa.fuid, ftype, fid, fusd;
                   """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql =  """insert into table analysis.user_lost_keyclient_info
                  partition (dt = "%(statdate)s" )
                  select "%(statdate)s" as fdate, aa.fbpid, aa.fuid, ftype, fid, 14 as fdate_diff, fusd,"成功" as fstatus,null
                      from (select a.fbpid, a.fuid
                              from stage.active_user_mid a
                              left outer join (
                              select fbpid,fuid from stage.active_user_mid
                              where fdate >  "%(ld_14daybefore)s"
                               and fdate <  "%(ld_1daylater)s" )b
                                on a.fbpid = b.fbpid
                               and a.fuid = b.fuid
                              join analysis.bpid_platform_game_ver_map c
                                on a.fbpid = c.fbpid
                               and c.fgamefsk = 4125815876
                             where a.fdate = "%(ld_14daybefore)s"
                               and b.fuid is null) aa
                      join (select fbpid, fuid, max(fusd) as fusd
                              from stage.payment_stream_all
                              where fusd >= %(pd_usd)s
                              group by fbpid, fuid) bb -- 付费大于2美金
                        on aa.fbpid = bb.fbpid
                       and aa.fuid = bb.fuid
                      join stage.user_ids_stg cc
                        on aa.fbpid = cc.fbpid
                       and aa.fuid = cc.fuid
                       and ftype = 'phone'
                     group by aa.fbpid, aa.fuid, ftype, fid, fusd
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
a = agg_keyclient_phone(statDate, eid)
a()


