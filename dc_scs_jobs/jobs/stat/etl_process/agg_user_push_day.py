#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_push_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_push_fct
        (
          fdate         date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpushcnt      bigint,
          fpushusercnt  bigint,
          fopenusercnt  int,
          fcolseusercnt int,
          frececnt      int,
          freceusercnt  int,
          factivecnt    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_push_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    --
        insert overwrite table analysis.user_push_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                sum(fpushcnt) fpushcnt,
                sum(fpushusercnt) fpushusercnt,
                sum(fopenusercnt) fopenusercnt,
                sum(fcolseusercnt) fcolseusercnt,
                sum(frececnt) frececnt,
                sum(freceusercnt) freceusercnt,
                sum(factivecnt) factivecnt
            from (
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    count(*) fpushcnt, count(distinct fuid) fpushusercnt,
                    0 fopenusercnt, 0 fcolseusercnt, 0 frececnt, 0 freceusercnt, 0 factivecnt
                from stage.push_send_stg a
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    0 fpushcnt, 0 fpushusercnt, 0 fopenusercnt, 0 fcolseusercnt,
                    count(*) frececnt, count(distinct fuid) freceusercnt, 0 factivecnt
                from stage.push_rece_stg a
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, 0 fpushcnt, 0 fpushusercnt,
                    count(case when fstatus = 1 then fuid else null end) fopenusercnt,
                    count(case when fstatus = 0 then fuid else null end) fcolseusercnt,
                    0 frececnt, 0 freceusercnt, 0 factivecnt
                from stage.push_edit_stg a
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    0 fpushcnt, 0 fpushusercnt, 0 fopenusercnt, 0 fcolseusercnt,
                    0 frececnt, 0 freceusercnt, count(distinct a.fuid) factivecnt
                from stage.active_user_mid a
                join stage.push_rece_stg b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid and b.dt="%(statdate)s"
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where  a.dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
            ) mtp
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
        """    % query
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
a = agg_user_push_day(statDate, eid)
a()
