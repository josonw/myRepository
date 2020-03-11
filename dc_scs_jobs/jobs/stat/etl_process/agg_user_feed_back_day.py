#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_feed_back_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_feed_back_fct
        (
          fdate            date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fagefsk        bigint,
          foccupationalfsk        bigint,
          fsexfsk        bigint,
          fcityfsk        bigint,
          f1daybackcnt     bigint,
          f2daybackcnt     bigint,
          f3daybackcnt     bigint,
          f4daybackcnt     bigint,
          f5daybackcnt     bigint,
          f6daybackcnt     bigint,
          f7daybackcnt     bigint,
          f14daybackcnt    bigint,
          f30daybackcnt    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_feed_back_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate, 'ld_1dayago':PublicFunc.add_days(statDate, -1),
            'ld_2dayago':PublicFunc.add_days(statDate, -2), 'ld_3dayago':PublicFunc.add_days(statDate, -3),
            'ld_4dayago':PublicFunc.add_days(statDate, -4), 'ld_5dayago':PublicFunc.add_days(statDate, -5),
            'ld_6dayago':PublicFunc.add_days(statDate, -6), 'ld_7dayago':PublicFunc.add_days(statDate, -7),
            'ld_14dayago':PublicFunc.add_days(statDate, -14), 'ld_30dayago':PublicFunc.add_days(statDate, -30)
        }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    --
        insert overwrite table analysis.user_feed_back_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                nvl(ad.fsk, 1) fagefsk,
                case when cast(ud.fprofession as bigint) is null then 1 when cast(ud.fprofession as bigint)=0 then 1 else cast(ud.fprofession as bigint)+4446684 end foccupationalfsk,
                case when ud.fgender=0 then 79 when ud.fgender=1 then 80 else 1 end fsexfsk,
                nvl(cd.fsk, 1) fcityfsk,
                count(case when urt.dt="%(ld_1dayago)s" then urt.fuid else null end) f1daybackcnt,
                count(case when urt.dt="%(ld_2dayago)s" then urt.fuid else null end) f2daybackcnt,
                count(case when urt.dt="%(ld_3dayago)s" then urt.fuid else null end) f3daybackcnt,
                count(case when urt.dt="%(ld_4dayago)s" then urt.fuid else null end) f4daybackcnt,
                count(case when urt.dt="%(ld_5dayago)s" then urt.fuid else null end) f5daybackcnt,
                count(case when urt.dt="%(ld_6dayago)s" then urt.fuid else null end) f6daybackcnt,
                count(case when urt.dt="%(ld_7dayago)s" then urt.fuid else null end) f7daybackcnt,
                count(case when urt.dt="%(ld_14dayago)s" then urt.fuid else null end) f14daybackcnt,
                count(case when urt.dt="%(ld_30dayago)s" then urt.fuid else null end) f30daybackcnt
            from (
                select distinct a.fbpid, a.fuid, b.dt
                from stage.active_user_mid a
                join stage.feed_clicked_stg b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid and b.ffeed_as = 2
                    and cast(b.dt as string) in
                    ("%(ld_1dayago)s", "%(ld_2dayago)s", "%(ld_3dayago)s", "%(ld_4dayago)s", "%(ld_5dayago)s", "%(ld_6dayago)s", "%(ld_7dayago)s", "%(ld_14dayago)s", "%(ld_30dayago)s")
                where a.dt = "%(statdate)s"
            ) urt
            join analysis.bpid_platform_game_ver_map bpm
                on urt.fbpid = bpm.fbpid
            left outer join stage.user_dim ud
                on urt.fbpid = ud.fbpid and urt.fuid = ud.fuid
            left outer join analysis.age_dim ad
                on ud.fage = ad.fagenum
            left outer join analysis.city_dim cd
                on ud.fcity = cd.fcityname
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                nvl(ad.fsk, 1),
                case when cast(ud.fprofession as bigint) is null then 1 when cast(ud.fprofession as bigint)=0 then 1 else cast(ud.fprofession as bigint)+4446684 end,
                case when ud.fgender=0 then 79 when ud.fgender=1 then 80 else 1 end,
                nvl(cd.fsk, 1)
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
a = agg_user_feed_back_day(statDate, eid)
a()
