#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_feed_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_feed_fct
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
          fgradefsk        bigint,
          ffeedtypefsk        bigint,
          fusercnt         bigint,
          ffeedcnt         bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_feed_fct'
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
        insert overwrite table analysis.user_feed_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                1 fagefsk,
                1 foccupationalfsk,
                1 fsexfsk,
                1 fcityfsk,
                1 fgradefsk,
                case when feed.ffeed_as=1 then 4507577
                    when feed.ffeed_as=2 then 4507578
                    when feed.ffeed_as=3 then 4507579
                else 1
                end ffeedtypefsk,
                count(distinct feed.fuid) fusercnt,
                count(*) ffeedcnt
            from (
                select fbpid, fuid, 3 ffeed_as
                      from stage.feed_send_stg ss
                     where ss.dt = "%(statdate)s"
                    union all
                    select fbpid, fuid, ffeed_as
                      from stage.feed_clicked_stg cs
                     where cs.dt = "%(statdate)s"
            ) feed
            join analysis.bpid_platform_game_ver_map bpm
                on feed.fbpid = bpm.fbpid
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                case when feed.ffeed_as=1 then 4507577
                    when feed.ffeed_as=2 then 4507578
                    when feed.ffeed_as=3 then 4507579
                else 1 end
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
a = agg_user_feed_day(statDate, eid)
a()
