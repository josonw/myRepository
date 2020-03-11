#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_place_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_play_user_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fplayusercnt bigint,
          fgamenum     bigint,
          fmatchnum    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_play_user_fct'
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

        hql = """
        insert overwrite table analysis.gameparty_play_user_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, d.fgamefsk fgamefsk, d.fplatformfsk fplatformfsk,
                d.fversionfsk fversionfsk, d.fterminalfsk fterminalfsk,
                count(distinct c.fuid) fplayusercnt,
                count(distinct case when c.fpname='游戏场' then c.fuid else null end) fgamenum,
                count(distinct case when c.fpname='比赛场' then c.fuid else null end) fmatchnum
            from stage.user_gameparty_stg c
            join analysis.bpid_platform_game_ver_map d
                on c.fbpid = d.fbpid
            where dt="%(statdate)s" and fpalyer_cnt != 0
            group by d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk
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
a = agg_gameparty_place_data(statDate, eid)
a()
