#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_room_hour_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_playuser_hour_fct_ext
        (
          fdate        date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fhourfsk        bigint,
          fpname       varchar(200),
          fsubname     varchar(200),
          fplayusernum bigint,
          fsignupnum   bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_playuser_hour_fct_ext'
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

        hql = """ -- 按小时的牌局类型划分每小时的玩牌人数
        insert overwrite table analysis.user_playuser_hour_fct_ext
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, a.fplatformfsk fplatformfsk, a.fgamefsk fgamefsk,
                a.fversionfsk fversionfsk, a.fterminalfsk fterminalfsk,
                a.fhourfsk fhourfsk,
                a.fpname fpname,
                a.fsubname fsubname,
                max(a.fplayusernum) fplayusernum,
                max(a.fsignupnum) fsignupnum
            from (
                select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk, bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                    hd.fsk fhourfsk, pu.fpname fpname, pu.fsubname fsubname,
                    count(distinct pu.fuid) fplayusernum, 0 fsignupnum
                from stage.user_gameparty_stg pu
                join analysis.bpid_platform_game_ver_map bpm
                    on pu.fbpid = bpm.fbpid
                join analysis.hour_dim hd
                    on hd.fhourid = hour(pu.flts_at)
                where pu.dt="%(statdate)s"
                group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, hd.fsk, pu.fpname, pu.fsubname
                    union all
                select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk, bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                    hd.fsk fhourfsk, '比赛场' fpname,pu.fname fsubname,
                    0 fplayusernum, count(distinct pu.fuid) fsignupnum
                from stage.join_gameparty_stg pu
                join analysis.bpid_platform_game_ver_map bpm
                    on pu.fbpid = bpm.fbpid
                join analysis.hour_dim hd
                    on hd.fhourid = hour(pu.flts_at)
                where pu.dt="%(statdate)s"
                group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, hd.fsk, '比赛场', pu.fname
            ) a
            group by a.fplatformfsk, a.fgamefsk, a.fversionfsk, a.fterminalfsk, a.fhourfsk, a.fpname, a.fsubname
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
a = agg_gameparty_room_hour_data(statDate, eid)
a()
