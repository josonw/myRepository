#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_ante_hour_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_ante_hour_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpname       varchar(100),
          fante        bigint,
          fhourfsk        bigint,
          fusernum     bigint,
          fplayusernum bigint,
          fpartynum    bigint,
          fcharge      decimal(30,2)
        )
        partitioned by(dt date)
        location '/dw/analysis/user_gameparty_ante_hour_fct'
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

        hql = """ -- 台费小时牌局
        insert overwrite table analysis.user_gameparty_ante_hour_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpname,
                fblind_1 fante,
                hour(flts_at)+1 fhourfsk,
                count(1) fusernum,
                count(distinct fuid) fplayusernum,
                count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
                sum(fcharge) fcharge
            from analysis.bpid_platform_game_ver_map bpm
            join stage.user_gameparty_stg gs
                on gs.fbpid = bpm.fbpid and gs.dt="%(statdate)s"
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpname, fblind_1, hour(flts_at)+1
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
a = agg_gameparty_ante_hour_data(statDate, eid)
a()
