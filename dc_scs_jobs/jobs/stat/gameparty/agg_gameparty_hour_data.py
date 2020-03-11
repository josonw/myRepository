#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_hour_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_playuser_hour_fct
        (
          fdate           date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fhourfsk        bigint,
          fplayusernum bigint,
          fpartynum    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_playuser_hour_fct'
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

        hql = """
        insert overwrite table analysis.user_playuser_hour_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                hour(pu.fls_at)+1 fhourfsk,
                count(distinct fuid) fplayusernum,
                count(distinct concat(gameparty_id,tbl_id) ) fpartynum
            from stage.finished_gameparty_uid_mid pu
            join analysis.bpid_platform_game_ver_map bpm
              on bpm.fbpid = pu.fbpid
            where pu.dt="%(statdate)s"
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, hour(pu.fls_at)+1
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
a = agg_gameparty_hour_data(statDate, eid)
a()
