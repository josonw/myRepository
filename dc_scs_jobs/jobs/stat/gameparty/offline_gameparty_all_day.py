#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class offline_gameparty_all_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.offline_gameparty_all_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpartyname   varchar(50),
          fpartynum    bigint,
          fusernum     bigint,
          fusercnt     bigint
        )
        location '/dw/analysis/offline_gameparty_all_fct'
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

        hql = """ -- 离线牌局
        insert overwrite table analysis.offline_gameparty_all_fct
            select to_date(urt.fplay_at) fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                urt.fname fpartyname, sum(urt.fcnt) fpartynum,
                count(distinct urt.fuid) fusernum, count(urt.fuid) fusercnt
            from stage.offline_gameparty_stg urt
            join analysis.bpid_platform_game_ver_map bpm
                on urt.fbpid = bpm.fbpid
            group by to_date(urt.fplay_at), bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk,
                bpm.fterminalfsk, urt.fname
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
a = offline_gameparty_all_day(statDate, eid)
a()