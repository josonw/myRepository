#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reg_gameparty_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_partytype_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fname        varchar(100),
          fparty_num   bigint,
          fparty_user  bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_gameparty_partytype_fct'
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
        insert overwrite table analysis.user_gameparty_partytype_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                b.fname fname, count(b.fuid) fparty_num, count(distinct b.fuid) fparty_user
            from analysis.bpid_platform_game_ver_map bpm
            join stage.user_dim a
                on a.fbpid = bpm.fbpid and a.dt="%(statdate)s"
            left join stage.finished_gameparty_uid_mid b
                on a.fbpid = b.fbpid and a.fuid = b.fuid and b.dt="%(statdate)s"
            group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, b.fname
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
a = agg_reg_gameparty_data(statDate, eid)
a()
