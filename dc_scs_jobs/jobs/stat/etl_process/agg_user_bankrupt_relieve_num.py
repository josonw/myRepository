#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

#破产用户且领取救济用户
class agg_user_bankrupt_relieve_num(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_bankrupt_relieve_num_fct
        (
          fdate           date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          f1relievecnt    bigint,
          f2relievecnt    bigint,
          f3relievecnt    bigint,
          fmorerelievecnt bigint,
          f0relievecnt    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_bankrupt_relieve_num_fct'
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
        insert overwrite table analysis.user_bankrupt_relieve_num_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate,
                b.fplatformfsk fplatformfsk, b.fgamefsk fgamefsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                count(distinct case when a.fcnt=1 then a.fuid else null end) f1relievecnt,
                count(distinct case when a.fcnt=2 then a.fuid else null end) f2relievecnt,
                count(distinct case when a.fcnt=3 then a.fuid else null end) f3relievecnt,
                count(distinct case when a.fcnt>3 then a.fuid else null end) fmorerelievecnt,
                count(distinct case when a.fcnt=0 then a.fuid else null end) f0relievecnt
            from stage.user_bankrupt_relieve_stg a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            where dt="%(statdate)s"
            group by b.fplatformfsk , b.fgamefsk, b.fversionfsk, b.fterminalfsk


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
a = agg_user_bankrupt_relieve_num(statDate, eid)
a()
