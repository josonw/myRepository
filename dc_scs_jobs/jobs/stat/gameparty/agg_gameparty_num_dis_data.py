#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_num_dis_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_mustblind_user_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fmust_blind  bigint,
          fpname       varchar(200),
          fsubname     varchar(200),
          f1usernum    bigint,
          f10usernum   bigint,
          f20usernum   bigint,
          f50usernum   bigint,
          f100usernum  bigint,
          f150usernum  bigint,
          f250usernum  bigint,
          f400usernum  bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_mustblind_user_fct'
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
        insert overwrite table analysis.gameparty_mustblind_user_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                gs.fblind_1 fmust_blind, nvl(gs.fpname,'其他') fpname, nvl(gs.fsubname,'其他') fsubname,
                count(distinct case when gs.fpartynum < 10 then gs.fuid end) f1usernum,
                count(distinct case when gs.fpartynum >= 10 and gs.fpartynum < 20 then gs.fuid end) f10usernum,
                count(distinct case when gs.fpartynum >= 20 and gs.fpartynum < 50 then gs.fuid end) f20usernum,
                count(distinct case when gs.fpartynum >= 50 and gs.fpartynum < 100 then gs.fuid end) f50usernum,
                count(distinct case when gs.fpartynum >= 100 and gs.fpartynum < 150 then gs.fuid end) f100usernum,
                count(distinct case when gs.fpartynum >= 150 and gs.fpartynum < 250 then gs.fuid end) f150usernum,
                count(distinct case when gs.fpartynum >= 250 and gs.fpartynum < 400 then gs.fuid end) f250usernum,
                count(distinct case when gs.fpartynum >= 400 then gs.fuid end) f400usernum
            from (
                  select fbpid, fuid, fpname, fsubname, fblind_1, count(1) fpartynum
                    from stage.user_gameparty_stg
                   where dt = "%(statdate)s"
                     and fblind_1 != 0
                     and fpalyer_cnt != 0
                   group by fbpid, fuid, fpname, fsubname, fblind_1
            ) gs
            join analysis.bpid_platform_game_ver_map bpm
               on gs.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
               gs.fblind_1, nvl(gs.fpname,'其他'), nvl(gs.fsubname,'其他')
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
a = agg_gameparty_num_dis_data(statDate, eid)
a()
