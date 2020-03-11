#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_mobile_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_mobile_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpname       varchar(200),
          fsubname     varchar(200),
          fmust_blind  bigint,
          fusernum     bigint,
          fpartynum    bigint,
          fcharge      decimal(20,4),
          fplayusernum bigint,
          f2partynum   bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_mobile_fct'
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
        insert overwrite table analysis.gameparty_mobile_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, d.fgamefsk fgamefsk, d.fplatformfsk fplatformfsk,
                d.fversionfsk fversionfsk, d.fterminalfsk fterminalfsk,
                c.fpname fpname,
                c.fsubname fsubname,
                c.fblind_1 fmust_blind,
                sum(c.fusernum) fusernum,
                sum(c.fpartynum) fpartynum,
                sum(c.fcharge) fcharge,
                sum(c.fplayusernum) fplayusernum,
                sum(c.f2partynum) f2partynum
            from (
                select a.fbpid fbpid, a.fpname fpname, a.fsubname fsubname, a.fblind_1 fblind_1, a.ftbl_id ftbl_id,
                    count(fuid) fusernum,
                    count(distinct concat_ws('0', a.ftbl_id, a.finning_id)) fpartynum,
                    sum(fcharge) fcharge,
                    count(distinct fuid) fplayusernum,
                    count(distinct case when a.fpalyer_cnt=2 then concat_ws('0', a.ftbl_id, a.finning_id) end) f2partynum
                from stage.user_gameparty_stg a
                join analysis.poker_tbl_info_dim b
                    on a.fbpid = b.fbpid and a.ftbl_id = b.ftbl_id
                    and b.fdev_limit = 2
                where a.fpalyer_cnt != 0 and a.dt="%(statdate)s"
                group by a.fbpid, a.fpname, a.fsubname, a.fblind_1, a.ftbl_id
            ) c
            join analysis.bpid_platform_game_ver_map d
                on c.fbpid = d.fbpid
            group by d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk, c.fpname, c.fsubname, c.fblind_1
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
a = agg_gameparty_mobile_data(statDate, eid)
a()
