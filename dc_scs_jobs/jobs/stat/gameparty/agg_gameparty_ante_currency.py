#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_ante_currency(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_ante_currency_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpname       varchar(100),
          fante        bigint,
          fname        varchar(50),
          fpartynum    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_ante_currency_fct'
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

        hql = """ -- 牌局流通
        insert overwrite table analysis.gameparty_ante_currency_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                fpname, fante, fname, count(1) fpartynum
            from (
                select fbpid, fpname, fblind_1 fante, concat_ws('0', ftbl_id, finning_id) gameparty_id,
                    case
                        when sum(abs(fgamecoins)) <= 0 then  '0' --存string
                        when sum(abs(fgamecoins)) >= 1 and sum(abs(fgamecoins)) < 5000 then '1-5000'
                        when sum(abs(fgamecoins)) >= 5000 and sum(abs(fgamecoins)) < 10000 then '5000-1万'
                        when sum(abs(fgamecoins)) >= 10000 and sum(abs(fgamecoins)) < 50000 then  '1万-5万'
                        when sum(abs(fgamecoins)) >= 50000 and sum(abs(fgamecoins)) < 100000 then '5万-10万'
                        when sum(abs(fgamecoins)) >= 100000 and sum(abs(fgamecoins)) < 500000 then '10万-50万'
                        when sum(abs(fgamecoins)) >= 500000 and sum(abs(fgamecoins)) < 1000000 then '50万-100万'
                        when sum(abs(fgamecoins)) >= 1000000 and sum(abs(fgamecoins)) < 5000000 then '100万-500万'
                        when sum(abs(fgamecoins)) >= 5000000 and sum(abs(fgamecoins)) < 10000000 then '500万-1000万'
                        when sum(abs(fgamecoins)) >= 10000000 and sum(abs(fgamecoins)) < 50000000 then '1000万-5000万'
                        when sum(abs(fgamecoins)) >= 50000000 and sum(abs(fgamecoins)) < 100000000 then '5000万-1亿'
                        when sum(abs(fgamecoins)) >= 100000000 and sum(abs(fgamecoins)) < 1000000000 then '1亿-10亿'
                    else '10亿+' end fname
                from stage.user_gameparty_stg
                where dt="%(statdate)s"
                group by fbpid, fpname, fblind_1, concat_ws('0', ftbl_id, finning_id)
            ) a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpname, fante, fname
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
a = agg_gameparty_ante_currency(statDate, eid)
a()
