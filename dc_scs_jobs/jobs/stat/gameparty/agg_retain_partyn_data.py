#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_retain_partyn_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_retained_party_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpartycnt    bigint,
          f1dusernum   bigint,
          f7dusernum   bigint,
          f30dusernum  bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_retained_party_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate, 'ld_31dayago': PublicFunc.add_days(statDate, -31) }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    -- 留存-玩牌数分布  1d 7d 30d
        insert overwrite table analysis.user_retained_party_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                c.fpartycnt fpartycnt,
                count(case when c.fretain_day = 1 then c.fuid end ) f1dusernum,
                count(case when c.fretain_day = 7 then c.fuid end ) f7dusernum,
                count(case when c.fretain_day = 30 then c.fuid end ) f30dusernum
            from (
                select a.fbpid fbpid, a.retday fretain_day, a.fuid, sum(b.fparty_num) fpartycnt
                from stage.user_reg_retained_uid_tmp a
                join stage.user_gameparty_info_mid b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid and a.dt = b.fdate
                     and b.dt>="%(ld_31dayago)s" and b.dt<="%(statdate)s"
                where a.retday in (1,7,30)
                group by a.fbpid, a.retday, a.fuid
            ) c
            join analysis.bpid_platform_game_ver_map bpm
                on c.fbpid = bpm.fbpid
            group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, c.fpartycnt
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
a = agg_retain_partyn_data(statDate, eid)
a()
