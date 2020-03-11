#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_offline_only(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_offline_only_fct
        (
          fdate            date,
          fgamefsk      bigint,
          fplatformfsk  bigint,
          fversionfsk   bigint,
          fterminalfsk  bigint,
          fpartyname       varchar(50),
          fact_offonly  bigint,
          freg_offonly  bigint
        )
        partitioned by (dt date)
        location '/dw/analysis/gameparty_offline_only_fct'
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

        hql = """ -- 离线牌局 --联网状态下
        insert overwrite table analysis.gameparty_offline_only_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                d.fname fpartyname,
                count(distinct(case when d.is_online is null then d.fuid end)) fact_offonly,
                 count(distinct(case when d.is_online is null and d.is_reg is not null then d.fuid end)) freg_offonly
            from (
                select a.fbpid fbpid, a.fuid fuid, a.fname fname, b.fuid is_online, c.fuid is_reg
                from stage.offline_gameparty_stg a
                left outer join stage.user_gameparty_info_mid b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid and b.dt = "%(statdate)s"
                left outer join stage.user_dim c
                    on a.fbpid = c.fbpid and a.fuid = c.fuid and c.dt = "%(statdate)s"
                where a.dt = "%(statdate)s" and to_date(a.fplay_at) = "%(statdate)s"
            ) d
            join analysis.bpid_platform_game_ver_map bpm
                on d.fbpid = bpm.fbpid
            group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, d.fname
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
a = agg_gameparty_offline_only(statDate, eid)
a()
