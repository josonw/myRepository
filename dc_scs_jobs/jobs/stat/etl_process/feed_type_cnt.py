#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class feed_type_cnt(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_feed_type_fct
        (
          fdate        date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fcnt         bigint,
          fusercnt     bigint,
          fregcnt      bigint,
          flogincnt    bigint,
          ffeed_id     string,
          fclickfcnt   bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_feed_type_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        use analysis;
        create external table if not exists analysis.feed_feed_dim
        (
          fgamefsk        bigint,
          ffeed_id       string,
          fdis_name      varchar(50)
        )
        location '/dw/analysis/feed_feed_dim'
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
        insert overwrite table analysis.user_feed_type_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, a.fplatformfsk fplatformfsk, a.fgamefsk fgamefsk,
                a.fversionfsk fversionfsk, a.fterminalfsk fterminalfsk,
                max(a.fcnt) fcnt,
                max(a.fusercnt) fusercnt,
                max(a.fregcnt) fregcnt,
                max(a.flogincnt) flogincnt,
                a.ffeed_id ffeed_id,
                max(a.fclickfcnt) fclickfcnt
            from (
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    count(1) fcnt, count(distinct ss.fuid) fusercnt,
                    0 fregcnt, 0 flogincnt, ss.ffeed_tpl_id ffeed_id, 0 fclickfcnt
                from stage.feed_send_stg ss
                join analysis.bpid_platform_game_ver_map bpm
                    on ss.fbpid = bpm.fbpid
                where ss.dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, ss.ffeed_tpl_id
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    0 fcnt, 0 fusercnt,
                    count(distinct case when fcs.ffeed_as = 2 then fcs.fuid end) fregcnt,
                    count(distinct case when fcs.ffeed_as = 1 then fcs.fuid end) flogincnt,
                    fcs.ffeed_tpl_id ffeed_id,
                    count(case when fcs.ffeed_as = 0 then 1 end) fclickfcnt
                from stage.feed_clicked_stg fcs
                join analysis.bpid_platform_game_ver_map bpm
                    on fcs.fbpid = bpm.fbpid
                where fcs.dt="%(statdate)s" and fcs.ffeed_tpl_id is not null
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fcs.ffeed_tpl_id
            ) a
            group by a.fplatformfsk, a.fgamefsk, a.fversionfsk, a.fterminalfsk, a.ffeed_id
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert into table analysis.feed_feed_dim
            select a.fgamefsk fgamefsk, a.ffeed_id ffeed_id, a.ffeed_id fdis_name
            from (
                select distinct fgamefsk, ffeed_id
                from analysis.user_feed_type_fct
                where dt = "%(statdate)s"
            ) a
            left outer join analysis.feed_feed_dim fd
                on fd.fgamefsk = a.fgamefsk and fd.ffeed_id=a.ffeed_id
            where fd.fgamefsk is null
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
a = feed_type_cnt(statDate, eid)
a()
