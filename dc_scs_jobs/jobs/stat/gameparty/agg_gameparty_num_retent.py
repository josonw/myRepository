#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_num_retent(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_num_retent_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpartynumfsk        bigint,
          flaterdays   bigint,
          fregusernum  bigint,
          f1dretunum   bigint,
          f2dretunum   bigint,
          f3dretunum   bigint,
          f4dretunum   bigint,
          f5dretunum   bigint,
          f6dretunum   bigint,
          f7dretunum   bigint,
          f14dretunum  bigint,
          f30dretunum  bigint
        )
        partitioned by (dt date)
        location '/dw/analysis/user_gameparty_num_retent_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate, 'ld_30dayago': PublicFunc.add_days(statDate, -30) }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """ -- 玩牌局数分布，各分布用户之后n天留存的活跃留存
        use analysis;
        insert overwrite table analysis.user_gameparty_num_retent_fct
        partition( dt )
            select gp.fdate fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk, gp.fpartynumfsk fpartynumfsk,
                0 flaterdays,
                count( distinct gp.fuid) fregusernum,
                count( distinct case when u.dt=date_add(gp.fdate, 1) then gp.fuid else null end ) f1dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 2) then gp.fuid else null end ) f2dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 3) then gp.fuid else null end ) f3dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 4) then gp.fuid else null end ) f4dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 5) then gp.fuid else null end ) f5dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 6) then gp.fuid else null end ) f6dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 7) then gp.fuid else null end ) f7dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 14) then gp.fuid else null end ) f14dretunum,
                count( distinct case when u.dt=date_add(gp.fdate, 30) then gp.fuid else null end ) f30dretunum,
                gp.fdate dt
            from (
                select a.dt fdate, a.fbpid fbpid, a.fuid fuid,
                    case when sum(nvl(fparty_num,0))=0 then 1777140815
                        when sum(nvl(fparty_num,0))=1 then 1782301487
                        when 2<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=5 then 1782301488
                        when 6<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=10 then 1782301489
                        when 11<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=20 then 1777140817
                        when 21<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=30 then 1777140818
                        when 31<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=40 then 1777140819
                        when 41<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=50 then 1777140820
                        when 51<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=60 then 1777140821
                        when 61<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=70 then 1777140822
                        when 71<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=80 then 1777140823
                        when 81<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=90 then 1777140824
                        when 91<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=100 then 1777140825
                        when 101<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=150 then 1777140826
                        when 151<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=200 then 1777140827
                        when 201<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=300 then 1777140828
                        when 301<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=400 then 1777140829
                        when 401<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=500 then 1777140830
                        when 501<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=1000 then 1777140831
                        else 1777140832
                    end fpartynumfsk
                from stage.user_dim a
                left join stage.user_gameparty_info_mid b
                on a.fbpid = b.fbpid and a.fuid = b.fuid and a.dt = b.dt
                and b.dt>="%(ld_30dayago)s" and b.dt<="%(statdate)s"
                where a.dt>="%(ld_30dayago)s" and a.dt<="%(statdate)s"
                group by a.dt, a.fbpid, a.fuid
            ) gp
            left join (
                select dt, fuid, fbpid
                from stage.active_user_mid
                where dt>="%(ld_30dayago)s" and dt<="%(statdate)s"
            ) u
                on gp.fuid = u.fuid and gp.fbpid = u.fbpid
            join analysis.bpid_platform_game_ver_map bpm
                on gp.fbpid = bpm.fbpid
            group by gp.fdate, bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, gp.fpartynumfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        insert overwrite table analysis.user_gameparty_num_retent_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fpartynumfsk,
        flaterdays,
        fregusernum,
        f1dretunum,
        f2dretunum,
        f3dretunum,
        f4dretunum,
        f5dretunum,
        f6dretunum,
        f7dretunum,
        f14dretunum,
        f30dretunum
        from analysis.user_gameparty_num_retent_fct
        where dt>="%(ld_30dayago)s" and dt<="%(statdate)s"

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
a = agg_gameparty_num_retent(statDate, eid)
a()
