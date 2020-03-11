#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_flow_loss_day(BaseStat):

    def create_tab(self):
        hql = """    -- 新增用户流失漏斗
        use analysis;
        create table if not exists analysis.user_flow_loss_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          f1dusernum   bigint,
          f2dusernum   bigint,
          f3dusernum   bigint,
          f4dusernum   bigint,
          f5dusernum   bigint,
          f6dusernum   bigint,
          f7dusernum   bigint,
          f14dusernum  bigint,
          f30dusernum  bigint
        )
        partitioned by (dt date)
        location '/dw/analysis/user_flow_loss_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate, "num_begin": statDate.replace('-', ''),
            'ld_89dayago':PublicFunc.add_days(statDate, -89)}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    --最近89天的重新生成
        use stage;
        drop table if exists stage.agg_user_flow_loss_day_%(num_begin)s;

        create table stage.agg_user_flow_loss_day_%(num_begin)s as
            select src.fdate fdate, bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                count( distinct case when src.fact_day>=date_add(fdate, 1) then src.fuid end ) f1dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 2) then src.fuid end ) f2dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 3) then src.fuid end ) f3dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 4) then src.fuid end ) f4dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 5) then src.fuid end ) f5dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 6) then src.fuid end ) f6dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 7) then src.fuid end ) f7dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 14) then src.fuid end ) f14dusernum,
                count( distinct case when src.fact_day>=date_add(fdate, 30) then src.fuid end ) f30dusernum
            from (
                select a.dt fdate, a.fbpid fbpid, b.dt fact_day, b.fuid fuid
                from stage.user_dim a
                join stage.active_user_mid b
                    on a.fbpid=b.fbpid and a.fuid=b.fuid
                    and b.dt>"%(ld_89dayago)s" and b.dt<="%(statdate)s"
                where a.dt>="%(ld_89dayago)s" and a.dt<="%(statdate)s"
            ) src
            join analysis.bpid_platform_game_ver_map bpm
                on src.fbpid=bpm.fbpid
            group by src.fdate, bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        use analysis;
        insert overwrite table analysis.user_flow_loss_fct
        partition( dt )
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                f1dusernum, f2dusernum, f3dusernum, f4dusernum, f5dusernum, f6dusernum,
                f7dusernum, f14dusernum, f30dusernum, fdate dt
            from stage.agg_user_flow_loss_day_%(num_begin)s
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_user_flow_loss_day_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res



        hql = """
        insert overwrite table analysis.user_flow_loss_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        f1dusernum,
        f2dusernum,
        f3dusernum,
        f4dusernum,
        f5dusernum,
        f6dusernum,
        f7dusernum,
        f14dusernum,
        f30dusernum
        from analysis.user_flow_loss_fct
        where dt>="%(ld_89dayago)s" and dt<="%(statdate)s"
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
a = agg_user_flow_loss_day(statDate, eid)
a()
