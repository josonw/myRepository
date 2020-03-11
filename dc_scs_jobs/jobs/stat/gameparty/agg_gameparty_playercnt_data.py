#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_playercnt_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_playercnt_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fplayer_cnt     bigint,
          fusernum        bigint,
          fpartynum       bigint,
          fcharge         decimal(30,2),
          fplayusernum    bigint,
          fregplayusernum bigint,
          factplayusernum bigint,
          fpname          varchar(200),
          fsubname        varchar(200),
          falltime        bigint,
          fpartytime      bigint,
          flose           bigint,
          fwin            bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_playercnt_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate, "num_begin": statDate.replace('-', '') }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        drop table if exists stage.agg_gameparty_playercnt_data_%(num_begin)s;

        create table stage.agg_gameparty_playercnt_data_%(num_begin)s as
            select "%(statdate)s" fdate, b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                a.fpalyer_cnt fplayer_cnt,
                count(a.fuid) fusernum,
                count(distinct concat_ws('0', a.ftbl_id, a.finning_id)) fpartynum,
                sum(a.fcharge) fcharge,
                count(distinct a.fuid) fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                a.fpname fpname,
                a.fsubname fsubname,
                sum(case when a.fs_timer = '1970-01-01 00:00:00' then 0
                        when a.fe_timer = '1970-01-01 00:00:00' then 0
                        else unix_timestamp(a.fe_timer)-unix_timestamp(a.fs_timer)
                    end
                ) falltime,
                0 fpartytime,
                sum( case when a.fgamecoins < 0 then abs(a.fgamecoins) else 0 end) flose,
                sum( case when a.fgamecoins > 0 then a.fgamecoins else 0 end) fwin
            from stage.user_gameparty_stg a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            where a.fpalyer_cnt!=0 and a.dt="%(statdate)s"
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                a.fpalyer_cnt, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """ -- 牌局时长，取桌子最长牌局时间再汇总
        insert into table stage.agg_gameparty_playercnt_data_%(num_begin)s
            select "%(statdate)s" fdate, b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                a.fpalyer_cnt fplayer_cnt,
                0 fusernum,
                0 fpartynum,
                0 fcharge,
                0 fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                a.fpname fpname,
                a.fsubname fsubname,
                0 falltime,
                sum(a.ts) fpartytime,
                0 flose,
                0 fwin
            from (
                select fbpid, ftbl_id, finning_id, fpname, fsubname, fpalyer_cnt,
                    max(case when fs_timer = '1970-01-01 00:00:00' then 0
                        when fe_timer = '1970-01-01 00:00:00' then 0
                        else unix_timestamp(fe_timer)-unix_timestamp(fs_timer)
                        end
                    ) ts
                from stage.user_gameparty_stg
                where fpalyer_cnt!=0 and dt = "%(statdate)s"
                group by fbpid, ftbl_id, finning_id, fpname, fsubname, fpalyer_cnt
            ) a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                a.fpalyer_cnt, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.gameparty_playercnt_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fplayer_cnt,
                sum(fusernum) fusernum,
                sum(fpartynum) fpartynum,
                sum(fcharge) fcharge,
                sum(fplayusernum) fplayusernum,
                sum(fregplayusernum) fregplayusernum,
                sum(factplayusernum) factplayusernum,
                fpname,
                fsubname,
                sum(falltime) falltime,
                sum(fpartytime) fpartytime,
                sum(flose) flose,
                sum(fwin) fwin
            from stage.agg_gameparty_playercnt_data_%(num_begin)s
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fplayer_cnt, fpname, fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_playercnt_data_%(num_begin)s;
        """ % query
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
a = agg_gameparty_playercnt_data(statDate, eid)
a()
