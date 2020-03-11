#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_room_blind_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_mustblind_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fmust_blind     bigint,
          fusernum        bigint,
          fpartynum       bigint,
          fcharge         decimal(20,4),
          fplayusernum    bigint,
          fregplayusernum bigint,
          factplayusernum bigint,
          f2partynum      bigint,
          f3partynum      bigint,
          fpname          varchar(200),
          fsubname        varchar(200),
          fcommon_usernum bigint,
          fmatch_usernum  bigint,
          falltime        bigint,
          fpartytime      bigint,
          fwin            bigint,
          flose           bigint,
          f2win              bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_mustblind_fct'
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

        hql = """    --
        drop table if exists stage.agg_gameparty_room_blind_data_%(num_begin)s;

        create table stage.agg_gameparty_room_blind_data_%(num_begin)s as
            select bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                gs.fblind_1 fmust_blind,
                sum(gs.fusernum) fusernum,
                sum(gs.fpartynum) fpartynum,
                sum(gs.fcharge) fcharge,
                sum(gs.fplayusernum) fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                0 f2partynum,
                0 f3partynum,
                nvl(gs.fpname,'未知') fpname,
                nvl(gs.fsubname,'未知') fsubname,
                0 fcommon_usernum,
                0 fmatch_usernum,
                0 falltime,
                0 fpartytime,
                sum(gs.fwin_num) fwin,
                sum(gs.flose_num) flose,
                sum(gs.f2win) f2win
            from (
                select fbpid, fpname, fsubname, fblind_1,
                    count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
                    sum(fcharge) fcharge,
                    count(distinct fuid) fplayusernum,
                    count(fuid) fusernum,
                    sum( case when fgamecoins > 0 then fgamecoins else 0 end ) fwin_num,
                    sum( case when fgamecoins < 0 then abs(fgamecoins) else 0 end) flose_num,
                    sum( case when fgamecoins > 0 and fpalyer_cnt = 2 then fgamecoins else 0 end) f2win
                from stage.user_gameparty_stg
                where dt="%(statdate)s" and fpalyer_cnt!=0
                group by fbpid, fpname, fsubname, fblind_1
            ) gs
            join analysis.bpid_platform_game_ver_map bpm
                on gs.fbpid = bpm.fbpid
            group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk,
                gs.fblind_1, nvl(gs.fpname,'未知'), nvl(gs.fsubname,'未知')
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 加入最大牌局时长 从user_gameparty_time中拉来的
        insert into table stage.agg_gameparty_room_blind_data_%(num_begin)s
            select bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                a.fblind_1 fmust_blind,
                0 fusernum,
                0 fpartynum,
                0 fcharge,
                0 fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                count(distinct case when fpalyer_cnt=2 then concat_ws('0', ftbl_id, finning_id) end) f2partynum,
                0 f3partynum,
                nvl(a.fpname,'未知') fpname,
                nvl(a.fsubname,'未知') fsubname,
                0 fcommon_usernum,
                0 fmatch_usernum,
                sum(a.sts) falltime,
                sum(a.ts) fpartytime,
                0 fwin,
                0 flose,
                0 f2win
            from (
                select fbpid, ftbl_id, finning_id, fsubname, fpname, fblind_1, max(fpalyer_cnt) fpalyer_cnt,
                    sum( case when fs_timer = '1970-01-01 00:00:00' then 0
                            when fe_timer = '1970-01-01 00:00:00' then 0
                        else unix_timestamp(fe_timer)-unix_timestamp(fs_timer) end
                    ) sts,
                    max(
                        case when fs_timer = '1970-01-01 00:00:00' then 0
                            when fe_timer = '1970-01-01 00:00:00' then 0
                        else unix_timestamp(fe_timer)-unix_timestamp(fs_timer)
                        end
                    ) ts
                from stage.user_gameparty_stg
                where dt = "%(statdate)s" and fpalyer_cnt!=0
                group by fbpid, ftbl_id, finning_id, fsubname,
                          fpname, fblind_1
            ) a
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk,
                a.fblind_1, nvl(a.fpname,'未知'), nvl(a.fsubname,'未知')
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 组合数据导入到表 analysis.gameparty_mustblind_fct
        insert overwrite table analysis.gameparty_mustblind_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                fmust_blind,
                sum(fusernum) fusernum,
                sum(fpartynum) fpartynum,
                sum(fcharge) fcharge,
                sum(fplayusernum) fplayusernum,
                sum(fregplayusernum) fregplayusernum,
                sum(factplayusernum) factplayusernum,
                sum(f2partynum) f2partynum,
                sum(f3partynum) f3partynum,
                fpname,
                fsubname,
                sum(fcommon_usernum) fcommon_usernum,
                sum(fmatch_usernum) fmatch_usernum,
                sum(falltime) falltime,
                sum(fpartytime) fpartytime,
                sum(fwin) fwin,
                sum(flose) flose,
                sum(f2win) f2win
            from stage.agg_gameparty_room_blind_data_%(num_begin)s
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fmust_blind, fpname, fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_room_blind_data_%(num_begin)s;
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
a = agg_gameparty_room_blind_data(statDate, eid)
a()
