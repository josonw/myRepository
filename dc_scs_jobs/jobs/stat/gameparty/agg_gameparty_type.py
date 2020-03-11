#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_type(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_type_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpname          varchar(200),
          fsubname        varchar(200),
          fusernum        bigint,
          fpartynum       bigint,
          fcharge         decimal(20,4),
          fplayusernum    bigint,
          fregplayusernum bigint,
          factplayusernum bigint,
          f2partynum      bigint,
          f3partynum      bigint,
          flose           bigint,
          fwin            bigint,
          falltime        bigint,
          fpartytime      bigint,
          ftrustee_num    bigint,
          fweedout_num    bigint,
          fbankrupt_num   bigint,
          fwin_cnt        bigint,
          flose_cnt       bigint,
          fentry_fee      bigint,
          fentry_cnt      bigint,
          fentry_num      bigint,
          fmatch_cnt      bigint,
          fquit_num       bigint,
          fmatch_usercnt  bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_type_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate, "num_begin": statDate.replace('-', '')}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    --
        drop table if exists stage.agg_gameparty_type_%(num_begin)s;

        create table stage.agg_gameparty_type_%(num_begin)s as
            select bpm.fgamefsk fgamefsk, bpm.fplatformfsk fplatformfsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                gs.fpname fpname, gs.fsubname fsubname,
                sum(gs.fusernum) fusernum,
                sum(gs.fpartynum) fpartynum,
                sum(gs.fcharge) fcharge,
                sum(gs.fplayusernum) fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                sum(gs.f2partynum) f2partynum,
                0 f3partynum,
                cast(0 as bigint) flose,
                cast(0 as bigint) fwin,
                0 falltime,
                0 fpartytime,
                sum(gs.ftrustee_num) ftrustee_num,
                sum(gs.fweedout_num) fweedout_num,
                sum(gs.fbankrupt_num) fbankrupt_num,
                0 fwin_cnt,
                0 flose_cnt,
                0 fentry_fee,
                0 fentry_cnt,
                0 fentry_num,
                sum(gs.fmatch_cnt) fmatch_cnt,
                sum(gs.fquit_num) fquit_num,
                sum(gs.fmatch_usercnt) fmatch_usercnt
            from (
                select fbpid, fpname, fsubname, count(fuid) fusernum,
                    count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
                    sum(fcharge) fcharge, count(distinct fuid) fplayusernum,
                    count(distinct case when fpalyer_cnt = 2 then concat_ws('0', ftbl_id, finning_id) end) f2partynum,
                    count(distinct case when ftrustee_num=0 then null else fuid end) ftrustee_num,
                    count(distinct case when fis_weedout=0 then null else fuid end) fweedout_num,
                    count(distinct case when fis_bankrupt=0 then null else fuid end) fbankrupt_num,
                    count(distinct case when fmatch_id='0' then null else fmatch_id end) fmatch_cnt,
                    count(distinct case when fis_end=0 then null else fuid end) fquit_num,
                    count(distinct case when fmatch_id='0' then null else concat(fuid,fmatch_id) end) fmatch_usercnt
                from stage.user_gameparty_stg
                where dt="%(statdate)s" and fpalyer_cnt != 0
                group by fbpid, fpname, fsubname
            ) gs
            join analysis.bpid_platform_game_ver_map bpm
                on gs.fbpid = bpm.fbpid
            group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, gs.fpname, gs.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 加入输赢总数
        insert into table stage.agg_gameparty_type_%(num_begin)s
            select b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                a.fpname fpname, a.fsubname fsubname,
                0 fusernum, 0 fpartynum, 0 fcharge, 0 fplayusernum, 0 fregplayusernum,
                0 factplayusernum, 0 f2partynum, 0 f3partynum,
                sum(a.flose_num) flose,
                sum(a.fwin_num) fwin,
                sum(a.fplaytime) falltime,
                0 fpartytime, 0 ftrustee_num, 0 fweedout_num, 0 fbankrupt_num,
                sum(a.fwin_party_num) fwin_cnt,
                sum(a.flose_party_num) flose_cnt,
                0 fentry_fee, 0 fentry_cnt, 0 fentry_num, 0 fmatch_cnt, 0 fquit_num, 0 fmatch_usercnt
            from analysis.bpid_platform_game_ver_map b
            join stage.user_gameparty_info_mid a
                on a.fbpid = b.fbpid and a.dt="%(statdate)s"
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        insert into table stage.agg_gameparty_type_%(num_begin)s
            select b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                a.fpname fpname, a.fsubname fsubname,
                0 fusernum, 0 fpartynum, 0 fcharge, 0 fplayusernum, 0 fregplayusernum,
                0 factplayusernum, 0 f2partynum, 0 f3partynum, 0 flose, 0 fwin, 0 falltime,
                sum(a.ts) fpartytime,
                0 ftrustee_num, 0 fweedout_num, 0 fbankrupt_num, 0 fwin_cnt, 0 flose_cnt,
                0 fentry_fee, 0 fentry_cnt, 0 fentry_num, 0 fmatch_cnt, 0 fquit_num, 0 fmatch_usercnt
            from (
                select fbpid, ftbl_id, finning_id, fsubname, fpname, fpalyer_cnt,
                    max(
                        case when fs_timer = '1970-01-01 00:00:00' then 0
                            when fe_timer = '1970-01-01 00:00:00' then 0
                        else unix_timestamp(fe_timer)-unix_timestamp(fs_timer)
                        end
                    ) ts
                from stage.user_gameparty_stg
                where dt="%(statdate)s"
                 group by fbpid, ftbl_id, finning_id, fsubname, fpname, fpalyer_cnt
            ) a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 组合数据导入到表 analysis.gameparty_type_fct
        insert overwrite table analysis.gameparty_type_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, coalesce(fpname,'未知') fpname, fsubname,
                max(fusernum) fusernum,
                max(fpartynum) fpartynum,
                max(fcharge) fcharge,
                max(fplayusernum) fplayusernum,
                max(fregplayusernum) fregplayusernum,
                max(factplayusernum) factplayusernum,
                max(f2partynum) f2partynum,
                max(f3partynum) f3partynum,
                max(flose) flose,
                max(fwin) fwin,
                max(falltime) falltime,
                max(fpartytime) fpartytime,
                max(ftrustee_num) ftrustee_num,
                max(fweedout_num) fweedout_num,
                max(fbankrupt_num) fbankrupt_num,
                max(fwin_cnt) fwin_cnt,
                max(flose_cnt) flose_cnt,
                max(fentry_fee) fentry_fee,
                max(fentry_cnt) fentry_cnt,
                max(fentry_num) fentry_num,
                max(fmatch_cnt) fmatch_cnt,
                max(fquit_num) fquit_num,
                max(fmatch_usercnt) fmatch_usercnt
            from stage.agg_gameparty_type_%(num_begin)s
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, coalesce(fpname,'未知'), fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_type_%(num_begin)s;
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
a = agg_gameparty_type(statDate, eid)
a()
