#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class gameparty_ante_detail(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_settlement_fct
        (
          fdate             date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fante             bigint,
          fplayuser_num     bigint,
          fbankuser_num     bigint,
          fparty_num        bigint,
          fwinplayer_cnt    bigint,
          floseplayer_cnt   bigint,
          fbankparty_num    bigint,
          fwingc_sum        bigint,
          flosegc_sum       bigint,
          fwingc_avg        bigint,
          flosegc_avg       bigint,
          fcharge           bigint,
          fmultiple_avg     bigint,
          f1bankrupt_num    bigint,
          f2bankrupt_num    bigint,
          fbankuser_cnt     bigint,
          frb_num           bigint,
          frb_win_coins     bigint,
          frb_lost_coins    bigint,
          frb_win_party     bigint,
          frb_party         bigint,
          fbankpayusercnt   bigint,
          fbankusercnt      bigint,
          f10bankpayusercnt bigint,
          fpname            string,
            fsubname          string
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_settlement_fct'
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
        drop table if exists stage.gameparty_ante_detail_1_%(num_begin)s;

        create table stage.gameparty_ante_detail_1_%(num_begin)s as
            select b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                a.fante fante,
                0 fplayuser_num,
                count(1) fparty_num,
                sum(a.fwin_player_cnt) fwinplayer_cnt,
                sum(a.flose_player_cnt) floseplayer_cnt,
                count(case when a.fbankrupt_num > 0 then 1 end) fbankparty_num,
                sum(a.fwin_gc_num) fwingc_sum,
                sum(a.flose_gc_num) flosegc_sum,
                round(avg(a.fwin_gc_num)) fwingc_avg,
                round(avg(a.flose_gc_num)) flosegc_avg,
                sum(a.fcharge) fcharge,
                round(avg(a.fmultiple)) fmultiple_avg,
                count(case when a.fbankrupt_num=1 then 1 else null end) f1bankrupt_num,
                count(case when a.fbankrupt_num=2 then 1 else null end) f2bankrupt_num,
                sum(a.frobots_num) frb_num,
                sum(abs(a.frobots_gcin_num)) frb_win_coins,
                sum(abs(a.frobots_gcout_num)) frb_lost_coins,
                count(case when a.frobots_num > 0 and a.frobots_gcin_num > 0 then 1 end) frb_win_party,
                count(case when a.frobots_num > 0 then 1 end) frb_party,
                a.fpname fpname, a.fsubname fsubname
            from (
                select fbpid, fante, fmultiple, fcharge, fsubname, fpname, fbankrupt_num, frobots_num, frobots_gcin_num,
                    frobots_gcout_num, fwin_player_cnt, flose_player_cnt, fwin_gc_num, flose_gc_num
                from stage.gameparty_settlement_stg
                where dt = "%(statdate)s"
            ) a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, a.fante, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        insert into table stage.gameparty_ante_detail_1_%(num_begin)s
            select b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                a.fblind_1 fante,
                count( distinct a.fuid ) fplayuser_num,
                0 fparty_num,
                0 fwinplayer_cnt,
                0 floseplayer_cnt,
                0 fbankparty_num,
                0 fwingc_sum,
                0 flosegc_sum,
                0 fwingc_avg,
                0 flosegc_avg,
                0 fcharge,
                0 fmultiple_avg,
                0 f1bankrupt_num,
                0 f2bankrupt_num,
                0 frb_num,
                0 frb_win_coins,
                0 frb_lost_coins,
                0 frb_win_party,
                0 frb_party,
                a.fpname fpname, a.fsubname fsubname
            from stage.user_gameparty_stg a
            join analysis.bpid_platform_game_ver_map b
                on b.fbpid = a.fbpid
            where  a.dt="%(statdate)s"
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, a.fblind_1, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        drop table if exists stage.gameparty_ante_detail_2_%(num_begin)s;
        CREATE TABLE stage.gameparty_ante_detail_2_%(num_begin)s AS
        SELECT b.fgamefsk fgamefsk,
               b.fplatformfsk fplatformfsk,
               b.fversionfsk fversionfsk,
               b.fterminalfsk fterminalfsk,
               a.fuphill_pouring fante,
               count(DISTINCT a.fuid) fbankuser_num,
               count(1) fbankuser_cnt,
               0 fbankpayusercnt,
                0 fbankusercnt,
                0 f10bankpayusercnt,
                fpname,
                a.fplayground_title fsubname
        FROM analysis.bpid_platform_game_ver_map b
        JOIN stage.user_bankrupt_stg a ON b.fbpid = a.fbpid
        AND a.dt="%(statdate)s"
        GROUP BY b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 a.fuphill_pouring,
                 fpname,
                 a.fplayground_title
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 计算10,30分钟内坡长付费率
        insert into table stage.gameparty_ante_detail_2_%(num_begin)s
            select a.fgamefsk fgamefsk, a.fplatformfsk fplatformfsk,
                a.fversionfsk fversionfsk, a.fterminalfsk fterminalfsk,
                a.fuphill_pouring fante,
                0 fbankuser_num,
                0 fbankuser_cnt,
                count(distinct a.f30uid) fbankpayusercnt,
                count(distinct a.fuid) fbankusercnt,
                count(distinct a.f10uid) f10bankpayusercnt,
                fpname,
                a.fplayground_title fsubname
            from (
                select b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk,
                    b.fuid fuid,
                    case when c.fuid is null then null
                        when unix_timestamp(c.fpay_at)-1800 <= unix_timestamp(b.frupt_at) then c.fuid
                        else null
                    end f30uid,
                    case when c.fuid is null then null
                        when unix_timestamp(c.fpay_at)-600 <= unix_timestamp(b.frupt_at) then c.fuid
                        else null
                    end f10uid,
                    b.fuphill_pouring fuphill_pouring,
                    fpname,
                    b.fplayground_title fplayground_title
                from (
                    select bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                        ubs.fuid fuid, ubs.frupt_at frupt_at,
                        ubs.fuphill_pouring fuphill_pouring,
                        fpname,
                        ubs.fplayground_title fplayground_title
                    from stage.user_bankrupt_stg ubs
                    join analysis.bpid_platform_game_ver_map bpm
                        on ubs.fbpid = bpm.fbpid
                    where ubs.dt="%(statdate)s"
                ) b
                left join (
                    select bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                        e.fuid fuid, d.fdate fpay_at
                    from stage.payment_stream_stg d
                    join stage.pay_user_mid e
                        on e.fbpid = d.fbpid and e.fplatform_uid = d.fplatform_uid
                    join analysis.bpid_platform_game_ver_map bpm
                        on d.fbpid = bpm.fbpid
                    where d.dt="%(statdate)s"
                ) c
                    on b.fplatformfsk = c.fplatformfsk
                    and b.fgamefsk = c.fgamefsk
                    and b.fversionfsk = c.fversionfsk
                    and b.fuid = c.fuid
            ) a
            group by a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk,
                a.fuphill_pouring,fpname, a.fplayground_title
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.gameparty_settlement_fct
        partition( dt="%(statdate)s" )
        SELECT "%(statdate)s" fdate,
           fgamefsk ,
           fplatformfsk ,
           fversionfsk ,
           fterminalfsk ,
           fante ,
           max(fplayuser_num) fplayuser_num ,
           max(fbankuser_num) fbankuser_num ,
           max(fparty_num) fparty_num ,
           max(fwinplayer_cnt) fwinplayer_cnt ,
           max(floseplayer_cnt) floseplayer_cnt ,
           max(fbankparty_num) fbankparty_num ,
           max(fwingc_sum) fwingc_sum ,
           max(flosegc_sum) flosegc_sum ,
           max(fwingc_avg) fwingc_avg ,
           max(flosegc_avg) flosegc_avg ,
           max(fcharge) fcharge ,
           max(fmultiple_avg) fmultiple_avg ,
           max(f1bankrupt_num) f1bankrupt_num ,
           max(f2bankrupt_num) f2bankrupt_num ,
           max(fbankuser_cnt) fbankuser_cnt ,
           max(frb_num) frb_num ,
           max(frb_win_coins) frb_win_coins ,
           max(frb_lost_coins) frb_lost_coins ,
           max(frb_win_party) frb_win_party ,
           max(frb_party) frb_party ,
           max(fbankpayusercnt) fbankpayusercnt ,
           max(fbankusercnt) fbankusercnt ,
           max(f10bankpayusercnt) f10bankpayusercnt,
           fpname,
           fsubname
    FROM
      ( SELECT fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fante,
               sum(fplayuser_num) fplayuser_num,
               0 fbankuser_num,
                 sum(fparty_num) fparty_num,
                 sum(fwinplayer_cnt) fwinplayer_cnt,
                 sum(floseplayer_cnt) floseplayer_cnt,
                 sum(fbankparty_num) fbankparty_num,
                 sum(fwingc_sum) fwingc_sum,
                 sum(flosegc_sum) flosegc_sum,
                 sum(fwingc_avg) fwingc_avg,
                 sum(flosegc_avg) flosegc_avg,
                 sum(fcharge) fcharge,
                 sum(fmultiple_avg) fmultiple_avg,
                 sum(f1bankrupt_num) f1bankrupt_num,
                 sum(f2bankrupt_num) f2bankrupt_num,
                 0 fbankuser_cnt,
               sum(frb_num) frb_num,
               sum(frb_win_coins) frb_win_coins,
               sum(frb_lost_coins) frb_lost_coins,
               sum(frb_win_party) frb_win_party,
               sum(frb_party) frb_party,
                0 fbankpayusercnt,
                0 fbankusercnt,
                0 f10bankpayusercnt,
                coalesce(fpname, '未知') fpname,
                fsubname
       FROM stage.gameparty_ante_detail_1_%(num_begin)s
       GROUP BY fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fante,
                coalesce(fpname, '未知'),
                fsubname
       UNION ALL
       SELECT fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk,
                        fante,
                        0,
                        sum(fbankuser_num) fbankuser_num,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        sum(fbankuser_cnt) fbankuser_cnt,
                        0,
                        0,
                        0,
                        0,
                        0,
                        sum(fbankpayusercnt) fbankpayusercnt,
                        sum(fbankusercnt) fbankusercnt,
                        sum(f10bankpayusercnt) f10bankpayusercnt,
                        coalesce(fpname, '未知') fpname,
                        fsubname
       FROM stage.gameparty_ante_detail_2_%(num_begin)s
       GROUP BY fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fante,
                coalesce(fpname, '未知'),
                fsubname ) a
    GROUP BY fgamefsk,
             fplatformfsk,
             fversionfsk,
             fterminalfsk,
             fante,
             fpname,
             fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res



        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.gameparty_ante_detail_1_%(num_begin)s;

        drop table if exists stage.gameparty_ante_detail_2_%(num_begin)s;
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
a = gameparty_ante_detail(statDate, eid)
a()
