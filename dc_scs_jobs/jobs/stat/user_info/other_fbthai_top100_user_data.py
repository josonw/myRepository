#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class other_fbthai_top100_user_data(BaseStat):
    """ 用户来源，广告用户，feed，push
    """
    def create_tab(self):
        hql = """create table if not exists analysis.fbthai_e2p_top100_user_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fparty_num bigint,
                fwin_gamecoin bigint,
                flose_gamecoin bigint,
                ftwo_gameparty_num bigint,
                ftwo_win_gamecoin bigint,
                ftwo_lose_gamecoin bigint,
                fpay_num bigint,
                fpay_total bigint,
                fdip bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        hql = """
        drop table if exists stage.fbthai_e2p_top100_data;
        create table stage.fbthai_e2p_top100_data
        as
        select fbpid, fplatform_uid, fuid, dip
          from (select a.fbpid, a.fplatform_uid,
                   b.fuid,
                   round(sum(fcoins_num * frate), 2) dip
              from stage.payment_stream_stg a
              join stage.pay_user_mid b
               on a.fbpid = b.fbpid
              and a.fplatform_uid = b.fplatform_uid
               and a.fbpid = 'F101C9E6455ED16255D0F8A6F0154C4F'
               and a.fm_name in ('EIcard(SDK)', 'eicard')
             where a.dt = '%(ld_daybegin)s'
             group by a.fbpid, a.fplatform_uid, b.fuid
             order by dip desc) tmp limit 100;

        insert overwrite table analysis.fbthai_e2p_top100_user_fct
        partition (dt='%(ld_daybegin)s')
          select /*+ mapjoin(c) */
           '%(ld_daybegin)s' fdate,
           fgamefsk,
           fplatformfsk,
           fversionfsk,
           fterminalfsk,
           0 fparty_num,
           0 fwin_gamecoin,
           0 flose_gamecoin,
           count(distinct concat( b.finning_id, b.ftbl_id)) ftwo_gameparty_num,
           sum(case when b.fgamecoins >= 0 then b.fgamecoins end) ftwo_win_gamecoin,
           sum(case when b.fgamecoins < 0 then abs(b.fgamecoins) end) ftwo_lose_gamecoin,
           0 fpay_num,
           0 fpay_total,
           0 fdip
            from stage.fbthai_e2p_top100_data fb
            join stage.user_gameparty_stg b
              on b.fbpid = fb.fbpid
             and b.fuid = fb.fuid
             and b.dt = '%(ld_daybegin)s'
             and b.fpalyer_cnt = 2
            join analysis.bpid_platform_game_ver_map c
              on b.fbpid = c.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;


        insert overwrite table analysis.fbthai_e2p_top100_user_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 max(fparty_num) fparty_num,
                 max(fwin_gamecoin) fwin_gamecoin,
                 max(flose_gamecoin) flose_gamecoin,
                 max(ftwo_gameparty_num) ftwo_gameparty_num,
                 max(ftwo_win_gamecoin) ftwo_win_gamecoin,
                 max(ftwo_lose_gamecoin) ftwo_lose_gamecoin,
                 max(fpay_num) fpay_num,
                 max(fpay_total) fpay_total,
                 max(fdip) fdip
            from (select fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         fparty_num,
                         fwin_gamecoin,
                         flose_gamecoin,
                         ftwo_gameparty_num,
                         ftwo_win_gamecoin,
                         ftwo_lose_gamecoin,
                         fpay_num,
                         fpay_total,
                         fdip
                    from analysis.fbthai_e2p_top100_user_fct
                   where dt = '%(ld_daybegin)s'
                  union all
                  select fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         count(distinct concat(a.finning_id, a.ftbl_id)) fparty_num,
                         sum(case when a.fgamecoins >= 0 then a.fgamecoins end) fwin_gamecoin,
                         sum(case when a.fgamecoins < 0 then abs(a.fgamecoins) end) flose_gamecoin,
                         0 ftwo_gameparty_num,
                         0 ftwo_win_gamecoin,
                         0 ftwo_lose_gamecoin,
                         0 fpay_num,
                         0 fpay_total,
                         0 fdip
                    from stage.fbthai_e2p_top100_data b
                    join stage.user_gameparty_stg a
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and a.dt = '%(ld_daybegin)s'
                    join analysis.bpid_platform_game_ver_map c
                      on b.fbpid = c.fbpid
                   group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk) tmp
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;

       insert overwrite table analysis.fbthai_e2p_top100_user_fct
       partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 max(fparty_num) fparty_num,
                 max(fwin_gamecoin) fwin_gamecoin,
                 max(flose_gamecoin) flose_gamecoin,
                 max(ftwo_gameparty_num) ftwo_gameparty_num,
                 max(ftwo_win_gamecoin) ftwo_win_gamecoin,
                 max(ftwo_lose_gamecoin) ftwo_lose_gamecoin,
                 max(fpay_num) fpay_num,
                 max(fpay_total) fpay_total,
                 max(fdip) fdip
            from (select fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         fparty_num,
                         fwin_gamecoin,
                         flose_gamecoin,
                         ftwo_gameparty_num,
                         ftwo_win_gamecoin,
                         ftwo_lose_gamecoin,
                         fpay_num,
                         fpay_total,
                         fdip
                    from analysis.fbthai_e2p_top100_user_fct
                   where dt = '%(ld_daybegin)s'
                  union all
                  select fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         0 fparty_num,
                         0 fwin_gamecoin,
                         0 flose_gamecoin,
                         0 ftwo_gameparty_num,
                         0 ftwo_win_gamecoin,
                         0 ftwo_lose_gamecoin,
                         count(if(pay.dt = '%(ld_daybegin)s', a.fuid, null)) fpay_num,
                         round(sum(fcoins_num * frate), 2) fpay_total,
                         sum(if(pay.dt = '%(ld_daybegin)s', fcoins_num * frate, 0)) fdip
                    from stage.fbthai_e2p_top100_data a
                    join stage.payment_stream_stg pay
                      on a.fplatform_uid = pay.fplatform_uid
                     and a.fbpid = pay.fbpid
                     and pay.dt < '%(ld_dayend)s'
                    join analysis.bpid_platform_game_ver_map c
                      on a.fbpid = c.fbpid
                   group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk) tmp
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


#愉快的统计要开始啦
global statDate
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
#生成统计实例
a = other_fbthai_top100_user_data(statDate)
a()
